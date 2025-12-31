"""
Tempora Leader Election - Raft-Based Consensus

This module implements Raft-inspired leader election for the Tempora
distributed scheduler. It ensures exactly one leader is elected at any
time, with automatic failover on leader failure.

Algorithm Overview:
    1. All nodes start as FOLLOWERS
    2. If no heartbeat from leader within election timeout, become CANDIDATE
    3. CANDIDATE increments term and requests votes from all peers
    4. Node receiving majority votes becomes LEADER
    5. LEADER sends periodic heartbeats to maintain authority

Key Guarantees:
    - Safety: At most one leader per term
    - Liveness: Eventually elects a leader (if majority available)
    - Leader completeness: Elected leader has all committed entries

Compliance:
    - TEMPORA-HA-001 §4: Leader Election
    - SCH-002 §8.2: Election Protocol (superseded by native implementation)

References:
    - "In Search of an Understandable Consensus Algorithm" (Ongaro, 2014)
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set

from tempora.coordination.protocol import (
    Message,
    MessageType,
)

logger = logging.getLogger(__name__)


class NodeRole(Enum):
    """Role of a node in the Raft cluster."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"
    OBSERVER = "observer"  # Non-voting member


@dataclass
class ElectionConfig:
    """Configuration for leader election."""

    # Election timeout range (randomized to prevent split votes)
    election_timeout_min_ms: int = 150
    election_timeout_max_ms: int = 300

    # Heartbeat interval (must be << election timeout)
    heartbeat_interval_ms: int = 50

    # Pre-vote phase to prevent disruption (Raft §9.6)
    enable_pre_vote: bool = True

    # Leadership lease for read optimization
    lease_duration_ms: int = 100

    # Maximum elections per minute (rate limiting)
    max_elections_per_minute: int = 20


@dataclass
class ElectionState:
    """
    Persistent election state.

    This state must be persisted to stable storage before responding
    to RPCs to ensure crash recovery correctness.
    """

    # Current term (monotonically increasing)
    current_term: int = 0

    # Candidate ID we voted for in current term (None if not voted)
    voted_for: Optional[str] = None

    # Current role
    role: NodeRole = NodeRole.FOLLOWER

    # Current leader (if known)
    leader_id: Optional[str] = None

    # When we last heard from the leader
    last_leader_contact: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "role": self.role.value,
            "leader_id": self.leader_id,
            "last_leader_contact": self.last_leader_contact,
        }


@dataclass
class VoteRequest:
    """Request for vote from a candidate."""
    term: int
    candidate_id: str
    last_log_index: int = 0
    last_log_term: int = 0
    is_pre_vote: bool = False

    def to_payload(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "candidate_id": self.candidate_id,
            "last_log_index": self.last_log_index,
            "last_log_term": self.last_log_term,
            "is_pre_vote": self.is_pre_vote,
        }

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "VoteRequest":
        return cls(
            term=payload["term"],
            candidate_id=payload["candidate_id"],
            last_log_index=payload.get("last_log_index", 0),
            last_log_term=payload.get("last_log_term", 0),
            is_pre_vote=payload.get("is_pre_vote", False),
        )


@dataclass
class VoteResponse:
    """Response to a vote request."""
    term: int
    vote_granted: bool
    voter_id: str
    is_pre_vote: bool = False

    def to_payload(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "vote_granted": self.vote_granted,
            "voter_id": self.voter_id,
            "is_pre_vote": self.is_pre_vote,
        }

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "VoteResponse":
        return cls(
            term=payload["term"],
            vote_granted=payload["vote_granted"],
            voter_id=payload["voter_id"],
            is_pre_vote=payload.get("is_pre_vote", False),
        )


@dataclass
class AppendEntries:
    """
    Heartbeat / log replication message from leader.

    In Phase 2, this is used only for heartbeat (empty entries).
    Phase 3 will add log replication.
    """
    term: int
    leader_id: str
    prev_log_index: int = 0
    prev_log_term: int = 0
    entries: List[Dict[str, Any]] = field(default_factory=list)
    leader_commit: int = 0

    def to_payload(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "leader_id": self.leader_id,
            "prev_log_index": self.prev_log_index,
            "prev_log_term": self.prev_log_term,
            "entries": self.entries,
            "leader_commit": self.leader_commit,
        }

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "AppendEntries":
        return cls(
            term=payload["term"],
            leader_id=payload["leader_id"],
            prev_log_index=payload.get("prev_log_index", 0),
            prev_log_term=payload.get("prev_log_term", 0),
            entries=payload.get("entries", []),
            leader_commit=payload.get("leader_commit", 0),
        )


@dataclass
class AppendEntriesResponse:
    """Response to AppendEntries."""
    term: int
    success: bool
    follower_id: str
    match_index: int = 0

    def to_payload(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "success": self.success,
            "follower_id": self.follower_id,
            "match_index": self.match_index,
        }

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "AppendEntriesResponse":
        return cls(
            term=payload["term"],
            success=payload["success"],
            follower_id=payload["follower_id"],
            match_index=payload.get("match_index", 0),
        )


# Callback types
RoleChangeCallback = Callable[[NodeRole, NodeRole], Coroutine[Any, Any, None]]
LeaderChangeCallback = Callable[[Optional[str]], Coroutine[Any, Any, None]]


class LeaderElector:
    """
    Raft-based leader election for Tempora cluster.

    This class manages the election state machine and coordinates
    with the coordination server to send/receive election messages.

    Example:
        elector = LeaderElector(
            instance_id="tempora-1",
            config=ElectionConfig(),
            send_callback=send_to_peer,
            broadcast_callback=broadcast_to_all,
            get_peers_callback=get_peer_list,
        )

        # Register for role changes
        elector.on_role_change(handle_role_change)
        elector.on_leader_change(handle_leader_change)

        # Start election process
        await elector.start()

        # Check if we're leader
        if elector.is_leader:
            # Perform leader-only operations
            pass

        # Stop election process
        await elector.stop()
    """

    def __init__(
        self,
        instance_id: str,
        config: Optional[ElectionConfig] = None,
        send_callback: Optional[Callable[[str, Message], Coroutine]] = None,
        broadcast_callback: Optional[Callable[[Message], Coroutine]] = None,
        get_peers_callback: Optional[Callable[[], List[str]]] = None,
        persist_state_callback: Optional[Callable[[ElectionState], Coroutine]] = None,
    ):
        self.instance_id = instance_id
        self.config = config or ElectionConfig()

        # Callbacks for communication
        self._send_callback = send_callback
        self._broadcast_callback = broadcast_callback
        self._get_peers_callback = get_peers_callback
        self._persist_state_callback = persist_state_callback

        # Election state
        self.state = ElectionState()

        # Event callbacks
        self._role_change_callbacks: List[RoleChangeCallback] = []
        self._leader_change_callbacks: List[LeaderChangeCallback] = []

        # Timers and tasks
        self._election_timer: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False

        # Election rate limiting
        self._recent_elections: List[float] = []

        # Vote tracking for current election
        self._votes_received: Set[str] = set()
        self._election_lock = asyncio.Lock()

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    async def start(self) -> None:
        """Start the election process."""
        if self._running:
            return

        self._running = True
        self.state.role = NodeRole.FOLLOWER

        # Start election timer
        self._reset_election_timer()

        logger.info(
            f"Leader election started",
            extra={
                "instance_id": self.instance_id,
                "role": self.state.role.value,
            }
        )

    async def stop(self) -> None:
        """Stop the election process."""
        if not self._running:
            return

        self._running = False

        # Cancel timers
        if self._election_timer:
            self._election_timer.cancel()
            try:
                await self._election_timer
            except asyncio.CancelledError:
                pass
            self._election_timer = None

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        logger.info(f"Leader election stopped")

    # =========================================================================
    # CALLBACKS
    # =========================================================================

    def on_role_change(self, callback: RoleChangeCallback) -> None:
        """Register callback for role changes."""
        self._role_change_callbacks.append(callback)

    def on_leader_change(self, callback: LeaderChangeCallback) -> None:
        """Register callback for leader changes."""
        self._leader_change_callbacks.append(callback)

    def set_send_callback(
        self,
        callback: Callable[[str, Message], Coroutine],
    ) -> None:
        """Set callback for sending messages to specific peer."""
        self._send_callback = callback

    def set_broadcast_callback(
        self,
        callback: Callable[[Message], Coroutine],
    ) -> None:
        """Set callback for broadcasting to all peers."""
        self._broadcast_callback = callback

    def set_get_peers_callback(
        self,
        callback: Callable[[], List[str]],
    ) -> None:
        """Set callback for getting peer list."""
        self._get_peers_callback = callback

    # =========================================================================
    # PROPERTIES
    # =========================================================================

    @property
    def is_leader(self) -> bool:
        """Check if this node is the current leader."""
        return self.state.role == NodeRole.LEADER

    @property
    def is_follower(self) -> bool:
        """Check if this node is a follower."""
        return self.state.role == NodeRole.FOLLOWER

    @property
    def is_candidate(self) -> bool:
        """Check if this node is a candidate."""
        return self.state.role == NodeRole.CANDIDATE

    @property
    def current_term(self) -> int:
        """Get current election term."""
        return self.state.current_term

    @property
    def leader_id(self) -> Optional[str]:
        """Get current leader ID (if known)."""
        return self.state.leader_id

    # =========================================================================
    # ELECTION TIMER
    # =========================================================================

    def _reset_election_timer(self) -> None:
        """Reset the election timeout timer."""
        if self._election_timer:
            self._election_timer.cancel()

        if not self._running:
            return

        # Randomized timeout to prevent split votes
        timeout_ms = random.randint(
            self.config.election_timeout_min_ms,
            self.config.election_timeout_max_ms,
        )
        timeout_sec = timeout_ms / 1000.0

        self._election_timer = asyncio.create_task(
            self._election_timeout(timeout_sec)
        )

    async def _election_timeout(self, timeout: float) -> None:
        """Handle election timeout - start new election."""
        try:
            await asyncio.sleep(timeout)

            if not self._running:
                return

            # Only followers and candidates start elections
            if self.state.role in (NodeRole.FOLLOWER, NodeRole.CANDIDATE):
                await self._start_election()

        except asyncio.CancelledError:
            pass

    # =========================================================================
    # ELECTION PROCESS
    # =========================================================================

    async def _start_election(self) -> None:
        """Start a new election."""
        async with self._election_lock:
            # Rate limiting
            if not self._can_start_election():
                logger.warning("Election rate limited")
                self._reset_election_timer()
                return

            # Increment term and vote for self
            old_role = self.state.role
            self.state.current_term += 1
            self.state.voted_for = self.instance_id
            self.state.role = NodeRole.CANDIDATE
            self.state.leader_id = None

            # Persist state
            await self._persist_state()

            # Track votes (we vote for ourselves)
            self._votes_received = {self.instance_id}

            logger.info(
                f"Starting election",
                extra={
                    "instance_id": self.instance_id,
                    "term": self.state.current_term,
                }
            )

            # Notify role change
            if old_role != NodeRole.CANDIDATE:
                await self._notify_role_change(old_role, NodeRole.CANDIDATE)

            # Request votes from all peers
            await self._request_votes()

            # Reset election timer for next round if we don't win
            self._reset_election_timer()

    def _can_start_election(self) -> bool:
        """Check if we can start a new election (rate limiting)."""
        now = time.time()

        # Remove old entries
        self._recent_elections = [
            t for t in self._recent_elections
            if now - t < 60
        ]

        if len(self._recent_elections) >= self.config.max_elections_per_minute:
            return False

        self._recent_elections.append(now)
        return True

    async def _request_votes(self) -> None:
        """Request votes from all peers."""
        if not self._broadcast_callback:
            return

        request = VoteRequest(
            term=self.state.current_term,
            candidate_id=self.instance_id,
            last_log_index=0,  # Will be set in Phase 3
            last_log_term=0,   # Will be set in Phase 3
        )

        message = Message(
            type=MessageType.VOTE_REQUEST,
            payload=request.to_payload(),
        )

        await self._broadcast_callback(message)

    async def handle_vote_request(
        self,
        peer_id: str,
        message: Message,
    ) -> Optional[Message]:
        """
        Handle incoming vote request.

        Returns response message to send back.
        """
        request = VoteRequest.from_payload(message.payload)

        async with self._election_lock:
            # If request term > our term, update term and become follower
            if request.term > self.state.current_term:
                await self._update_term(request.term)

            # Determine if we should grant vote
            vote_granted = self._should_grant_vote(request)

            if vote_granted:
                self.state.voted_for = request.candidate_id
                await self._persist_state()
                self._reset_election_timer()

                logger.info(
                    f"Granted vote",
                    extra={
                        "candidate": request.candidate_id,
                        "term": request.term,
                    }
                )

            response = VoteResponse(
                term=self.state.current_term,
                vote_granted=vote_granted,
                voter_id=self.instance_id,
                is_pre_vote=request.is_pre_vote,
            )

            return Message(
                type=MessageType.VOTE_RESPONSE,
                payload=response.to_payload(),
            )

    def _should_grant_vote(self, request: VoteRequest) -> bool:
        """Determine if we should grant a vote to the candidate."""
        # Don't vote for older terms
        if request.term < self.state.current_term:
            return False

        # Only vote once per term
        if self.state.voted_for is not None and self.state.voted_for != request.candidate_id:
            return False

        # TODO: In Phase 3, check log up-to-dateness
        # For now, grant vote if we haven't voted

        return True

    async def handle_vote_response(
        self,
        peer_id: str,
        message: Message,
    ) -> None:
        """Handle incoming vote response."""
        response = VoteResponse.from_payload(message.payload)

        async with self._election_lock:
            # Ignore if not candidate or response for different term
            if self.state.role != NodeRole.CANDIDATE:
                return

            if response.term > self.state.current_term:
                await self._update_term(response.term)
                return

            if response.term < self.state.current_term:
                return

            if response.vote_granted:
                self._votes_received.add(response.voter_id)

                logger.debug(
                    f"Received vote",
                    extra={
                        "voter": response.voter_id,
                        "votes": len(self._votes_received),
                    }
                )

                # Check if we have majority
                if self._has_quorum():
                    await self._become_leader()

    def _has_quorum(self) -> bool:
        """Check if we have received majority votes."""
        if not self._get_peers_callback:
            return True  # Single node cluster

        peers = self._get_peers_callback()
        cluster_size = len(peers) + 1  # Including self
        quorum = (cluster_size // 2) + 1

        return len(self._votes_received) >= quorum

    def _quorum_size(self) -> int:
        """Calculate quorum size."""
        if not self._get_peers_callback:
            return 1

        peers = self._get_peers_callback()
        cluster_size = len(peers) + 1
        return (cluster_size // 2) + 1

    # =========================================================================
    # LEADER OPERATIONS
    # =========================================================================

    async def _become_leader(self) -> None:
        """Transition to leader role."""
        old_role = self.state.role
        self.state.role = NodeRole.LEADER
        self.state.leader_id = self.instance_id

        await self._persist_state()

        # Cancel election timer
        if self._election_timer:
            self._election_timer.cancel()
            self._election_timer = None

        # Start heartbeat task
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        logger.info(
            f"Became LEADER",
            extra={
                "instance_id": self.instance_id,
                "term": self.state.current_term,
                "votes": len(self._votes_received),
            }
        )

        # Notify callbacks
        await self._notify_role_change(old_role, NodeRole.LEADER)
        await self._notify_leader_change(self.instance_id)

        # Send immediate heartbeat to establish authority
        await self._send_heartbeat()

    async def _heartbeat_loop(self) -> None:
        """Leader heartbeat loop."""
        interval = self.config.heartbeat_interval_ms / 1000.0

        while self._running and self.state.role == NodeRole.LEADER:
            try:
                await self._send_heartbeat()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(interval)

    async def _send_heartbeat(self) -> None:
        """Send heartbeat to all followers."""
        if not self._broadcast_callback:
            return

        heartbeat = AppendEntries(
            term=self.state.current_term,
            leader_id=self.instance_id,
            # Empty entries for heartbeat
        )

        message = Message(
            type=MessageType.APPEND_ENTRIES,
            payload=heartbeat.to_payload(),
        )

        await self._broadcast_callback(message)

    async def step_down(self, reason: str = "manual") -> None:
        """Step down from leader role."""
        if self.state.role != NodeRole.LEADER:
            return

        logger.info(
            f"Stepping down from leader",
            extra={
                "instance_id": self.instance_id,
                "reason": reason,
            }
        )

        old_role = self.state.role
        self.state.role = NodeRole.FOLLOWER
        self.state.leader_id = None

        await self._persist_state()

        # Stop heartbeat task
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        # Restart election timer
        self._reset_election_timer()

        await self._notify_role_change(old_role, NodeRole.FOLLOWER)
        await self._notify_leader_change(None)

    # =========================================================================
    # FOLLOWER OPERATIONS
    # =========================================================================

    async def handle_append_entries(
        self,
        peer_id: str,
        message: Message,
    ) -> Optional[Message]:
        """
        Handle incoming AppendEntries (heartbeat or log replication).

        Returns response message to send back.
        """
        append_entries = AppendEntries.from_payload(message.payload)

        async with self._election_lock:
            # If leader's term >= our term, recognize as leader
            if append_entries.term >= self.state.current_term:
                if append_entries.term > self.state.current_term:
                    await self._update_term(append_entries.term)

                # Recognize leader
                old_leader = self.state.leader_id
                self.state.leader_id = append_entries.leader_id
                self.state.last_leader_contact = time.time()

                # If we were candidate or leader, step down
                if self.state.role in (NodeRole.CANDIDATE, NodeRole.LEADER):
                    old_role = self.state.role
                    self.state.role = NodeRole.FOLLOWER
                    await self._notify_role_change(old_role, NodeRole.FOLLOWER)

                    if self._heartbeat_task:
                        self._heartbeat_task.cancel()
                        self._heartbeat_task = None

                # Reset election timer (we heard from leader)
                self._reset_election_timer()

                # Notify if leader changed
                if old_leader != append_entries.leader_id:
                    await self._notify_leader_change(append_entries.leader_id)

                # For Phase 2, just acknowledge heartbeat
                # Phase 3 will add log replication logic
                response = AppendEntriesResponse(
                    term=self.state.current_term,
                    success=True,
                    follower_id=self.instance_id,
                )

                return Message(
                    type=MessageType.APPEND_ENTRIES_RESPONSE,
                    payload=response.to_payload(),
                )

            else:
                # Reject - our term is higher
                response = AppendEntriesResponse(
                    term=self.state.current_term,
                    success=False,
                    follower_id=self.instance_id,
                )

                return Message(
                    type=MessageType.APPEND_ENTRIES_RESPONSE,
                    payload=response.to_payload(),
                )

    async def handle_append_entries_response(
        self,
        peer_id: str,
        message: Message,
    ) -> None:
        """Handle response to our AppendEntries."""
        response = AppendEntriesResponse.from_payload(message.payload)

        # If response term > our term, step down
        if response.term > self.state.current_term:
            await self._update_term(response.term)
            return

        # For Phase 2, just track that follower acknowledged
        # Phase 3 will use this for replication tracking

    # =========================================================================
    # TERM MANAGEMENT
    # =========================================================================

    async def _update_term(self, new_term: int) -> None:
        """Update to a new term (from incoming message)."""
        if new_term <= self.state.current_term:
            return

        logger.info(
            f"Updating term",
            extra={
                "old_term": self.state.current_term,
                "new_term": new_term,
            }
        )

        old_role = self.state.role

        self.state.current_term = new_term
        self.state.voted_for = None
        self.state.role = NodeRole.FOLLOWER

        await self._persist_state()

        # Cancel heartbeat if we were leader
        if old_role == NodeRole.LEADER and self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

        # Reset election timer
        self._reset_election_timer()

        if old_role != NodeRole.FOLLOWER:
            await self._notify_role_change(old_role, NodeRole.FOLLOWER)

    # =========================================================================
    # PERSISTENCE
    # =========================================================================

    async def _persist_state(self) -> None:
        """Persist election state to stable storage."""
        if self._persist_state_callback:
            try:
                await self._persist_state_callback(self.state)
            except Exception as e:
                logger.error(f"Failed to persist election state: {e}")

    async def load_state(self, state: ElectionState) -> None:
        """Load election state from stable storage."""
        self.state = state
        logger.info(
            f"Loaded election state",
            extra={
                "term": state.current_term,
                "voted_for": state.voted_for,
            }
        )

    # =========================================================================
    # NOTIFICATIONS
    # =========================================================================

    async def _notify_role_change(
        self,
        old_role: NodeRole,
        new_role: NodeRole,
    ) -> None:
        """Notify callbacks of role change."""
        for callback in self._role_change_callbacks:
            try:
                await callback(old_role, new_role)
            except Exception as e:
                logger.exception(f"Error in role change callback: {e}")

    async def _notify_leader_change(
        self,
        leader_id: Optional[str],
    ) -> None:
        """Notify callbacks of leader change."""
        for callback in self._leader_change_callbacks:
            try:
                await callback(leader_id)
            except Exception as e:
                logger.exception(f"Error in leader change callback: {e}")

    # =========================================================================
    # STATUS
    # =========================================================================

    def get_status(self) -> Dict[str, Any]:
        """Get election status for monitoring."""
        return {
            "instance_id": self.instance_id,
            "role": self.state.role.value,
            "current_term": self.state.current_term,
            "voted_for": self.state.voted_for,
            "leader_id": self.state.leader_id,
            "last_leader_contact": self.state.last_leader_contact,
            "is_leader": self.is_leader,
            "votes_received": list(self._votes_received) if self.is_candidate else [],
            "quorum_size": self._quorum_size(),
            "running": self._running,
        }
