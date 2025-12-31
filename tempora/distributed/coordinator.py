"""
Tempora Distributed Coordinator - Integration Layer

This module integrates the coordination server with leader election,
providing a unified interface for distributed Tempora operations.

Responsibilities:
    - Start and manage coordination server
    - Start and manage leader election
    - Wire election to server message handlers
    - Persist election state to database
    - Expose status for monitoring

Design Principles:
    1. Single entry point for distributed features
    2. Clean lifecycle management
    3. Callback-based event handling
    4. Database persistence for crash recovery

Compliance:
    - TEMPORA-HA-001 §6: Coordinator Integration
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from tempora.models import DistributedLogEntry

from tempora.coordination.server import (
    CoordinationServer,
    CoordinationServerConfig,
    PeerInfo,
)
from tempora.coordination.protocol import Message, MessageType
from tempora.coordination.heartbeat import HeartbeatManager, HeartbeatConfig

from .election import (
    LeaderElector,
    ElectionConfig,
    ElectionState,
    NodeRole,
)
from .replication import (
    StateReplicator,
    ReplicationConfig,
)

logger = logging.getLogger(__name__)


@dataclass
class DistributedConfig:
    """Configuration for distributed Tempora operations."""

    # Instance identity
    instance_id: str

    # Server configuration
    bind_host: str = "0.0.0.0"
    bind_port: int = 9500
    cluster_secret: Optional[str] = None

    # Election configuration
    election_timeout_min_ms: int = 150
    election_timeout_max_ms: int = 300
    heartbeat_interval_ms: int = 50

    # Heartbeat configuration (for failure detection)
    peer_heartbeat_interval_ms: int = 100
    peer_heartbeat_timeout_ms: int = 300

    # Replication configuration (Phase 3)
    max_entries_per_batch: int = 100
    replication_timeout_ms: int = 1000

    # Persistence
    persist_election_state: bool = True


class DistributedCoordinator:
    """
    Central coordinator for distributed Tempora operations.

    This class ties together:
    - CoordinationServer: TCP server for peer communication
    - LeaderElector: Raft-based leader election
    - HeartbeatManager: Failure detection

    Example:
        coordinator = DistributedCoordinator(
            config=DistributedConfig(
                instance_id="tempora-1",
                bind_port=9500,
                cluster_secret="my-secret",
            )
        )

        # Register for leader changes
        coordinator.on_become_leader(handle_leader_promotion)
        coordinator.on_lose_leadership(handle_leadership_loss)

        # Start coordinator
        await coordinator.start()

        # Check leadership
        if coordinator.is_leader:
            # Do leader-only work
            pass

        # Stop coordinator
        await coordinator.stop()
    """

    def __init__(self, config: DistributedConfig):
        self.config = config
        self.instance_id = config.instance_id

        # Create coordination server
        self._server = CoordinationServer(
            CoordinationServerConfig(
                instance_id=config.instance_id,
                bind_host=config.bind_host,
                bind_port=config.bind_port,
                cluster_secret=config.cluster_secret,
            )
        )

        # Create leader elector
        self._elector = LeaderElector(
            instance_id=config.instance_id,
            config=ElectionConfig(
                election_timeout_min_ms=config.election_timeout_min_ms,
                election_timeout_max_ms=config.election_timeout_max_ms,
                heartbeat_interval_ms=config.heartbeat_interval_ms,
            ),
        )

        # Create heartbeat manager
        self._heartbeat = HeartbeatManager(
            config=HeartbeatConfig(
                interval_ms=config.peer_heartbeat_interval_ms,
                timeout_ms=config.peer_heartbeat_timeout_ms,
            )
        )

        # Create state replicator (Phase 3)
        self._replicator = StateReplicator(
            instance_id=config.instance_id,
            config=ReplicationConfig(
                max_entries_per_batch=config.max_entries_per_batch,
                replication_timeout_ms=config.replication_timeout_ms,
            ),
            get_current_term=lambda: self._elector.current_term,
            is_leader=lambda: self._elector.is_leader,
            send_callback=self._send_to_peer,
            get_peers_callback=self._get_peer_ids,
        )

        # Wire components together
        self._wire_election_to_server()
        self._wire_heartbeat_to_server()
        self._wire_replication_to_election()

        # Lifecycle state
        self._running = False

        # Callbacks
        self._on_become_leader: List[Callable[[], Coroutine]] = []
        self._on_lose_leadership: List[Callable[[], Coroutine]] = []

    def _wire_election_to_server(self) -> None:
        """Wire election message handlers to server."""
        # Register election message handlers
        self._server.register_handler(
            MessageType.VOTE_REQUEST,
            self._handle_vote_request,
        )
        self._server.register_handler(
            MessageType.VOTE_RESPONSE,
            self._handle_vote_response,
        )
        self._server.register_handler(
            MessageType.APPEND_ENTRIES,
            self._handle_append_entries,
        )
        self._server.register_handler(
            MessageType.APPEND_ENTRIES_RESPONSE,
            self._handle_append_entries_response,
        )

        # Set up election callbacks
        self._elector.set_send_callback(self._send_to_peer)
        self._elector.set_broadcast_callback(self._broadcast)
        self._elector.set_get_peers_callback(self._get_peer_ids)

        # Register for role changes
        self._elector.on_role_change(self._on_role_change)
        self._elector.on_leader_change(self._on_leader_change)

        # Set up persistence callback
        if self.config.persist_election_state:
            self._elector._persist_state_callback = self._persist_election_state

    def _wire_heartbeat_to_server(self) -> None:
        """Wire heartbeat manager to server."""
        # Add/remove peers from heartbeat manager
        self._server.on_peer_connected(self._on_peer_connected)
        self._server.on_peer_disconnected(self._on_peer_disconnected)

    def _wire_replication_to_election(self) -> None:
        """Wire replication to election events."""
        # Start/stop replication when leadership changes
        self._elector.on_role_change(self._handle_replication_role_change)

    async def _handle_replication_role_change(
        self,
        old_role: NodeRole,
        new_role: NodeRole,
    ) -> None:
        """Handle role changes for replication."""
        if new_role == NodeRole.LEADER and old_role != NodeRole.LEADER:
            # Became leader - start replication
            self._replicator.start_leader_replication()
        elif old_role == NodeRole.LEADER and new_role != NodeRole.LEADER:
            # Lost leadership - stop replication
            self._replicator.stop_leader_replication()

    # =========================================================================
    # ELECTION MESSAGE HANDLERS
    # =========================================================================

    async def _handle_vote_request(
        self,
        peer: PeerInfo,
        message: Message,
    ) -> Optional[Message]:
        """Handle VOTE_REQUEST from peer."""
        return await self._elector.handle_vote_request(peer.instance_id, message)

    async def _handle_vote_response(
        self,
        peer: PeerInfo,
        message: Message,
    ) -> Optional[Message]:
        """Handle VOTE_RESPONSE from peer."""
        await self._elector.handle_vote_response(peer.instance_id, message)
        return None

    async def _handle_append_entries(
        self,
        peer: PeerInfo,
        message: Message,
    ) -> Optional[Message]:
        """Handle APPEND_ENTRIES (heartbeat) from leader."""
        return await self._elector.handle_append_entries(peer.instance_id, message)

    async def _handle_append_entries_response(
        self,
        peer: PeerInfo,
        message: Message,
    ) -> Optional[Message]:
        """Handle response to our APPEND_ENTRIES."""
        await self._elector.handle_append_entries_response(peer.instance_id, message)
        return None

    # =========================================================================
    # ELECTION COMMUNICATION CALLBACKS
    # =========================================================================

    async def _send_to_peer(self, peer_id: str, message: Message) -> None:
        """Send message to specific peer."""
        await self._server.send_to_peer(peer_id, message)

    async def _broadcast(self, message: Message) -> None:
        """Broadcast message to all peers."""
        await self._server.broadcast(message)

    def _get_peer_ids(self) -> List[str]:
        """Get list of connected peer IDs."""
        return [p.instance_id for p in self._server.get_peers()]

    # =========================================================================
    # ELECTION CALLBACKS
    # =========================================================================

    async def _on_role_change(
        self,
        old_role: NodeRole,
        new_role: NodeRole,
    ) -> None:
        """Handle election role changes."""
        logger.info(
            f"Role changed",
            extra={
                "instance_id": self.instance_id,
                "old_role": old_role.value,
                "new_role": new_role.value,
            }
        )

        if new_role == NodeRole.LEADER and old_role != NodeRole.LEADER:
            # Became leader
            for callback in self._on_become_leader:
                try:
                    await callback()
                except Exception as e:
                    logger.exception(f"Error in become_leader callback: {e}")

        elif old_role == NodeRole.LEADER and new_role != NodeRole.LEADER:
            # Lost leadership
            for callback in self._on_lose_leadership:
                try:
                    await callback()
                except Exception as e:
                    logger.exception(f"Error in lose_leadership callback: {e}")

    async def _on_leader_change(self, leader_id: Optional[str]) -> None:
        """Handle leader change events."""
        logger.info(
            f"Leader changed",
            extra={
                "instance_id": self.instance_id,
                "new_leader": leader_id,
            }
        )

        # Update ClusterMember model if available
        if self.config.persist_election_state:
            await self._update_leader_in_database(leader_id)

    # =========================================================================
    # HEARTBEAT CALLBACKS
    # =========================================================================

    async def _on_peer_connected(self, peer: PeerInfo) -> None:
        """Handle peer connection."""
        self._heartbeat.add_peer(peer.instance_id)
        self._replicator.add_follower(peer.instance_id)
        logger.debug(f"Added peer to heartbeat and replicator: {peer.instance_id}")

    async def _on_peer_disconnected(self, peer: PeerInfo) -> None:
        """Handle peer disconnection."""
        self._heartbeat.remove_peer(peer.instance_id)
        self._replicator.remove_follower(peer.instance_id)
        logger.debug(f"Removed peer from heartbeat and replicator: {peer.instance_id}")

    # =========================================================================
    # PERSISTENCE
    # =========================================================================

    async def _persist_election_state(self, state: ElectionState) -> None:
        """Persist election state to database."""
        try:
            from tempora.models import ClusterMember, ClusterMemberRole

            # Map role
            role_map = {
                NodeRole.FOLLOWER: ClusterMemberRole.FOLLOWER,
                NodeRole.CANDIDATE: ClusterMemberRole.CANDIDATE,
                NodeRole.LEADER: ClusterMemberRole.LEADER,
                NodeRole.OBSERVER: ClusterMemberRole.OBSERVER,
            }

            # Update or create member
            await asyncio.get_event_loop().run_in_executor(
                None,
                self._sync_persist_election_state,
                state,
                role_map,
            )

        except ImportError:
            # Models not available - skip persistence
            pass
        except Exception as e:
            logger.error(f"Failed to persist election state: {e}")

    def _sync_persist_election_state(
        self,
        state: ElectionState,
        role_map: Dict,
    ) -> None:
        """Synchronous persistence (runs in executor)."""
        from tempora.models import ClusterMember, ClusterMemberRole

        try:
            member, _ = ClusterMember.objects.update_or_create(
                instance_id=self.instance_id,
                defaults={
                    "role": role_map.get(state.role, ClusterMemberRole.FOLLOWER),
                    "current_term": state.current_term,
                    "voted_for": state.voted_for,
                }
            )
        except Exception as e:
            logger.error(f"Database error persisting state: {e}")

    async def _update_leader_in_database(self, leader_id: Optional[str]) -> None:
        """Update leader in database."""
        try:
            await asyncio.get_event_loop().run_in_executor(
                None,
                self._sync_update_leader,
                leader_id,
            )
        except Exception as e:
            logger.error(f"Failed to update leader in database: {e}")

    def _sync_update_leader(self, leader_id: Optional[str]) -> None:
        """Synchronous leader update (runs in executor)."""
        try:
            from tempora.models import ClusterMember, ClusterMemberRole

            # Clear old leader
            ClusterMember.objects.filter(
                role=ClusterMemberRole.LEADER
            ).exclude(
                instance_id=leader_id
            ).update(
                role=ClusterMemberRole.FOLLOWER
            )

            # Set new leader
            if leader_id:
                ClusterMember.objects.filter(
                    instance_id=leader_id
                ).update(
                    role=ClusterMemberRole.LEADER
                )
        except Exception as e:
            logger.error(f"Database error updating leader: {e}")

    async def _load_election_state(self) -> Optional[ElectionState]:
        """Load election state from database on startup."""
        try:
            from tempora.models import ClusterMember

            state = await asyncio.get_event_loop().run_in_executor(
                None,
                self._sync_load_election_state,
            )
            return state

        except ImportError:
            return None
        except Exception as e:
            logger.error(f"Failed to load election state: {e}")
            return None

    def _sync_load_election_state(self) -> Optional[ElectionState]:
        """Synchronous state loading (runs in executor)."""
        try:
            from tempora.models import ClusterMember

            member = ClusterMember.objects.filter(
                instance_id=self.instance_id
            ).first()

            if member:
                return ElectionState(
                    current_term=member.current_term,
                    voted_for=member.voted_for,
                    role=NodeRole.FOLLOWER,  # Always start as follower
                )

        except Exception as e:
            logger.error(f"Database error loading state: {e}")

        return None

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    async def start(self) -> None:
        """Start the distributed coordinator."""
        if self._running:
            return

        logger.info(
            f"Starting distributed coordinator",
            extra={
                "instance_id": self.instance_id,
                "bind": f"{self.config.bind_host}:{self.config.bind_port}",
            }
        )

        # Load persisted state
        if self.config.persist_election_state:
            state = await self._load_election_state()
            if state:
                await self._elector.load_state(state)

        # Start components
        await self._server.start()
        await self._heartbeat.start()
        await self._elector.start()
        await self._replicator.start()

        self._running = True

        logger.info(f"Distributed coordinator started")

    async def stop(self) -> None:
        """Stop the distributed coordinator."""
        if not self._running:
            return

        logger.info("Stopping distributed coordinator")
        self._running = False

        # Stop components in reverse order
        await self._replicator.stop()
        await self._elector.stop()
        await self._heartbeat.stop()
        await self._server.stop()

        logger.info("Distributed coordinator stopped")

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    @property
    def is_leader(self) -> bool:
        """Check if this instance is the leader."""
        return self._elector.is_leader

    @property
    def leader_id(self) -> Optional[str]:
        """Get current leader ID."""
        return self._elector.leader_id

    @property
    def current_term(self) -> int:
        """Get current election term."""
        return self._elector.current_term

    @property
    def role(self) -> NodeRole:
        """Get current role."""
        return self._elector.state.role

    @property
    def peer_count(self) -> int:
        """Get number of connected peers."""
        return self._server.peer_count

    def on_become_leader(self, callback: Callable[[], Coroutine]) -> None:
        """Register callback for when this instance becomes leader."""
        self._on_become_leader.append(callback)

    def on_lose_leadership(self, callback: Callable[[], Coroutine]) -> None:
        """Register callback for when this instance loses leadership."""
        self._on_lose_leadership.append(callback)

    async def step_down(self, reason: str = "manual") -> None:
        """Manually step down from leadership."""
        await self._elector.step_down(reason)

    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive status for monitoring."""
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "role": self.role.value,
            "is_leader": self.is_leader,
            "leader_id": self.leader_id,
            "current_term": self.current_term,
            "peer_count": self.peer_count,
            "server": self._server.get_status(),
            "election": self._elector.get_status(),
            "heartbeat": self._heartbeat.get_status(),
            "replication": self._replicator.get_status(),
        }

    # =========================================================================
    # REPLICATION API (Phase 3)
    # =========================================================================

    async def append_command(
        self,
        tenant_id: "uuid.UUID",
        command: str,
        data: Dict[str, Any],
    ) -> "DistributedLogEntry":
        """
        Append a command to the distributed log (leader only).

        This method should be used for all state-changing operations
        in the scheduler when running in distributed mode.

        Args:
            tenant_id: Tenant isolation identifier
            command: Command type from DistributedLogCommand
            data: Command payload

        Returns:
            The created log entry

        Raises:
            NotLeaderError: If not the current leader
        """
        from tempora.distributed.replication import NotLeaderError

        if not self.is_leader:
            raise NotLeaderError(
                f"Not leader. Current leader: {self.leader_id}"
            )

        return await self._replicator.append_command(
            tenant_id=tenant_id,
            command=command,
            data=data,
        )

    async def append_and_wait(
        self,
        tenant_id: "uuid.UUID",
        command: str,
        data: Dict[str, Any],
        timeout_seconds: float = 30.0,
    ) -> bool:
        """
        Append a command and wait for it to be committed.

        This is the preferred method for operations that need
        confirmation of replication to quorum.

        Args:
            tenant_id: Tenant isolation identifier
            command: Command type from DistributedLogCommand
            data: Command payload
            timeout_seconds: Maximum time to wait for commit

        Returns:
            True if committed, False if timeout

        Raises:
            NotLeaderError: If not the current leader
        """
        entry = await self.append_command(tenant_id, command, data)
        return await self._replicator.wait_for_commit(
            entry.index,
            timeout_seconds=timeout_seconds,
        )

    @property
    def commit_index(self) -> int:
        """Get current commit index."""
        return self._replicator.commit_index

    @property
    def last_applied(self) -> int:
        """Get last applied index."""
        return self._replicator.last_applied

    def get_peers(self) -> List[Dict[str, Any]]:
        """Get list of connected peers with health info."""
        peers = []
        for peer in self._server.get_peers():
            health = self._heartbeat.get_peer_health(peer.instance_id)
            peer_info = peer.to_dict()
            if health:
                peer_info["health"] = health.to_dict()
            peers.append(peer_info)
        return peers
