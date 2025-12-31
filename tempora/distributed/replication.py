"""
Tempora State Replication - Raft Log Replication

This module implements distributed log replication for the Tempora
scheduler, ensuring consistent state across all cluster members.

Algorithm Overview:
    1. Leader receives commands and appends to local log
    2. Leader replicates entries to followers via AppendEntries
    3. Followers acknowledge receipt and apply to their logs
    4. Leader commits entry when replicated to quorum
    5. Leader sends commit index to followers
    6. All nodes apply committed entries to state machine

Key Guarantees:
    - Log Matching: If two logs contain an entry with same index/term,
      the logs are identical in all previous entries
    - Leader Append-Only: Leader never overwrites or deletes entries
    - State Machine Safety: If a server applies entry at index N,
      no other server applies a different entry at that index

Compliance:
    - TEMPORA-HA-001 §5: State Replication
    - Raft Consensus Algorithm

References:
    - "In Search of an Understandable Consensus Algorithm" (Ongaro, 2014)
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set

from tempora.coordination.protocol import Message, MessageType

logger = logging.getLogger(__name__)


@dataclass
class ReplicationConfig:
    """Configuration for state replication."""

    # Batch size for replication
    max_entries_per_batch: int = 100

    # Timeout for replication RPC
    replication_timeout_ms: int = 1000

    # How often to check for uncommitted entries
    commit_check_interval_ms: int = 50

    # Snapshot threshold (entries before snapshot)
    snapshot_threshold: int = 10000


@dataclass
class FollowerProgress:
    """
    Tracks replication progress for a follower.

    The leader maintains this state for each follower.
    """

    follower_id: str

    # Next log index to send to this follower
    next_index: int = 1

    # Highest log index known to be replicated
    match_index: int = 0

    # Whether we're actively probing this follower
    probing: bool = True

    # Last successful replication time
    last_replicated: Optional[float] = None


@dataclass
class LogEntryData:
    """Data structure for log entries in protocol messages."""
    index: int
    term: int
    command: str
    data: Dict[str, Any]
    tenant_id: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "index": self.index,
            "term": self.term,
            "command": self.command,
            "data": self.data,
            "tenant_id": self.tenant_id,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "LogEntryData":
        return cls(
            index=d["index"],
            term=d["term"],
            command=d["command"],
            data=d["data"],
            tenant_id=d["tenant_id"],
        )


class StateReplicator:
    """
    Manages log replication between leader and followers.

    This class is used by the leader to replicate log entries
    to followers, and by followers to receive and acknowledge
    entries from the leader.

    Example (Leader):
        replicator = StateReplicator(
            instance_id="tempora-1",
            config=ReplicationConfig(),
            get_current_term=lambda: elector.current_term,
            send_callback=send_to_peer,
            get_peers_callback=get_peer_list,
        )

        # When new command received
        entry = await replicator.append_command(
            tenant_id=tenant_id,
            command="TASK_CREATED",
            data={"task_id": str(task.id)},
        )

        # Wait for commit
        success = await replicator.wait_for_commit(entry.index)

    Example (Follower):
        # Handle incoming AppendEntries
        response = await replicator.handle_append_entries(message)
    """

    def __init__(
        self,
        instance_id: str,
        config: Optional[ReplicationConfig] = None,
        get_current_term: Optional[Callable[[], int]] = None,
        is_leader: Optional[Callable[[], bool]] = None,
        send_callback: Optional[Callable[[str, Message], Coroutine]] = None,
        get_peers_callback: Optional[Callable[[], List[str]]] = None,
    ):
        self.instance_id = instance_id
        self.config = config or ReplicationConfig()

        # Callbacks
        self._get_current_term = get_current_term or (lambda: 0)
        self._is_leader = is_leader or (lambda: False)
        self._send_callback = send_callback
        self._get_peers_callback = get_peers_callback

        # Leader state: progress for each follower
        self._follower_progress: Dict[str, FollowerProgress] = {}

        # Commit index (highest index known to be committed)
        self._commit_index: int = 0

        # Last applied index (highest index applied to state machine)
        self._last_applied: int = 0

        # Pending commit waiters: index -> Event
        self._commit_waiters: Dict[int, asyncio.Event] = {}

        # Lock for log operations
        self._log_lock = asyncio.Lock()

        # Replication task
        self._replication_task: Optional[asyncio.Task] = None
        self._running = False

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    async def start(self) -> None:
        """Start the replicator."""
        if self._running:
            return

        self._running = True

        logger.info(
            f"State replicator started",
            extra={"instance_id": self.instance_id},
        )

    async def stop(self) -> None:
        """Stop the replicator."""
        if not self._running:
            return

        self._running = False

        if self._replication_task:
            self._replication_task.cancel()
            try:
                await self._replication_task
            except asyncio.CancelledError:
                pass
            self._replication_task = None

        logger.info("State replicator stopped")

    # =========================================================================
    # LEADER OPERATIONS
    # =========================================================================

    def start_leader_replication(self) -> None:
        """Called when this node becomes leader."""
        # Initialize follower progress
        peers = self._get_peers_callback() if self._get_peers_callback else []

        # Get last log index from database
        last_index = self._get_last_log_index()

        for peer_id in peers:
            self._follower_progress[peer_id] = FollowerProgress(
                follower_id=peer_id,
                next_index=last_index + 1,
                match_index=0,
                probing=True,
            )

        logger.info(
            f"Started leader replication",
            extra={
                "followers": list(self._follower_progress.keys()),
                "last_index": last_index,
            },
        )

    def stop_leader_replication(self) -> None:
        """Called when this node loses leadership."""
        self._follower_progress.clear()
        logger.info("Stopped leader replication")

    def add_follower(self, follower_id: str) -> None:
        """Add a new follower to track."""
        if self._is_leader():
            last_index = self._get_last_log_index()
            self._follower_progress[follower_id] = FollowerProgress(
                follower_id=follower_id,
                next_index=last_index + 1,
                match_index=0,
                probing=True,
            )
            logger.debug(f"Added follower {follower_id}")

    def remove_follower(self, follower_id: str) -> None:
        """Remove a follower from tracking."""
        if follower_id in self._follower_progress:
            del self._follower_progress[follower_id]
            logger.debug(f"Removed follower {follower_id}")

    async def append_command(
        self,
        tenant_id: uuid.UUID,
        command: str,
        data: Dict[str, Any],
    ) -> "DistributedLogEntry":
        """
        Append a new command to the log (leader only).

        This creates a new log entry and initiates replication.
        The entry is not yet committed - use wait_for_commit().

        Args:
            tenant_id: Tenant ID for isolation
            command: Command type (from DistributedLogCommand)
            data: Command payload

        Returns:
            The created log entry

        Raises:
            NotLeaderError: If not the leader
        """
        if not self._is_leader():
            raise NotLeaderError("Only the leader can append commands")

        from tempora.models import DistributedLogEntry

        async with self._log_lock:
            term = self._get_current_term()

            entry = DistributedLogEntry.append(
                tenant_id=tenant_id,
                term=term,
                command=command,
                data=data,
                created_by=self.instance_id,
            )

            logger.debug(
                f"Appended log entry",
                extra={
                    "index": entry.index,
                    "term": entry.term,
                    "command": command,
                },
            )

            # Initiate replication
            await self._replicate_to_all()

            return entry

    async def wait_for_commit(
        self,
        index: int,
        timeout_seconds: float = 30.0,
    ) -> bool:
        """
        Wait for a log entry to be committed.

        Args:
            index: Log index to wait for
            timeout_seconds: Maximum time to wait

        Returns:
            True if committed, False if timeout
        """
        # Check if already committed
        if index <= self._commit_index:
            return True

        # Create waiter
        if index not in self._commit_waiters:
            self._commit_waiters[index] = asyncio.Event()

        try:
            await asyncio.wait_for(
                self._commit_waiters[index].wait(),
                timeout=timeout_seconds,
            )
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            # Cleanup waiter
            self._commit_waiters.pop(index, None)

    async def _replicate_to_all(self) -> None:
        """Replicate entries to all followers."""
        if not self._is_leader():
            return

        tasks = []
        for follower_id, progress in self._follower_progress.items():
            task = asyncio.create_task(
                self._replicate_to_follower(follower_id, progress)
            )
            tasks.append(task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _replicate_to_follower(
        self,
        follower_id: str,
        progress: FollowerProgress,
    ) -> None:
        """Replicate entries to a specific follower."""
        if not self._send_callback:
            return

        from tempora.models import DistributedLogEntry

        # Get entries to send
        # For now, we get entries from the first tenant (simplified)
        # In production, this would need to handle multi-tenant properly
        entries = list(DistributedLogEntry.objects.filter(
            index__gte=progress.next_index,
        ).order_by("index")[:self.config.max_entries_per_batch])

        # Get previous log entry for consistency check
        prev_index = progress.next_index - 1
        prev_term = 0
        if prev_index > 0:
            prev_entry = DistributedLogEntry.objects.filter(
                index=prev_index,
            ).first()
            if prev_entry:
                prev_term = prev_entry.term

        # Build entry list for message
        entry_dicts = []
        for entry in entries:
            entry_dicts.append({
                "index": entry.index,
                "term": entry.term,
                "command": entry.command,
                "data": entry.data,
                "tenant_id": str(entry.tenant_id),
            })

        message = Message(
            type=MessageType.APPEND_ENTRIES,
            payload={
                "term": self._get_current_term(),
                "leader_id": self.instance_id,
                "prev_log_index": prev_index,
                "prev_log_term": prev_term,
                "entries": entry_dicts,
                "leader_commit": self._commit_index,
            },
        )

        try:
            await self._send_callback(follower_id, message)
        except Exception as e:
            logger.warning(
                f"Failed to replicate to {follower_id}: {e}",
                extra={"follower_id": follower_id},
            )

    def handle_append_entries_response(
        self,
        follower_id: str,
        success: bool,
        match_index: int,
        term: int,
    ) -> None:
        """
        Handle AppendEntries response from a follower.

        Called by the coordinator when a response is received.
        """
        if not self._is_leader():
            return

        if follower_id not in self._follower_progress:
            return

        progress = self._follower_progress[follower_id]

        if success:
            # Update progress
            progress.match_index = max(progress.match_index, match_index)
            progress.next_index = match_index + 1
            progress.probing = False

            logger.debug(
                f"Follower {follower_id} matched up to index {match_index}",
                extra={
                    "follower_id": follower_id,
                    "match_index": match_index,
                },
            )

            # Check if we can advance commit index
            self._maybe_advance_commit()

        else:
            # Decrement next_index and retry (log inconsistency)
            if progress.next_index > 1:
                progress.next_index -= 1
                progress.probing = True

            logger.debug(
                f"Follower {follower_id} rejected, backing up to {progress.next_index}",
                extra={"follower_id": follower_id},
            )

    def _maybe_advance_commit(self) -> None:
        """
        Check if we can advance the commit index.

        An entry is committed when replicated to a majority.
        """
        if not self._is_leader():
            return

        # Get all match indices
        match_indices = [
            p.match_index for p in self._follower_progress.values()
        ]
        # Include self
        match_indices.append(self._get_last_log_index())

        # Sort descending
        match_indices.sort(reverse=True)

        # Quorum is majority
        quorum = (len(match_indices) // 2) + 1

        if len(match_indices) >= quorum:
            # The quorum-th highest match_index is the new commit
            new_commit = match_indices[quorum - 1]

            if new_commit > self._commit_index:
                self._advance_commit_to(new_commit)

    def _advance_commit_to(self, new_commit: int) -> None:
        """Advance commit index and notify waiters."""
        old_commit = self._commit_index
        self._commit_index = new_commit

        logger.info(
            f"Advanced commit index",
            extra={
                "old_commit": old_commit,
                "new_commit": new_commit,
            },
        )

        # Commit entries in database
        from tempora.models import DistributedLogEntry
        committed = DistributedLogEntry.commit_up_to(
            tenant_id=uuid.UUID(int=0),  # Placeholder - need proper tenant
            commit_index=new_commit,
        )

        # Apply to state machine
        self._apply_committed_entries(new_commit)

        # Notify waiters
        for index in list(self._commit_waiters.keys()):
            if index <= new_commit:
                event = self._commit_waiters.get(index)
                if event:
                    event.set()

    def _apply_committed_entries(self, up_to_index: int) -> None:
        """Apply committed entries to the state machine."""
        from tempora.models import DistributedLogEntry

        entries = DistributedLogEntry.objects.filter(
            index__gt=self._last_applied,
            index__lte=up_to_index,
            committed=True,
        ).order_by("index")

        for entry in entries:
            try:
                entry.apply_to_state_machine()
                self._last_applied = entry.index
            except Exception as e:
                logger.error(
                    f"Failed to apply log entry {entry.index}: {e}",
                    extra={"index": entry.index, "command": entry.command},
                )
                break

    # =========================================================================
    # FOLLOWER OPERATIONS
    # =========================================================================

    async def handle_append_entries(
        self,
        leader_id: str,
        prev_log_index: int,
        prev_log_term: int,
        entries: List[Dict[str, Any]],
        leader_commit: int,
        term: int,
    ) -> tuple[bool, int]:
        """
        Handle incoming AppendEntries from leader (follower).

        Args:
            leader_id: ID of the leader
            prev_log_index: Index of log entry before new ones
            prev_log_term: Term of log entry before new ones
            entries: New entries to append
            leader_commit: Leader's commit index
            term: Leader's term

        Returns:
            (success, match_index) tuple
        """
        from tempora.models import DistributedLogEntry

        async with self._log_lock:
            # Check previous log consistency
            if prev_log_index > 0:
                prev_entry = DistributedLogEntry.objects.filter(
                    index=prev_log_index,
                ).first()

                if not prev_entry:
                    # Missing entry
                    logger.debug(
                        f"Missing log entry at index {prev_log_index}",
                    )
                    return False, 0

                if prev_entry.term != prev_log_term:
                    # Term mismatch - delete this and all following
                    DistributedLogEntry.objects.filter(
                        index__gte=prev_log_index,
                    ).delete()
                    logger.debug(
                        f"Deleted conflicting entries from {prev_log_index}",
                    )
                    return False, prev_log_index - 1

            # Append new entries
            match_index = prev_log_index
            for entry_data in entries:
                index = entry_data["index"]
                entry_term = entry_data["term"]

                # Check for existing entry
                existing = DistributedLogEntry.objects.filter(
                    index=index,
                ).first()

                if existing:
                    if existing.term != entry_term:
                        # Conflict - delete from here
                        DistributedLogEntry.objects.filter(
                            index__gte=index,
                        ).delete()
                        existing = None

                if not existing:
                    # Create new entry
                    DistributedLogEntry.objects.create(
                        tenant_id=uuid.UUID(entry_data["tenant_id"]),
                        index=index,
                        term=entry_term,
                        command=entry_data["command"],
                        data=entry_data["data"],
                        created_by=leader_id,
                    )

                match_index = index

            # Update commit index
            if leader_commit > self._commit_index:
                last_new_index = match_index
                self._commit_index = min(leader_commit, last_new_index)

                # Apply committed entries
                self._apply_committed_entries(self._commit_index)

            logger.debug(
                f"Accepted entries from leader",
                extra={
                    "leader_id": leader_id,
                    "entries_count": len(entries),
                    "match_index": match_index,
                    "commit_index": self._commit_index,
                },
            )

            return True, match_index

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_last_log_index(self) -> int:
        """Get the index of the last log entry."""
        from tempora.models import DistributedLogEntry

        last_entry = DistributedLogEntry.objects.order_by("-index").first()
        return last_entry.index if last_entry else 0

    def _get_last_log_term(self) -> int:
        """Get the term of the last log entry."""
        from tempora.models import DistributedLogEntry

        last_entry = DistributedLogEntry.objects.order_by("-index").first()
        return last_entry.term if last_entry else 0

    # =========================================================================
    # STATUS
    # =========================================================================

    def get_status(self) -> Dict[str, Any]:
        """Get replication status for monitoring."""
        return {
            "instance_id": self.instance_id,
            "commit_index": self._commit_index,
            "last_applied": self._last_applied,
            "is_leader": self._is_leader(),
            "follower_progress": {
                fid: {
                    "next_index": p.next_index,
                    "match_index": p.match_index,
                    "probing": p.probing,
                }
                for fid, p in self._follower_progress.items()
            },
            "running": self._running,
        }

    @property
    def commit_index(self) -> int:
        """Get current commit index."""
        return self._commit_index

    @property
    def last_applied(self) -> int:
        """Get last applied index."""
        return self._last_applied


class NotLeaderError(Exception):
    """Raised when a leader-only operation is attempted on a non-leader."""
    pass
