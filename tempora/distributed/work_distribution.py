"""
Tempora Work Distribution - Distributed Task Assignment

This module implements work distribution strategies for assigning tasks
to cluster members in the distributed Tempora scheduler.

Strategies:
    - ROUND_ROBIN: Rotate assignments evenly across members
    - LEAST_LOADED: Assign to member with fewest active tasks
    - AFFINITY: Prefer same member for related tasks
    - PRIORITY_AWARE: High priority to best-performing members

Algorithm:
    1. Leader receives new task
    2. Leader selects target member using strategy
    3. Leader logs assignment to distributed log
    4. All members apply assignment when committed
    5. Target member executes task

Rebalancing:
    - Triggered when member joins/leaves
    - Triggered when load imbalance exceeds threshold
    - Only uncommitted tasks can be rebalanced

Compliance:
    - TEMPORA-HA-001 §6: Work Distribution
    - SCH-002 §work_queue_management
"""

from __future__ import annotations

import asyncio
import logging
import random
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class DistributionStrategy(Enum):
    """Task distribution strategies."""
    ROUND_ROBIN = "round_robin"
    LEAST_LOADED = "least_loaded"
    AFFINITY = "affinity"
    PRIORITY_AWARE = "priority_aware"


@dataclass
class WorkDistributionConfig:
    """Configuration for work distribution."""

    # Default distribution strategy
    strategy: DistributionStrategy = DistributionStrategy.LEAST_LOADED

    # Rebalancing
    enable_rebalancing: bool = True
    rebalance_threshold_percent: float = 20.0  # Trigger if load diff > 20%
    rebalance_check_interval_seconds: float = 30.0

    # Task affinity
    affinity_weight: float = 0.3  # Weight for affinity in combined scoring
    affinity_ttl_seconds: int = 300  # How long to remember task affinity

    # Performance tracking
    track_member_performance: bool = True
    performance_window_seconds: int = 300


@dataclass
class MemberLoad:
    """Load information for a cluster member."""
    member_id: str
    active_tasks: int = 0
    pending_tasks: int = 0
    completed_tasks_5min: int = 0
    failed_tasks_5min: int = 0
    avg_task_duration_ms: float = 0.0

    @property
    def total_load(self) -> int:
        """Total task load."""
        return self.active_tasks + self.pending_tasks

    @property
    def success_rate(self) -> float:
        """Success rate in last 5 minutes."""
        total = self.completed_tasks_5min + self.failed_tasks_5min
        if total == 0:
            return 1.0  # Assume perfect if no data
        return self.completed_tasks_5min / total


@dataclass
class TaskAssignment:
    """Assignment of a task to a cluster member."""
    task_id: uuid.UUID
    target_member: str
    strategy_used: DistributionStrategy
    assigned_at: float
    affinity_key: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": str(self.task_id),
            "target_member": self.target_member,
            "strategy": self.strategy_used.value,
            "assigned_at": self.assigned_at,
            "affinity_key": self.affinity_key,
        }


class WorkDistributor:
    """
    Manages distribution of tasks across cluster members.

    This class runs on the leader and is responsible for:
    - Selecting which member should execute each task
    - Tracking load across members
    - Rebalancing when members join/leave
    - Maintaining task affinity mappings

    Example:
        distributor = WorkDistributor(
            instance_id="tempora-1",
            config=WorkDistributionConfig(),
            get_members_callback=get_active_members,
            assign_callback=assign_task,
        )

        # Assign a task
        assignment = await distributor.assign_task(
            task_id=task.id,
            tenant_id=task.tenant_id,
            priority=task.priority,
            affinity_key=task.correlation_id,
        )

        # Handle member changes
        await distributor.handle_member_joined("tempora-2")
        await distributor.handle_member_left("tempora-3")
    """

    def __init__(
        self,
        instance_id: str,
        config: Optional[WorkDistributionConfig] = None,
        get_members_callback: Optional[Callable[[], List[str]]] = None,
        get_member_load_callback: Optional[Callable[[str], MemberLoad]] = None,
        on_assignment_callback: Optional[
            Callable[[TaskAssignment], Coroutine]
        ] = None,
    ):
        self.instance_id = instance_id
        self.config = config or WorkDistributionConfig()

        # Callbacks
        self._get_members = get_members_callback or (lambda: [])
        self._get_member_load = get_member_load_callback
        self._on_assignment = on_assignment_callback

        # Round-robin state
        self._rr_index: int = 0

        # Affinity mappings: affinity_key -> member_id
        self._affinity_map: Dict[str, str] = {}
        self._affinity_timestamps: Dict[str, float] = {}

        # Load cache
        self._load_cache: Dict[str, MemberLoad] = {}
        self._load_cache_timestamp: float = 0.0

        # Rebalancing
        self._rebalance_task: Optional[asyncio.Task] = None
        self._running = False

        # Pending assignments (not yet committed)
        self._pending_assignments: Dict[uuid.UUID, TaskAssignment] = {}

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    async def start(self) -> None:
        """Start the work distributor."""
        if self._running:
            return

        self._running = True

        if self.config.enable_rebalancing:
            self._rebalance_task = asyncio.create_task(self._rebalance_loop())

        logger.info(
            f"Work distributor started",
            extra={
                "instance_id": self.instance_id,
                "strategy": self.config.strategy.value,
            },
        )

    async def stop(self) -> None:
        """Stop the work distributor."""
        if not self._running:
            return

        self._running = False

        if self._rebalance_task:
            self._rebalance_task.cancel()
            try:
                await self._rebalance_task
            except asyncio.CancelledError:
                pass
            self._rebalance_task = None

        logger.info("Work distributor stopped")

    # =========================================================================
    # TASK ASSIGNMENT
    # =========================================================================

    async def assign_task(
        self,
        task_id: uuid.UUID,
        tenant_id: uuid.UUID,
        priority: int = 2,
        affinity_key: Optional[str] = None,
        strategy: Optional[DistributionStrategy] = None,
    ) -> TaskAssignment:
        """
        Assign a task to a cluster member.

        Args:
            task_id: Task identifier
            tenant_id: Tenant identifier
            priority: Task priority (0=highest)
            affinity_key: Key for task affinity (e.g., correlation_id)
            strategy: Override distribution strategy

        Returns:
            TaskAssignment with target member

        Raises:
            NoAvailableMembersError: If no members available
        """
        import time

        members = self._get_members()
        if not members:
            raise NoAvailableMembersError("No cluster members available")

        # Use specified strategy or default
        use_strategy = strategy or self.config.strategy

        # Select target based on strategy
        if use_strategy == DistributionStrategy.ROUND_ROBIN:
            target = self._select_round_robin(members)
        elif use_strategy == DistributionStrategy.LEAST_LOADED:
            target = self._select_least_loaded(members)
        elif use_strategy == DistributionStrategy.AFFINITY:
            target = self._select_with_affinity(members, affinity_key)
        elif use_strategy == DistributionStrategy.PRIORITY_AWARE:
            target = self._select_priority_aware(members, priority)
        else:
            target = self._select_round_robin(members)

        assignment = TaskAssignment(
            task_id=task_id,
            target_member=target,
            strategy_used=use_strategy,
            assigned_at=time.time(),
            affinity_key=affinity_key,
        )

        # Track pending assignment
        self._pending_assignments[task_id] = assignment

        # Update affinity map if key provided
        if affinity_key:
            self._affinity_map[affinity_key] = target
            self._affinity_timestamps[affinity_key] = time.time()

        # Notify callback
        if self._on_assignment:
            await self._on_assignment(assignment)

        logger.debug(
            f"Assigned task to {target}",
            extra={
                "task_id": str(task_id),
                "target": target,
                "strategy": use_strategy.value,
            },
        )

        return assignment

    def confirm_assignment(self, task_id: uuid.UUID) -> None:
        """Confirm that an assignment has been committed."""
        self._pending_assignments.pop(task_id, None)

    def cancel_assignment(self, task_id: uuid.UUID) -> None:
        """Cancel a pending assignment."""
        self._pending_assignments.pop(task_id, None)

    # =========================================================================
    # SELECTION STRATEGIES
    # =========================================================================

    def _select_round_robin(self, members: List[str]) -> str:
        """Select member using round-robin."""
        target = members[self._rr_index % len(members)]
        self._rr_index += 1
        return target

    def _select_least_loaded(self, members: List[str]) -> str:
        """Select member with lowest load."""
        min_load = float('inf')
        target = members[0]

        for member_id in members:
            load = self._get_load(member_id)
            if load.total_load < min_load:
                min_load = load.total_load
                target = member_id

        return target

    def _select_with_affinity(
        self,
        members: List[str],
        affinity_key: Optional[str],
    ) -> str:
        """Select member with affinity preference."""
        import time

        # Check if we have affinity mapping
        if affinity_key and affinity_key in self._affinity_map:
            preferred = self._affinity_map[affinity_key]

            # Check if affinity is still valid
            timestamp = self._affinity_timestamps.get(affinity_key, 0)
            if time.time() - timestamp < self.config.affinity_ttl_seconds:
                # Check if preferred member is available
                if preferred in members:
                    return preferred

            # Clean up expired affinity
            del self._affinity_map[affinity_key]
            self._affinity_timestamps.pop(affinity_key, None)

        # Fall back to least loaded
        return self._select_least_loaded(members)

    def _select_priority_aware(
        self,
        members: List[str],
        priority: int,
    ) -> str:
        """Select member based on priority and performance."""
        # For high priority (0-1), prefer best performers
        if priority <= 1:
            best_rate = 0.0
            target = members[0]

            for member_id in members:
                load = self._get_load(member_id)
                if load.success_rate > best_rate:
                    best_rate = load.success_rate
                    target = member_id
                elif load.success_rate == best_rate:
                    # Tie-break by load
                    if load.total_load < self._get_load(target).total_load:
                        target = member_id

            return target

        # For normal/low priority, use least loaded
        return self._select_least_loaded(members)

    def _get_load(self, member_id: str) -> MemberLoad:
        """Get load for a member."""
        if self._get_member_load:
            return self._get_member_load(member_id)

        # Return cached or default
        return self._load_cache.get(
            member_id,
            MemberLoad(member_id=member_id),
        )

    # =========================================================================
    # MEMBER CHANGES
    # =========================================================================

    async def handle_member_joined(self, member_id: str) -> None:
        """Handle a new member joining the cluster."""
        logger.info(f"Member joined: {member_id}")

        # Add to load cache
        self._load_cache[member_id] = MemberLoad(member_id=member_id)

        # Trigger rebalance if enabled
        if self.config.enable_rebalancing:
            await self._trigger_rebalance()

    async def handle_member_left(self, member_id: str) -> None:
        """Handle a member leaving the cluster."""
        logger.info(f"Member left: {member_id}")

        # Remove from load cache
        self._load_cache.pop(member_id, None)

        # Clean up affinity mappings pointing to this member
        keys_to_remove = [
            k for k, v in self._affinity_map.items()
            if v == member_id
        ]
        for key in keys_to_remove:
            del self._affinity_map[key]
            self._affinity_timestamps.pop(key, None)

        # Reassign pending tasks from this member
        await self._reassign_from_member(member_id)

    async def _reassign_from_member(self, member_id: str) -> None:
        """Reassign pending tasks from a departed member."""
        tasks_to_reassign = [
            assignment for assignment in self._pending_assignments.values()
            if assignment.target_member == member_id
        ]

        members = self._get_members()
        if not members:
            logger.warning("No members available for reassignment")
            return

        for assignment in tasks_to_reassign:
            # Reassign using least loaded strategy
            new_target = self._select_least_loaded(members)
            new_assignment = TaskAssignment(
                task_id=assignment.task_id,
                target_member=new_target,
                strategy_used=DistributionStrategy.LEAST_LOADED,
                assigned_at=assignment.assigned_at,
                affinity_key=assignment.affinity_key,
            )
            self._pending_assignments[assignment.task_id] = new_assignment

            if self._on_assignment:
                await self._on_assignment(new_assignment)

            logger.info(
                f"Reassigned task from {member_id} to {new_target}",
                extra={"task_id": str(assignment.task_id)},
            )

    # =========================================================================
    # REBALANCING
    # =========================================================================

    async def _rebalance_loop(self) -> None:
        """Periodic rebalancing check."""
        while self._running:
            try:
                await asyncio.sleep(
                    self.config.rebalance_check_interval_seconds
                )
                await self._check_rebalance()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error in rebalance loop: {e}")

    async def _trigger_rebalance(self) -> None:
        """Trigger immediate rebalance check."""
        await self._check_rebalance()

    async def _check_rebalance(self) -> None:
        """Check if rebalancing is needed."""
        members = self._get_members()
        if len(members) < 2:
            return

        # Get loads
        loads = [self._get_load(m) for m in members]
        min_load = min(l.total_load for l in loads)
        max_load = max(l.total_load for l in loads)

        if min_load == 0:
            return

        # Calculate imbalance percentage
        imbalance = ((max_load - min_load) / min_load) * 100

        if imbalance > self.config.rebalance_threshold_percent:
            logger.info(
                f"Load imbalance detected: {imbalance:.1f}%",
                extra={
                    "min_load": min_load,
                    "max_load": max_load,
                    "threshold": self.config.rebalance_threshold_percent,
                },
            )
            # Rebalancing would be handled here
            # For now, we just log - actual rebalancing is complex
            # and involves the distributed log

    # =========================================================================
    # LOAD TRACKING
    # =========================================================================

    def update_member_load(self, load: MemberLoad) -> None:
        """Update cached load for a member."""
        self._load_cache[load.member_id] = load

    def record_task_started(self, member_id: str) -> None:
        """Record that a task started on a member."""
        if member_id in self._load_cache:
            self._load_cache[member_id].active_tasks += 1

    def record_task_completed(self, member_id: str, duration_ms: float) -> None:
        """Record that a task completed on a member."""
        if member_id in self._load_cache:
            load = self._load_cache[member_id]
            load.active_tasks = max(0, load.active_tasks - 1)
            load.completed_tasks_5min += 1
            # Update running average
            if load.avg_task_duration_ms == 0:
                load.avg_task_duration_ms = duration_ms
            else:
                alpha = 0.2
                load.avg_task_duration_ms = (
                    alpha * duration_ms +
                    (1 - alpha) * load.avg_task_duration_ms
                )

    def record_task_failed(self, member_id: str) -> None:
        """Record that a task failed on a member."""
        if member_id in self._load_cache:
            load = self._load_cache[member_id]
            load.active_tasks = max(0, load.active_tasks - 1)
            load.failed_tasks_5min += 1

    # =========================================================================
    # STATUS
    # =========================================================================

    def get_status(self) -> Dict[str, Any]:
        """Get distribution status for monitoring."""
        return {
            "instance_id": self.instance_id,
            "strategy": self.config.strategy.value,
            "running": self._running,
            "pending_assignments": len(self._pending_assignments),
            "affinity_mappings": len(self._affinity_map),
            "member_loads": {
                m: {
                    "active": l.active_tasks,
                    "pending": l.pending_tasks,
                    "success_rate": l.success_rate,
                }
                for m, l in self._load_cache.items()
            },
        }

    def get_assignment(self, task_id: uuid.UUID) -> Optional[TaskAssignment]:
        """Get assignment for a task."""
        return self._pending_assignments.get(task_id)


class NoAvailableMembersError(Exception):
    """Raised when no cluster members are available for task assignment."""
    pass
