"""
Tempora Scheduler Models
========================

Django models for the distributed task scheduler implementing task management,
execution tracking, scheduling patterns, and dead letter queue handling.

Models:
-------
- CognitiveTask: Unit of work with cognitive metadata
- TaskExecution: Execution attempt history
- Schedule: Recurring schedule definitions
- DeadLetterEntry: Failed tasks for manual review
- CircuitBreakerState: Per-service circuit breaker state
- ClusterMember: Cluster membership for distributed coordination
- DistributedLogEntry: Raft log entries for state replication
- FencingToken: Split-brain prevention tokens
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from django.db import models, transaction
from django.utils import timezone

from tempora.base import TemporaEntity

from tempora.types import (
    CASCADE_BUDGET,
    CircuitBreakerConfig,
    CircuitState,
    CronSchedule,
    IntervalSchedule,
    QueueType,
    RetryConfig,
    RetryStrategy,
    ScheduleType,
    TaskContext,
    TaskPriority,
    TaskState,
    TenantQuota,
    get_cascade_limit,
)

logger = logging.getLogger(__name__)


# =============================================================================
# COGNITIVE TASK MODEL (SCH-001 §core_concepts.task)
# =============================================================================


class CognitiveTask(TemporaEntity):
    """
    Unit of work in the cognitive scheduler per SCH-001 §5.

    A CognitiveTask represents a callable unit with cognitive metadata
    enabling intelligent scheduling decisions based on priority, urgency,
    confidence, and governance risk scores.

    Standard: SCH-001 §core_concepts.task
    Compliance: ISO 9001:2015 §9.5
    """

    # =========================================================================
    # Identity
    # =========================================================================

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
        help_text="Unique task identifier",
    )
    tenant_id = models.UUIDField(
        db_index=True,
        help_text="Tenant isolation identifier",
    )
    task_name = models.CharField(
        max_length=255,
        db_index=True,
        help_text="Task type/handler name (e.g., 'reflex.escalation.handler')",
    )

    # =========================================================================
    # Correlation and Lineage (SCH-001 §task_context)
    # =========================================================================

    correlation_id = models.UUIDField(
        default=uuid.uuid4,
        db_index=True,
        help_text="Correlation ID for distributed tracing",
    )
    root_correlation_id = models.UUIDField(
        null=True,
        blank=True,
        db_index=True,
        help_text="Root correlation for cascade chains",
    )
    parent_task_id = models.UUIDField(
        null=True,
        blank=True,
        db_index=True,
        help_text="Parent task ID for cascade lineage",
    )
    cascade_depth = models.PositiveSmallIntegerField(
        default=0,
        help_text="Depth in cascade chain (0 = root task)",
    )

    # =========================================================================
    # Idempotency (SCH-002 §6, TD-SCH-002)
    # =========================================================================

    idempotency_key = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        db_index=True,
        help_text="Unique key for idempotent task creation (prevents duplicates)",
    )
    idempotency_expires_at = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text="When idempotency key expires (for TTL cleanup)",
    )

    # =========================================================================
    # Task Configuration
    # =========================================================================

    payload = models.JSONField(
        default=dict,
        help_text="Task arguments/payload as JSON",
    )
    queue = models.CharField(
        max_length=50,
        default=QueueType.CORE.value,
        db_index=True,
        help_text="Target queue for routing",
    )
    priority = models.PositiveSmallIntegerField(
        default=TaskPriority.NORMAL.value,
        db_index=True,
        help_text="Task priority (0=critical, 4=batch)",
    )

    # =========================================================================
    # Cognitive Attributes (SCH-001 §task_context)
    # =========================================================================

    reflex_source = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Originating reflex ID if task was triggered by reflex",
    )
    confidence_score = models.FloatField(
        default=1.0,
        help_text="Confidence in task necessity (0.0-1.0)",
    )
    urgency = models.FloatField(
        default=0.5,
        help_text="Task urgency level (0.0-1.0)",
    )
    governance_risk = models.FloatField(
        default=0.0,
        help_text="Governance risk score (0.0-1.0, higher=riskier)",
    )
    resource_weight = models.FloatField(
        default=1.0,
        help_text="Relative resource consumption weight",
    )
    priority_score = models.FloatField(
        default=0.5,
        db_index=True,
        help_text="Computed priority score for scheduling",
    )

    # =========================================================================
    # State and Execution (SCH-001 §task_states)
    # =========================================================================

    state = models.CharField(
        max_length=20,
        default=TaskState.PENDING.value,
        db_index=True,
        choices=[(s.value, s.value) for s in TaskState],
        help_text="Current task state",
    )
    attempts = models.PositiveSmallIntegerField(
        default=0,
        help_text="Number of execution attempts",
    )
    max_attempts = models.PositiveSmallIntegerField(
        default=3,
        help_text="Maximum execution attempts before DLQ",
    )

    # =========================================================================
    # Retry Configuration (SCH-002 §retry_strategies)
    # =========================================================================

    retry_strategy = models.CharField(
        max_length=30,
        default=RetryStrategy.EXPONENTIAL.value,
        choices=[(s.value, s.value) for s in RetryStrategy],
        help_text="Retry backoff strategy",
    )
    retry_base_delay = models.PositiveIntegerField(
        default=1,
        help_text="Base delay for retry in seconds",
    )
    retry_max_delay = models.PositiveIntegerField(
        default=3600,
        help_text="Maximum retry delay in seconds",
    )
    next_retry_at = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text="Scheduled time for next retry attempt",
    )

    # =========================================================================
    # Timing
    # =========================================================================

    created_at = models.DateTimeField(
        auto_now_add=True,
        db_index=True,
        help_text="Task creation timestamp",
    )
    scheduled_at = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text="When task was scheduled for execution",
    )
    started_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Execution start time",
    )
    completed_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Execution completion time",
    )
    deadline = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text="Hard deadline for task completion",
    )
    timeout_seconds = models.PositiveIntegerField(
        default=60,
        help_text="Execution timeout in seconds",
    )

    # =========================================================================
    # Results
    # =========================================================================

    result = models.JSONField(
        null=True,
        blank=True,
        help_text="Task execution result",
    )
    error_message = models.TextField(
        null=True,
        blank=True,
        help_text="Error message if failed",
    )
    error_type = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Error type classification",
    )

    # =========================================================================
    # Scheduling Reference
    # =========================================================================

    schedule = models.ForeignKey(
        "Schedule",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="tasks",
        help_text="Associated schedule for recurring tasks",
    )

    # =========================================================================
    # FLD-001 §3.1: Standard timestamp fields
    # =========================================================================

    updated_at = models.DateTimeField(
        auto_now=True,
        help_text="Last modification timestamp (FLD-001)",
    )

    class Meta(TemporaEntity.Meta):
        db_table = "sched_cognitive_task"
        verbose_name = "Cognitive Task"
        verbose_name_plural = "Cognitive Tasks"
        ordering = ["-priority_score", "created_at"]
        indexes = [
            models.Index(
                fields=["tenant_id", "state", "priority_score"],
                name="idx_task_tenant_state_prio",
            ),
            models.Index(
                fields=["tenant_id", "queue", "state"],
                name="idx_task_tenant_queue_state",
            ),
            models.Index(
                fields=["state", "next_retry_at"],
                name="idx_task_retry_pending",
            ),
            models.Index(
                fields=["correlation_id"],
                name="idx_task_correlation",
            ),
            models.Index(
                fields=["root_correlation_id"],
                name="idx_task_root_correlation",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.task_name}:{self.id} [{self.state}]"

    def save(self, *args, **kwargs):
        """Compute priority score before saving."""
        self._compute_priority_score()
        if self.root_correlation_id is None:
            self.root_correlation_id = self.correlation_id
        super().save(*args, **kwargs)

    def _compute_priority_score(self) -> None:
        """
        Compute priority score from cognitive attributes per SCH-001 §task_context.

        Formula: (confidence * 0.3) + (urgency * 0.4) + ((1 - governance_risk) * 0.3)
        """
        self.priority_score = (
            self.confidence_score * 0.3
            + self.urgency * 0.4
            + (1 - self.governance_risk) * 0.3
        )

    # =========================================================================
    # State Transitions (SCH-001 §task_states)
    # =========================================================================

    def can_transition_to(self, new_state: TaskState) -> bool:
        """Check if transition to new state is valid."""
        current = TaskState(self.state)
        valid = TaskState.valid_transitions()
        return new_state in valid.get(current, [])

    def transition_to(self, new_state: TaskState, **kwargs) -> bool:
        """
        Transition task to new state with validation.

        Returns True if transition succeeded.
        """
        if not self.can_transition_to(new_state):
            logger.warning(
                f"Invalid state transition: {self.state} -> {new_state.value} "
                f"for task {self.id}"
            )
            return False

        old_state = self.state
        self.state = new_state.value

        # Update timestamps based on transition
        now = timezone.now()
        if new_state == TaskState.SCHEDULED:
            self.scheduled_at = now
        elif new_state == TaskState.RUNNING:
            self.started_at = now
        elif new_state.is_terminal:
            self.completed_at = now

        # Handle additional kwargs
        if "error_message" in kwargs:
            self.error_message = kwargs["error_message"]
        if "error_type" in kwargs:
            self.error_type = kwargs["error_type"]
        if "result" in kwargs:
            self.result = kwargs["result"]

        self.save()

        # Log transition
        logger.info(
            f"Task {self.id} transitioned: {old_state} -> {new_state.value}",
            extra={
                "task_id": str(self.id),
                "correlation_id": str(self.correlation_id),
                "old_state": old_state,
                "new_state": new_state.value,
            },
        )

        return True

    # =========================================================================
    # Retry Logic (SCH-002 §retry_strategies)
    # =========================================================================

    def schedule_retry(self) -> Optional[datetime]:
        """
        Schedule next retry attempt per SCH-002 §12.

        Returns next retry time or None if max attempts exceeded.
        """
        self.attempts += 1

        if self.attempts >= self.max_attempts:
            self.transition_to(TaskState.DEAD_LETTERED)
            return None

        config = RetryConfig(
            strategy=RetryStrategy(self.retry_strategy),
            max_attempts=self.max_attempts,
            base_delay_seconds=self.retry_base_delay,
            max_delay_seconds=self.retry_max_delay,
        )

        delay = config.get_delay(self.attempts)
        self.next_retry_at = timezone.now() + delay
        self.transition_to(TaskState.RETRYING)

        return self.next_retry_at

    @property
    def has_attempts_remaining(self) -> bool:
        """Check if retry attempts remain."""
        return self.attempts < self.max_attempts

    @property
    def is_past_deadline(self) -> bool:
        """Check if task is past its deadline."""
        if self.deadline is None:
            return False
        return timezone.now() > self.deadline

    # =========================================================================
    # Context Conversion
    # =========================================================================

    def to_context(self) -> TaskContext:
        """Convert to TaskContext for scheduler operations."""
        return TaskContext(
            task_id=str(self.id),
            correlation_id=str(self.correlation_id),
            tenant_id=str(self.tenant_id),
            root_correlation_id=str(self.root_correlation_id) if self.root_correlation_id else None,
            parent_task_id=str(self.parent_task_id) if self.parent_task_id else None,
            cascade_depth=self.cascade_depth,
            reflex_source=self.reflex_source,
            confidence_score=self.confidence_score,
            urgency=self.urgency,
            governance_risk=self.governance_risk,
            resource_weight=self.resource_weight,
            created_at=self.created_at,
            scheduled_at=self.scheduled_at,
            deadline=self.deadline,
            attempts=self.attempts,
            max_attempts=self.max_attempts,
        )

    # =========================================================================
    # Factory Methods
    # =========================================================================

    # Default idempotency TTL: 24 hours
    IDEMPOTENCY_TTL_HOURS = 24

    @classmethod
    def create_task(
        cls,
        tenant_id: uuid.UUID,
        task_name: str,
        payload: Dict[str, Any],
        priority: TaskPriority = TaskPriority.NORMAL,
        queue: QueueType = QueueType.CORE,
        correlation_id: Optional[uuid.UUID] = None,
        parent_task: Optional["CognitiveTask"] = None,
        urgency: float = 0.5,
        confidence_score: float = 1.0,
        governance_risk: float = 0.0,
        deadline: Optional[datetime] = None,
        timeout_seconds: int = 60,
        retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
        max_attempts: int = 3,
        idempotency_key: Optional[str] = None,
        idempotency_ttl_hours: int = 24,
    ) -> Tuple["CognitiveTask", bool]:
        """
        Factory method to create a new cognitive task with idempotency support.

        Standard: SCH-001 §core_concepts.task, SCH-002 §6 (Idempotency)

        Args:
            ... (existing args)
            idempotency_key: Optional unique key for deduplication
            idempotency_ttl_hours: Hours until idempotency key expires (default 24)

        Returns:
            Tuple of (CognitiveTask, was_created)
            - was_created is False if task already exists (idempotency hit)
        """
        # SCH-002 §6: Check for existing task with same idempotency key
        if idempotency_key:
            existing = cls.objects.filter(
                tenant_id=tenant_id,
                idempotency_key=idempotency_key,
                idempotency_expires_at__gt=timezone.now(),
            ).first()

            if existing:
                logger.info(
                    f"[SCHED IDEMPOTENCY] Duplicate task prevented: {idempotency_key}",
                    extra={"existing_task_id": str(existing.id)},
                )
                return existing, False

        cascade_depth = 0
        root_correlation_id = correlation_id
        parent_task_id = None

        if parent_task:
            cascade_depth = parent_task.cascade_depth + 1
            root_correlation_id = parent_task.root_correlation_id or parent_task.correlation_id
            parent_task_id = parent_task.id

            # Check cascade budget
            limit = get_cascade_limit(cascade_depth)
            if limit == 0:
                raise ValueError(
                    f"Cascade depth {cascade_depth} exceeds maximum "
                    f"allowed depth {CASCADE_BUDGET['max_depth']}"
                )

        # Calculate idempotency expiration
        idempotency_expires_at = None
        if idempotency_key:
            idempotency_expires_at = timezone.now() + timedelta(hours=idempotency_ttl_hours)

        task = cls(
            tenant_id=tenant_id,
            task_name=task_name,
            payload=payload,
            priority=priority.value,
            queue=queue.value,
            correlation_id=correlation_id or uuid.uuid4(),
            root_correlation_id=root_correlation_id,
            parent_task_id=parent_task_id,
            cascade_depth=cascade_depth,
            urgency=urgency,
            confidence_score=confidence_score,
            governance_risk=governance_risk,
            deadline=deadline,
            timeout_seconds=timeout_seconds,
            retry_strategy=retry_strategy.value,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            idempotency_expires_at=idempotency_expires_at,
        )
        task.save()
        return task, True

    @classmethod
    def cleanup_expired_idempotency(cls) -> int:
        """
        Remove expired idempotency keys (TTL cleanup).

        Standard: SCH-002 §6

        Returns:
            Number of tasks with expired keys cleaned up
        """
        expired_count = cls.objects.filter(
            idempotency_key__isnull=False,
            idempotency_expires_at__lt=timezone.now(),
        ).update(
            idempotency_key=None,
            idempotency_expires_at=None,
        )
        if expired_count > 0:
            logger.info(f"[SCHED IDEMPOTENCY] Cleaned up {expired_count} expired keys")
        return expired_count

    @classmethod
    def generate_idempotency_key(
        cls,
        tenant_id: str,
        task_name: str,
        payload: Dict[str, Any],
    ) -> str:
        """
        Generate a deterministic idempotency key from task parameters.

        Useful when callers want automatic deduplication based on content.

        Standard: SCH-002 §6
        """
        # Create deterministic hash of task content
        content = json.dumps({
            "tenant_id": str(tenant_id),
            "task_name": task_name,
            "payload": payload,
        }, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:32]


# =============================================================================
# TASK EXECUTION MODEL (SCH-001 §task_execution)
# =============================================================================


class TaskExecution(TemporaEntity):
    """
    Execution attempt record for a cognitive task per SCH-001 §6.

    Each execution attempt is recorded for audit trail and debugging.

    Standard: SCH-001 §task_execution
    Compliance: SOC 2 CC7.2
    """

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )
    task = models.ForeignKey(
        CognitiveTask,
        on_delete=models.CASCADE,
        related_name="executions",
        help_text="Parent task",
    )
    attempt_number = models.PositiveSmallIntegerField(
        help_text="Attempt number (1-indexed)",
    )
    worker_id = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Worker instance that executed this attempt",
    )

    # Timing
    started_at = models.DateTimeField(
        auto_now_add=True,
        help_text="Execution start time",
    )
    completed_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Execution completion time",
    )
    duration_ms = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Execution duration in milliseconds",
    )

    # Result
    success = models.BooleanField(
        default=False,
        help_text="Whether execution succeeded",
    )
    result = models.JSONField(
        null=True,
        blank=True,
        help_text="Execution result",
    )
    error_message = models.TextField(
        null=True,
        blank=True,
        help_text="Error message if failed",
    )
    error_type = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Error classification (transient/permanent/rate_limited)",
    )
    error_traceback = models.TextField(
        null=True,
        blank=True,
        help_text="Full error traceback",
    )

    # Resource metrics
    memory_peak_mb = models.FloatField(
        null=True,
        blank=True,
        help_text="Peak memory usage in MB",
    )
    cpu_time_ms = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="CPU time in milliseconds",
    )

    # FLD-001 §3.1: Standard timestamp field
    updated_at = models.DateTimeField(
        auto_now=True,
        help_text="Last modification timestamp (FLD-001)",
    )

    class Meta(TemporaEntity.Meta):
        db_table = "sched_task_execution"
        verbose_name = "Task Execution"
        verbose_name_plural = "Task Executions"
        ordering = ["-started_at"]
        indexes = [
            models.Index(
                fields=["task", "attempt_number"],
                name="idx_execution_task_attempt",
            ),
        ]
        unique_together = [["task", "attempt_number"]]

    def __str__(self) -> str:
        status = "✓" if self.success else "✗"
        return f"{self.task.task_name} attempt #{self.attempt_number} [{status}]"

    def complete(
        self,
        success: bool,
        result: Any = None,
        error_message: Optional[str] = None,
        error_type: Optional[str] = None,
        error_traceback: Optional[str] = None,
    ) -> None:
        """Mark execution as complete."""
        self.completed_at = timezone.now()
        self.duration_ms = int(
            (self.completed_at - self.started_at).total_seconds() * 1000
        )
        self.success = success
        self.result = result
        self.error_message = error_message
        self.error_type = error_type
        self.error_traceback = error_traceback
        self.save()


# =============================================================================
# SCHEDULE MODEL (SCH-001 §temporal_patterns)
# =============================================================================


class Schedule(TemporaEntity):
    """
    Recurring schedule definition per SCH-001 §6.

    Supports cron expressions, fixed intervals, and one-time delayed execution.

    Standard: SCH-001 §temporal_patterns
    """

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )
    tenant_id = models.UUIDField(
        db_index=True,
        help_text="Tenant isolation identifier",
    )
    schedule_id = models.CharField(
        max_length=100,
        db_index=True,
        help_text="Human-readable schedule identifier",
    )
    name = models.CharField(
        max_length=255,
        help_text="Schedule name/description",
    )

    # Task template
    task_name = models.CharField(
        max_length=255,
        help_text="Task handler name to invoke",
    )
    payload_template = models.JSONField(
        default=dict,
        help_text="Template for task payload",
    )
    queue = models.CharField(
        max_length=50,
        default=QueueType.CORE.value,
        help_text="Target queue for scheduled tasks",
    )
    priority = models.PositiveSmallIntegerField(
        default=TaskPriority.NORMAL.value,
        help_text="Priority for scheduled tasks",
    )

    # Schedule type and configuration
    schedule_type = models.CharField(
        max_length=20,
        choices=[(s.value, s.value) for s in ScheduleType],
        help_text="Schedule type (cron/interval/once)",
    )

    # Cron fields (for CRON type)
    cron_minute = models.CharField(max_length=50, default="*")
    cron_hour = models.CharField(max_length=50, default="*")
    cron_day_of_month = models.CharField(max_length=50, default="*")
    cron_month = models.CharField(max_length=50, default="*")
    cron_day_of_week = models.CharField(max_length=50, default="*")

    # Interval fields (for INTERVAL type)
    interval_seconds = models.PositiveIntegerField(
        default=0,
        help_text="Interval in seconds",
    )

    # One-time execution (for ONCE type)
    run_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="One-time execution timestamp",
    )

    # State
    enabled = models.BooleanField(
        default=True,
        db_index=True,
        help_text="Whether schedule is active",
    )
    last_run_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Last successful execution time",
    )
    next_run_at = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text="Next scheduled execution time",
    )
    run_count = models.PositiveIntegerField(
        default=0,
        help_text="Total execution count",
    )

    # Limits
    max_runs = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Maximum number of runs (null = unlimited)",
    )
    expires_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Schedule expiration time",
    )

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Creator identifier",
    )

    class Meta(TemporaEntity.Meta):
        db_table = "sched_schedule"
        verbose_name = "Schedule"
        verbose_name_plural = "Schedules"
        ordering = ["next_run_at"]
        indexes = [
            models.Index(
                fields=["tenant_id", "enabled", "next_run_at"],
                name="idx_schedule_tenant_enabled",
            ),
        ]
        unique_together = [["tenant_id", "schedule_id"]]

    def __str__(self) -> str:
        status = "enabled" if self.enabled else "disabled"
        return f"{self.name} ({self.schedule_type}) [{status}]"

    @property
    def cron_expression(self) -> str:
        """Get full cron expression."""
        return (
            f"{self.cron_minute} {self.cron_hour} {self.cron_day_of_month} "
            f"{self.cron_month} {self.cron_day_of_week}"
        )

    def calculate_next_run(self, from_time: Optional[datetime] = None) -> Optional[datetime]:
        """
        Calculate next run time based on schedule type.

        For production use with cron, consider using croniter library.
        """
        from_time = from_time or timezone.now()

        if self.schedule_type == ScheduleType.ONCE.value:
            if self.run_at and self.run_at > from_time:
                return self.run_at
            return None

        if self.schedule_type == ScheduleType.INTERVAL.value:
            if self.last_run_at:
                return self.last_run_at + timedelta(seconds=self.interval_seconds)
            return from_time + timedelta(seconds=self.interval_seconds)

        if self.schedule_type == ScheduleType.CRON.value:
            # Simplified cron calculation - for production use croniter
            # This is a placeholder that returns next hour for "0 * * * *"
            if self.cron_minute == "0" and self.cron_hour == "*":
                next_hour = from_time.replace(minute=0, second=0, microsecond=0)
                if next_hour <= from_time:
                    next_hour += timedelta(hours=1)
                return next_hour
            # Default: next minute
            return from_time + timedelta(minutes=1)

        return None

    def update_next_run(self) -> None:
        """Update next_run_at based on current time."""
        # Check limits
        if self.max_runs and self.run_count >= self.max_runs:
            self.enabled = False
            self.next_run_at = None
        elif self.expires_at and timezone.now() >= self.expires_at:
            self.enabled = False
            self.next_run_at = None
        else:
            self.next_run_at = self.calculate_next_run()
        self.save()

    def record_run(self) -> None:
        """Record a successful run."""
        self.last_run_at = timezone.now()
        self.run_count += 1
        self.update_next_run()

    @classmethod
    def create_cron_schedule(
        cls,
        tenant_id: uuid.UUID,
        schedule_id: str,
        name: str,
        task_name: str,
        cron: CronSchedule,
        **kwargs,
    ) -> "Schedule":
        """Create a cron-based schedule."""
        schedule = cls(
            tenant_id=tenant_id,
            schedule_id=schedule_id,
            name=name,
            task_name=task_name,
            schedule_type=ScheduleType.CRON.value,
            cron_minute=cron.minute,
            cron_hour=cron.hour,
            cron_day_of_month=cron.day_of_month,
            cron_month=cron.month,
            cron_day_of_week=cron.day_of_week,
            **kwargs,
        )
        schedule.next_run_at = schedule.calculate_next_run()
        schedule.save()
        return schedule

    @classmethod
    def create_interval_schedule(
        cls,
        tenant_id: uuid.UUID,
        schedule_id: str,
        name: str,
        task_name: str,
        interval: IntervalSchedule,
        **kwargs,
    ) -> "Schedule":
        """Create an interval-based schedule."""
        schedule = cls(
            tenant_id=tenant_id,
            schedule_id=schedule_id,
            name=name,
            task_name=task_name,
            schedule_type=ScheduleType.INTERVAL.value,
            interval_seconds=interval.total_seconds,
            **kwargs,
        )
        schedule.next_run_at = schedule.calculate_next_run()
        schedule.save()
        return schedule


# =============================================================================
# DEAD LETTER QUEUE MODEL (SCH-002 §dlq_handling)
# =============================================================================


class DeadLetterEntry(TemporaEntity):
    """
    Dead Letter Queue entry for tasks that exhausted retries per SCH-002 §13.

    Failed tasks are moved here for manual review and potential reprocessing.

    Standard: SCH-002 §dlq_handling
    Compliance: SOC 2 CC7.2
    """

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )
    tenant_id = models.UUIDField(
        db_index=True,
        help_text="Tenant isolation identifier",
    )
    original_task = models.OneToOneField(
        CognitiveTask,
        on_delete=models.CASCADE,
        related_name="dlq_entry",
        help_text="Original failed task",
    )

    # Failure details
    failure_reason = models.TextField(
        help_text="Reason for DLQ placement",
    )
    failure_count = models.PositiveIntegerField(
        help_text="Total failure count before DLQ",
    )
    last_error_message = models.TextField(
        null=True,
        blank=True,
        help_text="Last error message",
    )
    last_error_type = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Last error classification",
    )
    last_error_traceback = models.TextField(
        null=True,
        blank=True,
        help_text="Last error traceback",
    )

    # State
    status = models.CharField(
        max_length=20,
        default="pending",
        db_index=True,
        choices=[
            ("pending", "Pending Review"),
            ("reviewing", "Under Review"),
            ("reprocessing", "Reprocessing"),
            ("resolved", "Resolved"),
            ("discarded", "Discarded"),
        ],
        help_text="DLQ entry status",
    )
    resolution_notes = models.TextField(
        null=True,
        blank=True,
        help_text="Notes on resolution",
    )
    resolved_by = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Who resolved this entry",
    )
    resolved_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Resolution timestamp",
    )

    # Reprocessing
    reprocess_count = models.PositiveIntegerField(
        default=0,
        help_text="Number of reprocess attempts",
    )
    reprocessed_task_id = models.UUIDField(
        null=True,
        blank=True,
        help_text="ID of reprocessed task if any",
    )

    # FLD-001 §3.1: Standard timestamp fields
    created_at = models.DateTimeField(
        auto_now_add=True,
        db_index=True,
        help_text="Entry creation timestamp (FLD-001)",
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        help_text="Last modification timestamp (FLD-001)",
    )

    class Meta(TemporaEntity.Meta):
        db_table = "sched_dead_letter"
        verbose_name = "Dead Letter Entry"
        verbose_name_plural = "Dead Letter Entries"
        ordering = ["-created_at"]
        indexes = [
            models.Index(
                fields=["tenant_id", "status"],
                name="idx_dlq_tenant_status",
            ),
        ]

    def __str__(self) -> str:
        return f"DLQ: {self.original_task.task_name} [{self.status}]"

    @classmethod
    def create_from_task(
        cls,
        task: CognitiveTask,
        failure_reason: str,
    ) -> "DeadLetterEntry":
        """Create DLQ entry from a failed task."""
        # Get last execution for error details
        last_execution = task.executions.order_by("-attempt_number").first()

        entry = cls(
            tenant_id=task.tenant_id,
            original_task=task,
            failure_reason=failure_reason,
            failure_count=task.attempts,
            last_error_message=last_execution.error_message if last_execution else task.error_message,
            last_error_type=last_execution.error_type if last_execution else task.error_type,
            last_error_traceback=last_execution.error_traceback if last_execution else None,
        )
        entry.save()

        logger.warning(
            f"Task {task.id} moved to DLQ: {failure_reason}",
            extra={
                "task_id": str(task.id),
                "correlation_id": str(task.correlation_id),
                "failure_reason": failure_reason,
                "failure_count": task.attempts,
            },
        )

        return entry

    def reprocess(self, modified_payload: Optional[Dict[str, Any]] = None) -> CognitiveTask:
        """
        Create a new task from this DLQ entry for reprocessing.

        Returns the newly created task.
        """
        original = self.original_task
        payload = modified_payload or original.payload

        new_task = CognitiveTask.create_task(
            tenant_id=original.tenant_id,
            task_name=original.task_name,
            payload=payload,
            priority=TaskPriority(original.priority),
            queue=QueueType(original.queue),
            correlation_id=original.correlation_id,
            urgency=original.urgency,
            confidence_score=original.confidence_score,
            governance_risk=original.governance_risk,
            timeout_seconds=original.timeout_seconds,
            retry_strategy=RetryStrategy(original.retry_strategy),
            max_attempts=original.max_attempts,
        )

        self.status = "reprocessing"
        self.reprocess_count += 1
        self.reprocessed_task_id = new_task.id
        self.save()

        logger.info(
            f"DLQ entry {self.id} reprocessing as task {new_task.id}",
            extra={
                "dlq_id": str(self.id),
                "new_task_id": str(new_task.id),
                "reprocess_count": self.reprocess_count,
            },
        )

        return new_task

    def resolve(self, status: str, notes: str, resolved_by: str) -> None:
        """Mark DLQ entry as resolved."""
        self.status = status
        self.resolution_notes = notes
        self.resolved_by = resolved_by
        self.resolved_at = timezone.now()
        self.save()


# =============================================================================
# CIRCUIT BREAKER STATE MODEL (SCH-002 §circuit_breaker)
# =============================================================================


class CircuitBreakerState(TemporaEntity):
    """
    Per-service circuit breaker state per SCH-002 §4.

    Tracks circuit breaker state for external services to prevent
    cascade failures.

    Standard: SCH-002 §circuit_breaker
    """

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
    )
    service_name = models.CharField(
        max_length=255,
        unique=True,
        db_index=True,
        help_text="Service identifier (e.g., 'external_api', 'database')",
    )

    # State
    state = models.CharField(
        max_length=20,
        default=CircuitState.CLOSED.value,
        choices=[(s.value, s.value) for s in CircuitState],
        help_text="Current circuit state",
    )

    # Counters
    failure_count = models.PositiveIntegerField(
        default=0,
        help_text="Consecutive failure count",
    )
    success_count = models.PositiveIntegerField(
        default=0,
        help_text="Success count in half-open state",
    )
    half_open_calls = models.PositiveIntegerField(
        default=0,
        help_text="Calls made in half-open state",
    )

    # Configuration
    failure_threshold = models.PositiveIntegerField(
        default=5,
        help_text="Failures before opening circuit",
    )
    recovery_timeout_seconds = models.PositiveIntegerField(
        default=30,
        help_text="Seconds before testing recovery",
    )
    success_threshold = models.PositiveIntegerField(
        default=3,
        help_text="Successes needed to close circuit",
    )
    half_open_max_calls = models.PositiveIntegerField(
        default=3,
        help_text="Max calls in half-open state",
    )

    # Timing
    last_failure_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Last failure timestamp",
    )
    opened_at = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When circuit was opened",
    )

    # FLD-001 §3.1: Standard timestamp fields
    created_at = models.DateTimeField(
        auto_now_add=True,
        help_text="Circuit breaker creation timestamp (FLD-001)",
    )
    updated_at = models.DateTimeField(
        auto_now=True,
        help_text="Last modification timestamp (FLD-001)",
    )

    class Meta(TemporaEntity.Meta):
        db_table = "sched_circuit_breaker"
        verbose_name = "Circuit Breaker State"
        verbose_name_plural = "Circuit Breaker States"

    def __str__(self) -> str:
        return f"{self.service_name}: {self.state}"

    def record_success(self) -> None:
        """Record a successful call."""
        if self.state == CircuitState.HALF_OPEN.value:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self._close_circuit()
        elif self.state == CircuitState.CLOSED.value:
            # Reset failure count on success
            if self.failure_count > 0:
                self.failure_count = 0
                self.save()

    def record_failure(self) -> None:
        """Record a failed call."""
        self.failure_count += 1
        self.last_failure_at = timezone.now()

        if self.state == CircuitState.HALF_OPEN.value:
            # Any failure in half-open reopens circuit
            self._open_circuit()
        elif self.state == CircuitState.CLOSED.value:
            if self.failure_count >= self.failure_threshold:
                self._open_circuit()
            else:
                self.save()

    def can_execute(self) -> Tuple[bool, str]:
        """
        Check if execution is allowed through this circuit.

        Returns (allowed, reason).
        """
        if self.state == CircuitState.CLOSED.value:
            return True, "Circuit closed"

        if self.state == CircuitState.OPEN.value:
            # Check if recovery timeout has passed
            if self.opened_at:
                recovery_time = self.opened_at + timedelta(
                    seconds=self.recovery_timeout_seconds
                )
                if timezone.now() >= recovery_time:
                    self._half_open_circuit()
                    return True, "Circuit half-open (testing)"
            return False, "Circuit open"

        if self.state == CircuitState.HALF_OPEN.value:
            if self.half_open_calls < self.half_open_max_calls:
                self.half_open_calls += 1
                self.save()
                return True, "Circuit half-open (testing)"
            return False, "Circuit half-open (max test calls reached)"

        return False, "Unknown circuit state"

    def _open_circuit(self) -> None:
        """Open the circuit."""
        self.state = CircuitState.OPEN.value
        self.opened_at = timezone.now()
        self.success_count = 0
        self.half_open_calls = 0
        self.save()

        logger.warning(
            f"Circuit breaker OPENED for {self.service_name}",
            extra={
                "service": self.service_name,
                "failure_count": self.failure_count,
            },
        )

    def _half_open_circuit(self) -> None:
        """Transition to half-open state."""
        self.state = CircuitState.HALF_OPEN.value
        self.success_count = 0
        self.half_open_calls = 0
        self.save()

        logger.info(
            f"Circuit breaker HALF-OPEN for {self.service_name}",
            extra={"service": self.service_name},
        )

    def _close_circuit(self) -> None:
        """Close the circuit."""
        self.state = CircuitState.CLOSED.value
        self.failure_count = 0
        self.success_count = 0
        self.half_open_calls = 0
        self.opened_at = None
        self.save()

        logger.info(
            f"Circuit breaker CLOSED for {self.service_name}",
            extra={"service": self.service_name},
        )

    @classmethod
    def get_or_create_for_service(
        cls,
        service_name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ) -> "CircuitBreakerState":
        """Get or create circuit breaker state for a service."""
        config = config or CircuitBreakerConfig()

        state, created = cls.objects.get_or_create(
            service_name=service_name,
            defaults={
                "failure_threshold": config.failure_threshold,
                "recovery_timeout_seconds": config.recovery_timeout_seconds,
                "success_threshold": config.success_threshold,
                "half_open_max_calls": config.half_open_max_calls,
            },
        )

        return state


# =============================================================================
# CLUSTER MEMBER MODEL (TEMPORA-HA-001 §4)
# =============================================================================


class ClusterMemberRole(models.TextChoices):
    """Role of a cluster member in the Tempora distributed scheduler."""
    LEADER = "leader", "Leader"
    FOLLOWER = "follower", "Follower"
    CANDIDATE = "candidate", "Candidate"
    OBSERVER = "observer", "Observer"


class ClusterMemberStatus(models.TextChoices):
    """Status of a cluster member."""
    ACTIVE = "active", "Active"
    SUSPECTED = "suspected", "Suspected"
    FAILED = "failed", "Failed"
    DRAINING = "draining", "Draining"
    OFFLINE = "offline", "Offline"


class ClusterMember(TemporaEntity):
    """
    Registered cluster member for Tempora distributed scheduler.

    Tracks cluster membership for the native HA implementation.
    Each Tempora instance registers itself and maintains heartbeats.

    Standard: TEMPORA-HA-001 §4 (Cluster Membership)
    Compliance: ISO 9001:2015 §9.5
    """

    # =========================================================================
    # Identity
    # =========================================================================

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
        help_text="Unique cluster member identifier",
    )

    instance_id = models.CharField(
        max_length=255,
        unique=True,
        db_index=True,
        help_text="Unique instance identifier (e.g., 'tempora-1')",
    )

    # =========================================================================
    # Network Configuration
    # =========================================================================

    host = models.CharField(
        max_length=255,
        help_text="Host address for coordination (IP or hostname)",
    )

    port = models.IntegerField(
        default=9500,
        help_text="Port for coordination protocol",
    )

    # =========================================================================
    # Cluster State
    # =========================================================================

    role = models.CharField(
        max_length=20,
        choices=ClusterMemberRole.choices,
        default=ClusterMemberRole.FOLLOWER,
        db_index=True,
        help_text="Current role in the cluster",
    )

    status = models.CharField(
        max_length=20,
        choices=ClusterMemberStatus.choices,
        default=ClusterMemberStatus.ACTIVE,
        db_index=True,
        help_text="Current status",
    )

    # =========================================================================
    # Election State (Phase 2)
    # =========================================================================

    current_term = models.BigIntegerField(
        default=0,
        help_text="Current election term",
    )

    voted_for = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Instance ID voted for in current term",
    )

    # =========================================================================
    # Health Tracking
    # =========================================================================

    last_heartbeat = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text="Last heartbeat received from this member",
    )

    heartbeat_latency_ms = models.FloatField(
        null=True,
        blank=True,
        help_text="Average heartbeat round-trip time in milliseconds",
    )

    consecutive_missed = models.IntegerField(
        default=0,
        help_text="Number of consecutive missed heartbeats",
    )

    # =========================================================================
    # Metadata
    # =========================================================================

    joined_at = models.DateTimeField(
        auto_now_add=True,
        help_text="When this member joined the cluster",
    )

    metadata = models.JSONField(
        default=dict,
        blank=True,
        help_text="Additional metadata (version, capabilities, etc.)",
    )

    class Meta(TemporaEntity.Meta):
        db_table = "sched_cluster_member"
        verbose_name = "Cluster Member"
        verbose_name_plural = "Cluster Members"
        ordering = ["-role", "instance_id"]
        indexes = [
            models.Index(fields=["status", "role"]),
            models.Index(fields=["last_heartbeat"]),
        ]

    def __str__(self) -> str:
        return f"{self.instance_id} ({self.role}/{self.status})"

    # =========================================================================
    # Health Methods
    # =========================================================================

    def record_heartbeat(self, latency_ms: Optional[float] = None) -> None:
        """
        Record a successful heartbeat from this member.

        Args:
            latency_ms: Optional round-trip latency in milliseconds
        """
        self.last_heartbeat = timezone.now()
        self.consecutive_missed = 0

        if latency_ms is not None:
            # Exponential moving average for latency
            if self.heartbeat_latency_ms:
                alpha = 0.2  # Smoothing factor
                self.heartbeat_latency_ms = (
                    alpha * latency_ms + (1 - alpha) * self.heartbeat_latency_ms
                )
            else:
                self.heartbeat_latency_ms = latency_ms

        if self.status == ClusterMemberStatus.SUSPECTED:
            self.status = ClusterMemberStatus.ACTIVE

        self.save(update_fields=[
            "last_heartbeat",
            "consecutive_missed",
            "heartbeat_latency_ms",
            "status",
            "updated_at",
        ])

    def record_missed_heartbeat(self) -> bool:
        """
        Record a missed heartbeat.

        Returns:
            True if member should be marked as failed
        """
        self.consecutive_missed += 1

        # Update status based on missed count
        if self.consecutive_missed >= 3:
            self.status = ClusterMemberStatus.FAILED
            should_fail = True
        elif self.consecutive_missed >= 2:
            self.status = ClusterMemberStatus.SUSPECTED
            should_fail = False
        else:
            should_fail = False

        self.save(update_fields=[
            "consecutive_missed",
            "status",
            "updated_at",
        ])

        return should_fail

    @property
    def is_healthy(self) -> bool:
        """Check if member is considered healthy."""
        return self.status == ClusterMemberStatus.ACTIVE

    @property
    def is_leader(self) -> bool:
        """Check if this member is the current leader."""
        return self.role == ClusterMemberRole.LEADER

    @property
    def seconds_since_heartbeat(self) -> Optional[float]:
        """Get seconds since last heartbeat."""
        if not self.last_heartbeat:
            return None
        return (timezone.now() - self.last_heartbeat).total_seconds()

    # =========================================================================
    # Election Methods (Phase 2)
    # =========================================================================

    def update_term(self, term: int) -> None:
        """Update to a new term."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            if self.role == ClusterMemberRole.LEADER:
                self.role = ClusterMemberRole.FOLLOWER
            self.save(update_fields=[
                "current_term",
                "voted_for",
                "role",
                "updated_at",
            ])

    def vote_for(self, candidate_id: str, term: int) -> bool:
        """
        Vote for a candidate in an election.

        Returns:
            True if vote was granted
        """
        if term < self.current_term:
            return False

        if term > self.current_term:
            self.update_term(term)

        if self.voted_for is None or self.voted_for == candidate_id:
            self.voted_for = candidate_id
            self.save(update_fields=["voted_for", "updated_at"])
            return True

        return False

    def become_leader(self) -> None:
        """Transition to leader role."""
        self.role = ClusterMemberRole.LEADER
        self.save(update_fields=["role", "updated_at"])
        logger.info(f"Cluster member {self.instance_id} became LEADER")

    def step_down(self) -> None:
        """Step down from leader role."""
        if self.role == ClusterMemberRole.LEADER:
            self.role = ClusterMemberRole.FOLLOWER
            self.save(update_fields=["role", "updated_at"])
            logger.info(f"Cluster member {self.instance_id} stepped down to FOLLOWER")

    # =========================================================================
    # Class Methods
    # =========================================================================

    @classmethod
    def register(
        cls,
        instance_id: str,
        host: str,
        port: int,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "ClusterMember":
        """
        Register a new cluster member or update existing.

        Args:
            instance_id: Unique instance identifier
            host: Host address
            port: Coordination port
            metadata: Optional metadata

        Returns:
            ClusterMember: The registered member
        """
        member, created = cls.objects.update_or_create(
            instance_id=instance_id,
            defaults={
                "host": host,
                "port": port,
                "status": ClusterMemberStatus.ACTIVE,
                "last_heartbeat": timezone.now(),
                "consecutive_missed": 0,
                "metadata": metadata or {},
            },
        )

        if created:
            logger.info(f"Registered new cluster member: {instance_id}")
        else:
            logger.info(f"Updated cluster member: {instance_id}")

        return member

    @classmethod
    def deregister(cls, instance_id: str) -> bool:
        """
        Deregister a cluster member.

        Returns:
            True if member was found and removed
        """
        deleted, _ = cls.objects.filter(instance_id=instance_id).delete()
        if deleted:
            logger.info(f"Deregistered cluster member: {instance_id}")
        return deleted > 0

    @classmethod
    def get_leader(cls) -> Optional["ClusterMember"]:
        """Get the current cluster leader."""
        return cls.objects.filter(
            role=ClusterMemberRole.LEADER,
            status=ClusterMemberStatus.ACTIVE,
        ).first()

    @classmethod
    def get_active_members(cls) -> models.QuerySet:
        """Get all active cluster members."""
        return cls.objects.filter(status=ClusterMemberStatus.ACTIVE)

    @classmethod
    def get_healthy_followers(cls) -> models.QuerySet:
        """Get healthy followers (for replication)."""
        return cls.objects.filter(
            role=ClusterMemberRole.FOLLOWER,
            status=ClusterMemberStatus.ACTIVE,
        )

    @classmethod
    def cleanup_stale_members(cls, max_age_seconds: int = 300) -> int:
        """
        Remove members that haven't sent heartbeats.

        Args:
            max_age_seconds: Maximum age in seconds

        Returns:
            Number of members removed
        """
        cutoff = timezone.now() - timedelta(seconds=max_age_seconds)
        deleted, _ = cls.objects.filter(
            last_heartbeat__lt=cutoff,
            status__in=[
                ClusterMemberStatus.FAILED,
                ClusterMemberStatus.SUSPECTED,
            ],
        ).delete()

        if deleted:
            logger.info(f"Cleaned up {deleted} stale cluster members")

        return deleted

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for protocol messages."""
        return {
            "instance_id": self.instance_id,
            "host": self.host,
            "port": self.port,
            "role": self.role,
            "status": self.status,
            "current_term": self.current_term,
            "last_heartbeat": (
                self.last_heartbeat.isoformat() if self.last_heartbeat else None
            ),
            "heartbeat_latency_ms": self.heartbeat_latency_ms,
            "joined_at": self.joined_at.isoformat() if self.joined_at else None,
        }


# =============================================================================
# DISTRIBUTED LOG ENTRY MODEL (TEMPORA-HA-001 §5)
# =============================================================================


class DistributedLogCommand(models.TextChoices):
    """Commands that can be stored in the distributed log."""
    TASK_CREATED = "TASK_CREATED", "Task Created"
    TASK_ASSIGNED = "TASK_ASSIGNED", "Task Assigned"
    TASK_COMPLETED = "TASK_COMPLETED", "Task Completed"
    TASK_FAILED = "TASK_FAILED", "Task Failed"
    SCHEDULE_ACTIVATED = "SCHEDULE_ACTIVATED", "Schedule Activated"
    SCHEDULE_PAUSED = "SCHEDULE_PAUSED", "Schedule Paused"
    PRIORITY_CHANGED = "PRIORITY_CHANGED", "Priority Changed"
    CONFIG_CHANGED = "CONFIG_CHANGED", "Config Changed"


class DistributedLogEntry(TemporaEntity):
    """
    Entry in the distributed command log for state replication.

    All state changes flow through this log and are replicated to
    followers before commit. Implements Raft log replication.

    Standard: TEMPORA-HA-001 §5 (State Replication)
    Compliance: Raft Consensus Algorithm
    """

    # =========================================================================
    # Identity
    # =========================================================================

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
        help_text="Unique log entry identifier",
    )

    tenant_id = models.UUIDField(
        db_index=True,
        help_text="Tenant isolation identifier",
    )

    # =========================================================================
    # Log Position (Raft)
    # =========================================================================

    index = models.BigIntegerField(
        db_index=True,
        help_text="Sequential position in the log (monotonically increasing)",
    )

    term = models.BigIntegerField(
        help_text="Election term when entry was created",
    )

    # =========================================================================
    # Command
    # =========================================================================

    command = models.CharField(
        max_length=50,
        choices=DistributedLogCommand.choices,
        help_text="Type of state change command",
    )

    data = models.JSONField(
        default=dict,
        help_text="Command payload (task_id, parameters, etc.)",
    )

    # =========================================================================
    # Replication State
    # =========================================================================

    committed = models.BooleanField(
        default=False,
        db_index=True,
        help_text="True when replicated to majority and committed",
    )

    created_by = models.CharField(
        max_length=255,
        help_text="Instance ID that created this entry (always the leader)",
    )

    commit_timestamp = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When entry was committed (replicated to quorum)",
    )

    # =========================================================================
    # Timestamps
    # =========================================================================

    created_at = models.DateTimeField(
        auto_now_add=True,
        help_text="When entry was created",
    )

    class Meta(TemporaEntity.Meta):
        db_table = "sched_distributed_log"
        verbose_name = "Distributed Log Entry"
        verbose_name_plural = "Distributed Log Entries"
        ordering = ["index"]
        indexes = [
            models.Index(
                fields=["tenant_id", "index", "term"],
                name="idx_distlog_tenant_index_term",
            ),
            models.Index(
                fields=["committed", "index"],
                name="idx_distlog_committed_index",
            ),
            models.Index(
                fields=["command"],
                name="idx_distlog_command",
            ),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["tenant_id", "index", "term"],
                name="unique_log_entry",
            ),
            models.CheckConstraint(
                check=models.Q(index__gt=0),
                name="positive_log_index",
            ),
            models.CheckConstraint(
                check=models.Q(term__gte=0),
                name="non_negative_log_term",
            ),
        ]

    def __str__(self) -> str:
        status = "committed" if self.committed else "pending"
        return f"Log[{self.index}:{self.term}] {self.command} [{status}]"

    # =========================================================================
    # Methods
    # =========================================================================

    def mark_committed(self) -> None:
        """Mark entry as committed and set commit_timestamp."""
        self.committed = True
        self.commit_timestamp = timezone.now()
        self.save(update_fields=["committed", "commit_timestamp", "updated_at"])

    def apply_to_state_machine(self) -> None:
        """
        Apply command to scheduler state machine.

        This is called after an entry is committed.
        """
        if not self.committed:
            raise ValueError("Cannot apply uncommitted entry")

        # Apply based on command type
        if self.command == DistributedLogCommand.TASK_CREATED:
            self._apply_task_created()
        elif self.command == DistributedLogCommand.TASK_ASSIGNED:
            self._apply_task_assigned()
        elif self.command == DistributedLogCommand.TASK_COMPLETED:
            self._apply_task_completed()
        elif self.command == DistributedLogCommand.TASK_FAILED:
            self._apply_task_failed()
        # Add other command handlers as needed

        logger.debug(
            f"Applied log entry {self.index} to state machine",
            extra={
                "log_index": self.index,
                "term": self.term,
                "command": self.command,
            },
        )

    def _apply_task_created(self) -> None:
        """Apply TASK_CREATED command."""
        # Task creation is handled by the command itself
        pass

    def _apply_task_assigned(self) -> None:
        """Apply TASK_ASSIGNED command."""
        task_id = self.data.get("task_id")
        target_instance = self.data.get("target_instance")
        if task_id:
            CognitiveTask.objects.filter(id=task_id).update(
                state=TaskState.SCHEDULED.value,
                scheduled_at=timezone.now(),
            )

    def _apply_task_completed(self) -> None:
        """Apply TASK_COMPLETED command."""
        task_id = self.data.get("task_id")
        result = self.data.get("result")
        if task_id:
            CognitiveTask.objects.filter(id=task_id).update(
                state=TaskState.COMPLETED.value,
                completed_at=timezone.now(),
                result=result,
            )

    def _apply_task_failed(self) -> None:
        """Apply TASK_FAILED command."""
        task_id = self.data.get("task_id")
        error_message = self.data.get("error_message")
        error_type = self.data.get("error_type")
        if task_id:
            CognitiveTask.objects.filter(id=task_id).update(
                state=TaskState.FAILED.value,
                completed_at=timezone.now(),
                error_message=error_message,
                error_type=error_type,
            )

    # =========================================================================
    # Class Methods
    # =========================================================================

    @classmethod
    def get_next_index(cls, tenant_id: uuid.UUID) -> int:
        """Get the next available log index for a tenant."""
        last_entry = cls.objects.filter(
            tenant_id=tenant_id
        ).order_by("-index").first()

        return (last_entry.index + 1) if last_entry else 1

    @classmethod
    def get_uncommitted(cls, tenant_id: uuid.UUID) -> models.QuerySet:
        """Get uncommitted log entries for a tenant."""
        return cls.objects.filter(
            tenant_id=tenant_id,
            committed=False,
        ).order_by("index")

    @classmethod
    def get_entries_after(
        cls,
        tenant_id: uuid.UUID,
        after_index: int,
        limit: int = 100,
    ) -> models.QuerySet:
        """Get log entries after a specific index."""
        return cls.objects.filter(
            tenant_id=tenant_id,
            index__gt=after_index,
        ).order_by("index")[:limit]

    @classmethod
    def append(
        cls,
        tenant_id: uuid.UUID,
        term: int,
        command: str,
        data: Dict[str, Any],
        created_by: str,
    ) -> "DistributedLogEntry":
        """
        Append a new entry to the log.

        Only the leader should call this method.
        """
        with transaction.atomic():
            index = cls.get_next_index(tenant_id)
            entry = cls.objects.create(
                tenant_id=tenant_id,
                index=index,
                term=term,
                command=command,
                data=data,
                created_by=created_by,
            )
            return entry

    @classmethod
    def commit_up_to(cls, tenant_id: uuid.UUID, commit_index: int) -> int:
        """
        Commit all entries up to and including commit_index.

        Returns number of entries committed.
        """
        return cls.objects.filter(
            tenant_id=tenant_id,
            index__lte=commit_index,
            committed=False,
        ).update(
            committed=True,
            commit_timestamp=timezone.now(),
        )


# =============================================================================
# FENCING TOKEN MODEL (TEMPORA-HA-001 §7)
# =============================================================================


class FencingToken(TemporaEntity):
    """
    Monotonically increasing token for split-brain prevention.

    Fencing tokens prevent stale leaders from corrupting state
    after partition healing.

    Standard: TEMPORA-HA-001 §7 (Split-Brain Prevention)
    """

    # =========================================================================
    # Identity
    # =========================================================================

    id = models.UUIDField(
        primary_key=True,
        default=uuid.uuid4,
        editable=False,
        help_text="Unique token record identifier",
    )

    tenant_id = models.UUIDField(
        db_index=True,
        help_text="Tenant isolation identifier",
    )

    # =========================================================================
    # Token Value
    # =========================================================================

    token = models.BigIntegerField(
        db_index=True,
        help_text="Monotonically increasing token value",
    )

    issued_to = models.CharField(
        max_length=255,
        help_text="Instance ID that holds this token",
    )

    term = models.BigIntegerField(
        help_text="Election term when token was issued",
    )

    # =========================================================================
    # State
    # =========================================================================

    is_current = models.BooleanField(
        default=True,
        db_index=True,
        help_text="Whether this is the current valid token",
    )

    issued_at = models.DateTimeField(
        auto_now_add=True,
        help_text="When token was issued",
    )

    class Meta(TemporaEntity.Meta):
        db_table = "sched_fencing_token"
        verbose_name = "Fencing Token"
        verbose_name_plural = "Fencing Tokens"
        ordering = ["-token"]
        indexes = [
            models.Index(
                fields=["tenant_id", "is_current"],
                name="idx_fencing_tenant_current",
            ),
        ]
        constraints = [
            models.CheckConstraint(
                check=models.Q(token__gt=0),
                name="positive_fencing_token",
            ),
        ]

    def __str__(self) -> str:
        status = "current" if self.is_current else "expired"
        return f"Token[{self.token}] issued to {self.issued_to} [{status}]"

    # =========================================================================
    # Class Methods
    # =========================================================================

    @classmethod
    def issue_new_token(
        cls,
        tenant_id: uuid.UUID,
        instance_id: str,
        term: int,
    ) -> "FencingToken":
        """
        Issue a new fencing token, invalidating all previous tokens.

        This should only be called when a new leader is elected.
        """
        with transaction.atomic():
            # Invalidate previous tokens
            cls.objects.filter(
                tenant_id=tenant_id,
                is_current=True,
            ).update(is_current=False)

            # Get next token value
            last_token = cls.objects.filter(
                tenant_id=tenant_id,
            ).order_by("-token").first()

            new_token_value = (last_token.token + 1) if last_token else 1

            # Create new token
            token = cls.objects.create(
                tenant_id=tenant_id,
                token=new_token_value,
                issued_to=instance_id,
                term=term,
                is_current=True,
            )

            logger.info(
                f"Issued fencing token {new_token_value} to {instance_id}",
                extra={
                    "token": new_token_value,
                    "instance_id": instance_id,
                    "term": term,
                },
            )

            return token

    @classmethod
    def validate(cls, tenant_id: uuid.UUID, token: int) -> bool:
        """
        Check if a token is the current valid token.

        Returns True if valid, False if stale.
        """
        return cls.objects.filter(
            tenant_id=tenant_id,
            token=token,
            is_current=True,
        ).exists()

    @classmethod
    def get_current(cls, tenant_id: uuid.UUID) -> Optional["FencingToken"]:
        """Get the current valid fencing token."""
        return cls.objects.filter(
            tenant_id=tenant_id,
            is_current=True,
        ).first()

    @classmethod
    def get_current_value(cls, tenant_id: uuid.UUID) -> Optional[int]:
        """Get the current fencing token value."""
        token = cls.get_current(tenant_id)
        return token.token if token else None
