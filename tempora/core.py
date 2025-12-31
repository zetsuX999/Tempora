"""
Tempora Scheduler Core
======================

Core scheduler engine implementing intelligent task scheduling,
priority queuing, and cognitive execution decisions.

Key Features:
- Priority-based scheduling with cognitive scoring
- Cascade throttling and budget enforcement
- Circuit breaker pattern for external services
- Dead Letter Queue (DLQ) for failed tasks
- Tenant isolation and quota enforcement
- Native Django/async support
- Distributed coordination with Raft consensus
"""

from __future__ import annotations

import asyncio
import logging
import signal
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Type

from django.db import models, transaction
from django.utils import timezone

from tempora.events import (
    SCHEDULER_EVENTS,
    build_cascade_child_payload,
    build_dlq_enqueued_payload,
    build_quota_exceeded_payload,
    build_retry_scheduled_payload,
    build_schedule_triggered_payload,
    build_task_completed_payload,
    build_task_created_payload,
    build_task_failed_payload,
    build_task_started_payload,
    emit_scheduler_event,
)
from tempora.exceptions import (
    CascadeLimitError,
    CircuitOpenError,
    QuotaExceededError,
    ThrottledError,
)
from tempora.models import (
    CircuitBreakerState,
    CognitiveTask,
    DeadLetterEntry,
    Schedule,
    TaskExecution,
)
from tempora.types import (
    CASCADE_BUDGET,
    CIRCUIT_CONFIGS,
    QUEUE_CONFIG,
    TENANT_QUOTAS,
    CircuitBreakerConfig,
    CircuitState,
    QueueType,
    RetryStrategy,
    TaskContext,
    TaskPriority,
    TaskState,
    TenantQuota,
    get_cascade_limit,
)

# Backpressure integration (SCH-004)
from tempora.backpressure import (
    BackpressureController,
    BackpressureConfig,
    ThrottleLevel,
    HealthStatus,
)

# Temporal reflex integration (SCH-006)
from tempora.temporal import (
    TemporalController,
    TemporalControllerConfig,
    TemporalPolicy,
    TemporalReflex,
    CompensatingTask,
)

logger = logging.getLogger(__name__)


# =============================================================================
# TASK HANDLER REGISTRY
# =============================================================================


class TaskRegistry:
    """
    Registry for task handlers per SCH-001 §handler_registration.

    Maps task names to callable handlers for execution.
    """

    _handlers: Dict[str, Callable] = {}
    _handler_metadata: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def register(
        cls,
        task_name: str,
        handler: Callable,
        queue: QueueType = QueueType.CORE,
        priority: TaskPriority = TaskPriority.NORMAL,
        timeout_seconds: int = 60,
        retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
        max_attempts: int = 3,
        circuit_breaker: Optional[str] = None,
    ) -> None:
        """
        Register a task handler.

        Args:
            task_name: Unique task identifier
            handler: Callable to execute
            queue: Default queue for this task type
            priority: Default priority
            timeout_seconds: Execution timeout
            retry_strategy: Retry behavior
            max_attempts: Max retry attempts
            circuit_breaker: Service name for circuit breaker (optional)
        """
        cls._handlers[task_name] = handler
        cls._handler_metadata[task_name] = {
            "queue": queue,
            "priority": priority,
            "timeout_seconds": timeout_seconds,
            "retry_strategy": retry_strategy,
            "max_attempts": max_attempts,
            "circuit_breaker": circuit_breaker,
        }

        logger.info(
            f"Registered task handler: {task_name}",
            extra={"task_name": task_name, "queue": queue.value},
        )

    @classmethod
    def get_handler(cls, task_name: str) -> Optional[Callable]:
        """Get handler for task name."""
        return cls._handlers.get(task_name)

    @classmethod
    def get_metadata(cls, task_name: str) -> Dict[str, Any]:
        """Get metadata for task name."""
        return cls._handler_metadata.get(task_name, {})

    @classmethod
    def list_handlers(cls) -> List[str]:
        """List all registered task names."""
        return list(cls._handlers.keys())


def task(
    task_name: str,
    queue: QueueType = QueueType.CORE,
    priority: TaskPriority = TaskPriority.NORMAL,
    timeout_seconds: int = 60,
    retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL,
    max_attempts: int = 3,
    circuit_breaker: Optional[str] = None,
):
    """
    Decorator to register a function as a cognitive task handler.

    Usage:
        @task("myapp.process_order", queue=QueueType.CORE)
        def process_order(order_id: str, context: TaskContext) -> Dict:
            # Process the order
            return {"status": "processed"}

    Standard: SCH-001 §handler_registration
    """
    def decorator(func: Callable) -> Callable:
        TaskRegistry.register(
            task_name=task_name,
            handler=func,
            queue=queue,
            priority=priority,
            timeout_seconds=timeout_seconds,
            retry_strategy=retry_strategy,
            max_attempts=max_attempts,
            circuit_breaker=circuit_breaker,
        )
        return func

    return decorator


# =============================================================================
# COGNITIVE SCHEDULER (SCH-001 §scheduler_core)
# =============================================================================


class CognitiveScheduler:
    """
    Main scheduler engine implementing cognitive task scheduling per SCH-001.

    The CognitiveScheduler orchestrates:
    - Task creation and queuing
    - Priority-based scheduling with cognitive scoring
    - Cascade management and throttling
    - Schedule management (cron/interval/one-time)
    - Tenant quota enforcement

    Standard: SCH-001 §scheduler_core
    Compliance: ISO 9001:2015 §9.5
    """

    def __init__(
        self,
        worker_count: int = 4,
        queues: Optional[List[QueueType]] = None,
        backpressure_config: Optional[BackpressureConfig] = None,
        enable_backpressure: bool = True,
        temporal_config: Optional[TemporalControllerConfig] = None,
        enable_temporal: bool = True,
    ):
        """
        Initialize the cognitive scheduler.

        Args:
            worker_count: Number of worker threads
            queues: Queues to process (default: all)
            backpressure_config: Configuration for backpressure controller
            enable_backpressure: Enable backpressure system (SCH-004)
            temporal_config: Configuration for temporal controller
            enable_temporal: Enable temporal reflex system (SCH-006)
        """
        self.worker_count = worker_count
        self.queues = queues or list(QueueType)
        self._running = False
        self._shutdown_event = threading.Event()
        self._workers: List[CognitiveWorker] = []
        self._executor: Optional[ThreadPoolExecutor] = None
        self._schedule_thread: Optional[threading.Thread] = None

        # Backpressure controller (SCH-004)
        self._enable_backpressure = enable_backpressure
        self._backpressure: Optional[BackpressureController] = None
        if enable_backpressure:
            bp_config = backpressure_config or BackpressureConfig(
                on_level_change=self._on_throttle_level_change,
                on_emergency=self._on_emergency_triggered,
            )
            self._backpressure = BackpressureController(config=bp_config)

        # Temporal controller (SCH-006)
        self._enable_temporal = enable_temporal
        self._temporal: Optional[TemporalController] = None
        if enable_temporal:
            t_config = temporal_config or TemporalControllerConfig(
                task_submitter=self._submit_compensating_task,
                context_provider=self._provide_scheduler_context,
            )
            self._temporal = TemporalController(config=t_config)

            # Wire up backpressure to temporal controller
            if self._backpressure:
                self._temporal.set_backpressure(self._backpressure)

        logger.info(
            f"CognitiveScheduler initialized",
            extra={
                "worker_count": worker_count,
                "queues": [q.value for q in self.queues],
                "backpressure_enabled": enable_backpressure,
                "temporal_enabled": enable_temporal,
            },
        )

    # =========================================================================
    # Task Submission (SCH-001 §task_submission)
    # =========================================================================

    def submit(
        self,
        task_name: str,
        payload: Dict[str, Any],
        tenant_id: uuid.UUID,
        priority: Optional[TaskPriority] = None,
        queue: Optional[QueueType] = None,
        correlation_id: Optional[uuid.UUID] = None,
        parent_task: Optional[CognitiveTask] = None,
        urgency: float = 0.5,
        confidence_score: float = 1.0,
        governance_risk: float = 0.0,
        deadline: Optional[datetime] = None,
        delay_seconds: int = 0,
    ) -> CognitiveTask:
        """
        Submit a task for execution.

        Args:
            task_name: Registered task handler name
            payload: Task arguments
            tenant_id: Tenant for isolation
            priority: Task priority (uses handler default if not specified)
            queue: Target queue (uses handler default if not specified)
            correlation_id: Correlation ID for tracing
            parent_task: Parent task for cascade lineage
            urgency: Task urgency (0.0-1.0)
            confidence_score: Confidence in task necessity (0.0-1.0)
            governance_risk: Governance risk score (0.0-1.0)
            deadline: Hard deadline for completion
            delay_seconds: Delay before scheduling

        Returns:
            Created CognitiveTask

        Raises:
            ValueError: If task_name not registered or cascade limit exceeded
            QuotaExceededError: If tenant quota exceeded

        Standard: SCH-001 §task_submission
        """
        # Get handler metadata for defaults
        metadata = TaskRegistry.get_metadata(task_name)
        if not metadata:
            logger.warning(f"Unregistered task submitted: {task_name}")

        # Apply backpressure checks (SCH-004 §4.3)
        if self._backpressure:
            is_batch = queue == QueueType.BATCH if queue else metadata.get("queue") == QueueType.BATCH
            decision = self._backpressure.should_schedule(
                task_name=task_name,
                priority=priority.value if priority else metadata.get("priority", TaskPriority.NORMAL).value,
                is_batch=is_batch,
            )

            if not decision.allow:
                logger.info(
                    f"Task submission throttled: {task_name}",
                    extra={
                        "task_name": task_name,
                        "throttle_level": decision.level.name,
                        "reasons": decision.reasons,
                    },
                )
                raise ThrottledError(
                    f"Task {task_name} throttled: {', '.join(decision.reasons)}",
                    throttle_level=decision.level,
                    retry_after_seconds=decision.schedule_delay_seconds,
                )

            # Apply confidence penalty to governance risk if degradation enabled
            if decision.confidence_penalty > 0 and confidence_score > 0:
                confidence_score = self._backpressure.adjust_governance_confidence(confidence_score)

        # Apply defaults from handler registration
        priority = priority or metadata.get("priority", TaskPriority.NORMAL)
        queue = queue or metadata.get("queue", QueueType.CORE)
        timeout_seconds = metadata.get("timeout_seconds", 60)
        retry_strategy = metadata.get("retry_strategy", RetryStrategy.EXPONENTIAL)
        max_attempts = metadata.get("max_attempts", 3)

        # Apply temporal adjustments (SCH-006 §5.1)
        if self._temporal:
            # Check tenant pause
            if self._temporal.is_tenant_paused(str(tenant_id)):
                raise ThrottledError(
                    f"Tenant {tenant_id} is temporarily paused by temporal policy",
                    retry_after_seconds=60,
                )

            # Apply priority adjustment
            priority_adjustment = self._temporal.get_priority_adjustment(task_name)
            if priority_adjustment != 0:
                adjusted_value = priority.value + priority_adjustment
                adjusted_value = max(0, min(4, adjusted_value))  # Clamp to valid range
                priority = TaskPriority(adjusted_value)
                logger.debug(
                    f"[TEMPORAL] Priority adjusted for {task_name}: {priority_adjustment}",
                    extra={"original": priority.value - priority_adjustment, "adjusted": priority.value},
                )

            # Apply TTL multiplier
            ttl_multiplier = self._temporal.get_ttl_multiplier(task_name)
            if ttl_multiplier != 1.0:
                timeout_seconds = int(timeout_seconds * ttl_multiplier)
                logger.debug(
                    f"[TEMPORAL] TTL adjusted for {task_name}: {ttl_multiplier}x",
                    extra={"timeout_seconds": timeout_seconds},
                )

        # Check cascade limits
        cascade_depth = 0
        if parent_task:
            cascade_depth = parent_task.cascade_depth + 1
            limit = get_cascade_limit(cascade_depth)
            if limit == 0:
                emit_scheduler_event(
                    "scheduler.cascade.depth_exceeded",
                    {
                        "parent_task_id": str(parent_task.id),
                        "attempted_depth": cascade_depth,
                        "max_depth": CASCADE_BUDGET["max_depth"],
                        "root_correlation_id": str(parent_task.root_correlation_id),
                        "tenant_id": str(tenant_id),
                    },
                )
                raise ValueError(
                    f"Cascade depth {cascade_depth} exceeds maximum {CASCADE_BUDGET['max_depth']}"
                )

        # Check tenant quotas
        self._check_tenant_quota(tenant_id, "queue_depth")

        # Create the task
        task = CognitiveTask.create_task(
            tenant_id=tenant_id,
            task_name=task_name,
            payload=payload,
            priority=priority,
            queue=queue,
            correlation_id=correlation_id,
            parent_task=parent_task,
            urgency=urgency,
            confidence_score=confidence_score,
            governance_risk=governance_risk,
            deadline=deadline,
            timeout_seconds=timeout_seconds,
            retry_strategy=retry_strategy,
            max_attempts=max_attempts,
        )

        # Apply delay if specified
        if delay_seconds > 0:
            task.scheduled_at = timezone.now() + timedelta(seconds=delay_seconds)
            task.save()

        # Emit creation event
        emit_scheduler_event(
            "scheduler.task.created",
            build_task_created_payload(task),
            correlation_id=str(task.correlation_id),
            tenant_id=str(tenant_id),
        )

        # Emit cascade event if child task
        if parent_task:
            emit_scheduler_event(
                "scheduler.cascade.child_created",
                build_cascade_child_payload(parent_task, task),
                correlation_id=str(task.correlation_id),
                tenant_id=str(tenant_id),
            )

        logger.info(
            f"Task submitted: {task.task_name}",
            extra={
                "task_id": str(task.id),
                "correlation_id": str(task.correlation_id),
                "priority": priority.value,
                "queue": queue.value,
            },
        )

        return task

    def submit_batch(
        self,
        tasks: List[Dict[str, Any]],
        tenant_id: uuid.UUID,
        correlation_id: Optional[uuid.UUID] = None,
    ) -> List[CognitiveTask]:
        """
        Submit multiple tasks as a batch.

        Args:
            tasks: List of task specifications (task_name, payload, etc.)
            tenant_id: Tenant for isolation
            correlation_id: Shared correlation ID

        Returns:
            List of created tasks

        Standard: SCH-001 §batch_submission
        """
        correlation_id = correlation_id or uuid.uuid4()
        created_tasks = []

        with transaction.atomic():
            for task_spec in tasks:
                task = self.submit(
                    tenant_id=tenant_id,
                    correlation_id=correlation_id,
                    **task_spec,
                )
                created_tasks.append(task)

        logger.info(
            f"Batch submitted: {len(created_tasks)} tasks",
            extra={
                "correlation_id": str(correlation_id),
                "tenant_id": str(tenant_id),
            },
        )

        return created_tasks

    # =========================================================================
    # Quota Enforcement (SCH-002 §resource_quotas)
    # =========================================================================

    def _check_tenant_quota(
        self,
        tenant_id: uuid.UUID,
        quota_type: str,
    ) -> bool:
        """
        Check and enforce tenant quota.

        Args:
            tenant_id: Tenant to check quota for
            quota_type: Type of quota ("queue_depth" or "concurrent_tasks")

        Returns:
            True if within quota, False if quota exceeded

        Raises:
            QuotaExceededError: For hard quota limits (queue_depth)

        Note: queue_depth raises exception (hard limit on submission)
              concurrent_tasks returns False (soft limit on execution)
        """
        # Get tenant quota configuration
        quota = self._get_tenant_quota(tenant_id)

        if quota_type == "queue_depth":
            # Hard limit - reject task submission
            current = CognitiveTask.objects.filter(
                tenant_id=tenant_id,
                state__in=[TaskState.PENDING.value, TaskState.SCHEDULED.value],
            ).count()

            if current >= quota.max_queue_depth:
                emit_scheduler_event(
                    "scheduler.quota.exceeded",
                    build_quota_exceeded_payload(
                        str(tenant_id),
                        "queue_depth",
                        current,
                        quota.max_queue_depth,
                        "rejected",
                    ),
                )
                raise QuotaExceededError(
                    f"Queue depth quota exceeded: {current}/{quota.max_queue_depth}"
                )
            return True

        elif quota_type == "concurrent_tasks":
            # Soft limit - don't execute task now, but keep it in queue
            current = CognitiveTask.objects.filter(
                tenant_id=tenant_id,
                state=TaskState.RUNNING.value,
            ).count()

            if current >= quota.max_concurrent_tasks:
                # Emit warning event
                emit_scheduler_event(
                    "scheduler.quota.warning",
                    {
                        "tenant_id": str(tenant_id),
                        "quota_type": "concurrent_tasks",
                        "current_value": current,
                        "limit": quota.max_concurrent_tasks,
                        "percent_used": 1.0,
                    },
                )
                return False  # Don't execute, task stays in queue

            return True

        # Unknown quota type
        logger.warning(f"Unknown quota type: {quota_type}")
        return True

    def _get_tenant_quota(self, tenant_id: uuid.UUID) -> TenantQuota:
        """Get quota configuration for tenant."""
        # In production, this would query tenant configuration
        # For now, return default quota
        return TENANT_QUOTAS.get("default", TenantQuota())

    # =========================================================================
    # Task Fetching (SCH-001 §task_selection)
    # =========================================================================

    def fetch_next_task(
        self,
        queues: List[QueueType],
        worker_id: str,
    ) -> Optional[CognitiveTask]:
        """
        Fetch next task to execute based on priority scoring.

        Tasks are selected by:
        1. Queue priority
        2. Priority score (cognitive scoring)
        3. Creation time (FIFO within same score)

        Standard: SCH-001 §task_selection

        Fixed: Race condition in quota check - now checks quota BEFORE selecting task
        """
        now = timezone.now()

        with transaction.atomic():
            # Find tasks ready for execution
            task = (
                CognitiveTask.objects.select_for_update(skip_locked=True)
                .filter(
                    queue__in=[q.value for q in queues],
                    state__in=[TaskState.PENDING.value, TaskState.RETRYING.value],
                )
                .filter(
                    # Either not scheduled or scheduled time has passed
                    models.Q(scheduled_at__isnull=True)
                    | models.Q(scheduled_at__lte=now)
                )
                .filter(
                    # For retrying tasks, check next_retry_at
                    models.Q(state=TaskState.PENDING.value)
                    | models.Q(
                        state=TaskState.RETRYING.value,
                        next_retry_at__lte=now,
                    )
                )
                .order_by("-priority_score", "created_at")
                .first()
            )

            if task:
                # Check tenant concurrent task quota
                # NOTE: If quota exceeded, task stays in PENDING state
                # and will be picked up later when quota is available
                if not self._check_tenant_quota(task.tenant_id, "concurrent_tasks"):
                    # Release the lock and let another task be selected
                    # The task remains in PENDING/RETRYING state
                    logger.debug(
                        f"Tenant {task.tenant_id} quota exceeded, skipping task {task.id}"
                    )
                    return None

                # Transition to scheduled
                task.transition_to(TaskState.SCHEDULED)

        return task

    # =========================================================================
    # Schedule Management (SCH-001 §temporal_patterns)
    # =========================================================================

    def process_schedules(self) -> int:
        """
        Process due schedules and create tasks.

        Returns number of tasks created.

        Standard: SCH-001 §temporal_patterns
        Compliance: SYS-200 INV-011 (transaction.atomic for batch integrity)
        """
        now = timezone.now()
        tasks_created = 0

        # Check backpressure before processing schedules (SCH-004 §4.4)
        if self._backpressure:
            decision = self._backpressure.should_trigger_schedule("batch")
            if decision.pause_schedules:
                logger.info(
                    "Schedule processing paused due to backpressure",
                    extra={
                        "throttle_level": decision.level.name,
                        "reasons": decision.reasons,
                    },
                )
                return 0

        # SYS-200 INV-011: Wrap select_for_update in transaction for row-level locking
        with transaction.atomic():
            # Find due schedules
            due_schedules = list(Schedule.objects.filter(
                enabled=True,
                next_run_at__lte=now,
            ).select_for_update(skip_locked=True))

            for schedule in due_schedules:
                try:
                    # Check temporal pause (SCH-006 §5.2)
                    if self._temporal and self._temporal.is_schedule_paused(schedule.schedule_id):
                        logger.info(
                            f"Schedule {schedule.schedule_id} paused by temporal policy",
                            extra={"schedule_id": schedule.schedule_id},
                        )
                        # Reschedule for later
                        schedule.next_run_at = timezone.now() + timedelta(minutes=5)
                        schedule.save()
                        continue

                    # Apply temporal interval multiplier (SCH-006 §5.2)
                    if self._temporal:
                        multiplier = self._temporal.get_schedule_interval_multiplier(schedule.schedule_id)
                        if multiplier != 1.0:
                            logger.debug(
                                f"[TEMPORAL] Schedule {schedule.schedule_id} interval multiplier: {multiplier}",
                                extra={"schedule_id": schedule.schedule_id, "multiplier": multiplier},
                            )

                    # Create task from schedule
                    task = self.submit(
                        task_name=schedule.task_name,
                        payload=schedule.payload_template,
                        tenant_id=schedule.tenant_id,
                        priority=TaskPriority(schedule.priority),
                        queue=QueueType(schedule.queue),
                    )

                    # Update schedule
                    schedule.record_run()

                    # Emit event
                    emit_scheduler_event(
                        "scheduler.schedule.triggered",
                        build_schedule_triggered_payload(schedule, task),
                        tenant_id=str(schedule.tenant_id),
                    )

                    tasks_created += 1

                    logger.info(
                        f"Schedule triggered: {schedule.schedule_id}",
                        extra={
                            "schedule_id": schedule.schedule_id,
                            "task_id": str(task.id),
                            "run_count": schedule.run_count,
                        },
                    )

                except Exception as e:
                    logger.error(
                        f"Failed to trigger schedule {schedule.schedule_id}: {e}",
                        extra={"schedule_id": schedule.schedule_id},
                    )

        return tasks_created

    # =========================================================================
    # Scheduler Lifecycle
    # =========================================================================

    def start(self) -> None:
        """
        Start the scheduler and workers.

        Standard: SCH-001 §scheduler_lifecycle
        """
        if self._running:
            logger.warning("Scheduler already running")
            return

        self._running = True
        self._shutdown_event.clear()

        # Start backpressure controller (SCH-004)
        if self._backpressure:
            self._backpressure.start()

        # Start temporal controller (SCH-006)
        if self._temporal:
            self._temporal.start()

        # Create thread pool for workers
        self._executor = ThreadPoolExecutor(max_workers=self.worker_count)

        # Start workers
        for i in range(self.worker_count):
            worker = CognitiveWorker(
                worker_id=f"worker-{i}",
                scheduler=self,
                queues=self.queues,
            )
            self._workers.append(worker)
            self._executor.submit(worker.run)

        # Start schedule processor thread
        self._schedule_thread = threading.Thread(
            target=self._schedule_loop,
            daemon=True,
            name="schedule-processor",
        )
        self._schedule_thread.start()

        # Emit started event
        emit_scheduler_event(
            "scheduler.started",
            {
                "version": "1.0.0",
                "worker_count": self.worker_count,
                "queues": [q.value for q in self.queues],
            },
        )

        logger.info(
            f"Scheduler started with {self.worker_count} workers",
            extra={
                "worker_count": self.worker_count,
                "queues": [q.value for q in self.queues],
            },
        )

    def stop(self, graceful: bool = True, timeout: int = 30) -> None:
        """
        Stop the scheduler and workers.

        Args:
            graceful: Wait for running tasks to complete
            timeout: Maximum wait time in seconds

        Standard: SCH-001 §scheduler_lifecycle
        """
        if not self._running:
            logger.warning("Scheduler not running")
            return

        logger.info(f"Stopping scheduler (graceful={graceful})")

        self._running = False
        self._shutdown_event.set()

        # Stop workers
        for worker in self._workers:
            worker.stop()

        # Wait for executor shutdown
        if self._executor:
            self._executor.shutdown(wait=graceful)

        # Stop backpressure controller (SCH-004)
        if self._backpressure:
            self._backpressure.stop()

        # Stop temporal controller (SCH-006)
        if self._temporal:
            self._temporal.stop()

        # Count pending tasks
        pending_count = CognitiveTask.objects.filter(
            state__in=[TaskState.PENDING.value, TaskState.SCHEDULED.value]
        ).count()

        # Emit stopped event
        emit_scheduler_event(
            "scheduler.stopped",
            {
                "reason": "requested",
                "graceful": graceful,
                "pending_tasks": pending_count,
            },
        )

        logger.info("Scheduler stopped")

    def _schedule_loop(self) -> None:
        """Background loop for processing schedules."""
        while self._running:
            try:
                self.process_schedules()
            except Exception as e:
                logger.error(f"Schedule processing error: {e}")

            # Wait before next check
            self._shutdown_event.wait(timeout=10)

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running

    @property
    def backpressure(self) -> Optional[BackpressureController]:
        """Get the backpressure controller."""
        return self._backpressure

    # =========================================================================
    # Backpressure Callbacks (SCH-004 §4.5)
    # =========================================================================

    def _on_throttle_level_change(
        self,
        old_level: ThrottleLevel,
        new_level: ThrottleLevel,
    ) -> None:
        """Handle throttle level changes."""
        emit_scheduler_event(
            "scheduler.backpressure.level_changed",
            {
                "old_level": old_level.name,
                "new_level": new_level.name,
                "old_value": old_level.value,
                "new_value": new_level.value,
                "timestamp": timezone.now().isoformat(),
            },
        )

        # Log escalation or de-escalation
        if new_level.value > old_level.value:
            logger.warning(
                f"Backpressure escalated: {old_level.name} → {new_level.name}",
                extra={
                    "old_level": old_level.name,
                    "new_level": new_level.name,
                },
            )
        else:
            logger.info(
                f"Backpressure reduced: {old_level.name} → {new_level.name}",
                extra={
                    "old_level": old_level.name,
                    "new_level": new_level.name,
                },
            )

    def _on_emergency_triggered(self, reason: str) -> None:
        """Handle emergency backpressure situations."""
        emit_scheduler_event(
            "scheduler.backpressure.emergency",
            {
                "reason": reason,
                "timestamp": timezone.now().isoformat(),
            },
        )

        logger.critical(
            f"BACKPRESSURE EMERGENCY: {reason}",
            extra={"reason": reason},
        )

        # In emergency, pause all schedules
        # Workers will still complete running tasks

    # =========================================================================
    # Temporal Controller Callbacks (SCH-006 §5.3)
    # =========================================================================

    def _submit_compensating_task(self, comp_task: CompensatingTask) -> CognitiveTask:
        """
        Submit a compensating task from temporal reflex.

        Standard: SCH-006 §5.3
        """
        # Get a default tenant_id for compensating tasks
        # In production, this would come from a system tenant or the triggering context
        from django.conf import settings
        system_tenant_id = getattr(settings, 'SYNARA_SYSTEM_TENANT_ID', uuid.UUID('00000000-0000-0000-0000-000000000000'))

        return self.submit(
            task_name=comp_task.task_name,
            payload=comp_task.payload,
            tenant_id=system_tenant_id,
            priority=TaskPriority(comp_task.priority),
            queue=QueueType(comp_task.queue),
            delay_seconds=comp_task.delay_seconds,
        )

    def _provide_scheduler_context(self) -> Dict[str, Any]:
        """
        Provide scheduler context for temporal policy evaluation.

        Standard: SCH-006 §5.3
        """
        # Get schedule names
        schedule_names = list(
            Schedule.objects.filter(enabled=True).values_list('schedule_id', flat=True)
        )

        # Get task counts
        pending = CognitiveTask.objects.filter(
            state__in=[TaskState.PENDING.value, TaskState.SCHEDULED.value]
        ).count()

        running = CognitiveTask.objects.filter(
            state=TaskState.RUNNING.value
        ).count()

        # Get max cascade depth
        max_cascade = CognitiveTask.objects.filter(
            state__in=[TaskState.PENDING.value, TaskState.RUNNING.value]
        ).order_by('-cascade_depth').values_list('cascade_depth', flat=True).first() or 0

        return {
            "schedules": schedule_names,
            "available_schedules": schedule_names,
            "pending_tasks": pending,
            "running_tasks": running,
            "max_cascade_depth": max_cascade,
            "cascade_budget_remaining": 1.0 - (max_cascade / CASCADE_BUDGET["max_depth"]) if CASCADE_BUDGET["max_depth"] > 0 else 1.0,
        }

    @property
    def temporal(self) -> Optional[TemporalController]:
        """Get the temporal controller."""
        return self._temporal


# =============================================================================
# COGNITIVE WORKER (SCH-001 §worker_pool)
# =============================================================================


class CognitiveWorker:
    """
    Worker thread for executing cognitive tasks per SCH-001 §worker_pool.

    Each worker:
    - Fetches tasks from assigned queues
    - Executes task handlers with timeout
    - Records execution history
    - Handles retries and DLQ placement
    - Respects circuit breakers

    Standard: SCH-001 §worker_pool
    """

    def __init__(
        self,
        worker_id: str,
        scheduler: CognitiveScheduler,
        queues: List[QueueType],
    ):
        self.worker_id = worker_id
        self.scheduler = scheduler
        self.queues = queues
        self._running = False
        self._current_task: Optional[CognitiveTask] = None

        # Metrics
        self.completed_tasks = 0
        self.failed_tasks = 0
        self.started_at: Optional[datetime] = None

    def run(self) -> None:
        """Main worker loop."""
        self._running = True
        self.started_at = timezone.now()

        emit_scheduler_event(
            "scheduler.worker.started",
            {
                "worker_id": self.worker_id,
                "queues": [q.value for q in self.queues],
                "concurrency": 1,
            },
        )

        logger.info(f"Worker {self.worker_id} started")

        while self._running:
            try:
                task = self.scheduler.fetch_next_task(self.queues, self.worker_id)

                if task:
                    self._execute_task(task)
                else:
                    # No task available, wait before polling again
                    time.sleep(0.5)

            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {e}")
                time.sleep(1)

        # Emit stopped event
        uptime = (timezone.now() - self.started_at).total_seconds() if self.started_at else 0
        emit_scheduler_event(
            "scheduler.worker.stopped",
            {
                "worker_id": self.worker_id,
                "reason": "shutdown",
                "tasks_completed": self.completed_tasks,
                "uptime_seconds": int(uptime),
            },
        )

        logger.info(f"Worker {self.worker_id} stopped")

    def stop(self) -> None:
        """Stop the worker."""
        self._running = False

    def _execute_task(self, task: CognitiveTask) -> None:
        """
        Execute a single task.

        Standard: SCH-001 §task_execution

        Fixed: Added comprehensive exception handling to ensure task state
               is always updated, even on critical failures.
        """
        self._current_task = task
        circuit_breaker_service = None
        execution = None

        try:
            handler = TaskRegistry.get_handler(task.task_name)
            metadata = TaskRegistry.get_metadata(task.task_name)

            # Check circuit breaker if configured
            circuit_breaker_service = metadata.get("circuit_breaker")
            if circuit_breaker_service:
                circuit = CircuitBreakerState.get_or_create_for_service(
                    circuit_breaker_service,
                    CIRCUIT_CONFIGS.get(circuit_breaker_service),
                )
                can_execute, reason = circuit.can_execute()
                if not can_execute:
                    emit_scheduler_event(
                        "scheduler.circuit.rejected",
                        {
                            "service_name": circuit_breaker_service,
                            "circuit_state": circuit.state,
                            "task_id": str(task.id),
                            "correlation_id": str(task.correlation_id),
                        },
                    )
                    # Reschedule for later
                    task.next_retry_at = timezone.now() + timedelta(seconds=30)
                    task.state = TaskState.PENDING.value
                    task.save()
                    return

            # Transition to running
            task.transition_to(TaskState.RUNNING)
            task.attempts += 1
            task.save()

            # Create execution record
            execution = TaskExecution.objects.create(
                task=task,
                attempt_number=task.attempts,
                worker_id=self.worker_id,
            )

            # Emit started event
            emit_scheduler_event(
                "scheduler.task.started",
                build_task_started_payload(task, self.worker_id, task.attempts),
                correlation_id=str(task.correlation_id),
                tenant_id=str(task.tenant_id),
            )

            start_time = time.time()
            success = False
            result = None
            error_message = None
            error_type = None
            error_traceback = None

            try:
                if handler is None:
                    raise ValueError(f"No handler registered for task: {task.task_name}")

                # Create task context
                context = task.to_context()

                # Execute with timeout
                # Note: For true timeout enforcement, use threading or asyncio
                result = handler(task.payload, context)
                success = True

                # Record circuit breaker success
                if circuit_breaker_service:
                    circuit.record_success()

            except Exception as e:
                error_message = str(e)
                error_type = self._classify_error(e)
                error_traceback = traceback.format_exc()

                logger.error(
                    f"Task {task.id} failed: {error_message}",
                    extra={
                        "task_id": str(task.id),
                        "correlation_id": str(task.correlation_id),
                        "error_type": error_type,
                    },
                )

                # Record circuit breaker failure
                if circuit_breaker_service:
                    circuit.record_failure()

            # Calculate duration
            duration_ms = int((time.time() - start_time) * 1000)

            # Complete execution record
            if execution:
                execution.complete(
                    success=success,
                    result=result,
                    error_message=error_message,
                    error_type=error_type,
                    error_traceback=error_traceback,
                )

            # Handle result
            if success:
                self._handle_success(task, result, duration_ms)
            else:
                self._handle_failure(task, error_message, error_type, duration_ms)

        except Exception as critical_error:
            # Critical failure in setup or result handling
            # Ensure task state is updated to prevent it from being stuck in RUNNING
            logger.error(
                f"Critical failure executing task {task.id}: {critical_error}",
                exc_info=True,
                extra={
                    "task_id": str(task.id),
                    "task_name": task.task_name,
                    "correlation_id": str(task.correlation_id),
                },
            )

            try:
                # Mark task as failed
                task.state = TaskState.FAILURE.value
                task.error_message = f"Critical execution error: {str(critical_error)}"
                task.error_type = "CriticalExecutionError"
                task.save()

                # Try to complete execution record if it exists
                if execution:
                    execution.complete(
                        success=False,
                        error_message=task.error_message,
                        error_type=task.error_type,
                        error_traceback=traceback.format_exc(),
                    )

                # Emit failure event
                emit_scheduler_event(
                    "scheduler.task.failed",
                    {
                        "task_id": str(task.id),
                        "task_name": task.task_name,
                        "error_type": "CriticalExecutionError",
                        "error_message": str(critical_error),
                        "attempt_number": task.attempts,
                        "will_retry": False,
                        "correlation_id": str(task.correlation_id),
                        "tenant_id": str(task.tenant_id),
                    },
                )

                self.failed_tasks += 1

            except Exception as save_error:
                # Last resort - log but don't raise
                logger.critical(
                    f"Failed to save task state after critical error: {save_error}",
                    extra={"task_id": str(task.id)},
                )

        finally:
            self._current_task = None

    def _handle_success(
        self,
        task: CognitiveTask,
        result: Any,
        duration_ms: int,
    ) -> None:
        """Handle successful task execution."""
        task.transition_to(TaskState.SUCCESS, result=result)
        self.completed_tasks += 1

        emit_scheduler_event(
            "scheduler.task.completed",
            build_task_completed_payload(task, duration_ms, task.attempts),
            correlation_id=str(task.correlation_id),
            tenant_id=str(task.tenant_id),
        )

        logger.info(
            f"Task completed: {task.task_name}",
            extra={
                "task_id": str(task.id),
                "duration_ms": duration_ms,
            },
        )

    def _handle_failure(
        self,
        task: CognitiveTask,
        error_message: str,
        error_type: str,
        duration_ms: int,
    ) -> None:
        """Handle failed task execution."""
        self.failed_tasks += 1
        will_retry = task.has_attempts_remaining and error_type != "permanent"

        emit_scheduler_event(
            "scheduler.task.failed",
            build_task_failed_payload(
                task, error_type, error_message, task.attempts, will_retry
            ),
            correlation_id=str(task.correlation_id),
            tenant_id=str(task.tenant_id),
        )

        if will_retry:
            # Schedule retry
            next_retry = task.schedule_retry()
            if next_retry:
                delay = (next_retry - timezone.now()).total_seconds()
                emit_scheduler_event(
                    "scheduler.retry.scheduled",
                    build_retry_scheduled_payload(task, int(delay)),
                    correlation_id=str(task.correlation_id),
                    tenant_id=str(task.tenant_id),
                )
        else:
            # Move to DLQ
            task.transition_to(
                TaskState.DEAD_LETTERED,
                error_message=error_message,
                error_type=error_type,
            )

            dlq_entry = DeadLetterEntry.create_from_task(
                task,
                failure_reason=f"Exhausted {task.attempts} attempts: {error_message}",
            )

            emit_scheduler_event(
                "scheduler.dlq.enqueued",
                build_dlq_enqueued_payload(dlq_entry),
                correlation_id=str(task.correlation_id),
                tenant_id=str(task.tenant_id),
            )

    def _classify_error(self, error: Exception) -> str:
        """
        Classify error type for retry decision.

        Returns: "transient", "permanent", or "rate_limited"
        """
        error_name = type(error).__name__

        # Permanent errors - don't retry
        permanent_errors = {
            "ValueError",
            "TypeError",
            "KeyError",
            "AttributeError",
            "NotImplementedError",
            "PermissionError",
        }
        if error_name in permanent_errors:
            return "permanent"

        # Rate limited - retry with longer delays
        if "rate" in str(error).lower() or "429" in str(error):
            return "rate_limited"

        # Default to transient
        return "transient"


# =============================================================================
# END OF TEMPORA SCHEDULER CORE
# =============================================================================
