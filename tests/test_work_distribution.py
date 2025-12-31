"""
Tests for Tempora Work Distribution

Tests the task distribution strategies and work distributor.

Standard: TEMPORA-HA-001 §6 (Work Distribution)
"""

import pytest
import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
import time

from tempora.distributed.work_distribution import (
    DistributionStrategy,
    WorkDistributionConfig,
    MemberLoad,
    TaskAssignment,
    WorkDistributor,
    NoAvailableMembersError,
)


class TestDistributionStrategy:
    """Tests for DistributionStrategy enum."""

    def test_all_strategies_defined(self):
        """Verify all expected strategies are defined."""
        assert DistributionStrategy.ROUND_ROBIN.value == "round_robin"
        assert DistributionStrategy.LEAST_LOADED.value == "least_loaded"
        assert DistributionStrategy.AFFINITY.value == "affinity"
        assert DistributionStrategy.PRIORITY_AWARE.value == "priority_aware"


class TestWorkDistributionConfig:
    """Tests for WorkDistributionConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = WorkDistributionConfig()

        assert config.strategy == DistributionStrategy.LEAST_LOADED
        assert config.enable_rebalancing is True
        assert config.rebalance_threshold_percent == 20.0
        assert config.rebalance_check_interval_seconds == 30.0
        assert config.affinity_weight == 0.3
        assert config.affinity_ttl_seconds == 300
        assert config.track_member_performance is True
        assert config.performance_window_seconds == 300

    def test_custom_config(self):
        """Test custom configuration."""
        config = WorkDistributionConfig(
            strategy=DistributionStrategy.ROUND_ROBIN,
            enable_rebalancing=False,
            rebalance_threshold_percent=30.0,
        )

        assert config.strategy == DistributionStrategy.ROUND_ROBIN
        assert config.enable_rebalancing is False
        assert config.rebalance_threshold_percent == 30.0


class TestMemberLoad:
    """Tests for MemberLoad."""

    def test_initial_state(self):
        """Test default member load."""
        load = MemberLoad(member_id="worker-1")

        assert load.member_id == "worker-1"
        assert load.active_tasks == 0
        assert load.pending_tasks == 0
        assert load.completed_tasks_5min == 0
        assert load.failed_tasks_5min == 0
        assert load.avg_task_duration_ms == 0.0

    def test_total_load(self):
        """Test total load calculation."""
        load = MemberLoad(
            member_id="worker-1",
            active_tasks=5,
            pending_tasks=3,
        )

        assert load.total_load == 8

    def test_success_rate_no_data(self):
        """Test success rate with no data."""
        load = MemberLoad(member_id="worker-1")

        assert load.success_rate == 1.0  # Assume perfect if no data

    def test_success_rate_with_data(self):
        """Test success rate calculation."""
        load = MemberLoad(
            member_id="worker-1",
            completed_tasks_5min=8,
            failed_tasks_5min=2,
        )

        assert load.success_rate == 0.8

    def test_success_rate_all_failed(self):
        """Test success rate when all failed."""
        load = MemberLoad(
            member_id="worker-1",
            completed_tasks_5min=0,
            failed_tasks_5min=5,
        )

        assert load.success_rate == 0.0


class TestTaskAssignment:
    """Tests for TaskAssignment."""

    def test_create_assignment(self):
        """Test creating task assignment."""
        task_id = uuid.uuid4()
        assignment = TaskAssignment(
            task_id=task_id,
            target_member="worker-1",
            strategy_used=DistributionStrategy.LEAST_LOADED,
            assigned_at=time.time(),
        )

        assert assignment.task_id == task_id
        assert assignment.target_member == "worker-1"
        assert assignment.strategy_used == DistributionStrategy.LEAST_LOADED
        assert assignment.affinity_key is None

    def test_to_dict(self):
        """Test conversion to dictionary."""
        task_id = uuid.uuid4()
        assignment = TaskAssignment(
            task_id=task_id,
            target_member="worker-2",
            strategy_used=DistributionStrategy.AFFINITY,
            assigned_at=1234567890.0,
            affinity_key="corr-123",
        )

        d = assignment.to_dict()

        assert d["task_id"] == str(task_id)
        assert d["target_member"] == "worker-2"
        assert d["strategy"] == "affinity"
        assert d["assigned_at"] == 1234567890.0
        assert d["affinity_key"] == "corr-123"


class TestWorkDistributor:
    """Tests for WorkDistributor."""

    def test_initialization(self):
        """Test distributor initialization."""
        distributor = WorkDistributor(
            instance_id="leader-1",
            config=WorkDistributionConfig(),
        )

        assert distributor.instance_id == "leader-1"
        assert distributor._rr_index == 0
        assert distributor._running is False

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self):
        """Test start and stop lifecycle."""
        config = WorkDistributionConfig(enable_rebalancing=False)
        distributor = WorkDistributor(
            instance_id="leader-1",
            config=config,
        )

        await distributor.start()
        assert distributor._running is True

        await distributor.stop()
        assert distributor._running is False

    @pytest.mark.asyncio
    async def test_assign_task_no_members_raises(self):
        """Test that assign_task raises when no members."""
        get_members = MagicMock(return_value=[])
        distributor = WorkDistributor(
            instance_id="leader-1",
            get_members_callback=get_members,
        )

        with pytest.raises(NoAvailableMembersError):
            await distributor.assign_task(
                task_id=uuid.uuid4(),
                tenant_id=uuid.uuid4(),
            )

    @pytest.mark.asyncio
    async def test_assign_task_round_robin(self):
        """Test round-robin task assignment."""
        get_members = MagicMock(return_value=["worker-1", "worker-2", "worker-3"])
        config = WorkDistributionConfig(strategy=DistributionStrategy.ROUND_ROBIN)
        distributor = WorkDistributor(
            instance_id="leader-1",
            config=config,
            get_members_callback=get_members,
        )

        # First assignment
        assignment1 = await distributor.assign_task(
            task_id=uuid.uuid4(),
            tenant_id=uuid.uuid4(),
        )
        # Second assignment
        assignment2 = await distributor.assign_task(
            task_id=uuid.uuid4(),
            tenant_id=uuid.uuid4(),
        )
        # Third assignment
        assignment3 = await distributor.assign_task(
            task_id=uuid.uuid4(),
            tenant_id=uuid.uuid4(),
        )

        # Should rotate through members
        targets = {assignment1.target_member, assignment2.target_member, assignment3.target_member}
        assert len(targets) == 3

    @pytest.mark.asyncio
    async def test_assign_task_least_loaded(self):
        """Test least-loaded task assignment."""
        get_members = MagicMock(return_value=["worker-1", "worker-2"])

        def get_load(member_id):
            if member_id == "worker-1":
                return MemberLoad(member_id="worker-1", active_tasks=5)
            else:
                return MemberLoad(member_id="worker-2", active_tasks=2)

        config = WorkDistributionConfig(strategy=DistributionStrategy.LEAST_LOADED)
        distributor = WorkDistributor(
            instance_id="leader-1",
            config=config,
            get_members_callback=get_members,
            get_member_load_callback=get_load,
        )

        assignment = await distributor.assign_task(
            task_id=uuid.uuid4(),
            tenant_id=uuid.uuid4(),
        )

        # Should pick worker-2 (less loaded)
        assert assignment.target_member == "worker-2"

    @pytest.mark.asyncio
    async def test_assign_task_with_affinity(self):
        """Test affinity-based task assignment."""
        get_members = MagicMock(return_value=["worker-1", "worker-2"])
        config = WorkDistributionConfig(strategy=DistributionStrategy.AFFINITY)
        distributor = WorkDistributor(
            instance_id="leader-1",
            config=config,
            get_members_callback=get_members,
        )

        affinity_key = "correlation-123"

        # First assignment with affinity key
        assignment1 = await distributor.assign_task(
            task_id=uuid.uuid4(),
            tenant_id=uuid.uuid4(),
            affinity_key=affinity_key,
        )

        # Second assignment with same affinity key should go to same member
        assignment2 = await distributor.assign_task(
            task_id=uuid.uuid4(),
            tenant_id=uuid.uuid4(),
            affinity_key=affinity_key,
        )

        assert assignment1.target_member == assignment2.target_member

    def test_confirm_assignment(self):
        """Test confirming an assignment."""
        distributor = WorkDistributor(instance_id="leader-1")
        task_id = uuid.uuid4()
        distributor._pending_assignments[task_id] = TaskAssignment(
            task_id=task_id,
            target_member="worker-1",
            strategy_used=DistributionStrategy.ROUND_ROBIN,
            assigned_at=time.time(),
        )

        distributor.confirm_assignment(task_id)

        assert task_id not in distributor._pending_assignments

    def test_cancel_assignment(self):
        """Test canceling an assignment."""
        distributor = WorkDistributor(instance_id="leader-1")
        task_id = uuid.uuid4()
        distributor._pending_assignments[task_id] = TaskAssignment(
            task_id=task_id,
            target_member="worker-1",
            strategy_used=DistributionStrategy.ROUND_ROBIN,
            assigned_at=time.time(),
        )

        distributor.cancel_assignment(task_id)

        assert task_id not in distributor._pending_assignments

    @pytest.mark.asyncio
    async def test_handle_member_joined(self):
        """Test handling member join."""
        config = WorkDistributionConfig(enable_rebalancing=False)
        distributor = WorkDistributor(
            instance_id="leader-1",
            config=config,
        )

        await distributor.handle_member_joined("worker-1")

        assert "worker-1" in distributor._load_cache

    @pytest.mark.asyncio
    async def test_handle_member_left(self):
        """Test handling member leave."""
        config = WorkDistributionConfig(enable_rebalancing=False)
        distributor = WorkDistributor(
            instance_id="leader-1",
            config=config,
        )
        distributor._load_cache["worker-1"] = MemberLoad(member_id="worker-1")
        distributor._affinity_map["key-1"] = "worker-1"
        distributor._affinity_timestamps["key-1"] = time.time()

        await distributor.handle_member_left("worker-1")

        assert "worker-1" not in distributor._load_cache
        assert "key-1" not in distributor._affinity_map

    def test_update_member_load(self):
        """Test updating member load."""
        distributor = WorkDistributor(instance_id="leader-1")
        load = MemberLoad(member_id="worker-1", active_tasks=10)

        distributor.update_member_load(load)

        assert distributor._load_cache["worker-1"].active_tasks == 10

    def test_record_task_started(self):
        """Test recording task start."""
        distributor = WorkDistributor(instance_id="leader-1")
        distributor._load_cache["worker-1"] = MemberLoad(
            member_id="worker-1",
            active_tasks=5,
        )

        distributor.record_task_started("worker-1")

        assert distributor._load_cache["worker-1"].active_tasks == 6

    def test_record_task_completed(self):
        """Test recording task completion."""
        distributor = WorkDistributor(instance_id="leader-1")
        distributor._load_cache["worker-1"] = MemberLoad(
            member_id="worker-1",
            active_tasks=5,
            completed_tasks_5min=10,
            avg_task_duration_ms=100.0,
        )

        distributor.record_task_completed("worker-1", 200.0)

        assert distributor._load_cache["worker-1"].active_tasks == 4
        assert distributor._load_cache["worker-1"].completed_tasks_5min == 11

    def test_record_task_failed(self):
        """Test recording task failure."""
        distributor = WorkDistributor(instance_id="leader-1")
        distributor._load_cache["worker-1"] = MemberLoad(
            member_id="worker-1",
            active_tasks=5,
            failed_tasks_5min=2,
        )

        distributor.record_task_failed("worker-1")

        assert distributor._load_cache["worker-1"].active_tasks == 4
        assert distributor._load_cache["worker-1"].failed_tasks_5min == 3

    def test_get_status(self):
        """Test getting distributor status."""
        config = WorkDistributionConfig(strategy=DistributionStrategy.ROUND_ROBIN)
        distributor = WorkDistributor(
            instance_id="leader-1",
            config=config,
        )
        distributor._running = True

        status = distributor.get_status()

        assert status["instance_id"] == "leader-1"
        assert status["strategy"] == "round_robin"
        assert status["running"] is True

    def test_get_assignment(self):
        """Test getting an assignment."""
        distributor = WorkDistributor(instance_id="leader-1")
        task_id = uuid.uuid4()
        assignment = TaskAssignment(
            task_id=task_id,
            target_member="worker-1",
            strategy_used=DistributionStrategy.ROUND_ROBIN,
            assigned_at=time.time(),
        )
        distributor._pending_assignments[task_id] = assignment

        result = distributor.get_assignment(task_id)

        assert result == assignment

    def test_get_assignment_not_found(self):
        """Test getting non-existent assignment."""
        distributor = WorkDistributor(instance_id="leader-1")

        result = distributor.get_assignment(uuid.uuid4())

        assert result is None


class TestNoAvailableMembersError:
    """Tests for NoAvailableMembersError."""

    def test_exception_message(self):
        """Test exception with message."""
        error = NoAvailableMembersError("No members")
        assert str(error) == "No members"

    def test_exception_inheritance(self):
        """Test that it's an Exception."""
        assert issubclass(NoAvailableMembersError, Exception)
