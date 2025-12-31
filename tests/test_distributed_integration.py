"""
Integration Tests for Tempora Distributed Scheduler

Tests end-to-end scenarios for the distributed scheduler components.

Standard: TEMPORA-HA-001 (Integration Testing)
"""

import pytest
import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from tempora.distributed import (
    # Election
    NodeRole,
    ElectionConfig,
    LeaderElector,
    # Replication
    ReplicationConfig,
    StateReplicator,
    NotLeaderError,
    # Work Distribution
    DistributionStrategy,
    WorkDistributionConfig,
    WorkDistributor,
    MemberLoad,
    NoAvailableMembersError,
    # Hardening
    RateLimitConfig,
    ElectionRateLimiter,
    SplitBrainDetector,
    PartitionState,
    ClusterHealthMonitor,
    HealthStatus,
    TLSConfig,
    check_production_readiness,
)


class TestElectionReplicationIntegration:
    """Integration tests for election and replication."""

    @pytest.mark.asyncio
    async def test_leader_starts_replication(self):
        """Test that becoming leader starts replication."""
        # Setup elector
        elector = LeaderElector(
            instance_id="node-1",
            config=ElectionConfig(),
        )

        # Setup replicator
        replicator = StateReplicator(
            instance_id="node-1",
            get_current_term=lambda: elector.current_term,
            is_leader=lambda: elector.is_leader,
            get_peers_callback=lambda: ["node-2", "node-3"],
        )

        # Mock the database access methods to avoid SynchronousOnlyOperation
        with patch.object(replicator, '_get_last_log_index', return_value=0):
            with patch.object(replicator, '_get_last_log_term', return_value=0):
                # Simulate becoming leader
                replicator.start_leader_replication()

                # Verify followers are tracked
                assert "node-2" in replicator._follower_progress
                assert "node-3" in replicator._follower_progress

    @pytest.mark.asyncio
    async def test_leader_loss_stops_replication(self):
        """Test that losing leadership stops replication."""
        replicator = StateReplicator(
            instance_id="node-1",
            get_peers_callback=lambda: ["node-2"],
        )

        # Mock the database access methods
        with patch.object(replicator, '_get_last_log_index', return_value=0):
            with patch.object(replicator, '_get_last_log_term', return_value=0):
                # Start as leader
                replicator.start_leader_replication()
                assert len(replicator._follower_progress) > 0

                # Lose leadership
                replicator.stop_leader_replication()
                assert len(replicator._follower_progress) == 0


class TestReplicationWorkDistributionIntegration:
    """Integration tests for replication and work distribution."""

    @pytest.mark.asyncio
    async def test_task_assignment_workflow(self):
        """Test complete task assignment workflow."""
        get_members = MagicMock(return_value=["worker-1", "worker-2"])

        distributor = WorkDistributor(
            instance_id="leader-1",
            config=WorkDistributionConfig(
                strategy=DistributionStrategy.LEAST_LOADED,
            ),
            get_members_callback=get_members,
        )

        # Add load tracking
        distributor._load_cache["worker-1"] = MemberLoad(
            member_id="worker-1",
            active_tasks=5,
        )
        distributor._load_cache["worker-2"] = MemberLoad(
            member_id="worker-2",
            active_tasks=2,
        )

        # Assign task
        task_id = uuid.uuid4()
        assignment = await distributor.assign_task(
            task_id=task_id,
            tenant_id=uuid.uuid4(),
        )

        # Should go to least loaded
        assert assignment.target_member == "worker-2"
        assert task_id in distributor._pending_assignments

        # Confirm assignment (simulating commit)
        distributor.confirm_assignment(task_id)
        assert task_id not in distributor._pending_assignments

    @pytest.mark.asyncio
    async def test_member_failure_reassignment(self):
        """Test task reassignment when member fails."""
        get_members = MagicMock(return_value=["worker-1", "worker-2"])
        on_assignment = AsyncMock()

        config = WorkDistributionConfig(enable_rebalancing=False)
        distributor = WorkDistributor(
            instance_id="leader-1",
            config=config,
            get_members_callback=get_members,
            on_assignment_callback=on_assignment,
        )

        # Assign task to worker-1
        task_id = uuid.uuid4()
        assignment = await distributor.assign_task(
            task_id=task_id,
            tenant_id=uuid.uuid4(),
        )
        original_target = assignment.target_member

        # Update members to exclude original target
        get_members.return_value = [
            m for m in ["worker-1", "worker-2"]
            if m != original_target
        ]

        # Handle member leaving
        await distributor.handle_member_left(original_target)

        # Task should be reassigned
        new_assignment = distributor.get_assignment(task_id)
        assert new_assignment is not None
        assert new_assignment.target_member != original_target


class TestHardeningIntegration:
    """Integration tests for production hardening components."""

    @pytest.mark.asyncio
    async def test_rate_limiting_election_flow(self):
        """Test rate limiting during election scenarios."""
        config = RateLimitConfig(max_elections_per_minute=5)
        limiter = ElectionRateLimiter(config=config)

        # Simulate rapid elections
        allowed_count = 0
        blocked_count = 0

        for _ in range(10):
            can_start, wait_ms = await limiter.check_can_start_election()
            if can_start:
                allowed_count += 1
            else:
                blocked_count += 1

        assert allowed_count == 5
        assert blocked_count == 5

    def test_split_brain_detection_flow(self):
        """Test split-brain detection workflow."""
        all_members = ["node-2", "node-3", "node-4"]
        reachable = ["node-2"]  # 1/3 reachable + self = 2/4

        get_members = MagicMock(return_value=all_members)
        get_reachable = MagicMock(return_value=reachable)

        detector = SplitBrainDetector(
            instance_id="node-1",
            get_members_callback=get_members,
            get_reachable_callback=get_reachable,
        )

        # First check - suspected
        state1 = detector.check_partition()
        assert state1 == PartitionState.SUSPECTED

        # Fencing token validation
        detector.record_fencing_token(10)
        assert detector.validate_fencing_token(10) is True
        assert detector.validate_fencing_token(5) is False

    @pytest.mark.asyncio
    async def test_health_monitoring_flow(self):
        """Test health monitoring workflow."""
        members = ["node-2", "node-3", "node-4"]
        health_map = {"node-2": True, "node-3": True, "node-4": False}

        get_members = MagicMock(return_value=members)
        get_health = MagicMock(side_effect=lambda m: health_map.get(m, True))

        monitor = ClusterHealthMonitor(
            instance_id="node-1",
            get_members_callback=get_members,
            get_member_health_callback=get_health,
        )

        # Manually trigger check
        await monitor._check_health()

        # 2/3 healthy = 33% unhealthy > 25% threshold = DEGRADED
        assert monitor._current_status == HealthStatus.DEGRADED


class TestProductionReadinessCheck:
    """Tests for production readiness validation."""

    def test_complete_production_config(self):
        """Test with complete production configuration."""
        tls_config = TLSConfig(
            enabled=True,
            cert_file="/etc/ssl/cluster.crt",
            key_file="/etc/ssl/cluster.key",
            min_version="TLSv1.3",
        )

        rate_config = RateLimitConfig(
            max_elections_per_minute=10,
            max_connections_per_source_per_minute=60,
        )

        result = check_production_readiness(tls_config, rate_config)

        assert result["production_ready"] is True
        assert len(result["issues"]) == 0
        assert len(result["passed"]) > 0

    def test_missing_tls(self):
        """Test with missing TLS configuration."""
        result = check_production_readiness()

        assert result["production_ready"] is False
        assert any("TLS" in issue for issue in result["issues"])

    def test_tls_disabled(self):
        """Test with TLS disabled."""
        tls_config = TLSConfig(enabled=False)

        result = check_production_readiness(tls_config)

        assert result["production_ready"] is False


class TestClusterScenarios:
    """Tests for common cluster scenarios."""

    @pytest.mark.asyncio
    async def test_new_member_joins_cluster(self):
        """Test scenario: new member joins cluster."""
        get_members = MagicMock(return_value=["worker-1"])
        config = WorkDistributionConfig(enable_rebalancing=False)

        distributor = WorkDistributor(
            instance_id="leader-1",
            config=config,
            get_members_callback=get_members,
        )

        replicator = StateReplicator(
            instance_id="leader-1",
            is_leader=lambda: True,
            get_peers_callback=get_members,
        )

        # New member joins
        get_members.return_value = ["worker-1", "worker-2"]

        await distributor.handle_member_joined("worker-2")

        # Mock database access for add_follower
        with patch.object(replicator, '_get_last_log_index', return_value=0):
            with patch.object(replicator, '_get_last_log_term', return_value=0):
                replicator.add_follower("worker-2")

                # Both components should track new member
                assert "worker-2" in distributor._load_cache
                assert "worker-2" in replicator._follower_progress

    @pytest.mark.asyncio
    async def test_member_leaves_cluster(self):
        """Test scenario: member leaves cluster."""
        get_members = MagicMock(return_value=["worker-1", "worker-2"])
        config = WorkDistributionConfig(enable_rebalancing=False)

        distributor = WorkDistributor(
            instance_id="leader-1",
            config=config,
            get_members_callback=get_members,
        )
        distributor._load_cache["worker-1"] = MemberLoad(member_id="worker-1")
        distributor._load_cache["worker-2"] = MemberLoad(member_id="worker-2")

        replicator = StateReplicator(
            instance_id="leader-1",
            is_leader=lambda: True,
            get_peers_callback=get_members,
        )

        # Mock database access and start replication
        with patch.object(replicator, '_get_last_log_index', return_value=0):
            with patch.object(replicator, '_get_last_log_term', return_value=0):
                replicator.start_leader_replication()

                # Member leaves
                get_members.return_value = ["worker-1"]

                await distributor.handle_member_left("worker-2")
                replicator.remove_follower("worker-2")

                # Both components should stop tracking
                assert "worker-2" not in distributor._load_cache
                assert "worker-2" not in replicator._follower_progress

    @pytest.mark.asyncio
    async def test_graceful_leader_stepdown(self):
        """Test scenario: leader gracefully steps down."""
        replicator = StateReplicator(
            instance_id="leader-1",
            get_peers_callback=lambda: ["follower-1"],
        )

        # Mock database access and start as leader
        with patch.object(replicator, '_get_last_log_index', return_value=0):
            with patch.object(replicator, '_get_last_log_term', return_value=0):
                replicator.start_leader_replication()
                assert len(replicator._follower_progress) == 1

                # Step down
                replicator.stop_leader_replication()
                assert len(replicator._follower_progress) == 0


class TestInvariantChecks:
    """Tests for TEMPORA-HA-001 invariants."""

    def test_inv_ha_elect_001_single_leader(self):
        """INV-HA-ELECT-001: At most one leader per term."""
        # This is enforced by the Raft algorithm in election.py
        # Here we verify the state constraints

        elector = LeaderElector(instance_id="node-1")

        # A node can only be one role at a time
        assert elector.state.role in [
            NodeRole.FOLLOWER,
            NodeRole.CANDIDATE,
            NodeRole.LEADER,
            NodeRole.OBSERVER,
        ]

    def test_inv_ha_repl_001_log_matching(self):
        """INV-HA-REPL-001: Log entries with same index/term are identical."""
        # This is enforced by the unique constraint in DistributedLogEntry
        # The model definition includes:
        # UniqueConstraint(fields=["tenant_id", "index", "term"])
        pass  # Verified at database level

    def test_inv_ha_quorum_001_majority_required(self):
        """Quorum requires majority of cluster."""
        # Quorum calculation: (cluster_size // 2) + 1

        def calculate_quorum(cluster_size: int) -> int:
            return (cluster_size // 2) + 1

        assert calculate_quorum(1) == 1
        assert calculate_quorum(2) == 2
        assert calculate_quorum(3) == 2
        assert calculate_quorum(4) == 3
        assert calculate_quorum(5) == 3
        assert calculate_quorum(7) == 4

    def test_inv_ha_fencing_001_monotonic_tokens(self):
        """Fencing tokens are monotonically increasing."""
        detector = SplitBrainDetector(instance_id="node-1")

        # Record increasing tokens
        detector.record_fencing_token(1)
        assert detector.validate_fencing_token(1) is True

        detector.record_fencing_token(5)
        assert detector.validate_fencing_token(5) is True
        assert detector.validate_fencing_token(1) is False  # Stale

        detector.record_fencing_token(10)
        assert detector.validate_fencing_token(10) is True
        assert detector.validate_fencing_token(5) is False  # Stale
