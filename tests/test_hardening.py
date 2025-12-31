"""
Tests for Tempora Production Hardening

Tests rate limiting, split-brain detection, and health monitoring.

Standard: TEMPORA-HA-001 §7 (Production Hardening)
"""

import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

from tempora.distributed.hardening import (
    RateLimitConfig,
    ElectionRateLimiter,
    ConnectionRateLimiter,
    PartitionState,
    SplitBrainDetector,
    HealthStatus,
    ClusterHealthConfig,
    ClusterHealthMonitor,
    TLSConfig,
    check_production_readiness,
)


class TestRateLimitConfig:
    """Tests for RateLimitConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = RateLimitConfig()

        assert config.max_elections_per_minute == 10
        assert config.election_backoff_base_ms == 100
        assert config.election_backoff_max_ms == 5000
        assert config.max_connections_per_source_per_minute == 60
        assert config.max_global_connections_per_minute == 1000
        assert config.connection_ban_duration_seconds == 300

    def test_custom_config(self):
        """Test custom configuration."""
        config = RateLimitConfig(
            max_elections_per_minute=20,
            max_connections_per_source_per_minute=120,
        )

        assert config.max_elections_per_minute == 20
        assert config.max_connections_per_source_per_minute == 120


class TestElectionRateLimiter:
    """Tests for ElectionRateLimiter."""

    @pytest.mark.asyncio
    async def test_allows_election_under_limit(self):
        """Test that elections are allowed under limit."""
        limiter = ElectionRateLimiter()

        can_start, wait_ms = await limiter.check_can_start_election()

        assert can_start is True
        assert wait_ms == 0

    @pytest.mark.asyncio
    async def test_blocks_election_over_limit(self):
        """Test that elections are blocked over limit."""
        config = RateLimitConfig(max_elections_per_minute=2)
        limiter = ElectionRateLimiter(config=config)

        # Use up the limit
        await limiter.check_can_start_election()
        await limiter.check_can_start_election()

        # Third should be blocked
        can_start, wait_ms = await limiter.check_can_start_election()

        assert can_start is False
        assert wait_ms > 0

    @pytest.mark.asyncio
    async def test_backoff_increases(self):
        """Test that backoff increases on repeated limits."""
        config = RateLimitConfig(
            max_elections_per_minute=1,
            election_backoff_base_ms=100,
        )
        limiter = ElectionRateLimiter(config=config)

        # First election
        await limiter.check_can_start_election()

        # Second blocked with initial backoff
        _, wait_ms1 = await limiter.check_can_start_election()

        # Third blocked with increased backoff
        _, wait_ms2 = await limiter.check_can_start_election()

        assert wait_ms2 > wait_ms1

    def test_get_stats(self):
        """Test getting rate limit stats."""
        limiter = ElectionRateLimiter()

        stats = limiter.get_stats()

        assert "elections_in_last_minute" in stats
        assert "max_per_minute" in stats
        assert "current_backoff_ms" in stats


class TestConnectionRateLimiter:
    """Tests for ConnectionRateLimiter."""

    @pytest.mark.asyncio
    async def test_allows_connection_under_limit(self):
        """Test that connections are allowed under limit."""
        limiter = ConnectionRateLimiter()

        allowed, reason = await limiter.check_can_connect("192.168.1.1")

        assert allowed is True
        assert reason == "OK"

    @pytest.mark.asyncio
    async def test_bans_source_over_limit(self):
        """Test that source is banned over limit."""
        config = RateLimitConfig(max_connections_per_source_per_minute=2)
        limiter = ConnectionRateLimiter(config=config)

        source = "192.168.1.1"

        # Use up the limit
        await limiter.check_can_connect(source)
        await limiter.check_can_connect(source)

        # Third should trigger ban
        allowed, reason = await limiter.check_can_connect(source)

        assert allowed is False
        assert "banned" in reason.lower()

    @pytest.mark.asyncio
    async def test_different_sources_independent(self):
        """Test that different sources are tracked independently."""
        config = RateLimitConfig(max_connections_per_source_per_minute=1)
        limiter = ConnectionRateLimiter(config=config)

        # Source 1 uses its limit
        await limiter.check_can_connect("192.168.1.1")

        # Source 2 should still be allowed
        allowed, reason = await limiter.check_can_connect("192.168.1.2")

        assert allowed is True

    def test_get_stats(self):
        """Test getting connection stats."""
        limiter = ConnectionRateLimiter()

        stats = limiter.get_stats()

        assert "global_connections_last_minute" in stats
        assert "banned_sources" in stats
        assert "sources_tracked" in stats


class TestPartitionState:
    """Tests for PartitionState enum."""

    def test_all_states_defined(self):
        """Verify all expected states are defined."""
        assert PartitionState.HEALTHY.value == "healthy"
        assert PartitionState.SUSPECTED.value == "suspected"
        assert PartitionState.PARTITIONED.value == "partitioned"
        assert PartitionState.HEALING.value == "healing"


class TestSplitBrainDetector:
    """Tests for SplitBrainDetector."""

    def test_initialization(self):
        """Test detector initialization."""
        detector = SplitBrainDetector(instance_id="node-1")

        assert detector.instance_id == "node-1"
        assert detector._state == PartitionState.HEALTHY

    def test_check_partition_healthy(self):
        """Test partition check when all healthy."""
        get_members = MagicMock(return_value=["node-2", "node-3"])
        get_reachable = MagicMock(return_value=["node-2", "node-3"])

        detector = SplitBrainDetector(
            instance_id="node-1",
            get_members_callback=get_members,
            get_reachable_callback=get_reachable,
        )

        state = detector.check_partition()

        assert state == PartitionState.HEALTHY

    def test_check_partition_suspected(self):
        """Test partition check when some unreachable."""
        get_members = MagicMock(return_value=["node-2", "node-3"])
        get_reachable = MagicMock(return_value=["node-2"])  # node-3 unreachable

        detector = SplitBrainDetector(
            instance_id="node-1",
            get_members_callback=get_members,
            get_reachable_callback=get_reachable,
        )

        state = detector.check_partition()

        assert state == PartitionState.SUSPECTED

    def test_validate_fencing_token_valid(self):
        """Test validating a current fencing token."""
        detector = SplitBrainDetector(instance_id="node-1")
        detector._fencing_token = 5

        result = detector.validate_fencing_token(5)

        assert result is True

    def test_validate_fencing_token_stale(self):
        """Test validating a stale fencing token."""
        detector = SplitBrainDetector(instance_id="node-1")
        detector._fencing_token = 5

        result = detector.validate_fencing_token(3)

        assert result is False

    def test_validate_fencing_token_no_token(self):
        """Test validating when no token set."""
        detector = SplitBrainDetector(instance_id="node-1")

        result = detector.validate_fencing_token(1)

        assert result is True  # No token set yet

    def test_record_fencing_token(self):
        """Test recording a fencing token."""
        detector = SplitBrainDetector(instance_id="node-1")

        detector.record_fencing_token(10)

        assert detector._fencing_token == 10

    def test_get_status(self):
        """Test getting detector status."""
        detector = SplitBrainDetector(instance_id="node-1")
        detector._fencing_token = 5

        status = detector.get_status()

        assert status["state"] == "healthy"
        assert status["current_fencing_token"] == 5


class TestHealthStatus:
    """Tests for HealthStatus enum."""

    def test_all_statuses_defined(self):
        """Verify all expected statuses are defined."""
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.DEGRADED.value == "degraded"
        assert HealthStatus.CRITICAL.value == "critical"
        assert HealthStatus.UNKNOWN.value == "unknown"


class TestClusterHealthConfig:
    """Tests for ClusterHealthConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ClusterHealthConfig()

        assert config.degraded_member_threshold == 0.25
        assert config.critical_member_threshold == 0.50
        assert config.health_check_interval_seconds == 10.0
        assert config.alert_on_degraded is True
        assert config.alert_on_critical is True


class TestClusterHealthMonitor:
    """Tests for ClusterHealthMonitor."""

    def test_initialization(self):
        """Test monitor initialization."""
        monitor = ClusterHealthMonitor(instance_id="node-1")

        assert monitor.instance_id == "node-1"
        assert monitor._current_status == HealthStatus.UNKNOWN
        assert monitor._running is False

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self):
        """Test start and stop lifecycle."""
        monitor = ClusterHealthMonitor(instance_id="node-1")

        await monitor.start()
        assert monitor._running is True

        await monitor.stop()
        assert monitor._running is False

    def test_is_healthy(self):
        """Test is_healthy property."""
        monitor = ClusterHealthMonitor(instance_id="node-1")

        monitor._current_status = HealthStatus.HEALTHY
        assert monitor.is_healthy is True

        monitor._current_status = HealthStatus.DEGRADED
        assert monitor.is_healthy is False

    def test_is_critical(self):
        """Test is_critical property."""
        monitor = ClusterHealthMonitor(instance_id="node-1")

        monitor._current_status = HealthStatus.CRITICAL
        assert monitor.is_critical is True

        monitor._current_status = HealthStatus.HEALTHY
        assert monitor.is_critical is False

    def test_get_status(self):
        """Test getting health status."""
        monitor = ClusterHealthMonitor(instance_id="node-1")
        monitor._current_status = HealthStatus.HEALTHY
        monitor._member_health = {"node-2": True, "node-3": False}

        status = monitor.get_status()

        assert status["status"] == "healthy"
        assert status["healthy_members"] == 1
        assert status["total_members"] == 2
        assert status["health_ratio"] == 0.5


class TestTLSConfig:
    """Tests for TLSConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = TLSConfig()

        assert config.enabled is True
        assert config.min_version == "TLSv1.3"
        assert config.require_client_cert is False
        assert len(config.cipher_suites) > 0

    def test_validate_for_production_valid(self):
        """Test validation with valid production config."""
        config = TLSConfig(
            enabled=True,
            cert_file="/path/to/cert.pem",
            key_file="/path/to/key.pem",
        )

        issues = config.validate_for_production()

        assert len(issues) == 0

    def test_validate_for_production_disabled(self):
        """Test validation with TLS disabled."""
        config = TLSConfig(enabled=False)

        issues = config.validate_for_production()

        assert any("disabled" in issue.lower() for issue in issues)

    def test_validate_for_production_missing_cert(self):
        """Test validation with missing certificate."""
        config = TLSConfig(
            enabled=True,
            key_file="/path/to/key.pem",
        )

        issues = config.validate_for_production()

        assert any("certificate" in issue.lower() for issue in issues)


class TestCheckProductionReadiness:
    """Tests for check_production_readiness function."""

    def test_production_ready(self):
        """Test with production-ready config."""
        tls_config = TLSConfig(
            enabled=True,
            cert_file="/path/to/cert.pem",
            key_file="/path/to/key.pem",
        )
        rate_config = RateLimitConfig()

        result = check_production_readiness(tls_config, rate_config)

        assert result["production_ready"] is True
        assert len(result["issues"]) == 0

    def test_not_production_ready(self):
        """Test with non-production config."""
        tls_config = TLSConfig(enabled=False)

        result = check_production_readiness(tls_config)

        assert result["production_ready"] is False
        assert len(result["issues"]) > 0

    def test_no_tls_config(self):
        """Test with no TLS config provided."""
        result = check_production_readiness()

        assert result["production_ready"] is False
        assert any("TLS" in issue for issue in result["issues"])
