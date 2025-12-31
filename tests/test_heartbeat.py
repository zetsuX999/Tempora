"""
Tests for Tempora Heartbeat Manager

Tests the failure detection via heartbeat monitoring.

Standard: TEMPORA-HA-001 §3.4 (Failure Detection)
"""

import pytest
import time
import asyncio
from unittest.mock import AsyncMock, MagicMock

from tempora.coordination.heartbeat import (
    HeartbeatManager,
    HeartbeatConfig,
    PeerHealth,
    HealthStatus,
)
from tempora.coordination.protocol import Message, MessageType


class TestHeartbeatConfig:
    """Tests for HeartbeatConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = HeartbeatConfig()

        assert config.interval_ms == 100
        assert config.timeout_ms == 300
        assert config.missed_before_suspected == 2
        assert config.missed_before_failed == 3
        assert config.adaptive_timeout is True

    def test_custom_config(self):
        """Test custom configuration."""
        config = HeartbeatConfig(
            interval_ms=200,
            timeout_ms=500,
            missed_before_failed=5,
        )

        assert config.interval_ms == 200
        assert config.timeout_ms == 500
        assert config.missed_before_failed == 5


class TestPeerHealth:
    """Tests for PeerHealth tracking."""

    def test_initial_state(self):
        """Test initial peer health state."""
        health = PeerHealth(peer_id="test-1")

        assert health.peer_id == "test-1"
        assert health.status == HealthStatus.UNKNOWN
        assert health.consecutive_missed == 0
        assert health.total_sent == 0
        assert health.total_received == 0

    def test_record_sent(self):
        """Test recording a sent heartbeat."""
        health = PeerHealth(peer_id="test-1")

        health.record_sent("msg-1")

        assert health.total_sent == 1
        assert health.last_heartbeat_sent is not None
        assert "msg-1" in health.pending_pings

    def test_record_received(self):
        """Test recording a received heartbeat response."""
        health = PeerHealth(peer_id="test-1")

        # First send a ping
        health.record_sent("msg-1")
        time.sleep(0.01)  # Small delay to get measurable RTT

        # Then receive pong
        rtt = health.record_received("msg-1")

        assert health.total_received == 1
        assert health.consecutive_missed == 0
        assert health.last_heartbeat_received is not None
        assert rtt is not None
        assert rtt >= 0
        assert "msg-1" not in health.pending_pings

    def test_record_missed(self):
        """Test recording missed heartbeats."""
        health = PeerHealth(peer_id="test-1")

        health.record_missed()
        assert health.consecutive_missed == 1
        assert health.total_missed == 1

        health.record_missed()
        assert health.consecutive_missed == 2
        assert health.total_missed == 2

    def test_rtt_statistics(self):
        """Test RTT statistical calculations."""
        health = PeerHealth(peer_id="test-1")
        health.rtt_samples = [10.0, 20.0, 30.0, 40.0, 50.0]

        assert health.mean_rtt == 30.0
        assert health.stddev_rtt is not None
        assert health.p99_rtt == 50.0

    def test_rtt_empty(self):
        """Test RTT statistics with no samples."""
        health = PeerHealth(peer_id="test-1")

        assert health.mean_rtt is None
        assert health.stddev_rtt is None
        assert health.p99_rtt is None

    def test_phi_calculation(self):
        """Test Phi Accrual failure detector calculation."""
        health = PeerHealth(peer_id="test-1")
        health.rtt_samples = [100.0] * 10  # 100ms consistent RTT
        health.last_heartbeat_received = time.time()

        # Should be low phi when just received heartbeat
        phi = health.calculate_phi()
        assert phi >= 0

    def test_phi_increases_over_time(self):
        """Test that phi increases when no heartbeats received."""
        health = PeerHealth(peer_id="test-1")
        health.rtt_samples = [100.0] * 10
        health.last_heartbeat_received = time.time() - 1.0  # 1 second ago

        phi = health.calculate_phi()
        # Phi should be higher after 1 second with 100ms expected interval
        assert phi > 1.0

    def test_to_dict(self):
        """Test conversion to dictionary."""
        health = PeerHealth(peer_id="test-1")
        health.status = HealthStatus.HEALTHY
        health.consecutive_missed = 1

        d = health.to_dict()

        assert d["peer_id"] == "test-1"
        assert d["status"] == "healthy"
        assert d["consecutive_missed"] == 1


class TestHeartbeatManager:
    """Tests for HeartbeatManager."""

    @pytest.fixture
    def manager(self):
        """Create a heartbeat manager for testing."""
        return HeartbeatManager(
            config=HeartbeatConfig(
                interval_ms=50,  # Fast for testing
                timeout_ms=100,
            )
        )

    def test_add_peer(self, manager):
        """Test adding a peer to monitor."""
        manager.add_peer("test-1")

        health = manager.get_peer_health("test-1")
        assert health is not None
        assert health.peer_id == "test-1"

    def test_remove_peer(self, manager):
        """Test removing a peer from monitoring."""
        manager.add_peer("test-1")
        manager.remove_peer("test-1")

        health = manager.get_peer_health("test-1")
        assert health is None

    @pytest.mark.asyncio
    async def test_handle_pong(self, manager):
        """Test handling a PONG response."""
        manager.add_peer("test-1")

        # Simulate sending a ping
        health = manager.get_peer_health("test-1")
        health.record_sent("ping-1")

        # Handle the pong (now has running event loop)
        pong = Message(
            type=MessageType.PONG,
            payload={"in_response_to": "ping-1"}
        )
        manager.handle_pong("test-1", pong)

        health = manager.get_peer_health("test-1")
        assert health.status == HealthStatus.HEALTHY
        assert health.total_received == 1

    @pytest.mark.asyncio
    async def test_handle_pong_recovers_suspected(self, manager):
        """Test that receiving PONG recovers a suspected peer."""
        manager.add_peer("test-1")

        health = manager.get_peer_health("test-1")
        health.status = HealthStatus.SUSPECTED
        health.record_sent("ping-1")

        pong = Message(
            type=MessageType.PONG,
            payload={"in_response_to": "ping-1"}
        )
        manager.handle_pong("test-1", pong)

        assert health.status == HealthStatus.HEALTHY

    def test_get_healthy_peers(self, manager):
        """Test getting list of healthy peers."""
        manager.add_peer("test-1")
        manager.add_peer("test-2")
        manager.add_peer("test-3")

        # Set statuses
        manager.get_peer_health("test-1").status = HealthStatus.HEALTHY
        manager.get_peer_health("test-2").status = HealthStatus.FAILED
        manager.get_peer_health("test-3").status = HealthStatus.HEALTHY

        healthy = manager.get_healthy_peers()

        assert "test-1" in healthy
        assert "test-3" in healthy
        assert "test-2" not in healthy

    def test_get_failed_peers(self, manager):
        """Test getting list of failed peers."""
        manager.add_peer("test-1")
        manager.add_peer("test-2")

        manager.get_peer_health("test-1").status = HealthStatus.HEALTHY
        manager.get_peer_health("test-2").status = HealthStatus.FAILED

        failed = manager.get_failed_peers()

        assert "test-2" in failed
        assert "test-1" not in failed

    def test_is_peer_healthy(self, manager):
        """Test checking if a specific peer is healthy."""
        manager.add_peer("test-1")
        manager.get_peer_health("test-1").status = HealthStatus.HEALTHY

        assert manager.is_peer_healthy("test-1") is True
        assert manager.is_peer_healthy("test-2") is False  # Not monitored

    def test_get_status(self, manager):
        """Test getting manager status."""
        manager.add_peer("test-1")
        manager.add_peer("test-2")
        manager.get_peer_health("test-1").status = HealthStatus.HEALTHY
        manager.get_peer_health("test-2").status = HealthStatus.FAILED

        status = manager.get_status()

        assert status["healthy_count"] == 1
        assert status["failed_count"] == 1
        assert "test-1" in status["peers"]
        assert "test-2" in status["peers"]


class TestHeartbeatManagerAsync:
    """Async tests for HeartbeatManager."""

    @pytest.mark.asyncio
    async def test_start_stop(self):
        """Test starting and stopping the manager."""
        manager = HeartbeatManager(
            config=HeartbeatConfig(interval_ms=50)
        )

        await manager.start()
        assert manager._running is True

        await manager.stop()
        assert manager._running is False

    @pytest.mark.asyncio
    async def test_status_change_callback(self):
        """Test that status change callbacks are invoked."""
        manager = HeartbeatManager(
            config=HeartbeatConfig(
                interval_ms=50,
                timeout_ms=100,
                missed_before_failed=2,
            )
        )

        callback_called = asyncio.Event()
        callback_args = []

        async def on_status_change(peer_id: str, status: HealthStatus):
            callback_args.append((peer_id, status))
            callback_called.set()

        manager.on_status_change(on_status_change)
        manager.add_peer("test-1")

        # Trigger status change via pong
        health = manager.get_peer_health("test-1")
        health.status = HealthStatus.SUSPECTED
        health.record_sent("ping-1")

        pong = Message(
            type=MessageType.PONG,
            payload={"in_response_to": "ping-1"}
        )
        manager.handle_pong("test-1", pong)

        # Wait for callback
        await asyncio.wait_for(callback_called.wait(), timeout=1.0)

        assert len(callback_args) > 0
        assert callback_args[0][0] == "test-1"
        assert callback_args[0][1] == HealthStatus.HEALTHY
