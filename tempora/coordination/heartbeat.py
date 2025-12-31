"""
Tempora Heartbeat Manager - Failure Detection

This module implements heartbeat-based failure detection for the
Tempora coordination protocol.

Algorithm:
    - Periodic PING messages sent to all peers
    - PONG responses measure round-trip time (RTT)
    - Adaptive timeout based on observed latency
    - Missing heartbeats trigger failure detection
    - Phi Accrual failure detector for accurate detection

Design Principles:
    1. Low overhead (100ms heartbeat interval default)
    2. Adaptive to network conditions
    3. Configurable thresholds for different environments
    4. Clean integration with coordination server/client

Compliance:
    - TEMPORA-HA-001 §3.4: Failure Detection
    - SCH-002 §8.4: Heartbeat Protocol (superseded)
"""

from __future__ import annotations

import asyncio
import logging
import math
import statistics
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set

from .protocol import (
    Message,
    MessageType,
    create_ping,
    create_pong,
)

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status of a peer."""
    HEALTHY = "healthy"
    SUSPECTED = "suspected"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass
class HeartbeatConfig:
    """Configuration for heartbeat manager."""

    # Heartbeat timing
    interval_ms: int = 100
    """Interval between heartbeats in milliseconds."""

    timeout_ms: int = 300
    """Timeout for heartbeat response in milliseconds."""

    # Failure detection
    missed_before_suspected: int = 2
    """Number of missed heartbeats before marking as suspected."""

    missed_before_failed: int = 3
    """Number of missed heartbeats before marking as failed."""

    # Adaptive timeout
    adaptive_timeout: bool = True
    """Enable adaptive timeout based on RTT."""

    adaptive_multiplier: float = 4.0
    """Multiplier for adaptive timeout (timeout = mean_rtt * multiplier)."""

    min_timeout_ms: int = 100
    """Minimum timeout in milliseconds."""

    max_timeout_ms: int = 5000
    """Maximum timeout in milliseconds."""

    # RTT tracking
    rtt_window_size: int = 100
    """Number of RTT samples to keep for statistics."""

    # Phi Accrual parameters (advanced failure detection)
    phi_threshold: float = 8.0
    """Phi value above which peer is considered failed."""


@dataclass
class PeerHealth:
    """
    Health tracking for a single peer.

    Tracks:
        - Heartbeat history
        - Round-trip times
        - Failure detection state
    """

    peer_id: str
    status: HealthStatus = HealthStatus.UNKNOWN
    last_heartbeat_sent: Optional[float] = None
    last_heartbeat_received: Optional[float] = None
    consecutive_missed: int = 0
    rtt_samples: List[float] = field(default_factory=list)
    pending_pings: Dict[str, float] = field(default_factory=dict)

    # Statistics
    total_sent: int = 0
    total_received: int = 0
    total_missed: int = 0

    def record_sent(self, message_id: str) -> None:
        """Record a heartbeat sent."""
        now = time.time()
        self.last_heartbeat_sent = now
        self.pending_pings[message_id] = now
        self.total_sent += 1

    def record_received(self, message_id: str) -> Optional[float]:
        """
        Record a heartbeat response received.

        Returns:
            RTT in milliseconds, or None if no matching ping
        """
        now = time.time()
        self.last_heartbeat_received = now
        self.consecutive_missed = 0
        self.total_received += 1

        # Calculate RTT
        sent_time = self.pending_pings.pop(message_id, None)
        if sent_time:
            rtt_ms = (now - sent_time) * 1000
            self.rtt_samples.append(rtt_ms)

            # Keep window size
            if len(self.rtt_samples) > 100:
                self.rtt_samples = self.rtt_samples[-100:]

            return rtt_ms
        return None

    def record_missed(self) -> None:
        """Record a missed heartbeat."""
        self.consecutive_missed += 1
        self.total_missed += 1

        # Clear old pending pings
        now = time.time()
        old_pings = [
            mid for mid, ts in self.pending_pings.items()
            if now - ts > 5.0  # 5 second cleanup
        ]
        for mid in old_pings:
            self.pending_pings.pop(mid, None)

    @property
    def mean_rtt(self) -> Optional[float]:
        """Get mean RTT in milliseconds."""
        if not self.rtt_samples:
            return None
        return statistics.mean(self.rtt_samples)

    @property
    def stddev_rtt(self) -> Optional[float]:
        """Get standard deviation of RTT."""
        if len(self.rtt_samples) < 2:
            return None
        return statistics.stdev(self.rtt_samples)

    @property
    def p99_rtt(self) -> Optional[float]:
        """Get 99th percentile RTT."""
        if not self.rtt_samples:
            return None
        sorted_samples = sorted(self.rtt_samples)
        idx = int(len(sorted_samples) * 0.99)
        return sorted_samples[min(idx, len(sorted_samples) - 1)]

    def calculate_phi(self) -> float:
        """
        Calculate Phi Accrual failure detector value.

        Higher phi = more likely the peer has failed.

        Based on: "The Phi Accrual Failure Detector" by Hayashibara et al.
        """
        if not self.last_heartbeat_received or not self.rtt_samples:
            return 0.0

        # Time since last heartbeat
        now = time.time()
        time_since_last = (now - self.last_heartbeat_received) * 1000  # ms

        # Use mean and stddev of inter-arrival times
        mean = self.mean_rtt or 100.0
        stddev = self.stddev_rtt or (mean * 0.5)

        # Avoid division by zero
        if stddev < 1.0:
            stddev = 1.0

        # Calculate phi using normal distribution CDF
        # phi = -log10(1 - CDF(time_since_last))
        # Approximation using error function
        z = (time_since_last - mean) / stddev
        cdf = 0.5 * (1 + math.erf(z / math.sqrt(2)))

        # Avoid log(0)
        if cdf >= 1.0:
            return 16.0  # Very high phi

        phi = -math.log10(1 - cdf) if cdf < 1.0 else 16.0
        return max(0.0, phi)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for monitoring."""
        return {
            "peer_id": self.peer_id,
            "status": self.status.value,
            "consecutive_missed": self.consecutive_missed,
            "total_sent": self.total_sent,
            "total_received": self.total_received,
            "total_missed": self.total_missed,
            "mean_rtt_ms": self.mean_rtt,
            "stddev_rtt_ms": self.stddev_rtt,
            "p99_rtt_ms": self.p99_rtt,
            "phi": self.calculate_phi(),
            "last_heartbeat_received": self.last_heartbeat_received,
        }


# Callback type for failure events
FailureCallback = Callable[[str, HealthStatus], Coroutine[Any, Any, None]]


class HeartbeatManager:
    """
    Manages heartbeat-based failure detection for peers.

    Responsibilities:
        - Send periodic heartbeats to peers
        - Track heartbeat responses
        - Detect peer failures
        - Notify on status changes

    Example:
        # Create manager
        heartbeat = HeartbeatManager(
            config=HeartbeatConfig(interval_ms=100),
            send_callback=send_ping,  # Async function to send pings
        )

        # Register for failure events
        heartbeat.on_status_change(handle_status_change)

        # Start monitoring
        await heartbeat.start()

        # Add peer to monitor
        heartbeat.add_peer("tempora-2")

        # Handle incoming pong
        heartbeat.handle_pong("tempora-2", pong_message)

        # Stop monitoring
        await heartbeat.stop()
    """

    def __init__(
        self,
        config: Optional[HeartbeatConfig] = None,
        send_callback: Optional[Callable[[str, Message], Coroutine]] = None,
    ):
        self.config = config or HeartbeatConfig()
        self._send_callback = send_callback

        # Peer health tracking
        self._peers: Dict[str, PeerHealth] = {}
        self._peers_lock = asyncio.Lock()

        # Status change callbacks
        self._status_callbacks: List[FailureCallback] = []

        # Running state
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._check_task: Optional[asyncio.Task] = None

    def set_send_callback(
        self,
        callback: Callable[[str, Message], Coroutine],
    ) -> None:
        """Set the callback for sending messages to peers."""
        self._send_callback = callback

    def on_status_change(self, callback: FailureCallback) -> None:
        """Register callback for status change events."""
        self._status_callbacks.append(callback)

    async def start(self) -> None:
        """Start the heartbeat manager."""
        if self._running:
            return

        self._running = True

        # Start heartbeat sending loop
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Start failure checking loop
        self._check_task = asyncio.create_task(self._check_loop())

        logger.info(
            "Heartbeat manager started",
            extra={
                "interval_ms": self.config.interval_ms,
                "timeout_ms": self.config.timeout_ms,
            }
        )

    async def stop(self) -> None:
        """Stop the heartbeat manager."""
        if not self._running:
            return

        self._running = False

        # Cancel tasks
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self._check_task:
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass
            self._check_task = None

        logger.info("Heartbeat manager stopped")

    def add_peer(self, peer_id: str) -> None:
        """Add a peer to monitor."""
        if peer_id not in self._peers:
            self._peers[peer_id] = PeerHealth(peer_id=peer_id)
            logger.debug(f"Added peer to heartbeat monitoring: {peer_id}")

    def remove_peer(self, peer_id: str) -> None:
        """Remove a peer from monitoring."""
        self._peers.pop(peer_id, None)
        logger.debug(f"Removed peer from heartbeat monitoring: {peer_id}")

    def handle_pong(self, peer_id: str, message: Message) -> None:
        """
        Handle a PONG response from a peer.

        Args:
            peer_id: The peer that sent the PONG
            message: The PONG message
        """
        health = self._peers.get(peer_id)
        if not health:
            return

        # Get the original ping message ID
        ping_id = message.payload.get("in_response_to")
        if ping_id:
            rtt = health.record_received(ping_id)
            if rtt is not None:
                logger.debug(
                    f"Heartbeat response from {peer_id}",
                    extra={"rtt_ms": rtt}
                )

        # Update status if previously unhealthy
        if health.status != HealthStatus.HEALTHY:
            old_status = health.status
            health.status = HealthStatus.HEALTHY
            asyncio.create_task(
                self._notify_status_change(peer_id, old_status, HealthStatus.HEALTHY)
            )

    def get_peer_health(self, peer_id: str) -> Optional[PeerHealth]:
        """Get health info for a peer."""
        return self._peers.get(peer_id)

    def get_healthy_peers(self) -> List[str]:
        """Get list of healthy peer IDs."""
        return [
            pid for pid, health in self._peers.items()
            if health.status == HealthStatus.HEALTHY
        ]

    def get_failed_peers(self) -> List[str]:
        """Get list of failed peer IDs."""
        return [
            pid for pid, health in self._peers.items()
            if health.status == HealthStatus.FAILED
        ]

    def is_peer_healthy(self, peer_id: str) -> bool:
        """Check if a peer is healthy."""
        health = self._peers.get(peer_id)
        return health is not None and health.status == HealthStatus.HEALTHY

    async def _heartbeat_loop(self) -> None:
        """Background task to send periodic heartbeats."""
        interval = self.config.interval_ms / 1000.0

        while self._running:
            try:
                await self._send_heartbeats()
            except Exception as e:
                logger.exception(f"Error in heartbeat loop: {e}")

            await asyncio.sleep(interval)

    async def _send_heartbeats(self) -> None:
        """Send heartbeat to all monitored peers."""
        if not self._send_callback:
            return

        for peer_id, health in list(self._peers.items()):
            try:
                ping = create_ping()
                health.record_sent(ping.message_id)

                await self._send_callback(peer_id, ping)

            except Exception as e:
                logger.debug(f"Failed to send heartbeat to {peer_id}: {e}")
                health.record_missed()

    async def _check_loop(self) -> None:
        """Background task to check for failed peers."""
        # Check more frequently than heartbeat interval
        check_interval = self.config.interval_ms / 1000.0 / 2

        while self._running:
            try:
                await self._check_peer_health()
            except Exception as e:
                logger.exception(f"Error in health check loop: {e}")

            await asyncio.sleep(check_interval)

    async def _check_peer_health(self) -> None:
        """Check health of all monitored peers."""
        now = time.time()

        for peer_id, health in list(self._peers.items()):
            old_status = health.status

            # Calculate timeout
            if self.config.adaptive_timeout and health.mean_rtt:
                timeout_ms = min(
                    max(
                        health.mean_rtt * self.config.adaptive_multiplier,
                        self.config.min_timeout_ms,
                    ),
                    self.config.max_timeout_ms,
                )
            else:
                timeout_ms = self.config.timeout_ms

            # Check for missed heartbeats
            if health.last_heartbeat_sent:
                time_since_sent = (now - health.last_heartbeat_sent) * 1000

                if time_since_sent > timeout_ms:
                    # Check if we already recorded this miss
                    if health.last_heartbeat_received:
                        time_since_received = (now - health.last_heartbeat_received) * 1000
                        expected_beats = int(time_since_received / self.config.interval_ms)
                        if expected_beats > health.consecutive_missed:
                            health.record_missed()

            # Determine status based on missed heartbeats
            if health.consecutive_missed >= self.config.missed_before_failed:
                health.status = HealthStatus.FAILED
            elif health.consecutive_missed >= self.config.missed_before_suspected:
                health.status = HealthStatus.SUSPECTED
            elif health.consecutive_missed == 0 and health.last_heartbeat_received:
                health.status = HealthStatus.HEALTHY

            # Also check phi for more accurate detection
            if self.config.phi_threshold > 0:
                phi = health.calculate_phi()
                if phi >= self.config.phi_threshold:
                    health.status = HealthStatus.FAILED

            # Notify on status change
            if health.status != old_status:
                await self._notify_status_change(peer_id, old_status, health.status)

    async def _notify_status_change(
        self,
        peer_id: str,
        old_status: HealthStatus,
        new_status: HealthStatus,
    ) -> None:
        """Notify callbacks of a status change."""
        logger.info(
            f"Peer status changed",
            extra={
                "peer_id": peer_id,
                "old_status": old_status.value,
                "new_status": new_status.value,
            }
        )

        for callback in self._status_callbacks:
            try:
                await callback(peer_id, new_status)
            except Exception as e:
                logger.exception(f"Error in status change callback: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Get heartbeat manager status for monitoring."""
        return {
            "running": self._running,
            "config": {
                "interval_ms": self.config.interval_ms,
                "timeout_ms": self.config.timeout_ms,
                "missed_before_failed": self.config.missed_before_failed,
                "phi_threshold": self.config.phi_threshold,
            },
            "peers": {
                pid: health.to_dict()
                for pid, health in self._peers.items()
            },
            "healthy_count": len(self.get_healthy_peers()),
            "failed_count": len(self.get_failed_peers()),
        }
