"""
Tempora Production Hardening - Security and Resilience

This module provides production-hardening features for the distributed
Tempora scheduler, including rate limiting, split-brain prevention,
and operational safeguards.

Components:
    - ElectionRateLimiter: Prevents election storms
    - ConnectionRateLimiter: Prevents connection exhaustion
    - SplitBrainDetector: Detects and handles network partitions
    - ClusterHealthMonitor: Monitors overall cluster health

Security Requirements (per TEMPORA-HA-001 §7):
    - TLS 1.3 MUST be enabled for production deployments
    - Fencing tokens MUST be validated on all state changes
    - Rate limiting MUST be enabled to prevent DoS

Compliance:
    - TEMPORA-HA-001 §7: Split-Brain Prevention
    - TEMPORA-HA-001 §security_controls
    - SEC-001 §rate_limiting
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# =============================================================================
# RATE LIMITING
# =============================================================================


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    # Election rate limiting (per CTL-RATE-001)
    max_elections_per_minute: int = 10
    election_backoff_base_ms: int = 100
    election_backoff_max_ms: int = 5000

    # Connection rate limiting (per CTL-RATE-002)
    max_connections_per_source_per_minute: int = 60
    max_global_connections_per_minute: int = 1000
    connection_ban_duration_seconds: int = 300


class ElectionRateLimiter:
    """
    Rate limiter for leader elections.

    Prevents election storms by limiting how often elections can occur
    and applying exponential backoff when limits are exceeded.

    Per TEMPORA-HA-001 CTL-RATE-001.
    """

    def __init__(self, config: Optional[RateLimitConfig] = None):
        self.config = config or RateLimitConfig()
        self._election_times: List[float] = []
        self._current_backoff_ms: int = 0
        self._lock = asyncio.Lock()

    async def check_can_start_election(self) -> tuple[bool, int]:
        """
        Check if an election can be started.

        Returns:
            (can_start, wait_ms) - if can_start is False, wait_ms indicates
            how long to wait before retrying
        """
        async with self._lock:
            now = time.time()

            # Clean old entries
            cutoff = now - 60  # 1 minute window
            self._election_times = [
                t for t in self._election_times if t > cutoff
            ]

            if len(self._election_times) >= self.config.max_elections_per_minute:
                # Rate limited - calculate backoff
                self._current_backoff_ms = min(
                    self._current_backoff_ms * 2 or self.config.election_backoff_base_ms,
                    self.config.election_backoff_max_ms,
                )

                logger.warning(
                    f"Election rate limited",
                    extra={
                        "elections_in_window": len(self._election_times),
                        "backoff_ms": self._current_backoff_ms,
                    },
                )

                return False, self._current_backoff_ms

            # Record this election
            self._election_times.append(now)

            # Reset backoff on successful election start
            self._current_backoff_ms = 0

            return True, 0

    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiting statistics."""
        now = time.time()
        recent = len([t for t in self._election_times if t > now - 60])
        return {
            "elections_in_last_minute": recent,
            "max_per_minute": self.config.max_elections_per_minute,
            "current_backoff_ms": self._current_backoff_ms,
        }


class ConnectionRateLimiter:
    """
    Rate limiter for incoming connections.

    Prevents connection exhaustion attacks by limiting connections
    per source and globally.

    Per TEMPORA-HA-001 CTL-RATE-002.
    """

    def __init__(self, config: Optional[RateLimitConfig] = None):
        self.config = config or RateLimitConfig()
        self._source_connections: Dict[str, List[float]] = defaultdict(list)
        self._global_connections: List[float] = []
        self._banned_sources: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def check_can_connect(self, source: str) -> tuple[bool, str]:
        """
        Check if a connection from source is allowed.

        Returns:
            (allowed, reason)
        """
        async with self._lock:
            now = time.time()

            # Check if source is banned
            if source in self._banned_sources:
                ban_expires = self._banned_sources[source]
                if now < ban_expires:
                    return False, f"Source banned until {ban_expires}"
                else:
                    del self._banned_sources[source]

            # Clean old entries
            cutoff = now - 60
            self._source_connections[source] = [
                t for t in self._source_connections[source] if t > cutoff
            ]
            self._global_connections = [
                t for t in self._global_connections if t > cutoff
            ]

            # Check per-source limit
            if len(self._source_connections[source]) >= self.config.max_connections_per_source_per_minute:
                # Ban this source
                self._banned_sources[source] = now + self.config.connection_ban_duration_seconds

                logger.warning(
                    f"Source {source} banned for excessive connections",
                    extra={
                        "source": source,
                        "connections": len(self._source_connections[source]),
                    },
                )

                return False, "Connection rate exceeded - source banned"

            # Check global limit
            if len(self._global_connections) >= self.config.max_global_connections_per_minute:
                return False, "Global connection rate exceeded"

            # Record connection
            self._source_connections[source].append(now)
            self._global_connections.append(now)

            return True, "OK"

    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiting statistics."""
        now = time.time()
        return {
            "global_connections_last_minute": len(
                [t for t in self._global_connections if t > now - 60]
            ),
            "banned_sources": len(self._banned_sources),
            "sources_tracked": len(self._source_connections),
        }


# =============================================================================
# SPLIT-BRAIN PREVENTION
# =============================================================================


class PartitionState(Enum):
    """State of cluster partitioning."""
    HEALTHY = "healthy"
    SUSPECTED = "suspected"
    PARTITIONED = "partitioned"
    HEALING = "healing"


@dataclass
class PartitionInfo:
    """Information about a detected partition."""
    detected_at: float
    local_members: Set[str] = field(default_factory=set)
    unreachable_members: Set[str] = field(default_factory=set)
    has_quorum: bool = True
    fencing_token: Optional[int] = None


class SplitBrainDetector:
    """
    Detects and handles network partitions.

    When a partition is detected, this class helps determine:
    - Which partition has quorum
    - Whether to step down from leadership
    - When the partition has healed

    Per TEMPORA-HA-001 §7: Split-Brain Prevention.
    """

    def __init__(
        self,
        instance_id: str,
        get_members_callback: Optional[Callable[[], List[str]]] = None,
        get_reachable_callback: Optional[Callable[[], List[str]]] = None,
        step_down_callback: Optional[Callable[[], Coroutine]] = None,
    ):
        self.instance_id = instance_id
        self._get_members = get_members_callback or (lambda: [])
        self._get_reachable = get_reachable_callback or (lambda: [])
        self._step_down = step_down_callback

        self._state = PartitionState.HEALTHY
        self._current_partition: Optional[PartitionInfo] = None
        self._fencing_token: Optional[int] = None

    def check_partition(self) -> PartitionState:
        """
        Check for partition and update state.

        Returns current partition state.
        """
        all_members = set(self._get_members())
        reachable = set(self._get_reachable())

        if not all_members:
            return PartitionState.HEALTHY

        unreachable = all_members - reachable

        if not unreachable:
            if self._state != PartitionState.HEALTHY:
                logger.info("Partition healed - all members reachable")
            self._state = PartitionState.HEALTHY
            self._current_partition = None
            return self._state

        # Some members unreachable
        total = len(all_members) + 1  # +1 for self
        reachable_count = len(reachable) + 1  # +1 for self
        quorum_size = (total // 2) + 1

        has_quorum = reachable_count >= quorum_size

        if self._state == PartitionState.HEALTHY:
            self._state = PartitionState.SUSPECTED
            self._current_partition = PartitionInfo(
                detected_at=time.time(),
                local_members=reachable | {self.instance_id},
                unreachable_members=unreachable,
                has_quorum=has_quorum,
            )

            logger.warning(
                f"Partition suspected",
                extra={
                    "reachable": list(reachable),
                    "unreachable": list(unreachable),
                    "has_quorum": has_quorum,
                },
            )

        elif self._state == PartitionState.SUSPECTED:
            # If still partitioned after threshold, confirm
            if self._current_partition:
                age = time.time() - self._current_partition.detected_at
                if age > 5.0:  # 5 second threshold
                    self._state = PartitionState.PARTITIONED
                    self._current_partition.has_quorum = has_quorum

                    logger.error(
                        f"Partition confirmed",
                        extra={
                            "has_quorum": has_quorum,
                            "partition_age_seconds": age,
                        },
                    )

        return self._state

    async def handle_partition(self, is_leader: bool) -> bool:
        """
        Handle a confirmed partition.

        Returns True if this node should continue operating,
        False if it should step down.
        """
        if self._state != PartitionState.PARTITIONED:
            return True

        if not self._current_partition:
            return True

        if not self._current_partition.has_quorum:
            # We're in the minority partition - step down
            logger.warning(
                f"In minority partition - stepping down",
                extra={"instance_id": self.instance_id},
            )

            if is_leader and self._step_down:
                await self._step_down()

            return False

        # We have quorum - continue operating
        return True

    def record_fencing_token(self, token: int) -> None:
        """Record the current fencing token."""
        self._fencing_token = token
        if self._current_partition:
            self._current_partition.fencing_token = token

    def validate_fencing_token(self, token: int) -> bool:
        """
        Validate that a fencing token is current.

        Returns True if valid, False if stale.
        """
        if self._fencing_token is None:
            return True  # No token set yet
        return token >= self._fencing_token

    def get_status(self) -> Dict[str, Any]:
        """Get partition detection status."""
        return {
            "state": self._state.value,
            "current_fencing_token": self._fencing_token,
            "partition_info": {
                "detected_at": self._current_partition.detected_at,
                "local_members": list(self._current_partition.local_members),
                "unreachable_members": list(self._current_partition.unreachable_members),
                "has_quorum": self._current_partition.has_quorum,
            } if self._current_partition else None,
        }


# =============================================================================
# CLUSTER HEALTH MONITORING
# =============================================================================


class HealthStatus(Enum):
    """Overall cluster health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class ClusterHealthConfig:
    """Configuration for cluster health monitoring."""

    # Thresholds
    degraded_member_threshold: float = 0.25  # 25% members unhealthy
    critical_member_threshold: float = 0.50  # 50% members unhealthy

    # Check intervals
    health_check_interval_seconds: float = 10.0

    # Alerting
    alert_on_degraded: bool = True
    alert_on_critical: bool = True


class ClusterHealthMonitor:
    """
    Monitors overall cluster health.

    Aggregates health from individual members and provides
    cluster-wide health status for alerting and decision making.

    Provides cluster-wide health aggregation for monitoring.
    """

    def __init__(
        self,
        instance_id: str,
        config: Optional[ClusterHealthConfig] = None,
        get_members_callback: Optional[Callable[[], List[str]]] = None,
        get_member_health_callback: Optional[Callable[[str], bool]] = None,
        on_status_change_callback: Optional[
            Callable[[HealthStatus, HealthStatus], Coroutine]
        ] = None,
    ):
        self.instance_id = instance_id
        self.config = config or ClusterHealthConfig()

        self._get_members = get_members_callback or (lambda: [])
        self._get_member_health = get_member_health_callback or (lambda _: True)
        self._on_status_change = on_status_change_callback

        self._current_status = HealthStatus.UNKNOWN
        self._member_health: Dict[str, bool] = {}
        self._last_check: float = 0.0

        self._monitor_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start health monitoring."""
        if self._running:
            return

        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())

        logger.info("Cluster health monitor started")

    async def stop(self) -> None:
        """Stop health monitoring."""
        if not self._running:
            return

        self._running = False

        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None

        logger.info("Cluster health monitor stopped")

    async def _monitor_loop(self) -> None:
        """Periodic health check loop."""
        while self._running:
            try:
                await asyncio.sleep(self.config.health_check_interval_seconds)
                await self._check_health()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error in health monitor: {e}")

    async def _check_health(self) -> None:
        """Perform health check."""
        members = self._get_members()
        if not members:
            await self._update_status(HealthStatus.UNKNOWN)
            return

        # Check each member
        healthy = 0
        for member_id in members:
            is_healthy = self._get_member_health(member_id)
            self._member_health[member_id] = is_healthy
            if is_healthy:
                healthy += 1

        self._last_check = time.time()

        # Calculate health ratio
        total = len(members)
        unhealthy_ratio = (total - healthy) / total

        # Determine status
        if unhealthy_ratio >= self.config.critical_member_threshold:
            new_status = HealthStatus.CRITICAL
        elif unhealthy_ratio >= self.config.degraded_member_threshold:
            new_status = HealthStatus.DEGRADED
        else:
            new_status = HealthStatus.HEALTHY

        await self._update_status(new_status)

    async def _update_status(self, new_status: HealthStatus) -> None:
        """Update status and notify if changed."""
        if new_status == self._current_status:
            return

        old_status = self._current_status
        self._current_status = new_status

        logger.info(
            f"Cluster health changed: {old_status.value} -> {new_status.value}",
            extra={
                "old_status": old_status.value,
                "new_status": new_status.value,
            },
        )

        # Notify callback
        if self._on_status_change:
            try:
                await self._on_status_change(old_status, new_status)
            except Exception as e:
                logger.exception(f"Error in status change callback: {e}")

    def get_status(self) -> Dict[str, Any]:
        """Get health status for monitoring."""
        healthy_count = sum(1 for h in self._member_health.values() if h)
        total_count = len(self._member_health)

        return {
            "status": self._current_status.value,
            "healthy_members": healthy_count,
            "total_members": total_count,
            "health_ratio": healthy_count / total_count if total_count > 0 else 0,
            "last_check": self._last_check,
            "member_health": dict(self._member_health),
        }

    @property
    def is_healthy(self) -> bool:
        """Check if cluster is healthy."""
        return self._current_status == HealthStatus.HEALTHY

    @property
    def is_critical(self) -> bool:
        """Check if cluster health is critical."""
        return self._current_status == HealthStatus.CRITICAL


# =============================================================================
# TLS CONFIGURATION HELPERS
# =============================================================================


@dataclass
class TLSConfig:
    """
    TLS configuration for cluster communication.

    Per TEMPORA-HA-001 CTL-TLS-001:
    - TLS 1.3 only for production
    - mTLS recommended for zero-trust
    """

    enabled: bool = True  # MUST be True for production
    cert_file: Optional[str] = None
    key_file: Optional[str] = None
    ca_file: Optional[str] = None

    # TLS settings
    min_version: str = "TLSv1.3"
    require_client_cert: bool = False  # Enable for mTLS

    # Cipher suites (TLS 1.3)
    cipher_suites: List[str] = field(default_factory=lambda: [
        "TLS_AES_256_GCM_SHA384",
        "TLS_CHACHA20_POLY1305_SHA256",
        "TLS_AES_128_GCM_SHA256",
    ])

    def validate_for_production(self) -> List[str]:
        """
        Validate configuration for production use.

        Returns list of issues (empty if valid).
        """
        issues = []

        if not self.enabled:
            issues.append("TLS is disabled - MUST be enabled for production")

        if not self.cert_file:
            issues.append("No certificate file specified")

        if not self.key_file:
            issues.append("No key file specified")

        if self.min_version != "TLSv1.3":
            issues.append(f"TLS version {self.min_version} not recommended - use TLSv1.3")

        return issues


def check_production_readiness(
    tls_config: Optional[TLSConfig] = None,
    rate_limit_config: Optional[RateLimitConfig] = None,
) -> Dict[str, Any]:
    """
    Check if configuration is production-ready.

    Returns a report of issues and recommendations.
    """
    issues = []
    warnings = []
    passed = []

    # Check TLS
    if tls_config:
        tls_issues = tls_config.validate_for_production()
        if tls_issues:
            issues.extend(tls_issues)
        else:
            passed.append("TLS configuration valid")
    else:
        issues.append("No TLS configuration provided")

    # Check rate limiting
    if rate_limit_config:
        if rate_limit_config.max_elections_per_minute > 20:
            warnings.append("Election rate limit is high")
        else:
            passed.append("Election rate limiting configured")

        if rate_limit_config.max_connections_per_source_per_minute > 100:
            warnings.append("Connection rate limit is high")
        else:
            passed.append("Connection rate limiting configured")
    else:
        warnings.append("Using default rate limit configuration")

    return {
        "production_ready": len(issues) == 0,
        "issues": issues,
        "warnings": warnings,
        "passed": passed,
    }
