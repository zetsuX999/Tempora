"""
Tempora Distributed Layer - Distributed Scheduler Logic

This package provides distributed scheduling logic for the Tempora scheduler,
building on the coordination layer to implement leader election, state
replication, and work distribution.

Architecture:
    - election.py: Raft-based leader election (Phase 2)
    - replication.py: State replication (Phase 3)
    - work_distribution.py: Task assignment (Phase 4)
    - coordinator.py: Central integration point

Design Principles:
    1. Build on coordination layer primitives
    2. PostgreSQL as distributed log storage
    3. Raft-inspired consensus (adapted for Django/PostgreSQL)
    4. Graceful degradation to single-instance mode

Compliance:
    - TEMPORA-HA-001: Native High Availability Standard
    - SCH-002 §8: Distributed Coordination (implementation)

Implementation Status:
    - Phase 1: Coordination layer (COMPLETE)
    - Phase 2: Leader election (COMPLETE)
    - Phase 3: State replication (COMPLETE)
    - Phase 4: Work distribution (COMPLETE)
    - Phase 5: Production hardening (COMPLETE)
"""

# Phase 2 exports - Leader Election
from tempora.distributed.election import (
    NodeRole,
    ElectionConfig,
    ElectionState,
    VoteRequest,
    VoteResponse,
    AppendEntries,
    AppendEntriesResponse,
    LeaderElector,
)
from tempora.distributed.coordinator import (
    DistributedConfig,
    DistributedCoordinator,
)

# Phase 3 exports - State Replication
from tempora.distributed.replication import (
    ReplicationConfig,
    FollowerProgress,
    LogEntryData,
    StateReplicator,
    NotLeaderError,
)

# Phase 4 exports - Work Distribution
from tempora.distributed.work_distribution import (
    DistributionStrategy,
    WorkDistributionConfig,
    MemberLoad,
    TaskAssignment,
    WorkDistributor,
    NoAvailableMembersError,
)

# Phase 5 exports - Production Hardening
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

__all__ = [
    # Roles and configuration
    "NodeRole",
    "ElectionConfig",
    "ElectionState",
    # Message types
    "VoteRequest",
    "VoteResponse",
    "AppendEntries",
    "AppendEntriesResponse",
    # Phase 2 - Leader Election
    "LeaderElector",
    "DistributedConfig",
    "DistributedCoordinator",
    # Phase 3 - State Replication
    "ReplicationConfig",
    "FollowerProgress",
    "LogEntryData",
    "StateReplicator",
    "NotLeaderError",
    # Phase 4 - Work Distribution
    "DistributionStrategy",
    "WorkDistributionConfig",
    "MemberLoad",
    "TaskAssignment",
    "WorkDistributor",
    "NoAvailableMembersError",
    # Phase 5 - Production Hardening
    "RateLimitConfig",
    "ElectionRateLimiter",
    "ConnectionRateLimiter",
    "PartitionState",
    "SplitBrainDetector",
    "HealthStatus",
    "ClusterHealthConfig",
    "ClusterHealthMonitor",
    "TLSConfig",
    "check_production_readiness",
]

# Package metadata for extraction
__package_name__ = "tempora-distributed"
__version__ = "0.1.0"
__author__ = "Tempora Contributors"
__description__ = "Distributed scheduling logic for Tempora"
