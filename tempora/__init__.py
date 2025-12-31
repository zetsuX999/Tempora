"""
Tempora - Native High Availability Distributed Scheduler

A production-ready distributed task scheduler with native clustering,
leader election, state replication, and work distribution.

Features:
- Raft-based leader election with pre-vote optimization
- Distributed log replication with commit tracking
- Multiple work distribution strategies (round-robin, least-loaded, affinity)
- Split-brain prevention with fencing tokens
- Production hardening (rate limiting, TLS, health monitoring)

Standard: TEMPORA-HA-001
"""

__version__ = "1.0.0"
__author__ = "Tempora Contributors"
__license__ = "MIT OR Commercial"

from tempora.coordination import (
    # Protocol
    Message,
    MessageType,
    ProtocolError,
    # Server
    CoordinationServer,
    CoordinationServerConfig,
    # Client
    CoordinationClient,
    PeerConnection,
    ConnectionPool,
    # Heartbeat
    HeartbeatManager,
    HeartbeatConfig,
    PeerHealth,
    HealthStatus,
    # Transport
    Connection,
    TransportLayer,
    TransportConfig,
)

from tempora.distributed import (
    # Election
    LeaderElector,
    ElectionConfig,
    ElectionState,
    NodeRole,
    VoteRequest,
    VoteResponse,
    # Replication
    StateReplicator,
    ReplicationConfig,
    FollowerProgress,
    LogEntryData,
    NotLeaderError,
    # Coordinator
    DistributedCoordinator,
    DistributedConfig,
    # Work Distribution
    WorkDistributor,
    WorkDistributionConfig,
    DistributionStrategy,
    MemberLoad,
    TaskAssignment,
    NoAvailableMembersError,
    # Hardening
    ElectionRateLimiter,
    ConnectionRateLimiter,
    SplitBrainDetector,
    ClusterHealthMonitor,
    TLSConfig,
    RateLimitConfig,
    PartitionState,
    check_production_readiness,
)

__all__ = [
    # Version
    "__version__",
    # Protocol
    "Message",
    "MessageType",
    "ProtocolError",
    # Server
    "CoordinationServer",
    "CoordinationServerConfig",
    # Client
    "CoordinationClient",
    "PeerConnection",
    "ConnectionPool",
    # Heartbeat
    "HeartbeatManager",
    "HeartbeatConfig",
    "PeerHealth",
    "HealthStatus",
    # Transport
    "Connection",
    "TransportLayer",
    "TransportConfig",
    # Election
    "LeaderElector",
    "ElectionConfig",
    "ElectionState",
    "NodeRole",
    "VoteRequest",
    "VoteResponse",
    # Replication
    "StateReplicator",
    "ReplicationConfig",
    "FollowerProgress",
    "LogEntryData",
    "NotLeaderError",
    # Coordinator
    "DistributedCoordinator",
    "DistributedConfig",
    # Work Distribution
    "WorkDistributor",
    "WorkDistributionConfig",
    "DistributionStrategy",
    "MemberLoad",
    "TaskAssignment",
    "NoAvailableMembersError",
    # Hardening
    "ElectionRateLimiter",
    "ConnectionRateLimiter",
    "SplitBrainDetector",
    "ClusterHealthMonitor",
    "TLSConfig",
    "RateLimitConfig",
    "PartitionState",
    "check_production_readiness",
]
