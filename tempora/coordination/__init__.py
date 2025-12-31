"""
Tempora Coordination Layer - Native Distributed Coordination

This package provides native distributed coordination for the Tempora scheduler,
enabling high availability without external dependencies like Redis.

Architecture:
    - protocol.py: Wire protocol message definitions
    - transport.py: Network transport abstraction (TCP)
    - server.py: Coordination server (accepts peer connections)
    - client.py: Peer connection client
    - heartbeat.py: Failure detection via heartbeats

Design Principles:
    1. No external dependencies - pure Python asyncio
    2. PostgreSQL for durability (via Django ORM)
    3. Modular for future extraction as standalone product
    4. Clear separation between transport and protocol

Compliance:
    - TEMPORA-HA-001: Native High Availability Standard
    - SCH-002 §8: Distributed Coordination (superseded by native approach)

Example:
    from tempora.coordination import CoordinationServer, CoordinationClient

    # Start server
    server = CoordinationServer(
        instance_id="tempora-1",
        bind_port=9500,
        cluster_secret="your-secret"
    )
    await server.start()

    # Connect to peer
    client = CoordinationClient(
        instance_id="tempora-1",
        cluster_secret="your-secret"
    )
    await client.connect("tempora-2", "192.168.1.2", 9500)
"""

from .protocol import (
    MessageType,
    Message,
    ProtocolError,
    PROTOCOL_VERSION,
)
from .transport import (
    TransportLayer,
    Connection,
    TransportError,
)
from .server import (
    CoordinationServer,
    ServerError,
)
from .client import (
    CoordinationClient,
    PeerConnection,
    ConnectionPool,
    ClientError,
)
from .heartbeat import (
    HeartbeatManager,
    HeartbeatConfig,
    PeerHealth,
)

__all__ = [
    # Protocol
    "MessageType",
    "Message",
    "ProtocolError",
    "PROTOCOL_VERSION",
    # Transport
    "TransportLayer",
    "Connection",
    "TransportError",
    # Server
    "CoordinationServer",
    "ServerError",
    # Client
    "CoordinationClient",
    "PeerConnection",
    "ConnectionPool",
    "ClientError",
    # Heartbeat
    "HeartbeatManager",
    "HeartbeatConfig",
    "PeerHealth",
]

# Package metadata for extraction
__package_name__ = "tempora-coordination"
__version__ = "0.1.0"
__author__ = "Tempora Contributors"
__description__ = "Native distributed coordination for Tempora scheduler"
