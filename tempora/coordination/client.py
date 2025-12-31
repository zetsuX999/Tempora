"""
Tempora Coordination Client - Peer Connection Management

This module implements the client side of the coordination protocol,
handling outgoing connections to peer Tempora instances.

Responsibilities:
    - Connect to peer instances
    - Authenticate with cluster secret
    - Manage connection pool
    - Handle reconnection on failure

Design Principles:
    1. Automatic reconnection with exponential backoff
    2. Connection pooling for efficiency
    3. Health-aware connection selection
    4. Clean separation from server logic

Compliance:
    - TEMPORA-HA-001 §3.3: Peer Client
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set
from contextlib import asynccontextmanager

from .protocol import (
    Message,
    MessageType,
    PROTOCOL_VERSION,
    create_hello,
    create_goodbye,
    create_ping,
    ProtocolError,
)
from .transport import (
    TransportLayer,
    TransportConfig,
    Connection,
    ConnectionClosedError,
    ConnectionTimeoutError,
    TransportError,
)

logger = logging.getLogger(__name__)


class ClientError(Exception):
    """Base exception for client errors."""
    pass


class PeerNotFoundError(ClientError):
    """Requested peer not found."""
    pass


class ConnectionFailedError(ClientError):
    """Failed to connect to peer."""
    pass


class PeerConnectionState(Enum):
    """State of a peer connection."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    AUTHENTICATING = "authenticating"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"


@dataclass
class PeerConnectionConfig:
    """Configuration for connecting to a peer."""
    instance_id: str
    host: str
    port: int
    connect_timeout: float = 5.0
    auth_timeout: float = 5.0
    reconnect_delay: float = 1.0
    max_reconnect_delay: float = 60.0
    reconnect_attempts: int = -1  # -1 = infinite


@dataclass
class PeerConnection:
    """
    Manages a connection to a single peer.

    Handles:
        - Connection establishment
        - Authentication handshake
        - Automatic reconnection
        - Message sending/receiving

    Example:
        peer = PeerConnection(
            local_instance_id="tempora-1",
            config=PeerConnectionConfig(
                instance_id="tempora-2",
                host="192.168.1.2",
                port=9500,
            ),
            cluster_secret="my-secret"
        )

        await peer.connect()
        await peer.send(message)
        response = await peer.receive()
        await peer.disconnect()
    """

    local_instance_id: str
    config: PeerConnectionConfig
    cluster_secret: Optional[str] = None

    # Internal state
    _connection: Optional[Connection] = field(default=None, init=False)
    _state: PeerConnectionState = field(
        default=PeerConnectionState.DISCONNECTED, init=False
    )
    _transport: TransportLayer = field(default_factory=TransportLayer, init=False)
    _reconnect_task: Optional[asyncio.Task] = field(default=None, init=False)
    _current_reconnect_delay: float = field(default=1.0, init=False)
    _reconnect_attempt: int = field(default=0, init=False)
    _connected_at: Optional[float] = field(default=None, init=False)
    _last_error: Optional[str] = field(default=None, init=False)
    _message_handlers: List[Callable] = field(default_factory=list, init=False)
    _receive_task: Optional[asyncio.Task] = field(default=None, init=False)

    @property
    def instance_id(self) -> str:
        """Get the peer's instance ID."""
        return self.config.instance_id

    @property
    def state(self) -> PeerConnectionState:
        """Get the connection state."""
        return self._state

    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._state == PeerConnectionState.CONNECTED

    async def connect(self) -> None:
        """
        Connect to the peer and authenticate.

        Raises:
            ConnectionFailedError: If connection fails
        """
        if self._state == PeerConnectionState.CONNECTED:
            return

        self._state = PeerConnectionState.CONNECTING

        try:
            # Establish TCP connection
            self._connection = await self._transport.connect(
                host=self.config.host,
                port=self.config.port,
                timeout=self.config.connect_timeout,
            )

            # Perform authentication handshake
            self._state = PeerConnectionState.AUTHENTICATING
            await self._authenticate()

            self._state = PeerConnectionState.CONNECTED
            self._connected_at = time.time()
            self._reconnect_attempt = 0
            self._current_reconnect_delay = self.config.reconnect_delay

            logger.info(
                f"Connected to peer",
                extra={
                    "peer_id": self.instance_id,
                    "remote": f"{self.config.host}:{self.config.port}",
                }
            )

        except Exception as e:
            self._state = PeerConnectionState.FAILED
            self._last_error = str(e)
            if self._connection:
                await self._connection.close()
                self._connection = None
            raise ConnectionFailedError(f"Failed to connect to {self.instance_id}: {e}")

    async def _authenticate(self) -> None:
        """
        Perform authentication handshake.

        Sends HELLO message and waits for HELLO_ACK.
        """
        if not self._connection:
            raise ConnectionFailedError("No connection")

        # Compute secret hash
        timestamp = time.time()
        secret_hash = self._compute_secret_hash(timestamp) if self.cluster_secret else None

        # Send HELLO
        hello = create_hello(
            instance_id=self.local_instance_id,
            host="",  # Server doesn't need our host
            port=0,   # Server doesn't need our port
            protocol_version=PROTOCOL_VERSION,
            cluster_secret_hash=secret_hash,
        )
        # Manually set timestamp to match hash
        hello.timestamp = timestamp

        await self._connection.send(hello)

        # Wait for HELLO_ACK
        response = await self._connection.receive(timeout=self.config.auth_timeout)

        if response.type == MessageType.ERROR:
            error_msg = response.payload.get("message", "Unknown error")
            raise ConnectionFailedError(f"Authentication failed: {error_msg}")

        if response.type != MessageType.HELLO_ACK:
            raise ConnectionFailedError(
                f"Expected HELLO_ACK, got {response.type.name}"
            )

        if not response.payload.get("accepted"):
            reason = response.payload.get("reason", "Unknown")
            raise ConnectionFailedError(f"Connection rejected: {reason}")

    def _compute_secret_hash(self, timestamp: float) -> str:
        """Compute HMAC of cluster secret for authentication."""
        if not self.cluster_secret:
            return ""

        message = f"{self.local_instance_id}:{int(timestamp)}".encode()
        return hmac.new(
            self.cluster_secret.encode(),
            message,
            hashlib.sha256
        ).hexdigest()

    async def disconnect(self, graceful: bool = True) -> None:
        """
        Disconnect from the peer.

        Args:
            graceful: If True, send GOODBYE before disconnecting
        """
        # Cancel reconnect task if running
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None

        # Cancel receive task if running
        if self._receive_task and not self._receive_task.done():
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
            self._receive_task = None

        if self._connection and self._state == PeerConnectionState.CONNECTED:
            if graceful:
                try:
                    goodbye = create_goodbye(
                        self.local_instance_id,
                        reason="client_disconnect"
                    )
                    await self._connection.send(goodbye)
                except Exception:
                    pass

            await self._connection.close()
            self._connection = None

        self._state = PeerConnectionState.DISCONNECTED

        logger.info(f"Disconnected from peer {self.instance_id}")

    async def send(self, message: Message) -> None:
        """
        Send a message to the peer.

        Args:
            message: Message to send

        Raises:
            ConnectionFailedError: If not connected
        """
        if not self._connection or self._state != PeerConnectionState.CONNECTED:
            raise ConnectionFailedError(
                f"Not connected to {self.instance_id}"
            )

        try:
            await self._connection.send(message)
        except ConnectionClosedError:
            self._state = PeerConnectionState.DISCONNECTED
            raise

    async def receive(self, timeout: Optional[float] = None) -> Message:
        """
        Receive a message from the peer.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            Message: Received message

        Raises:
            ConnectionFailedError: If not connected
        """
        if not self._connection or self._state != PeerConnectionState.CONNECTED:
            raise ConnectionFailedError(
                f"Not connected to {self.instance_id}"
            )

        try:
            return await self._connection.receive(timeout=timeout)
        except ConnectionClosedError:
            self._state = PeerConnectionState.DISCONNECTED
            raise

    async def request(
        self,
        message: Message,
        timeout: float = 5.0,
    ) -> Message:
        """
        Send a message and wait for a response.

        Args:
            message: Message to send
            timeout: Response timeout

        Returns:
            Message: Response message
        """
        await self.send(message)
        return await self.receive(timeout=timeout)

    async def start_reconnect_loop(self) -> None:
        """
        Start automatic reconnection loop.

        This runs in the background and attempts to reconnect
        on connection loss with exponential backoff.
        """
        if self._reconnect_task and not self._reconnect_task.done():
            return

        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        """Background task for automatic reconnection."""
        while True:
            if self._state == PeerConnectionState.CONNECTED:
                await asyncio.sleep(1)
                continue

            if self._state == PeerConnectionState.DISCONNECTED:
                # Check if we should reconnect
                max_attempts = self.config.reconnect_attempts
                if max_attempts >= 0 and self._reconnect_attempt >= max_attempts:
                    self._state = PeerConnectionState.FAILED
                    logger.error(
                        f"Max reconnection attempts reached for {self.instance_id}"
                    )
                    return

                # Attempt reconnection
                self._state = PeerConnectionState.RECONNECTING
                self._reconnect_attempt += 1

                logger.info(
                    f"Attempting reconnection to {self.instance_id}",
                    extra={
                        "attempt": self._reconnect_attempt,
                        "delay": self._current_reconnect_delay,
                    }
                )

                try:
                    await self.connect()
                except Exception as e:
                    logger.warning(
                        f"Reconnection failed: {e}",
                        extra={"peer_id": self.instance_id}
                    )

                    # Exponential backoff
                    await asyncio.sleep(self._current_reconnect_delay)
                    self._current_reconnect_delay = min(
                        self._current_reconnect_delay * 2,
                        self.config.max_reconnect_delay
                    )
                    self._state = PeerConnectionState.DISCONNECTED

            await asyncio.sleep(0.1)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for monitoring."""
        return {
            "instance_id": self.instance_id,
            "host": self.config.host,
            "port": self.config.port,
            "state": self._state.value,
            "connected_at": self._connected_at,
            "reconnect_attempt": self._reconnect_attempt,
            "last_error": self._last_error,
        }


class ConnectionPool:
    """
    Pool of connections to peer instances.

    Manages multiple PeerConnection instances and provides
    convenient methods for cluster-wide operations.

    Example:
        pool = ConnectionPool(
            local_instance_id="tempora-1",
            cluster_secret="my-secret"
        )

        # Add peers
        pool.add_peer("tempora-2", "192.168.1.2", 9500)
        pool.add_peer("tempora-3", "192.168.1.3", 9500)

        # Connect to all
        await pool.connect_all()

        # Broadcast message
        await pool.broadcast(message)

        # Send to specific peer
        await pool.send_to("tempora-2", message)

        # Disconnect all
        await pool.disconnect_all()
    """

    def __init__(
        self,
        local_instance_id: str,
        cluster_secret: Optional[str] = None,
        default_config: Optional[PeerConnectionConfig] = None,
    ):
        self.local_instance_id = local_instance_id
        self.cluster_secret = cluster_secret
        self.default_config = default_config

        self._peers: Dict[str, PeerConnection] = {}
        self._lock = asyncio.Lock()

    def add_peer(
        self,
        instance_id: str,
        host: str,
        port: int,
        config: Optional[PeerConnectionConfig] = None,
    ) -> PeerConnection:
        """
        Add a peer to the pool.

        Args:
            instance_id: Peer's instance ID
            host: Peer's host address
            port: Peer's port
            config: Optional custom configuration

        Returns:
            PeerConnection: The created peer connection
        """
        if instance_id in self._peers:
            return self._peers[instance_id]

        peer_config = config or PeerConnectionConfig(
            instance_id=instance_id,
            host=host,
            port=port,
        )

        peer = PeerConnection(
            local_instance_id=self.local_instance_id,
            config=peer_config,
            cluster_secret=self.cluster_secret,
        )

        self._peers[instance_id] = peer
        return peer

    def remove_peer(self, instance_id: str) -> Optional[PeerConnection]:
        """
        Remove a peer from the pool.

        Returns the removed peer connection (caller should disconnect).
        """
        return self._peers.pop(instance_id, None)

    def get_peer(self, instance_id: str) -> Optional[PeerConnection]:
        """Get a peer by instance ID."""
        return self._peers.get(instance_id)

    async def connect_all(self) -> Dict[str, bool]:
        """
        Connect to all peers.

        Returns:
            Dict mapping instance_id to success status
        """
        results = {}

        async def connect_peer(peer: PeerConnection) -> tuple[str, bool]:
            try:
                await peer.connect()
                return peer.instance_id, True
            except Exception as e:
                logger.warning(f"Failed to connect to {peer.instance_id}: {e}")
                return peer.instance_id, False

        tasks = [connect_peer(p) for p in self._peers.values()]
        for coro in asyncio.as_completed(tasks):
            instance_id, success = await coro
            results[instance_id] = success

        return results

    async def disconnect_all(self, graceful: bool = True) -> None:
        """Disconnect from all peers."""
        for peer in self._peers.values():
            try:
                await peer.disconnect(graceful=graceful)
            except Exception as e:
                logger.warning(f"Error disconnecting from {peer.instance_id}: {e}")

    async def broadcast(
        self,
        message: Message,
        exclude: Optional[Set[str]] = None,
    ) -> Dict[str, bool]:
        """
        Broadcast a message to all connected peers.

        Args:
            message: Message to broadcast
            exclude: Set of instance IDs to exclude

        Returns:
            Dict mapping instance_id to success status
        """
        exclude = exclude or set()
        results = {}

        for instance_id, peer in self._peers.items():
            if instance_id in exclude:
                continue

            if not peer.is_connected:
                results[instance_id] = False
                continue

            try:
                await peer.send(message)
                results[instance_id] = True
            except Exception as e:
                logger.warning(f"Failed to send to {instance_id}: {e}")
                results[instance_id] = False

        return results

    async def send_to(self, instance_id: str, message: Message) -> bool:
        """
        Send a message to a specific peer.

        Returns:
            True if sent successfully
        """
        peer = self._peers.get(instance_id)
        if not peer or not peer.is_connected:
            return False

        try:
            await peer.send(message)
            return True
        except Exception as e:
            logger.warning(f"Failed to send to {instance_id}: {e}")
            return False

    def get_connected_peers(self) -> List[PeerConnection]:
        """Get list of connected peers."""
        return [p for p in self._peers.values() if p.is_connected]

    def get_all_peers(self) -> List[PeerConnection]:
        """Get list of all peers."""
        return list(self._peers.values())

    @property
    def connected_count(self) -> int:
        """Get number of connected peers."""
        return len(self.get_connected_peers())

    @property
    def total_count(self) -> int:
        """Get total number of peers."""
        return len(self._peers)

    def get_status(self) -> Dict[str, Any]:
        """Get pool status for monitoring."""
        return {
            "local_instance_id": self.local_instance_id,
            "total_peers": self.total_count,
            "connected_peers": self.connected_count,
            "peers": [p.to_dict() for p in self._peers.values()],
        }


class CoordinationClient:
    """
    High-level client for coordinating with a Tempora cluster.

    Combines ConnectionPool with additional cluster-aware functionality.

    Example:
        client = CoordinationClient(
            instance_id="tempora-1",
            cluster_secret="my-secret"
        )

        # Configure peers from environment/config
        client.add_peer("tempora-2", "192.168.1.2", 9500)
        client.add_peer("tempora-3", "192.168.1.3", 9500)

        # Start client (connects to all peers)
        await client.start()

        # Broadcast to cluster
        await client.broadcast(message)

        # Stop client
        await client.stop()
    """

    def __init__(
        self,
        instance_id: str,
        cluster_secret: Optional[str] = None,
    ):
        self.instance_id = instance_id
        self.cluster_secret = cluster_secret

        self._pool = ConnectionPool(
            local_instance_id=instance_id,
            cluster_secret=cluster_secret,
        )

        self._running = False
        self._reconnect_tasks: List[asyncio.Task] = []

    def add_peer(
        self,
        instance_id: str,
        host: str,
        port: int,
    ) -> PeerConnection:
        """Add a peer to connect to."""
        return self._pool.add_peer(instance_id, host, port)

    async def start(self) -> None:
        """Start the client and connect to all peers."""
        if self._running:
            return

        self._running = True

        # Connect to all peers
        await self._pool.connect_all()

        # Start reconnection loops
        for peer in self._pool.get_all_peers():
            task = asyncio.create_task(peer.start_reconnect_loop())
            self._reconnect_tasks.append(task)

        logger.info(
            f"Coordination client started",
            extra={
                "instance_id": self.instance_id,
                "connected_peers": self._pool.connected_count,
            }
        )

    async def stop(self) -> None:
        """Stop the client and disconnect from all peers."""
        if not self._running:
            return

        self._running = False

        # Cancel reconnection tasks
        for task in self._reconnect_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._reconnect_tasks.clear()

        # Disconnect all peers
        await self._pool.disconnect_all()

        logger.info(f"Coordination client stopped")

    async def broadcast(
        self,
        message: Message,
        exclude: Optional[Set[str]] = None,
    ) -> Dict[str, bool]:
        """Broadcast a message to all connected peers."""
        return await self._pool.broadcast(message, exclude)

    async def send_to(self, instance_id: str, message: Message) -> bool:
        """Send a message to a specific peer."""
        return await self._pool.send_to(instance_id, message)

    def get_peer(self, instance_id: str) -> Optional[PeerConnection]:
        """Get a peer by instance ID."""
        return self._pool.get_peer(instance_id)

    def get_connected_peers(self) -> List[PeerConnection]:
        """Get list of connected peers."""
        return self._pool.get_connected_peers()

    @property
    def connected_count(self) -> int:
        """Get number of connected peers."""
        return self._pool.connected_count

    @property
    def is_running(self) -> bool:
        """Check if client is running."""
        return self._running

    def get_status(self) -> Dict[str, Any]:
        """Get client status for monitoring."""
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            **self._pool.get_status(),
        }
