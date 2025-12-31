"""
Tempora Coordination Server - TCP Server for Cluster Coordination

This module implements the coordination server that accepts connections
from peer Tempora instances and manages cluster membership.

Responsibilities:
    - Accept incoming peer connections
    - Authenticate peers using cluster secret
    - Track cluster membership
    - Route messages to appropriate handlers
    - Broadcast updates to all peers

Design Principles:
    1. Single async server per Tempora instance
    2. Message-driven architecture
    3. Pluggable message handlers for extensibility
    4. Clean separation from business logic

Compliance:
    - TEMPORA-HA-001 §3: Coordination Server Architecture
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
from uuid import uuid4

from .protocol import (
    Message,
    MessageType,
    PROTOCOL_VERSION,
    create_hello,
    create_hello_ack,
    create_error,
    create_join_response,
    create_member_update,
    create_pong,
)
from .transport import (
    TransportLayer,
    TransportConfig,
    Connection,
    ConnectionClosedError,
    TransportError,
)

logger = logging.getLogger(__name__)


class ServerError(Exception):
    """Base exception for server errors."""
    pass


class AuthenticationError(ServerError):
    """Peer authentication failed."""
    pass


class PeerState(Enum):
    """State of a peer connection."""
    CONNECTING = "connecting"
    AUTHENTICATING = "authenticating"
    ACTIVE = "active"
    DRAINING = "draining"
    DISCONNECTED = "disconnected"


@dataclass
class PeerInfo:
    """Information about a connected peer."""
    instance_id: str
    host: str
    port: int
    connection: Connection
    state: PeerState = PeerState.CONNECTING
    connected_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for protocol messages."""
        return {
            "instance_id": self.instance_id,
            "host": self.host,
            "port": self.port,
            "state": self.state.value,
            "connected_at": self.connected_at,
            "last_seen": self.last_seen,
        }


# Type alias for message handlers
MessageHandler = Callable[[PeerInfo, Message], Coroutine[Any, Any, Optional[Message]]]


@dataclass
class CoordinationServerConfig:
    """Configuration for the coordination server."""
    instance_id: str
    bind_host: str = "0.0.0.0"
    bind_port: int = 9500
    cluster_secret: Optional[str] = None
    auth_timeout: float = 5.0
    receive_timeout: float = 30.0
    max_peers: int = 100


class CoordinationServer:
    """
    TCP server for Tempora cluster coordination.

    The server accepts connections from peer instances, authenticates them,
    and manages the message flow for cluster operations.

    Example:
        server = CoordinationServer(
            config=CoordinationServerConfig(
                instance_id="tempora-1",
                bind_port=9500,
                cluster_secret="my-secret"
            )
        )

        # Register custom message handlers
        server.register_handler(MessageType.VOTE_REQUEST, handle_vote_request)

        # Start server
        await server.start()

        # Broadcast to all peers
        await server.broadcast(message)

        # Stop server
        await server.stop()
    """

    def __init__(self, config: CoordinationServerConfig):
        self.config = config
        self.instance_id = config.instance_id

        # Transport layer
        self._transport = TransportLayer(
            TransportConfig(
                bind_host=config.bind_host,
                bind_port=config.bind_port,
            )
        )

        # Peer tracking
        self._peers: Dict[str, PeerInfo] = {}
        self._peer_lock = asyncio.Lock()

        # Message handlers
        self._handlers: Dict[MessageType, List[MessageHandler]] = {}
        self._register_default_handlers()

        # Server state
        self._running = False
        self._started_at: Optional[float] = None

        # Callbacks for lifecycle events
        self._on_peer_connected: List[Callable[[PeerInfo], Coroutine]] = []
        self._on_peer_disconnected: List[Callable[[PeerInfo], Coroutine]] = []

    def _register_default_handlers(self) -> None:
        """Register handlers for core protocol messages."""
        self.register_handler(MessageType.PING, self._handle_ping)
        self.register_handler(MessageType.JOIN_REQUEST, self._handle_join_request)
        self.register_handler(MessageType.GOODBYE, self._handle_goodbye)

    def register_handler(
        self,
        msg_type: MessageType,
        handler: MessageHandler,
    ) -> None:
        """
        Register a message handler.

        Multiple handlers can be registered for the same message type.
        They will be called in order of registration.

        Args:
            msg_type: Message type to handle
            handler: Async handler function
        """
        if msg_type not in self._handlers:
            self._handlers[msg_type] = []
        self._handlers[msg_type].append(handler)

    def on_peer_connected(
        self,
        callback: Callable[[PeerInfo], Coroutine],
    ) -> None:
        """Register callback for peer connection events."""
        self._on_peer_connected.append(callback)

    def on_peer_disconnected(
        self,
        callback: Callable[[PeerInfo], Coroutine],
    ) -> None:
        """Register callback for peer disconnection events."""
        self._on_peer_disconnected.append(callback)

    async def start(self) -> None:
        """Start the coordination server."""
        if self._running:
            raise ServerError("Server already running")

        logger.info(
            f"Starting coordination server",
            extra={
                "instance_id": self.instance_id,
                "bind": f"{self.config.bind_host}:{self.config.bind_port}",
            }
        )

        await self._transport.start_server(self._handle_connection)
        self._running = True
        self._started_at = time.time()

        logger.info(
            f"Coordination server started",
            extra={
                "instance_id": self.instance_id,
                "port": self.config.bind_port,
            }
        )

    async def stop(self) -> None:
        """Stop the coordination server gracefully."""
        if not self._running:
            return

        logger.info("Stopping coordination server")
        self._running = False

        # Notify all peers of shutdown
        try:
            goodbye = Message(
                type=MessageType.GOODBYE,
                payload={
                    "instance_id": self.instance_id,
                    "reason": "server_shutdown",
                }
            )
            await self.broadcast(goodbye)
        except Exception as e:
            logger.warning(f"Error broadcasting goodbye: {e}")

        # Close all peer connections
        async with self._peer_lock:
            for peer in list(self._peers.values()):
                try:
                    await peer.connection.close()
                except Exception:
                    pass
            self._peers.clear()

        # Stop transport
        await self._transport.stop()

        logger.info("Coordination server stopped")

    async def _handle_connection(self, connection: Connection) -> None:
        """
        Handle a new incoming connection.

        Connection flow:
        1. Receive HELLO message with authentication
        2. Validate cluster secret
        3. Send HELLO_ACK
        4. Enter message loop
        """
        peer_info: Optional[PeerInfo] = None

        try:
            # Wait for HELLO message
            peer_info = await self._authenticate_peer(connection)

            if peer_info is None:
                return

            # Add to active peers
            async with self._peer_lock:
                if len(self._peers) >= self.config.max_peers:
                    await connection.send(create_error(
                        "MAX_PEERS",
                        f"Maximum peers ({self.config.max_peers}) reached"
                    ))
                    return

                self._peers[peer_info.instance_id] = peer_info
                peer_info.state = PeerState.ACTIVE

            logger.info(
                f"Peer authenticated and active",
                extra={
                    "peer_id": peer_info.instance_id,
                    "remote": connection.remote_addr,
                }
            )

            # Notify callbacks
            for callback in self._on_peer_connected:
                try:
                    await callback(peer_info)
                except Exception as e:
                    logger.exception(f"Error in peer connected callback: {e}")

            # Broadcast member update
            await self._broadcast_member_update(peer_info, "joined")

            # Enter message loop
            await self._message_loop(peer_info)

        except ConnectionClosedError:
            logger.info(f"Peer disconnected: {connection.remote_addr}")
        except Exception as e:
            logger.exception(f"Error handling connection: {e}")
        finally:
            # Clean up peer
            if peer_info:
                await self._remove_peer(peer_info)

    async def _authenticate_peer(
        self,
        connection: Connection,
    ) -> Optional[PeerInfo]:
        """
        Authenticate an incoming connection.

        Returns:
            PeerInfo if authentication succeeds, None otherwise
        """
        try:
            # Wait for HELLO message
            message = await connection.receive(timeout=self.config.auth_timeout)

            if message.type != MessageType.HELLO:
                await connection.send(create_error(
                    "EXPECTED_HELLO",
                    f"Expected HELLO, got {message.type.name}"
                ))
                return None

            # Extract peer info
            payload = message.payload
            peer_instance_id = payload.get("instance_id")
            peer_host = payload.get("host")
            peer_port = payload.get("port")
            protocol_version = payload.get("protocol_version")
            secret_hash = payload.get("cluster_secret_hash")

            # Validate required fields
            if not all([peer_instance_id, peer_host, peer_port]):
                await connection.send(create_error(
                    "INVALID_HELLO",
                    "Missing required fields in HELLO"
                ))
                return None

            # Check protocol version
            if protocol_version != PROTOCOL_VERSION:
                await connection.send(create_error(
                    "PROTOCOL_MISMATCH",
                    f"Expected protocol {PROTOCOL_VERSION}, got {protocol_version}"
                ))
                return None

            # Validate cluster secret
            if self.config.cluster_secret:
                expected_hash = self._compute_secret_hash(
                    peer_instance_id,
                    message.timestamp
                )
                if not hmac.compare_digest(secret_hash or "", expected_hash):
                    await connection.send(create_error(
                        "AUTH_FAILED",
                        "Invalid cluster secret"
                    ))
                    logger.warning(
                        f"Authentication failed for {peer_instance_id}",
                        extra={"remote": connection.remote_addr}
                    )
                    return None

            # Check for duplicate instance ID
            async with self._peer_lock:
                if peer_instance_id in self._peers:
                    await connection.send(create_error(
                        "DUPLICATE_INSTANCE",
                        f"Instance {peer_instance_id} already connected"
                    ))
                    return None

            # Send HELLO_ACK
            ack = create_hello_ack(
                instance_id=self.instance_id,
                accepted=True,
            )
            await connection.send(ack)

            # Create peer info
            connection.peer_instance_id = peer_instance_id
            return PeerInfo(
                instance_id=peer_instance_id,
                host=peer_host,
                port=peer_port,
                connection=connection,
                state=PeerState.AUTHENTICATING,
            )

        except asyncio.TimeoutError:
            logger.warning(f"Authentication timeout: {connection.remote_addr}")
            await connection.send(create_error(
                "AUTH_TIMEOUT",
                "Authentication timeout"
            ))
            return None

    def _compute_secret_hash(self, instance_id: str, timestamp: float) -> str:
        """
        Compute HMAC of cluster secret for authentication.

        The hash includes instance_id and timestamp to prevent replay attacks.
        Timestamp window is checked on the receiving end.
        """
        if not self.config.cluster_secret:
            return ""

        message = f"{instance_id}:{int(timestamp)}".encode()
        return hmac.new(
            self.config.cluster_secret.encode(),
            message,
            hashlib.sha256
        ).hexdigest()

    async def _message_loop(self, peer: PeerInfo) -> None:
        """
        Main message processing loop for a peer connection.
        """
        while self._running and peer.state == PeerState.ACTIVE:
            try:
                message = await peer.connection.receive(
                    timeout=self.config.receive_timeout
                )
                peer.last_seen = time.time()

                # Dispatch to handlers
                await self._dispatch_message(peer, message)

            except asyncio.TimeoutError:
                # No message received within timeout - this is normal
                # Heartbeat manager will handle liveness checks
                continue
            except ConnectionClosedError:
                break
            except Exception as e:
                logger.exception(f"Error in message loop for {peer.instance_id}: {e}")
                break

    async def _dispatch_message(
        self,
        peer: PeerInfo,
        message: Message,
    ) -> None:
        """
        Dispatch a message to registered handlers.
        """
        handlers = self._handlers.get(message.type, [])

        if not handlers:
            logger.debug(
                f"No handler for message type {message.type.name}",
                extra={"peer_id": peer.instance_id}
            )
            return

        for handler in handlers:
            try:
                response = await handler(peer, message)
                if response:
                    await peer.connection.send(response)
            except Exception as e:
                logger.exception(
                    f"Error in handler for {message.type.name}: {e}",
                    extra={"peer_id": peer.instance_id}
                )

    async def _remove_peer(self, peer: PeerInfo) -> None:
        """Remove a peer from the active set."""
        async with self._peer_lock:
            self._peers.pop(peer.instance_id, None)

        peer.state = PeerState.DISCONNECTED

        # Notify callbacks
        for callback in self._on_peer_disconnected:
            try:
                await callback(peer)
            except Exception as e:
                logger.exception(f"Error in peer disconnected callback: {e}")

        # Broadcast member update
        await self._broadcast_member_update(peer, "left")

        logger.info(
            f"Peer removed",
            extra={"peer_id": peer.instance_id}
        )

    async def _broadcast_member_update(
        self,
        peer: PeerInfo,
        status: str,
    ) -> None:
        """Broadcast a member update to all active peers."""
        update = create_member_update(
            instance_id=peer.instance_id,
            status=status,
            host=peer.host,
            port=peer.port,
        )
        await self.broadcast(update, exclude={peer.instance_id})

    # =========================================================================
    # DEFAULT MESSAGE HANDLERS
    # =========================================================================

    async def _handle_ping(
        self,
        peer: PeerInfo,
        message: Message,
    ) -> Optional[Message]:
        """Handle PING message with PONG response."""
        return create_pong(message.message_id)

    async def _handle_join_request(
        self,
        peer: PeerInfo,
        message: Message,
    ) -> Optional[Message]:
        """Handle JOIN_REQUEST from a peer."""
        # Get current member list
        async with self._peer_lock:
            members = [
                p.to_dict() for p in self._peers.values()
                if p.state == PeerState.ACTIVE
            ]

        return create_join_response(
            accepted=True,
            members=members,
            leader_id=None,  # Will be set by election module
        )

    async def _handle_goodbye(
        self,
        peer: PeerInfo,
        message: Message,
    ) -> Optional[Message]:
        """Handle GOODBYE from a peer."""
        reason = message.payload.get("reason", "unknown")
        logger.info(
            f"Peer saying goodbye",
            extra={
                "peer_id": peer.instance_id,
                "reason": reason,
            }
        )
        peer.state = PeerState.DRAINING
        return None

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    async def broadcast(
        self,
        message: Message,
        exclude: Optional[Set[str]] = None,
    ) -> int:
        """
        Broadcast a message to all active peers.

        Args:
            message: Message to broadcast
            exclude: Set of instance IDs to exclude

        Returns:
            Number of peers the message was sent to
        """
        exclude = exclude or set()
        sent_count = 0

        async with self._peer_lock:
            peers = [
                p for p in self._peers.values()
                if p.state == PeerState.ACTIVE and p.instance_id not in exclude
            ]

        for peer in peers:
            try:
                await peer.connection.send(message)
                sent_count += 1
            except Exception as e:
                logger.warning(
                    f"Failed to send to {peer.instance_id}: {e}"
                )

        return sent_count

    async def send_to_peer(
        self,
        peer_id: str,
        message: Message,
    ) -> bool:
        """
        Send a message to a specific peer.

        Args:
            peer_id: Instance ID of the peer
            message: Message to send

        Returns:
            True if sent successfully, False otherwise
        """
        async with self._peer_lock:
            peer = self._peers.get(peer_id)

        if not peer or peer.state != PeerState.ACTIVE:
            return False

        try:
            await peer.connection.send(message)
            return True
        except Exception as e:
            logger.warning(f"Failed to send to {peer_id}: {e}")
            return False

    def get_peers(self) -> List[PeerInfo]:
        """Get list of active peers."""
        return [
            p for p in self._peers.values()
            if p.state == PeerState.ACTIVE
        ]

    def get_peer(self, peer_id: str) -> Optional[PeerInfo]:
        """Get peer by instance ID."""
        return self._peers.get(peer_id)

    @property
    def peer_count(self) -> int:
        """Get number of active peers."""
        return len([
            p for p in self._peers.values()
            if p.state == PeerState.ACTIVE
        ])

    @property
    def is_running(self) -> bool:
        """Check if server is running."""
        return self._running

    def get_status(self) -> Dict[str, Any]:
        """Get server status for monitoring."""
        return {
            "instance_id": self.instance_id,
            "running": self._running,
            "started_at": self._started_at,
            "uptime": time.time() - self._started_at if self._started_at else 0,
            "bind_address": f"{self.config.bind_host}:{self.config.bind_port}",
            "peer_count": self.peer_count,
            "max_peers": self.config.max_peers,
            "peers": [p.to_dict() for p in self.get_peers()],
        }
