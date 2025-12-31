"""
Tempora Coordination Transport Layer - Network Abstraction

This module provides a clean abstraction over TCP networking, enabling
the coordination protocol to be transport-agnostic (could swap to Unix
sockets, TLS, etc. in the future).

Design Principles:
    1. Asyncio-native for high concurrency
    2. Clean separation of concerns (transport vs protocol)
    3. Connection lifecycle management
    4. Graceful error handling

Compliance:
    - TEMPORA-HA-001 §3.1: Transport Layer
"""

from __future__ import annotations

import asyncio
import logging
import ssl
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Optional, Dict
from contextlib import asynccontextmanager

from .protocol import (
    Message,
    HEADER_SIZE,
    MAX_MESSAGE_SIZE,
    ProtocolError,
    InvalidMessageError,
)

logger = logging.getLogger(__name__)


class TransportError(Exception):
    """Base exception for transport errors."""
    pass


class ConnectionClosedError(TransportError):
    """Connection was closed unexpectedly."""
    pass


class ConnectionTimeoutError(TransportError):
    """Connection or operation timed out."""
    pass


class ConnectionState(Enum):
    """Connection lifecycle states."""
    CONNECTING = "connecting"
    CONNECTED = "connected"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass
class ConnectionStats:
    """Statistics for a connection."""
    bytes_sent: int = 0
    bytes_received: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    errors: int = 0
    connected_at: Optional[float] = None
    last_activity: Optional[float] = None


@dataclass
class Connection:
    """
    A single TCP connection with message framing.

    Handles:
        - Reading/writing length-prefixed messages
        - Connection state tracking
        - Statistics collection
        - Graceful shutdown

    This class is not thread-safe. Use one connection per asyncio task.
    """

    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    remote_addr: tuple[str, int]
    local_addr: tuple[str, int]
    state: ConnectionState = ConnectionState.CONNECTED
    stats: ConnectionStats = field(default_factory=ConnectionStats)
    peer_instance_id: Optional[str] = None

    # Read/write locks for thread safety within asyncio
    _read_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def send(self, message: Message) -> None:
        """
        Send a message over the connection.

        Args:
            message: Message to send

        Raises:
            ConnectionClosedError: If connection is closed
            TransportError: If send fails
        """
        if self.state != ConnectionState.CONNECTED:
            raise ConnectionClosedError(
                f"Cannot send on connection in state {self.state}"
            )

        async with self._write_lock:
            try:
                data = message.encode()
                self.writer.write(data)
                await self.writer.drain()

                self.stats.bytes_sent += len(data)
                self.stats.messages_sent += 1
                self.stats.last_activity = asyncio.get_event_loop().time()

                logger.debug(
                    "Sent message",
                    extra={
                        "message_type": message.type.name,
                        "message_id": message.message_id,
                        "bytes": len(data),
                        "peer": self.remote_addr,
                    }
                )

            except (ConnectionResetError, BrokenPipeError) as e:
                self.state = ConnectionState.CLOSED
                self.stats.errors += 1
                raise ConnectionClosedError(f"Connection reset: {e}")
            except Exception as e:
                self.stats.errors += 1
                raise TransportError(f"Send failed: {e}")

    async def receive(self, timeout: Optional[float] = None) -> Message:
        """
        Receive a message from the connection.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            Message: Received message

        Raises:
            ConnectionClosedError: If connection is closed
            ConnectionTimeoutError: If timeout is reached
            ProtocolError: If message is malformed
        """
        if self.state != ConnectionState.CONNECTED:
            raise ConnectionClosedError(
                f"Cannot receive on connection in state {self.state}"
            )

        async with self._read_lock:
            try:
                # Read header with optional timeout
                if timeout:
                    header_data = await asyncio.wait_for(
                        self.reader.readexactly(HEADER_SIZE),
                        timeout=timeout
                    )
                else:
                    header_data = await self.reader.readexactly(HEADER_SIZE)

                # Parse header to get payload length
                payload_length, msg_type = Message.read_header(header_data)

                # Validate payload length
                if payload_length > MAX_MESSAGE_SIZE:
                    raise InvalidMessageError(
                        f"Payload too large: {payload_length} bytes"
                    )

                # Read payload
                if payload_length > 0:
                    if timeout:
                        payload_data = await asyncio.wait_for(
                            self.reader.readexactly(payload_length),
                            timeout=timeout
                        )
                    else:
                        payload_data = await self.reader.readexactly(payload_length)
                else:
                    payload_data = b""

                # Decode full message
                full_data = header_data + payload_data
                message = Message.decode(full_data)

                self.stats.bytes_received += len(full_data)
                self.stats.messages_received += 1
                self.stats.last_activity = asyncio.get_event_loop().time()

                logger.debug(
                    "Received message",
                    extra={
                        "message_type": message.type.name,
                        "message_id": message.message_id,
                        "bytes": len(full_data),
                        "peer": self.remote_addr,
                    }
                )

                return message

            except asyncio.IncompleteReadError:
                self.state = ConnectionState.CLOSED
                raise ConnectionClosedError("Connection closed by peer")
            except asyncio.TimeoutError:
                raise ConnectionTimeoutError("Receive timeout")
            except ProtocolError:
                self.stats.errors += 1
                raise
            except Exception as e:
                self.stats.errors += 1
                raise TransportError(f"Receive failed: {e}")

    async def close(self, graceful: bool = True) -> None:
        """
        Close the connection.

        Args:
            graceful: If True, wait for pending writes to complete
        """
        if self.state in (ConnectionState.CLOSING, ConnectionState.CLOSED):
            return

        self.state = ConnectionState.CLOSING

        try:
            if graceful:
                self.writer.write_eof()
                await self.writer.drain()
            self.writer.close()
            await self.writer.wait_closed()
        except Exception as e:
            logger.warning(f"Error during connection close: {e}")
        finally:
            self.state = ConnectionState.CLOSED

    @property
    def is_connected(self) -> bool:
        """Check if connection is in connected state."""
        return self.state == ConnectionState.CONNECTED

    def __repr__(self) -> str:
        return (
            f"Connection("
            f"peer={self.remote_addr}, "
            f"state={self.state.value}, "
            f"peer_id={self.peer_instance_id})"
        )


# Type alias for connection handler
ConnectionHandler = Callable[[Connection], Coroutine[Any, Any, None]]


@dataclass
class TransportConfig:
    """Configuration for the transport layer."""
    bind_host: str = "0.0.0.0"
    bind_port: int = 9500
    backlog: int = 100
    connect_timeout: float = 5.0
    read_timeout: float = 30.0
    write_timeout: float = 10.0
    ssl_context: Optional[ssl.SSLContext] = None


class TransportLayer:
    """
    Network transport layer for coordination protocol.

    Handles:
        - TCP server for accepting connections
        - Client connections to peers
        - Connection lifecycle management
        - TLS support (optional)

    Example:
        # Create server
        transport = TransportLayer(config)
        await transport.start_server(handler)

        # Connect to peer
        conn = await transport.connect("192.168.1.2", 9500)
        await conn.send(message)
    """

    def __init__(self, config: Optional[TransportConfig] = None):
        self.config = config or TransportConfig()
        self._server: Optional[asyncio.Server] = None
        self._connections: Dict[tuple[str, int], Connection] = {}
        self._running = False
        self._connection_handler: Optional[ConnectionHandler] = None

    async def start_server(self, handler: ConnectionHandler) -> None:
        """
        Start the TCP server.

        Args:
            handler: Async function to handle new connections
        """
        if self._running:
            raise TransportError("Server already running")

        self._connection_handler = handler
        self._running = True

        self._server = await asyncio.start_server(
            self._handle_new_connection,
            host=self.config.bind_host,
            port=self.config.bind_port,
            backlog=self.config.backlog,
            ssl=self.config.ssl_context,
        )

        addrs = ", ".join(str(s.getsockname()) for s in self._server.sockets)
        logger.info(f"Transport server listening on {addrs}")

    async def _handle_new_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle a new incoming connection."""
        peername = writer.get_extra_info("peername")
        sockname = writer.get_extra_info("sockname")

        connection = Connection(
            reader=reader,
            writer=writer,
            remote_addr=peername,
            local_addr=sockname,
            state=ConnectionState.CONNECTED,
        )
        connection.stats.connected_at = asyncio.get_event_loop().time()

        self._connections[peername] = connection

        logger.info(
            "New connection accepted",
            extra={"remote": peername, "local": sockname}
        )

        try:
            if self._connection_handler:
                await self._connection_handler(connection)
        except Exception as e:
            logger.exception(f"Error handling connection from {peername}: {e}")
        finally:
            await connection.close()
            self._connections.pop(peername, None)
            logger.info(f"Connection closed: {peername}")

    async def connect(
        self,
        host: str,
        port: int,
        timeout: Optional[float] = None,
    ) -> Connection:
        """
        Connect to a remote peer.

        Args:
            host: Remote host address
            port: Remote port
            timeout: Optional connection timeout

        Returns:
            Connection: Connected connection object

        Raises:
            ConnectionTimeoutError: If connection times out
            TransportError: If connection fails
        """
        timeout = timeout or self.config.connect_timeout

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host=host,
                    port=port,
                    ssl=self.config.ssl_context,
                ),
                timeout=timeout,
            )

            peername = writer.get_extra_info("peername")
            sockname = writer.get_extra_info("sockname")

            connection = Connection(
                reader=reader,
                writer=writer,
                remote_addr=peername,
                local_addr=sockname,
                state=ConnectionState.CONNECTED,
            )
            connection.stats.connected_at = asyncio.get_event_loop().time()

            self._connections[peername] = connection

            logger.info(
                "Connected to peer",
                extra={"remote": peername, "local": sockname}
            )

            return connection

        except asyncio.TimeoutError:
            raise ConnectionTimeoutError(
                f"Connection to {host}:{port} timed out after {timeout}s"
            )
        except OSError as e:
            raise TransportError(f"Connection to {host}:{port} failed: {e}")

    async def stop(self) -> None:
        """Stop the transport layer and close all connections."""
        self._running = False

        # Close all connections
        for conn in list(self._connections.values()):
            await conn.close()
        self._connections.clear()

        # Stop server
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        logger.info("Transport layer stopped")

    @property
    def is_running(self) -> bool:
        """Check if transport is running."""
        return self._running

    @property
    def active_connections(self) -> int:
        """Get number of active connections."""
        return len(self._connections)

    def get_connection(self, addr: tuple[str, int]) -> Optional[Connection]:
        """Get a connection by remote address."""
        return self._connections.get(addr)

    @asynccontextmanager
    async def connect_context(self, host: str, port: int):
        """
        Context manager for connecting to a peer.

        Example:
            async with transport.connect_context("host", 9500) as conn:
                await conn.send(message)
        """
        conn = await self.connect(host, port)
        try:
            yield conn
        finally:
            await conn.close()
