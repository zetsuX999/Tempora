"""
Tempora Coordination Protocol - Wire Protocol Definitions

This module defines the binary wire protocol for inter-instance communication.
The protocol is designed for simplicity, efficiency, and debuggability.

Wire Format:
    ┌─────────────────┬──────────────────┬──────────────────────────────────┐
    │ Length (4 bytes)│ Type (1 byte)    │ Payload (JSON, variable length)  │
    │ Big-endian      │ MessageType enum │ UTF-8 encoded JSON               │
    └─────────────────┴──────────────────┴──────────────────────────────────┘

    - Length: Total message length excluding the 4-byte length field itself
    - Type: Single byte identifying the message type
    - Payload: JSON-encoded message data (may be empty for some message types)

Design Decisions:
    1. JSON payload for debuggability (can switch to msgpack for performance)
    2. Length-prefixed for reliable framing over TCP
    3. Single-byte type for efficiency (supports 256 message types)
    4. Extensible via payload fields (no schema versioning needed)

Compliance:
    - TEMPORA-HA-001 §3.2: Wire Protocol
"""

from __future__ import annotations

import json
import struct
import time
from dataclasses import dataclass, field, asdict
from enum import IntEnum
from typing import Any, Optional, Dict, Type, TypeVar
from uuid import uuid4

# Protocol version for compatibility checking
PROTOCOL_VERSION = "1.0.0"

# Maximum message size (10 MB)
MAX_MESSAGE_SIZE = 10 * 1024 * 1024

# Header format: 4 bytes length (big-endian) + 1 byte type
HEADER_FORMAT = ">IB"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


class ProtocolError(Exception):
    """Base exception for protocol errors."""
    pass


class MessageTooLargeError(ProtocolError):
    """Message exceeds maximum allowed size."""
    pass


class InvalidMessageError(ProtocolError):
    """Message format is invalid."""
    pass


class UnknownMessageTypeError(ProtocolError):
    """Unknown message type received."""
    pass


class MessageType(IntEnum):
    """
    Message types for the coordination protocol.

    Categories:
        0x01-0x0F: Connection management
        0x10-0x1F: Cluster membership
        0x20-0x2F: Leader election
        0x30-0x3F: Log replication
        0x40-0x4F: State synchronization
        0xF0-0xFF: Reserved for extensions
    """

    # Connection management
    PING = 0x01
    PONG = 0x02
    HELLO = 0x03
    HELLO_ACK = 0x04
    GOODBYE = 0x05
    ERROR = 0x0F

    # Cluster membership
    JOIN_REQUEST = 0x10
    JOIN_RESPONSE = 0x11
    LEAVE_ANNOUNCE = 0x12
    MEMBER_LIST = 0x13
    MEMBER_UPDATE = 0x14

    # Leader election (Phase 2)
    VOTE_REQUEST = 0x20
    VOTE_RESPONSE = 0x21
    APPEND_ENTRIES = 0x22
    APPEND_ENTRIES_RESPONSE = 0x23
    LEADER_ANNOUNCE = 0x24

    # Log replication (Phase 3)
    LOG_APPEND = 0x30
    LOG_APPEND_ACK = 0x31
    LOG_SYNC_REQUEST = 0x32
    LOG_SYNC_RESPONSE = 0x33

    # State synchronization (Phase 3)
    STATE_SNAPSHOT = 0x40
    STATE_SNAPSHOT_ACK = 0x41


@dataclass
class Message:
    """
    Protocol message with type and payload.

    A message consists of a type identifier and an optional JSON payload.
    Messages are immutable after creation.

    Attributes:
        type: The message type (MessageType enum)
        payload: Optional dictionary payload (JSON-serializable)
        message_id: Unique identifier for this message (auto-generated)
        timestamp: Message creation timestamp (auto-generated)

    Example:
        # Create a ping message
        ping = Message(type=MessageType.PING)

        # Create a join request with payload
        join = Message(
            type=MessageType.JOIN_REQUEST,
            payload={
                "instance_id": "tempora-1",
                "host": "192.168.1.1",
                "port": 9500
            }
        )

        # Encode to bytes
        data = ping.encode()

        # Decode from bytes
        decoded = Message.decode(data)
    """

    type: MessageType
    payload: Dict[str, Any] = field(default_factory=dict)
    message_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: float = field(default_factory=time.time)

    def encode(self) -> bytes:
        """
        Encode message to wire format.

        Returns:
            bytes: Length-prefixed binary message

        Raises:
            MessageTooLargeError: If encoded message exceeds MAX_MESSAGE_SIZE
        """
        # Build payload with metadata
        full_payload = {
            "message_id": self.message_id,
            "timestamp": self.timestamp,
            **self.payload,
        }

        # Encode payload to JSON
        payload_bytes = json.dumps(full_payload, separators=(",", ":")).encode("utf-8")

        # Calculate total length (type byte + payload)
        total_length = 1 + len(payload_bytes)

        if total_length > MAX_MESSAGE_SIZE:
            raise MessageTooLargeError(
                f"Message size {total_length} exceeds maximum {MAX_MESSAGE_SIZE}"
            )

        # Pack header and payload
        header = struct.pack(HEADER_FORMAT, total_length, self.type.value)
        return header + payload_bytes

    @classmethod
    def decode(cls, data: bytes) -> "Message":
        """
        Decode message from wire format.

        Args:
            data: Raw bytes including header

        Returns:
            Message: Decoded message object

        Raises:
            InvalidMessageError: If message format is invalid
            UnknownMessageTypeError: If message type is not recognized
        """
        if len(data) < HEADER_SIZE:
            raise InvalidMessageError(
                f"Message too short: {len(data)} bytes, need at least {HEADER_SIZE}"
            )

        # Unpack header
        total_length, type_byte = struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])

        # Validate message type
        try:
            msg_type = MessageType(type_byte)
        except ValueError:
            raise UnknownMessageTypeError(f"Unknown message type: 0x{type_byte:02x}")

        # Extract and decode payload
        payload_bytes = data[HEADER_SIZE:]
        if len(payload_bytes) != total_length - 1:
            raise InvalidMessageError(
                f"Payload length mismatch: expected {total_length - 1}, got {len(payload_bytes)}"
            )

        if payload_bytes:
            try:
                full_payload = json.loads(payload_bytes.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                raise InvalidMessageError(f"Invalid JSON payload: {e}")
        else:
            full_payload = {}

        # Extract metadata
        message_id = full_payload.pop("message_id", str(uuid4()))
        timestamp = full_payload.pop("timestamp", time.time())

        return cls(
            type=msg_type,
            payload=full_payload,
            message_id=message_id,
            timestamp=timestamp,
        )

    @classmethod
    def read_header(cls, header_bytes: bytes) -> tuple[int, MessageType]:
        """
        Read and parse just the header.

        Useful for streaming reads where you need to know the payload
        length before reading the full message.

        Args:
            header_bytes: Exactly HEADER_SIZE bytes

        Returns:
            Tuple of (payload_length, message_type)

        Raises:
            InvalidMessageError: If header is invalid
        """
        if len(header_bytes) != HEADER_SIZE:
            raise InvalidMessageError(
                f"Header must be exactly {HEADER_SIZE} bytes, got {len(header_bytes)}"
            )

        total_length, type_byte = struct.unpack(HEADER_FORMAT, header_bytes)

        try:
            msg_type = MessageType(type_byte)
        except ValueError:
            raise UnknownMessageTypeError(f"Unknown message type: 0x{type_byte:02x}")

        # Payload length is total_length minus the type byte
        payload_length = total_length - 1

        return payload_length, msg_type

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary for debugging/logging."""
        return {
            "type": self.type.name,
            "type_code": f"0x{self.type.value:02x}",
            "message_id": self.message_id,
            "timestamp": self.timestamp,
            "payload": self.payload,
        }

    def __repr__(self) -> str:
        payload_preview = str(self.payload)[:50]
        if len(str(self.payload)) > 50:
            payload_preview += "..."
        return f"Message({self.type.name}, {payload_preview})"


# =============================================================================
# MESSAGE FACTORIES
# =============================================================================
# Convenience functions for creating common message types


def create_ping() -> Message:
    """Create a PING message for heartbeat."""
    return Message(type=MessageType.PING)


def create_pong(ping_message_id: str) -> Message:
    """Create a PONG response to a PING."""
    return Message(
        type=MessageType.PONG,
        payload={"in_response_to": ping_message_id}
    )


def create_hello(
    instance_id: str,
    host: str,
    port: int,
    protocol_version: str = PROTOCOL_VERSION,
    cluster_secret_hash: Optional[str] = None,
) -> Message:
    """
    Create a HELLO message for initial connection.

    The HELLO message is sent immediately after TCP connection to
    establish identity and verify protocol compatibility.
    """
    return Message(
        type=MessageType.HELLO,
        payload={
            "instance_id": instance_id,
            "host": host,
            "port": port,
            "protocol_version": protocol_version,
            "cluster_secret_hash": cluster_secret_hash,
        }
    )


def create_hello_ack(
    instance_id: str,
    accepted: bool,
    reason: Optional[str] = None,
) -> Message:
    """Create a HELLO_ACK response to a HELLO."""
    return Message(
        type=MessageType.HELLO_ACK,
        payload={
            "instance_id": instance_id,
            "accepted": accepted,
            "reason": reason,
        }
    )


def create_goodbye(instance_id: str, reason: str = "graceful_shutdown") -> Message:
    """Create a GOODBYE message for graceful disconnect."""
    return Message(
        type=MessageType.GOODBYE,
        payload={
            "instance_id": instance_id,
            "reason": reason,
        }
    )


def create_error(code: str, message: str, details: Optional[Dict] = None) -> Message:
    """Create an ERROR message."""
    return Message(
        type=MessageType.ERROR,
        payload={
            "code": code,
            "message": message,
            "details": details or {},
        }
    )


def create_join_request(
    instance_id: str,
    host: str,
    port: int,
    metadata: Optional[Dict] = None,
) -> Message:
    """Create a JOIN_REQUEST for cluster membership."""
    return Message(
        type=MessageType.JOIN_REQUEST,
        payload={
            "instance_id": instance_id,
            "host": host,
            "port": port,
            "metadata": metadata or {},
        }
    )


def create_join_response(
    accepted: bool,
    members: list[Dict[str, Any]],
    leader_id: Optional[str] = None,
    reason: Optional[str] = None,
) -> Message:
    """Create a JOIN_RESPONSE to a join request."""
    return Message(
        type=MessageType.JOIN_RESPONSE,
        payload={
            "accepted": accepted,
            "members": members,
            "leader_id": leader_id,
            "reason": reason,
        }
    )


def create_member_list(members: list[Dict[str, Any]]) -> Message:
    """Create a MEMBER_LIST broadcast."""
    return Message(
        type=MessageType.MEMBER_LIST,
        payload={"members": members}
    )


def create_member_update(
    instance_id: str,
    status: str,  # "joined", "left", "failed"
    host: Optional[str] = None,
    port: Optional[int] = None,
) -> Message:
    """Create a MEMBER_UPDATE notification."""
    return Message(
        type=MessageType.MEMBER_UPDATE,
        payload={
            "instance_id": instance_id,
            "status": status,
            "host": host,
            "port": port,
        }
    )
