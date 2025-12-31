"""
Tests for Tempora Coordination Protocol

Tests the wire protocol message encoding/decoding.

Standard: TEMPORA-HA-001 §3.2 (Wire Protocol)
"""

import pytest
import time
from uuid import uuid4

from tempora.coordination.protocol import (
    Message,
    MessageType,
    PROTOCOL_VERSION,
    HEADER_SIZE,
    MAX_MESSAGE_SIZE,
    ProtocolError,
    InvalidMessageError,
    MessageTooLargeError,
    UnknownMessageTypeError,
    create_ping,
    create_pong,
    create_hello,
    create_hello_ack,
    create_goodbye,
    create_error,
    create_join_request,
    create_join_response,
    create_member_list,
    create_member_update,
)


class TestMessageType:
    """Tests for MessageType enum."""

    def test_message_types_have_unique_values(self):
        """Each message type should have a unique value."""
        values = [mt.value for mt in MessageType]
        assert len(values) == len(set(values))

    def test_ping_pong_in_connection_range(self):
        """PING and PONG should be in 0x01-0x0F range."""
        assert 0x01 <= MessageType.PING.value <= 0x0F
        assert 0x01 <= MessageType.PONG.value <= 0x0F

    def test_join_in_cluster_range(self):
        """JOIN messages should be in 0x10-0x1F range."""
        assert 0x10 <= MessageType.JOIN_REQUEST.value <= 0x1F
        assert 0x10 <= MessageType.JOIN_RESPONSE.value <= 0x1F

    def test_vote_in_election_range(self):
        """Vote messages should be in 0x20-0x2F range."""
        assert 0x20 <= MessageType.VOTE_REQUEST.value <= 0x2F
        assert 0x20 <= MessageType.VOTE_RESPONSE.value <= 0x2F


class TestMessage:
    """Tests for Message class."""

    def test_create_message(self):
        """Test basic message creation."""
        msg = Message(type=MessageType.PING)
        assert msg.type == MessageType.PING
        assert isinstance(msg.message_id, str)
        assert isinstance(msg.timestamp, float)
        assert msg.payload == {}

    def test_create_message_with_payload(self):
        """Test message creation with payload."""
        payload = {"key": "value", "number": 42}
        msg = Message(type=MessageType.HELLO, payload=payload)
        assert msg.payload == payload

    def test_encode_decode_roundtrip(self):
        """Test encoding and decoding produces identical message."""
        original = Message(
            type=MessageType.HELLO,
            payload={"instance_id": "test-1", "host": "localhost", "port": 9500}
        )

        encoded = original.encode()
        decoded = Message.decode(encoded)

        assert decoded.type == original.type
        assert decoded.message_id == original.message_id
        assert decoded.timestamp == original.timestamp
        assert decoded.payload == original.payload

    def test_encode_produces_bytes(self):
        """Test that encode produces bytes."""
        msg = Message(type=MessageType.PING)
        encoded = msg.encode()
        assert isinstance(encoded, bytes)

    def test_encode_length_prefix(self):
        """Test that encoded message has correct length prefix."""
        msg = Message(type=MessageType.PING)
        encoded = msg.encode()

        # First 4 bytes are length (big-endian)
        import struct
        length = struct.unpack(">I", encoded[:4])[0]

        # Length should be total minus the 4-byte header
        assert length == len(encoded) - 4

    def test_decode_invalid_short_message(self):
        """Test decoding too-short message raises error."""
        with pytest.raises(InvalidMessageError):
            Message.decode(b"\x00\x00")

    def test_decode_unknown_type(self):
        """Test decoding unknown message type raises error."""
        import struct
        # Type 0xFF is not defined
        data = struct.pack(">IB", 1, 0xFF) + b""
        with pytest.raises(UnknownMessageTypeError):
            Message.decode(data)

    def test_decode_invalid_json(self):
        """Test decoding invalid JSON raises error."""
        import struct
        # Valid header but invalid JSON payload
        bad_json = b"not json"
        data = struct.pack(">IB", 1 + len(bad_json), MessageType.PING.value) + bad_json
        with pytest.raises(InvalidMessageError):
            Message.decode(data)

    def test_message_too_large(self):
        """Test that oversized message raises error."""
        # Create a huge payload
        huge_payload = {"data": "x" * (MAX_MESSAGE_SIZE + 1)}
        msg = Message(type=MessageType.HELLO, payload=huge_payload)

        with pytest.raises(MessageTooLargeError):
            msg.encode()

    def test_read_header(self):
        """Test reading just the header."""
        msg = Message(type=MessageType.PING)
        encoded = msg.encode()

        payload_length, msg_type = Message.read_header(encoded[:HEADER_SIZE])

        assert msg_type == MessageType.PING
        assert payload_length >= 0

    def test_to_dict(self):
        """Test converting message to dictionary."""
        msg = Message(type=MessageType.PING)
        d = msg.to_dict()

        assert d["type"] == "PING"
        assert "type_code" in d
        assert "message_id" in d
        assert "timestamp" in d
        assert "payload" in d


class TestMessageFactories:
    """Tests for message factory functions."""

    def test_create_ping(self):
        """Test creating a PING message."""
        msg = create_ping()
        assert msg.type == MessageType.PING

    def test_create_pong(self):
        """Test creating a PONG message."""
        ping_id = str(uuid4())
        msg = create_pong(ping_id)

        assert msg.type == MessageType.PONG
        assert msg.payload["in_response_to"] == ping_id

    def test_create_hello(self):
        """Test creating a HELLO message."""
        msg = create_hello(
            instance_id="test-1",
            host="localhost",
            port=9500,
        )

        assert msg.type == MessageType.HELLO
        assert msg.payload["instance_id"] == "test-1"
        assert msg.payload["host"] == "localhost"
        assert msg.payload["port"] == 9500
        assert msg.payload["protocol_version"] == PROTOCOL_VERSION

    def test_create_hello_with_secret(self):
        """Test creating a HELLO message with cluster secret hash."""
        msg = create_hello(
            instance_id="test-1",
            host="localhost",
            port=9500,
            cluster_secret_hash="abc123",
        )

        assert msg.payload["cluster_secret_hash"] == "abc123"

    def test_create_hello_ack(self):
        """Test creating a HELLO_ACK message."""
        msg = create_hello_ack(
            instance_id="server-1",
            accepted=True,
        )

        assert msg.type == MessageType.HELLO_ACK
        assert msg.payload["instance_id"] == "server-1"
        assert msg.payload["accepted"] is True

    def test_create_hello_ack_rejected(self):
        """Test creating a rejected HELLO_ACK message."""
        msg = create_hello_ack(
            instance_id="server-1",
            accepted=False,
            reason="Authentication failed",
        )

        assert msg.payload["accepted"] is False
        assert msg.payload["reason"] == "Authentication failed"

    def test_create_goodbye(self):
        """Test creating a GOODBYE message."""
        msg = create_goodbye("test-1", "shutdown")

        assert msg.type == MessageType.GOODBYE
        assert msg.payload["instance_id"] == "test-1"
        assert msg.payload["reason"] == "shutdown"

    def test_create_error(self):
        """Test creating an ERROR message."""
        msg = create_error("AUTH_FAILED", "Invalid credentials")

        assert msg.type == MessageType.ERROR
        assert msg.payload["code"] == "AUTH_FAILED"
        assert msg.payload["message"] == "Invalid credentials"

    def test_create_join_request(self):
        """Test creating a JOIN_REQUEST message."""
        msg = create_join_request(
            instance_id="test-1",
            host="localhost",
            port=9500,
            metadata={"version": "1.0.0"},
        )

        assert msg.type == MessageType.JOIN_REQUEST
        assert msg.payload["instance_id"] == "test-1"
        assert msg.payload["metadata"]["version"] == "1.0.0"

    def test_create_join_response(self):
        """Test creating a JOIN_RESPONSE message."""
        members = [
            {"instance_id": "server-1", "host": "192.168.1.1", "port": 9500},
            {"instance_id": "server-2", "host": "192.168.1.2", "port": 9500},
        ]
        msg = create_join_response(
            accepted=True,
            members=members,
            leader_id="server-1",
        )

        assert msg.type == MessageType.JOIN_RESPONSE
        assert msg.payload["accepted"] is True
        assert len(msg.payload["members"]) == 2
        assert msg.payload["leader_id"] == "server-1"

    def test_create_member_list(self):
        """Test creating a MEMBER_LIST message."""
        members = [{"instance_id": "test-1"}]
        msg = create_member_list(members)

        assert msg.type == MessageType.MEMBER_LIST
        assert msg.payload["members"] == members

    def test_create_member_update(self):
        """Test creating a MEMBER_UPDATE message."""
        msg = create_member_update(
            instance_id="test-1",
            status="joined",
            host="localhost",
            port=9500,
        )

        assert msg.type == MessageType.MEMBER_UPDATE
        assert msg.payload["instance_id"] == "test-1"
        assert msg.payload["status"] == "joined"


class TestProtocolCompatibility:
    """Tests for protocol compatibility."""

    def test_protocol_version_format(self):
        """Test protocol version follows semver format."""
        parts = PROTOCOL_VERSION.split(".")
        assert len(parts) == 3
        assert all(p.isdigit() for p in parts)

    def test_header_size_is_5_bytes(self):
        """Header should be 4 bytes length + 1 byte type."""
        assert HEADER_SIZE == 5

    def test_all_message_types_can_be_created(self):
        """All message types should be creatable."""
        for msg_type in MessageType:
            msg = Message(type=msg_type)
            assert msg.type == msg_type

    def test_all_message_types_roundtrip(self):
        """All message types should encode/decode correctly."""
        for msg_type in MessageType:
            msg = Message(type=msg_type, payload={"test": True})
            encoded = msg.encode()
            decoded = Message.decode(encoded)
            assert decoded.type == msg_type
