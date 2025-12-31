"""
Tests for Tempora Leader Election

Tests the Raft-based leader election implementation.

Standard: TEMPORA-HA-001 §4 (Leader Election)
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import time

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
from tempora.coordination.protocol import Message, MessageType


class TestNodeRole:
    """Tests for NodeRole enum."""

    def test_all_roles_defined(self):
        """Verify all expected roles are defined."""
        assert NodeRole.FOLLOWER.value == "follower"
        assert NodeRole.CANDIDATE.value == "candidate"
        assert NodeRole.LEADER.value == "leader"
        assert NodeRole.OBSERVER.value == "observer"


class TestElectionConfig:
    """Tests for ElectionConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ElectionConfig()

        assert config.election_timeout_min_ms == 150
        assert config.election_timeout_max_ms == 300
        assert config.heartbeat_interval_ms == 50
        assert config.enable_pre_vote is True
        assert config.lease_duration_ms == 100
        assert config.max_elections_per_minute == 20

    def test_custom_config(self):
        """Test custom configuration."""
        config = ElectionConfig(
            election_timeout_min_ms=200,
            election_timeout_max_ms=400,
            heartbeat_interval_ms=100,
        )

        assert config.election_timeout_min_ms == 200
        assert config.election_timeout_max_ms == 400
        assert config.heartbeat_interval_ms == 100


class TestElectionState:
    """Tests for ElectionState."""

    def test_initial_state(self):
        """Test default election state."""
        state = ElectionState()

        assert state.current_term == 0
        assert state.voted_for is None
        assert state.role == NodeRole.FOLLOWER
        assert state.leader_id is None
        assert state.last_leader_contact is None

    def test_to_dict(self):
        """Test conversion to dictionary."""
        state = ElectionState(
            current_term=5,
            voted_for="node-1",
            role=NodeRole.CANDIDATE,
            leader_id="node-0",
        )

        d = state.to_dict()

        assert d["current_term"] == 5
        assert d["voted_for"] == "node-1"
        assert d["role"] == "candidate"
        assert d["leader_id"] == "node-0"


class TestVoteRequest:
    """Tests for VoteRequest."""

    def test_create_vote_request(self):
        """Test creating a vote request."""
        request = VoteRequest(
            term=3,
            candidate_id="node-1",
            last_log_index=10,
            last_log_term=2,
        )

        assert request.term == 3
        assert request.candidate_id == "node-1"
        assert request.last_log_index == 10
        assert request.last_log_term == 2
        assert request.is_pre_vote is False

    def test_to_payload(self):
        """Test converting to payload."""
        request = VoteRequest(
            term=3,
            candidate_id="node-1",
        )

        payload = request.to_payload()

        assert payload["term"] == 3
        assert payload["candidate_id"] == "node-1"
        assert payload["is_pre_vote"] is False

    def test_from_payload(self):
        """Test creating from payload."""
        payload = {
            "term": 5,
            "candidate_id": "node-2",
            "last_log_index": 20,
            "last_log_term": 4,
            "is_pre_vote": True,
        }

        request = VoteRequest.from_payload(payload)

        assert request.term == 5
        assert request.candidate_id == "node-2"
        assert request.last_log_index == 20
        assert request.last_log_term == 4
        assert request.is_pre_vote is True

    def test_from_payload_defaults(self):
        """Test from_payload with missing optional fields."""
        payload = {
            "term": 1,
            "candidate_id": "node-1",
        }

        request = VoteRequest.from_payload(payload)

        assert request.last_log_index == 0
        assert request.last_log_term == 0
        assert request.is_pre_vote is False


class TestVoteResponse:
    """Tests for VoteResponse."""

    def test_create_vote_response(self):
        """Test creating a vote response."""
        response = VoteResponse(
            term=3,
            vote_granted=True,
            voter_id="node-2",
        )

        assert response.term == 3
        assert response.vote_granted is True
        assert response.voter_id == "node-2"

    def test_roundtrip(self):
        """Test payload roundtrip."""
        original = VoteResponse(
            term=5,
            vote_granted=False,
            voter_id="node-3",
            is_pre_vote=True,
        )

        payload = original.to_payload()
        restored = VoteResponse.from_payload(payload)

        assert restored.term == original.term
        assert restored.vote_granted == original.vote_granted
        assert restored.voter_id == original.voter_id
        assert restored.is_pre_vote == original.is_pre_vote


class TestAppendEntries:
    """Tests for AppendEntries."""

    def test_create_heartbeat(self):
        """Test creating a heartbeat (empty AppendEntries)."""
        ae = AppendEntries(
            term=5,
            leader_id="leader-1",
        )

        assert ae.term == 5
        assert ae.leader_id == "leader-1"
        assert ae.entries == []
        assert ae.leader_commit == 0

    def test_with_entries(self):
        """Test AppendEntries with log entries."""
        entries = [
            {"index": 1, "term": 5, "command": "SET x 1"},
            {"index": 2, "term": 5, "command": "SET y 2"},
        ]

        ae = AppendEntries(
            term=5,
            leader_id="leader-1",
            prev_log_index=0,
            prev_log_term=0,
            entries=entries,
            leader_commit=1,
        )

        assert len(ae.entries) == 2
        assert ae.leader_commit == 1

    def test_roundtrip(self):
        """Test payload roundtrip."""
        original = AppendEntries(
            term=10,
            leader_id="leader-2",
            prev_log_index=5,
            prev_log_term=8,
            entries=[{"index": 6, "term": 10, "data": "test"}],
            leader_commit=5,
        )

        payload = original.to_payload()
        restored = AppendEntries.from_payload(payload)

        assert restored.term == original.term
        assert restored.leader_id == original.leader_id
        assert restored.prev_log_index == original.prev_log_index
        assert restored.prev_log_term == original.prev_log_term
        assert restored.entries == original.entries
        assert restored.leader_commit == original.leader_commit


class TestAppendEntriesResponse:
    """Tests for AppendEntriesResponse."""

    def test_success_response(self):
        """Test successful response."""
        response = AppendEntriesResponse(
            term=5,
            success=True,
            follower_id="follower-1",
            match_index=10,
        )

        assert response.success is True
        assert response.match_index == 10

    def test_failure_response(self):
        """Test failure response."""
        response = AppendEntriesResponse(
            term=6,
            success=False,
            follower_id="follower-2",
        )

        assert response.success is False
        assert response.match_index == 0


class TestLeaderElector:
    """Tests for LeaderElector."""

    @pytest.fixture
    def elector(self):
        """Create an elector for testing."""
        return LeaderElector(
            instance_id="test-node",
            config=ElectionConfig(
                election_timeout_min_ms=50,
                election_timeout_max_ms=100,
                heartbeat_interval_ms=25,
            ),
        )

    def test_initial_state(self, elector):
        """Test initial elector state."""
        assert elector.instance_id == "test-node"
        assert elector.is_leader is False
        assert elector.is_follower is True
        assert elector.is_candidate is False
        assert elector.current_term == 0
        assert elector.leader_id is None

    def test_properties(self, elector):
        """Test role property checks."""
        elector.state.role = NodeRole.LEADER
        assert elector.is_leader is True
        assert elector.is_follower is False
        assert elector.is_candidate is False

        elector.state.role = NodeRole.CANDIDATE
        assert elector.is_leader is False
        assert elector.is_follower is False
        assert elector.is_candidate is True

    def test_set_callbacks(self, elector):
        """Test setting callbacks."""
        send_cb = AsyncMock()
        broadcast_cb = AsyncMock()
        get_peers_cb = MagicMock(return_value=["peer-1", "peer-2"])

        elector.set_send_callback(send_cb)
        elector.set_broadcast_callback(broadcast_cb)
        elector.set_get_peers_callback(get_peers_cb)

        assert elector._send_callback == send_cb
        assert elector._broadcast_callback == broadcast_cb
        assert elector._get_peers_callback == get_peers_cb

    def test_get_status(self, elector):
        """Test getting elector status."""
        elector.state.current_term = 5
        elector.state.role = NodeRole.FOLLOWER
        elector.state.leader_id = "leader-1"

        status = elector.get_status()

        assert status["instance_id"] == "test-node"
        assert status["role"] == "follower"
        assert status["current_term"] == 5
        assert status["leader_id"] == "leader-1"
        assert status["is_leader"] is False


class TestLeaderElectorAsync:
    """Async tests for LeaderElector."""

    @pytest.fixture
    def elector(self):
        """Create an elector for testing."""
        return LeaderElector(
            instance_id="test-node",
            config=ElectionConfig(
                election_timeout_min_ms=100,
                election_timeout_max_ms=150,
                heartbeat_interval_ms=50,
            ),
        )

    @pytest.mark.asyncio
    async def test_start_stop(self, elector):
        """Test starting and stopping the elector."""
        await elector.start()
        assert elector._running is True
        assert elector.state.role == NodeRole.FOLLOWER

        await elector.stop()
        assert elector._running is False

    @pytest.mark.asyncio
    async def test_handle_vote_request_grant(self, elector):
        """Test handling a vote request - granted."""
        await elector.start()

        # Create vote request
        message = Message(
            type=MessageType.VOTE_REQUEST,
            payload={
                "term": 1,
                "candidate_id": "candidate-1",
                "last_log_index": 0,
                "last_log_term": 0,
            },
        )

        response = await elector.handle_vote_request("candidate-1", message)

        assert response is not None
        assert response.type == MessageType.VOTE_RESPONSE
        assert response.payload["vote_granted"] is True
        assert response.payload["voter_id"] == "test-node"
        assert elector.state.voted_for == "candidate-1"

        await elector.stop()

    @pytest.mark.asyncio
    async def test_handle_vote_request_deny_already_voted(self, elector):
        """Test denying vote when already voted for another candidate."""
        await elector.start()
        elector.state.current_term = 1
        elector.state.voted_for = "other-candidate"

        message = Message(
            type=MessageType.VOTE_REQUEST,
            payload={
                "term": 1,
                "candidate_id": "candidate-1",
                "last_log_index": 0,
                "last_log_term": 0,
            },
        )

        response = await elector.handle_vote_request("candidate-1", message)

        assert response.payload["vote_granted"] is False

        await elector.stop()

    @pytest.mark.asyncio
    async def test_handle_vote_request_deny_old_term(self, elector):
        """Test denying vote for old term."""
        await elector.start()
        elector.state.current_term = 5

        message = Message(
            type=MessageType.VOTE_REQUEST,
            payload={
                "term": 3,  # Old term
                "candidate_id": "candidate-1",
                "last_log_index": 0,
                "last_log_term": 0,
            },
        )

        response = await elector.handle_vote_request("candidate-1", message)

        assert response.payload["vote_granted"] is False

        await elector.stop()

    @pytest.mark.asyncio
    async def test_handle_vote_request_updates_term(self, elector):
        """Test that receiving higher term updates our term."""
        await elector.start()
        elector.state.current_term = 1

        message = Message(
            type=MessageType.VOTE_REQUEST,
            payload={
                "term": 5,  # Higher term
                "candidate_id": "candidate-1",
                "last_log_index": 0,
                "last_log_term": 0,
            },
        )

        await elector.handle_vote_request("candidate-1", message)

        assert elector.state.current_term == 5
        assert elector.state.role == NodeRole.FOLLOWER

        await elector.stop()

    @pytest.mark.asyncio
    async def test_handle_append_entries_heartbeat(self, elector):
        """Test handling heartbeat from leader."""
        await elector.start()

        message = Message(
            type=MessageType.APPEND_ENTRIES,
            payload={
                "term": 1,
                "leader_id": "leader-1",
                "prev_log_index": 0,
                "prev_log_term": 0,
                "entries": [],
                "leader_commit": 0,
            },
        )

        response = await elector.handle_append_entries("leader-1", message)

        assert response is not None
        assert response.type == MessageType.APPEND_ENTRIES_RESPONSE
        assert response.payload["success"] is True
        assert elector.state.leader_id == "leader-1"

        await elector.stop()

    @pytest.mark.asyncio
    async def test_handle_append_entries_step_down_candidate(self, elector):
        """Test that candidate steps down on receiving heartbeat."""
        await elector.start()
        elector.state.role = NodeRole.CANDIDATE
        elector.state.current_term = 1

        message = Message(
            type=MessageType.APPEND_ENTRIES,
            payload={
                "term": 1,
                "leader_id": "leader-1",
                "prev_log_index": 0,
                "prev_log_term": 0,
                "entries": [],
                "leader_commit": 0,
            },
        )

        await elector.handle_append_entries("leader-1", message)

        assert elector.state.role == NodeRole.FOLLOWER
        assert elector.state.leader_id == "leader-1"

        await elector.stop()

    @pytest.mark.asyncio
    async def test_handle_append_entries_reject_old_term(self, elector):
        """Test rejecting append entries from old term."""
        await elector.start()
        elector.state.current_term = 5

        message = Message(
            type=MessageType.APPEND_ENTRIES,
            payload={
                "term": 3,  # Old term
                "leader_id": "old-leader",
                "prev_log_index": 0,
                "prev_log_term": 0,
                "entries": [],
                "leader_commit": 0,
            },
        )

        response = await elector.handle_append_entries("old-leader", message)

        assert response.payload["success"] is False
        assert elector.state.leader_id is None

        await elector.stop()

    @pytest.mark.asyncio
    async def test_role_change_callback(self, elector):
        """Test that role change callbacks are invoked."""
        callback_args = []

        async def on_role_change(old_role, new_role):
            callback_args.append((old_role, new_role))

        elector.on_role_change(on_role_change)
        await elector.start()

        # Trigger role change by receiving heartbeat with higher term as candidate
        elector.state.role = NodeRole.CANDIDATE
        elector.state.current_term = 1

        message = Message(
            type=MessageType.APPEND_ENTRIES,
            payload={
                "term": 2,
                "leader_id": "leader-1",
                "prev_log_index": 0,
                "prev_log_term": 0,
                "entries": [],
                "leader_commit": 0,
            },
        )

        await elector.handle_append_entries("leader-1", message)

        assert len(callback_args) > 0
        assert callback_args[-1][1] == NodeRole.FOLLOWER

        await elector.stop()

    @pytest.mark.asyncio
    async def test_leader_change_callback(self, elector):
        """Test that leader change callbacks are invoked."""
        callback_args = []

        async def on_leader_change(leader_id):
            callback_args.append(leader_id)

        elector.on_leader_change(on_leader_change)
        await elector.start()

        # Receive heartbeat from leader
        message = Message(
            type=MessageType.APPEND_ENTRIES,
            payload={
                "term": 1,
                "leader_id": "leader-1",
                "prev_log_index": 0,
                "prev_log_term": 0,
                "entries": [],
                "leader_commit": 0,
            },
        )

        await elector.handle_append_entries("leader-1", message)

        assert "leader-1" in callback_args

        await elector.stop()

    @pytest.mark.asyncio
    async def test_become_leader(self, elector):
        """Test becoming leader after receiving majority votes."""
        broadcast_mock = AsyncMock()
        get_peers_mock = MagicMock(return_value=["peer-1", "peer-2"])

        elector.set_broadcast_callback(broadcast_mock)
        elector.set_get_peers_callback(get_peers_mock)

        await elector.start()

        # Manually trigger election
        elector.state.current_term = 0
        elector.state.role = NodeRole.CANDIDATE
        elector.state.current_term = 1
        elector.state.voted_for = elector.instance_id
        elector._votes_received = {elector.instance_id}

        # Receive vote from peer-1
        vote_response = Message(
            type=MessageType.VOTE_RESPONSE,
            payload={
                "term": 1,
                "vote_granted": True,
                "voter_id": "peer-1",
            },
        )

        await elector.handle_vote_response("peer-1", vote_response)

        # With 2 votes (self + peer-1) out of 3 nodes, we have majority
        assert elector.is_leader is True
        assert elector.state.leader_id == elector.instance_id

        await elector.stop()

    @pytest.mark.asyncio
    async def test_step_down(self, elector):
        """Test stepping down from leader."""
        await elector.start()
        elector.state.role = NodeRole.LEADER
        elector.state.leader_id = elector.instance_id
        elector._heartbeat_task = asyncio.create_task(asyncio.sleep(100))

        await elector.step_down("test reason")

        assert elector.state.role == NodeRole.FOLLOWER
        assert elector.state.leader_id is None

        await elector.stop()

    @pytest.mark.asyncio
    async def test_load_state(self, elector):
        """Test loading persisted state."""
        saved_state = ElectionState(
            current_term=10,
            voted_for="node-5",
            role=NodeRole.FOLLOWER,
            leader_id="leader-old",
        )

        await elector.load_state(saved_state)

        assert elector.state.current_term == 10
        assert elector.state.voted_for == "node-5"


class TestQuorumCalculation:
    """Tests for quorum calculation."""

    @pytest.fixture
    def elector(self):
        return LeaderElector(instance_id="test-node")

    def test_single_node_quorum(self, elector):
        """Single node cluster always has quorum."""
        # No peers callback - single node
        assert elector._quorum_size() == 1

    def test_three_node_quorum(self, elector):
        """3-node cluster needs 2 for quorum."""
        elector.set_get_peers_callback(lambda: ["peer-1", "peer-2"])
        assert elector._quorum_size() == 2

    def test_five_node_quorum(self, elector):
        """5-node cluster needs 3 for quorum."""
        elector.set_get_peers_callback(lambda: ["p1", "p2", "p3", "p4"])
        assert elector._quorum_size() == 3

    def test_has_quorum_single_node(self, elector):
        """Single node always has quorum."""
        elector._votes_received = {elector.instance_id}
        assert elector._has_quorum() is True

    def test_has_quorum_three_nodes(self, elector):
        """Three node cluster quorum check."""
        elector.set_get_peers_callback(lambda: ["peer-1", "peer-2"])

        # Only self voted - no quorum
        elector._votes_received = {elector.instance_id}
        assert elector._has_quorum() is False

        # Self + one peer - has quorum
        elector._votes_received = {elector.instance_id, "peer-1"}
        assert elector._has_quorum() is True


class TestElectionRateLimiting:
    """Tests for election rate limiting."""

    @pytest.fixture
    def elector(self):
        return LeaderElector(
            instance_id="test-node",
            config=ElectionConfig(max_elections_per_minute=3),
        )

    def test_rate_limit_allows_initial_elections(self, elector):
        """Rate limiting should allow initial elections."""
        assert elector._can_start_election() is True
        assert elector._can_start_election() is True
        assert elector._can_start_election() is True

    def test_rate_limit_blocks_excessive_elections(self, elector):
        """Rate limiting should block excessive elections."""
        # Fill up the limit
        elector._can_start_election()
        elector._can_start_election()
        elector._can_start_election()

        # Fourth should be blocked
        assert elector._can_start_election() is False

    def test_rate_limit_expires(self, elector):
        """Old elections should expire from rate limit."""
        # Add old elections (more than 60 seconds ago)
        old_time = time.time() - 120
        elector._recent_elections = [old_time, old_time, old_time]

        # Should allow new election since old ones expired
        assert elector._can_start_election() is True
