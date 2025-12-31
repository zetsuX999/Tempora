"""
Tests for Tempora State Replication

Tests the Raft-based log replication implementation.

Standard: TEMPORA-HA-001 §5 (State Replication)
"""

import pytest
import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from tempora.distributed.replication import (
    ReplicationConfig,
    FollowerProgress,
    LogEntryData,
    StateReplicator,
    NotLeaderError,
)


class TestReplicationConfig:
    """Tests for ReplicationConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ReplicationConfig()

        assert config.max_entries_per_batch == 100
        assert config.replication_timeout_ms == 1000
        assert config.commit_check_interval_ms == 50
        assert config.snapshot_threshold == 10000

    def test_custom_config(self):
        """Test custom configuration."""
        config = ReplicationConfig(
            max_entries_per_batch=50,
            replication_timeout_ms=2000,
        )

        assert config.max_entries_per_batch == 50
        assert config.replication_timeout_ms == 2000


class TestFollowerProgress:
    """Tests for FollowerProgress."""

    def test_initial_state(self):
        """Test default follower progress."""
        progress = FollowerProgress(follower_id="follower-1")

        assert progress.follower_id == "follower-1"
        assert progress.next_index == 1
        assert progress.match_index == 0
        assert progress.probing is True
        assert progress.last_replicated is None

    def test_custom_state(self):
        """Test custom follower progress."""
        progress = FollowerProgress(
            follower_id="follower-2",
            next_index=10,
            match_index=9,
            probing=False,
        )

        assert progress.next_index == 10
        assert progress.match_index == 9
        assert progress.probing is False


class TestLogEntryData:
    """Tests for LogEntryData."""

    def test_create_entry(self):
        """Test creating log entry data."""
        entry = LogEntryData(
            index=1,
            term=1,
            command="TASK_CREATED",
            data={"task_id": "123"},
            tenant_id="tenant-1",
        )

        assert entry.index == 1
        assert entry.term == 1
        assert entry.command == "TASK_CREATED"
        assert entry.data == {"task_id": "123"}
        assert entry.tenant_id == "tenant-1"

    def test_to_dict(self):
        """Test conversion to dictionary."""
        entry = LogEntryData(
            index=5,
            term=2,
            command="TASK_COMPLETED",
            data={"task_id": "456", "result": "success"},
            tenant_id="tenant-2",
        )

        d = entry.to_dict()

        assert d["index"] == 5
        assert d["term"] == 2
        assert d["command"] == "TASK_COMPLETED"
        assert d["data"] == {"task_id": "456", "result": "success"}
        assert d["tenant_id"] == "tenant-2"

    def test_from_dict(self):
        """Test creation from dictionary."""
        d = {
            "index": 3,
            "term": 1,
            "command": "TASK_ASSIGNED",
            "data": {"target": "worker-1"},
            "tenant_id": "tenant-3",
        }

        entry = LogEntryData.from_dict(d)

        assert entry.index == 3
        assert entry.term == 1
        assert entry.command == "TASK_ASSIGNED"


class TestStateReplicator:
    """Tests for StateReplicator."""

    def test_initialization(self):
        """Test replicator initialization."""
        replicator = StateReplicator(
            instance_id="leader-1",
            config=ReplicationConfig(),
        )

        assert replicator.instance_id == "leader-1"
        assert replicator.commit_index == 0
        assert replicator.last_applied == 0

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self):
        """Test start and stop lifecycle."""
        replicator = StateReplicator(instance_id="leader-1")

        await replicator.start()
        assert replicator._running is True

        await replicator.stop()
        assert replicator._running is False

    def test_start_leader_replication(self):
        """Test starting leader replication."""
        get_peers = MagicMock(return_value=["follower-1", "follower-2"])

        replicator = StateReplicator(
            instance_id="leader-1",
            get_peers_callback=get_peers,
        )

        # Mock database access to avoid SynchronousOnlyOperation
        with patch.object(replicator, '_get_last_log_index', return_value=0):
            with patch.object(replicator, '_get_last_log_term', return_value=0):
                replicator.start_leader_replication()

                assert "follower-1" in replicator._follower_progress
                assert "follower-2" in replicator._follower_progress

    def test_stop_leader_replication(self):
        """Test stopping leader replication."""
        replicator = StateReplicator(instance_id="leader-1")
        replicator._follower_progress = {
            "follower-1": FollowerProgress(follower_id="follower-1"),
        }

        replicator.stop_leader_replication()

        assert len(replicator._follower_progress) == 0

    def test_add_follower(self):
        """Test adding a follower."""
        is_leader = MagicMock(return_value=True)
        replicator = StateReplicator(
            instance_id="leader-1",
            is_leader=is_leader,
        )

        # Mock database access to avoid SynchronousOnlyOperation
        with patch.object(replicator, '_get_last_log_index', return_value=0):
            with patch.object(replicator, '_get_last_log_term', return_value=0):
                replicator.add_follower("follower-1")

                assert "follower-1" in replicator._follower_progress

    def test_remove_follower(self):
        """Test removing a follower."""
        replicator = StateReplicator(instance_id="leader-1")
        replicator._follower_progress = {
            "follower-1": FollowerProgress(follower_id="follower-1"),
        }

        replicator.remove_follower("follower-1")

        assert "follower-1" not in replicator._follower_progress

    @pytest.mark.asyncio
    async def test_append_command_not_leader_raises(self):
        """Test that append_command raises when not leader."""
        is_leader = MagicMock(return_value=False)
        replicator = StateReplicator(
            instance_id="follower-1",
            is_leader=is_leader,
        )

        with pytest.raises(NotLeaderError):
            await replicator.append_command(
                tenant_id=uuid.uuid4(),
                command="TASK_CREATED",
                data={"task_id": "123"},
            )

    def test_handle_append_entries_response_success(self):
        """Test handling successful append entries response."""
        is_leader = MagicMock(return_value=True)
        replicator = StateReplicator(
            instance_id="leader-1",
            is_leader=is_leader,
        )
        replicator._follower_progress = {
            "follower-1": FollowerProgress(
                follower_id="follower-1",
                next_index=1,
                match_index=0,
            ),
        }

        # Mock database access for commit index update
        with patch.object(replicator, '_maybe_advance_commit'):
            replicator.handle_append_entries_response(
                follower_id="follower-1",
                success=True,
                match_index=5,
                term=1,
            )

            progress = replicator._follower_progress["follower-1"]
            assert progress.match_index == 5
            assert progress.next_index == 6
            assert progress.probing is False

    def test_handle_append_entries_response_failure(self):
        """Test handling failed append entries response."""
        is_leader = MagicMock(return_value=True)
        replicator = StateReplicator(
            instance_id="leader-1",
            is_leader=is_leader,
        )
        replicator._follower_progress = {
            "follower-1": FollowerProgress(
                follower_id="follower-1",
                next_index=10,
                match_index=0,
            ),
        }

        replicator.handle_append_entries_response(
            follower_id="follower-1",
            success=False,
            match_index=0,
            term=1,
        )

        progress = replicator._follower_progress["follower-1"]
        assert progress.next_index == 9  # Decremented
        assert progress.probing is True

    def test_get_status(self):
        """Test getting replication status."""
        replicator = StateReplicator(instance_id="leader-1")
        replicator._commit_index = 5
        replicator._last_applied = 4
        replicator._running = True

        status = replicator.get_status()

        assert status["instance_id"] == "leader-1"
        assert status["commit_index"] == 5
        assert status["last_applied"] == 4
        assert status["running"] is True


class TestNotLeaderError:
    """Tests for NotLeaderError."""

    def test_exception_message(self):
        """Test exception with message."""
        error = NotLeaderError("Not the leader")
        assert str(error) == "Not the leader"

    def test_exception_inheritance(self):
        """Test that it's an Exception."""
        assert issubclass(NotLeaderError, Exception)
