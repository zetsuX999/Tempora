# Changelog

All notable changes to Tempora will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-12-30

### Added

- **Raft Consensus Protocol**
  - Leader election with pre-vote optimization
  - Log replication with commit tracking
  - Term-based consistency guarantees
  - Automatic leader step-down on network partition

- **Coordination Layer**
  - TCP-based node communication
  - HMAC-SHA256 message authentication
  - Connection pooling with automatic reconnection
  - Health monitoring and heartbeat detection

- **Distributed Scheduling**
  - Work distribution strategies (round-robin, least-loaded, affinity)
  - Task persistence with PostgreSQL
  - Cron expression support
  - One-time and recurring task scheduling

- **High Availability**
  - Automatic failover with sub-second leader election
  - Split-brain prevention with fencing tokens
  - Graceful leadership transfer
  - Node health tracking and failure detection

- **Security**
  - TLS 1.3 support for cluster communication
  - Cluster secret authentication
  - Rate limiting for DoS protection
  - Connection limits per node

- **Django Integration**
  - Native Django ORM models
  - Database migrations
  - Management commands
  - Settings-based configuration

- **Production Hardening**
  - Comprehensive logging
  - Prometheus-compatible metrics
  - Health check endpoints
  - Graceful shutdown handling

### Technical Details

- 14,000+ lines of production code
- 191 unit and integration tests
- Full async/await support
- Type hints throughout codebase

## [Unreleased]

### Planned

- Snapshot and log compaction
- Dynamic cluster membership changes
- Web dashboard for monitoring
- Redis adapter for hybrid deployments
