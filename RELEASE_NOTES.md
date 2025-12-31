# Tempora v1.0.0 Release Notes

**Release Date:** December 30, 2024

## Overview

Tempora is a production-ready distributed task scheduler with native high availability for Python and Django. Unlike traditional task queues that require external coordination services, Tempora provides built-in Raft consensus for automatic leader election and failover.

## Highlights

- **Zero External Dependencies** - No Redis, ZooKeeper, or RabbitMQ required for coordination
- **Sub-second Failover** - Leader election completes in <300ms with pre-vote optimization
- **Battle-tested Raft Implementation** - Full consensus protocol with log replication and commit tracking
- **Django Native** - Seamless integration with Django ORM and PostgreSQL

## Features

### Raft Consensus Protocol
- Leader election with configurable timeouts (150-300ms default)
- Pre-vote optimization to prevent disruptive elections
- Term-based consistency guarantees
- Automatic leader step-down on network partition

### Distributed Coordination
- TCP-based node communication with connection pooling
- HMAC-SHA256 message authentication
- Automatic reconnection and health monitoring
- Heartbeat-based failure detection

### High Availability
- 3-node clusters tolerate 1 failure
- 5-node clusters tolerate 2 failures
- Split-brain prevention with fencing tokens
- Graceful leadership transfer for maintenance

### Work Distribution
- Round-robin scheduling
- Least-loaded worker selection
- Affinity-based task routing
- Cron expression support

### Security
- TLS 1.3 for cluster communication
- Cluster secret authentication (32+ bytes)
- Rate limiting for DoS protection
- Connection limits per node

## Requirements

- Python 3.11+
- Django 4.2+
- PostgreSQL 14+

## Installation

```bash
pip install tempora-scheduler
```

## Quick Start

```python
from tempora import TemporaCluster

cluster = TemporaCluster(
    node_id="node-1",
    cluster_secret="your-32-byte-secret-key",
    peers=[
        ("node-2", "10.0.0.2", 7000),
        ("node-3", "10.0.0.3", 7000),
    ]
)

await cluster.start()
```

## Metrics

| Metric | Value |
|--------|-------|
| Codebase | 14,000+ lines |
| Test Coverage | 191 tests passing |
| Leader Election | <300ms |
| Replication Lag | <10ms |

## License

Dual-licensed under MIT (open source) and Commercial (proprietary use).

## Links

- **Repository:** https://github.com/ewolters/Tempora
- **Documentation:** See README.md
- **Issues:** https://github.com/ewolters/Tempora/issues

---

**Full Changelog:** https://github.com/ewolters/Tempora/commits/master
