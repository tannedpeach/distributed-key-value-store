# Distributed Key-Value Store

A collection of fault-tolerant distributed systems implemented in Go, progressing from distributed computation and primary-backup replication to Paxos consensus, sharding, and model checking.

Developed for Columbia University’s **COMS 4113: Distributed Systems**.

## Highlights

- Built a distributed **MapReduce** framework with a coordinating master and parallel workers
- Implemented a **primary-backup key-value service** with view management, state replication, and failover
- Built a replicated **key-value store backed by Paxos consensus**
- Extended the system into a **sharded key-value service** with dynamic reconfiguration and shard migration
- Implemented a **model checker** that explores message deliveries, drops, duplicates, network partitions, and timer interleavings
- Verified Paxos behavior across failure, partition, and concurrent-proposer scenarios

## System Components

### Distributed MapReduce

Implements a master-worker execution model for distributing map and reduce tasks across workers. The coordinator schedules tasks, manages worker availability, and collects intermediate results.

### Primary-Backup Replication

Implements a fault-tolerant key-value service using primary-backup replication. A view service tracks the active primary and backup while replicated operations maintain consistent state across failures and restarts.

### Paxos-Based Key-Value Store

Uses Paxos consensus to replicate client operations across servers. Replicas agree on a totally ordered sequence of operations, allowing the service to remain correct despite unreliable communication and server failures.

### Sharded Key-Value Store

Partitions keys across replica groups for horizontal scalability. The system supports configuration changes and shard migration while preserving correct client-visible behavior.

### Paxos Model Checker

Models the distributed system as a state machine and uses breadth-first search to explore possible event orderings, including:

- Normal, dropped, and duplicated message delivery
- Network partitions
- Independent timer events
- Multiple concurrent proposers
- Safety invariants
- Failure and non-termination scenarios

## Repository Structure

```text
.
├── src/
│   ├── mapreduce/       # Distributed MapReduce framework
│   ├── viewservice/     # Primary-backup view management
│   ├── pbservice/       # Primary-backup key-value service
│   ├── paxos/           # Paxos consensus implementation
│   ├── kvpaxos/         # Paxos-backed key-value service
│   ├── shardmaster/     # Shard configuration manager
│   └── shardkv/         # Sharded key-value service
├── hw5/
│   └── pkg/
│       ├── base/        # Model-checking framework
│       └── paxos/       # Paxos model and verification scenarios
└── instructions/        # Assignment specifications
```

## Technologies and Concepts

- **Language:** Go
- **Communication:** RPC and asynchronous message simulation
- **Distributed systems:** replication, consensus, partitioning, reconfiguration, and fault tolerance
- **Correctness:** idempotency, operation ordering, state-machine replication, and invariant checking
- **Testing:** unit tests, randomized failure tests, and state-space exploration

## Coursework

The repository contains five progressive distributed-systems assignments:

1. Distributed MapReduce
2. Primary-backup key-value service
3. Paxos-based key-value service
4. Sharded key-value service
5. Paxos model checking

Refer to the corresponding files in [`instructions/`](instructions/) for component-specific setup and testing requirements.
