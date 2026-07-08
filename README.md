# Mini Kafka — Distributed Message Broker from Scratch

A message broker built from scratch in Go, implementing the core architecture of Apache Kafka — append-only segmented logs, partitioned topics, consumer groups with sticky rebalancing, and a custom binary TCP protocol.

## What This Is

The internal engine of a distributed message broker: durable storage, network protocol, consumer coordination. Built to understand (and demonstrate) how systems like Kafka work beneath the abstraction.

**Key results:**
- Zero data loss on kill -9 mid-write (verified by crash-recovery tests)
- StickyAssignor minimizes partition movement on rebalance (vs round-robin)
- Custom binary TCP protocol serving concurrent clients via goroutine-per-connection

## Architecture

```
┌──────────────┐         TCP          ┌──────────────────────────────────┐
│  Producer    │ ──── binary wire ───▶ │          BROKER                  │
│  (CLI)       │                       │                                  │
└──────────────┘                       │  ┌─ topic "orders" ───────────┐  │
                                       │  │  partition-0/ (Log)        │  │
┌──────────────┐         TCP           │  │    ├─ segment-000.log      │  │
│  Consumer    │ ──── binary wire ───▶ │  │    └─ segment-500.log      │  │
│  (CLI)       │                       │  │  partition-1/ (Log)        │  │
└──────────────┘                       │  │  partition-2/ (Log)        │  │
                                       │  └────────────────────────────┘  │
                                       │                                  │
                                       │  ┌─ Coordinator ─────────────┐  │
                                       │  │  Consumer groups           │  │
                                       │  │  Sticky assignment         │  │
                                       │  │  Heartbeat failure detect  │  │
                                       │  └────────────────────────────┘  │
                                       └──────────────────────────────────┘
```

## Quick Start

```bash
# Build
go build -o mini-kafka ./cmd/mini-kafka/

# Terminal 1: Start the broker
./mini-kafka broker

# Terminal 2: Create a topic and produce messages
./mini-kafka create-topic --topic orders --partitions 3
./mini-kafka produce --topic orders --key customer-42 --value "order #101"
./mini-kafka produce --topic orders --key customer-42 --value "order #102"

# Terminal 3: Consume messages
./mini-kafka consume --topic orders --partition 1 --offset 0
./mini-kafka consume --topic orders --partition 1 --offset 1

# Consumer offset management
./mini-kafka commit --group checkout-service --topic orders --partition 1 --offset 2
./mini-kafka fetch-offset --group checkout-service --topic orders --partition 1
```

## Crash Recovery Demo

```bash
./crash_demo.sh
```

Produces messages → kills the broker with `kill -9` → restarts → verifies all messages and consumer offsets survived. This is the core durability guarantee: per-write `fsync` ensures no acknowledged message is ever lost.

## How It Works

### Storage Engine

Each partition is an append-only log split into segment files:

```
data/orders/partition-0/
├── segment-0000000000000000000000.log   (10 MB, closed — read only)
├── segment-0000000000000000005000.log   (10 MB, closed — read only)
└── segment-0000000000000000010000.log   (active — writes go here)
```

Each message on disk:
```
[4-byte size][payload bytes]
```

- **Append:** write at end of active segment → fsync → add to in-memory index
- **Read:** look up `index[offset]` → seek to byte position → read size → read payload
- **Crash recovery:** on startup, scan segments byte-by-byte, rebuild index, truncate any torn trailing write
- **Retention:** when total size exceeds cap, delete oldest segment file (instant, atomic)

### Wire Protocol

Custom binary protocol with length-prefixed framing:

```
Frame:   [4 bytes: body length][body bytes]
Body:    [1 byte: request type][fields...]
Fields:  [2-byte len][string] for names, [4/8 bytes] for numbers
```

Request types: PRODUCE (1), CONSUME (2), CREATE_TOPIC (3), COMMIT (4), FETCH_OFFSET (5)

### Consumer Groups (StickyAssignor)

Partition assignment algorithm that minimizes partition movements on rebalance:

```
Before:  A=[0,1,2], B=[3,4,5]     (2 consumers, 6 partitions)
C joins: A=[0,1],   B=[3,4], C=[2,5]

A kept 2/3 original partitions. B kept 2/3. Only excess moved to C.
(Round-robin would shuffle ALL assignments — every consumer pauses.)
```

Failure detection via heartbeat timeout: if a member doesn't heartbeat within 10 seconds, it's declared dead and its partitions are redistributed to surviving members.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Append-only (no edits/inserts) | Sequential I/O is 50–500× faster than random |
| Per-write fsync | Acknowledged messages survive any crash (power loss, kill -9) |
| Length-prefixed binary protocol | No delimiter ambiguity, works with any payload content |
| Goroutine-per-connection | Cheap (4 KB stack), simple, scales to thousands |
| Sticky assignment | Minimizes rebalance disruption vs round-robin |
| Segments + retention | Bounded disk usage; old data deleted by removing one file |
| Key-based partition routing | Same key → same partition → per-key ordering guaranteed |

## What's Not Implemented (and Why)

| Feature | Why skipped |
|---------|-------------|
| **Replication** | Requires multiple brokers, leader election, ISR tracking. Single-node durability (fsync) is what each broker does internally; replication is the layer above. |
| **Exactly-once delivery** | Requires idempotent producers (PID + sequence dedup) and two-phase commit transactions. We provide at-least-once. |
| **Log compaction** | Planned. Keeps latest value per key for state-snapshot use cases. |
| **Zero-copy reads (sendfile)** | OS optimization that skips userspace copy. Straightforward to add but not architecturally interesting. |
| **Producer batching** | Planned. Would increase throughput ~100× by amortizing fsync over many messages. |

## Project Structure

```
├── segment.go          ← Single segment file (append, read, rebuild index)
├── log.go              ← Multi-segment log (rotation, retention, offset routing)
├── broker.go           ← Topic/partition management, key-based routing
├── coordinator.go      ← Consumer groups, sticky assignment, heartbeat reaping
├── offsets.go          ← Durable consumer offset storage
├── protocol.go         ← Binary encode/decode for all request types
├── server.go           ← TCP listener, goroutine-per-connection, request routing
├── cmd/mini-kafka/
│   └── main.go         ← CLI (broker, produce, consume, commit, fetch-offset)
├── crash_demo.sh       ← Kill-9 crash recovery demonstration
└── *_test.go           ← Tests for each component
```

## Running Tests

```bash
go test -v ./...
```

## Tech Stack

- **Language:** Go 1.26
- **Networking:** `net` (raw TCP)
- **Storage:** Direct file I/O with `fsync`
- **Hashing:** FNV-1a (partition routing)
- **No external dependencies** — standard library only

## References

- [Apache Kafka Design](https://kafka.apache.org/documentation/#design)
- [Kafka: a Distributed Messaging System for Log Processing](https://www.microsoft.com/en-us/research/publication/kafka-a-distributed-messaging-system-for-log-processing/) (LinkedIn, 2011)
- [StickyAssignor (KIP-54)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-54+-+Sticky+Partition+Assignment+Strategy)
