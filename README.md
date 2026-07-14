# Mini Kafka — Distributed Message Broker from Scratch

A message broker built from scratch in Go, implementing the core architecture of Apache Kafka — append-only segmented logs, partitioned topics, consumer groups with sticky rebalancing, and a custom binary TCP protocol. Includes a real-time WebSocket dashboard for visualizing message flow.

![Dashboard](https://img.shields.io/badge/Dashboard-React%20%2B%20WebSocket-blue)
![Go](https://img.shields.io/badge/Go-1.26-00ADD8)
![Tests](https://img.shields.io/badge/Tests-All%20Passing-green)

## What This Is

The internal engine of a distributed message broker: durable storage, network protocol, consumer coordination, and a live visualization. Built to understand (and demonstrate) how systems like Kafka work beneath the abstraction.

**Key results:**
- Zero data loss on kill -9 mid-write (verified by crash-recovery tests)
- **493× write throughput improvement** via batched fsync (270 msg/s → 133K msg/s)
- StickyAssignor minimizes partition movement on rebalance (verified end-to-end over TCP)
- CRC32 integrity checks detect silent data corruption
- Real-time dashboard showing message flow, consumer groups, and rebalancing live

## Live Dashboard

```
┌──────────────┬──────────────────────────┬───────────────────────┐
│  🎮 Controls │  📊 Partitions           │  📋 Live Events       │
│              │                          │                       │
│  [Create]    │  Topic: orders           │  PRODUCE partition=1  │
│  [Produce]   │  P0 ████████░░░░  847   │  JOIN worker-A        │
│  [Burst 10]  │  P1 █████░░░░░░░  612   │  REBALANCE A=[0,1]   │
│              │  P2 ██████████░░  921   │  CONSUME partition=0  │
│  👥 Groups   │                          │  COMMIT offset=847    │
│  processors  │                          │  LEAVE worker-B       │
│  ● worker-A  │                          │  REBALANCE A=[0,1,2] │
│    [0, 1]    │                          │                       │
└──────────────┴──────────────────────────┴───────────────────────┘
```

## Architecture

```
┌──────────────┐                        ┌──────────────────────────────────┐
│  Browser     │◀── WebSocket (:8080) ──│          BROKER                  │
│  Dashboard   │                        │                                  │
└──────────────┘                        │  ┌─ Storage ─────────────────┐  │
                                        │  │  Segmented append-only logs │  │
┌──────────────┐                        │  │  CRC32 + timestamps        │  │
│  Producer    │──── TCP (:9092) ──────▶│  │  Batch writes (493× faster)│  │
│  (CLI)       │                        │  │  Log compaction            │  │
└──────────────┘                        │  └────────────────────────────┘  │
                                        │                                  │
┌──────────────┐                        │  ┌─ Coordinator ─────────────┐  │
│  Consumer    │──── TCP (:9092) ──────▶│  │  Consumer groups           │  │
│  (CLI)       │                        │  │  StickyAssignor            │  │
└──────────────┘                        │  │  Heartbeat failure detect  │  │
                                        │  └────────────────────────────┘  │
                                        └──────────────────────────────────┘
```

## Quick Start

```bash
# Build
cd frontend && npm install && npx vite build && cd ..
go build -o mini-kafka ./cmd/mini-kafka/

# Start the broker (TCP on :9092, Dashboard on :8080)
./mini-kafka broker

# Open dashboard: http://localhost:8080

# In another terminal:
./mini-kafka create-topic --topic orders --partitions 3
./mini-kafka produce --topic orders --key customer-42 --value "order #101"

# Start a long-running consumer (joins group, heartbeats, auto-reads):
./mini-kafka consumer --topic orders --group processors --id worker-A
```

### Docker

```bash
docker build -t mini-kafka .
docker run -p 9092:9092 -p 8080:8080 mini-kafka
# Open http://localhost:8080
```

## Benchmark

```
Benchmark (100K messages × 256 bytes, Apple Silicon SSD):

  Write (fsync/msg):      0.07 MB/s  |      270 msg/s  |  p99 = 5.0ms
  Write (batched):       32.47 MB/s  |  133,009 msg/s
  Read (sequential):    310.94 MB/s  |  1,273,629 msg/s

  Batching speedup: 493× faster writes

  Tradeoff: per-message fsync guarantees zero acknowledged-message loss.
  Batched fsync amortizes the cost but a mid-batch crash loses up to
  batch-size unacknowledged messages. Real Kafka makes the same tradeoff
  and relies on replication for safety.
```

Run it yourself:
```bash
./mini-kafka bench --messages 100000 --size 256
```

## Crash Recovery Demo

```bash
./crash_demo.sh
```

Produces messages → kills the broker with `kill -9` mid-operation → restarts → verifies all messages and consumer offsets survived.

## How It Works

### Storage Engine

Each partition is an append-only log split into segment files:

```
data/orders/partition-0/
├── segment-0000000000000000000000.log   (10 MB, closed)
├── segment-0000000000000000005000.log   (10 MB, closed)
└── segment-0000000000000000010000.log   (active — writes go here)
```

Record format on disk:
```
[4-byte size][4-byte CRC32][8-byte timestamp][2-byte key len][key][value]
```

- **Append:** write to active segment → fsync → add to in-memory index
- **Batch append:** write N records → ONE fsync (493× faster)
- **Read:** `index[offset]` → seek → read → validate CRC32
- **Crash recovery:** scan segments, rebuild index, truncate torn writes
- **Retention:** delete oldest segment when total exceeds size cap
- **Compaction:** keep only latest value per key (for state-snapshot topics)

### Wire Protocol

Custom binary protocol with length-prefixed framing:

```
Frame:   [4 bytes: body length][body bytes]
Body:    [1 byte: request type][fields...]
```

Request types: PRODUCE, CONSUME, CREATE_TOPIC, COMMIT, FETCH_OFFSET, JOIN_GROUP, HEARTBEAT, LEAVE_GROUP, PRODUCE_BATCH

### Consumer Groups (StickyAssignor)

```
Before:  A=[0,1,2], B=[3,4,5]     (2 consumers, 6 partitions)
C joins: A=[0,1],   B=[3,4], C=[2,5]

A kept 2/3 original partitions. Only excess moved to C.
(Round-robin would shuffle ALL assignments.)
```

End-to-end verified: consumer dies → heartbeat timeout → partitions reassign → surviving consumer takes over from committed offset.

### Log Compaction

```
Before: [key=A,v=1] [key=B,v=1] [key=A,v=2] [key=B,v=2] [key=A,v=3]
After:  [key=A,v=3] [key=B,v=2]  ← only latest value per key survives
```

Enables using topics as state snapshots (user profiles, config, inventory).

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Append-only (no edits/inserts) | Sequential I/O is 50–500× faster than random |
| Per-write fsync (configurable) | Maximum durability; batched mode for throughput |
| CRC32 on every record | Detects silent corruption (bit rot, disk errors) |
| Length-prefixed binary protocol | No delimiter ambiguity, works with any payload |
| Goroutine-per-connection | Cheap (4 KB stack), simple, scales to thousands |
| StickyAssignor | Minimizes rebalance disruption vs round-robin |
| Segments + retention | Bounded disk; delete old data by removing one file |
| Log compaction | Keep latest-per-key for state-snapshot topics |
| WebSocket event bus | Dashboard gets every broker event in real-time |

## What's Not Implemented (and Why)

| Feature | Why skipped |
|---------|-------------|
| **Replication** | Requires multiple brokers, leader election, ISR tracking. Single-node durability (fsync) is what each broker does internally; replication protects against hardware death. |
| **Exactly-once delivery** | Requires idempotent producers (PID + sequence dedup) and two-phase commit transactions. We provide at-least-once. |
| **Zero-copy reads (sendfile)** | OS optimization that skips userspace copy. Straightforward to add but not architecturally interesting. |
| **Schema registry** | Application-layer concern, not a broker responsibility. |

## Project Structure

```
├── segment.go              ← Single segment file (append, read, rebuild index)
├── log.go                  ← Multi-segment log (rotation, retention, routing)
├── record.go               ← Record format (CRC32 + timestamp + key/value)
├── broker.go               ← Topic/partition management, key routing, batch writes
├── coordinator.go          ← Consumer groups, sticky assignment, heartbeat reaping
├── compactor.go            ← Log compaction (keep latest value per key)
├── offsets.go              ← Durable consumer offset storage
├── protocol.go             ← Binary encode/decode for all request types
├── server.go               ← TCP listener, goroutine-per-connection
├── events.go               ← Internal event bus (pub/sub for dashboard)
├── ws.go                   ← WebSocket server + HTTP API for dashboard
├── cmd/mini-kafka/
│   ├── main.go             ← CLI (broker, produce, consume, consumer, bench)
│   └── bench.go            ← Benchmark suite
├── frontend/               ← React + TypeScript dashboard
│   └── src/
│       ├── App.tsx
│       ├── components/     ← PartitionBars, ConsumerGroups, EventLog, Controls
│       └── hooks/          ← useWebSocket (auto-reconnect, state management)
├── Dockerfile              ← Multi-stage build (Go + Node → Alpine)
├── crash_demo.sh           ← Kill-9 crash recovery demonstration
└── *_test.go               ← Tests for each component
```

## Running Tests

```bash
go test -v ./...
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `mini-kafka broker` | Start the broker (TCP :9092, Dashboard :8080) |
| `mini-kafka create-topic --topic X --partitions N` | Create a topic |
| `mini-kafka produce --topic X --key K --value V` | Publish one message |
| `mini-kafka consume --topic X --partition P --offset O` | Read one message |
| `mini-kafka consumer --topic X --group G --id ID` | Long-running poll consumer (joins group, heartbeats) |
| `mini-kafka commit --group G --topic X --partition P --offset O` | Save consumer position |
| `mini-kafka fetch-offset --group G --topic X --partition P` | Get saved position |
| `mini-kafka bench --messages N --size S` | Run throughput benchmark |

## Tech Stack

- **Backend:** Go 1.26, raw TCP (`net`), `gorilla/websocket`
- **Storage:** Direct file I/O with `fsync`, CRC32 (`hash/crc32`)
- **Frontend:** React, TypeScript, Vite
- **Containerization:** Docker (multi-stage build)
- **Hashing:** FNV-1a (partition routing)

## References

- [Apache Kafka Design](https://kafka.apache.org/documentation/#design)
- [Kafka: a Distributed Messaging System for Log Processing](https://www.microsoft.com/en-us/research/publication/kafka-a-distributed-messaging-system-for-log-processing/) (LinkedIn, 2011)
- [StickyAssignor (KIP-54)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-54+-+Sticky+Partition+Assignment+Strategy)

## License

MIT
