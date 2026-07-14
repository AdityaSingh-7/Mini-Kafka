# Mini-Kafka Deep Dive

This document explains, from first principles, what this codebase builds and why. It assumes you know Java and Python well but have never touched Go or Kafka. Every concept is explained with concrete numbers, ASCII diagrams, and — where it helps — a Java-equivalent snippet so you can map the unfamiliar (Go) onto the familiar (Java).

> **Table of Contents**
> 1. [What Is Kafka? (The Problem It Solves)](#section-1-what-is-kafka-the-problem-it-solves)
> 2. [Core Concepts](#section-2-core-concepts)
> 3. [The Storage Engine](#section-3-the-storage-engine)
> 4. [The Record Format](#section-4-the-record-format)
> 5. [Networking — TCP, Framing, and Concurrency](#section-5-networking--tcp-framing-and-concurrency)
> 6. [The Wire Protocol (Every Request Type)](#section-6-the-wire-protocol-every-request-type)
> 7. [Consumer Groups — Full Deep Dive](#section-7-consumer-groups--full-deep-dive)
> 8. [Log Compaction](#section-8-log-compaction)
> 9. [Batch Writes — The Performance Story](#section-9-batch-writes--the-performance-story)
> 10. [Go Language Guide — Every Concept Used](#section-10-go-language-guide--every-concept-used)
> 11. [Interview Questions & Answers](#section-11-interview-questions--answers)
> 12. [Real Kafka vs Our Implementation](#section-12-real-kafka-vs-our-implementation)
> 13. [The Full Code Walkthrough](#section-13-the-full-code-walkthrough)

---

## Section 1: What Is Kafka? (The Problem It Solves)

### 1.1 The restaurant analogy

Imagine a restaurant with no order system. Every waiter runs directly into the kitchen and shouts an order at whichever cook happens to be free. That works when there are 2 waiters and 2 cooks. Now scale it up: 20 waiters, 8 cooks, a bar station, a dessert station, and a delivery-app integration that also needs to inject orders. Everyone is now shouting at everyone. If a cook steps away for 30 seconds, every order meant for them is lost, because nobody wrote it down anywhere — it only existed as a shout in the air.

The fix restaurants actually use: an **order ticket rail**. Waiters clip tickets onto the rail in the order they come in. Cooks pull tickets off at their own pace. The rail doesn't care who reads it, or how fast. If a cook is slow tonight, tickets just pile up on the rail — they're not lost. If a new cook starts mid-shift, they don't need to know anything about the waiters; they just start pulling tickets from wherever they want on the rail.

Kafka's log is the ticket rail. **Producers** are waiters. **Consumers** are cooks. The rail is ordered, append-only (new tickets go on the end, nobody edits old ones), and durable (paper tickets don't vanish because a cook blinked).

A factory conveyor belt is the same idea in a different costume: items get placed on the belt at one end (produced), and different workstations pick items off at their own pace (consumed), without the placer needing to know who's downstream or how fast they work.

### 1.2 What breaks without a log — direct service calls

Suppose you don't use a broker. `OrderService` calls `InventoryService`, `EmailService`, and `AnalyticsService` directly, e.g. via HTTP:

```
OrderService.placeOrder()
    ├── inventoryService.reserve(order)     // HTTP call
    ├── emailService.sendConfirmation(order) // HTTP call
    └── analyticsService.record(order)       // HTTP call
```

This is the "distributed monolith" trap. Concretely, it breaks in these ways:

| Problem | What actually happens |
|---|---|
| **Tight coupling** | `OrderService` must know the network address of every downstream service, forever. Add a 4th consumer (e.g. `FraudCheckService`) and you must edit and redeploy `OrderService`. |
| **Cascading failure** | If `EmailService` is down or slow, does `placeOrder()` hang? Fail? Retry forever? Whatever you pick, `OrderService`'s reliability is now capped by the *least* reliable downstream service. |
| **No replay** | `AnalyticsService` was down for an hour and missed 10,000 orders. They're gone — there's no rail to go back and re-read from. |
| **N×M wiring** | With 5 producers and 6 consumers, you have up to 30 point-to-point integrations, each with its own retry logic, backoff, and failure mode. |
| **Different consumption speeds impossible** | `EmailService` might process 1 request at a time; `AnalyticsService` might want to batch 10,000 at once. Direct calls force everyone into the producer's request/response rhythm. |

### 1.3 How a log in the middle fixes it

Insert an append-only log between producers and consumers. `OrderService` writes one record to the log and is done — it doesn't know or care who reads it, how many readers there are, or how fast they read.

```
                       ┌─────────────────────────────────────────┐
                       │              THE LOG (topic)             │
  Producers            │   offset:  0    1    2    3    4    5   │            Consumers
┌──────────────┐       │           ┌───┐┌───┐┌───┐┌───┐┌───┐┌───┐│       ┌──────────────────┐
│ OrderService │──────▶│  append──▶│rec││rec││rec││rec││rec││rec││◀──read│ EmailService      │
└──────────────┘       │           └───┘└───┘└───┘└───┘└───┘└───┘│       │ (reading offset 2) │
                       │            ▲                             │       └──────────────────┘
┌──────────────┐       │            │ new records                │
│ MobileApp    │──────▶│         always appended                 │       ┌──────────────────┐
└──────────────┘       │         at the END                       │◀──read│ AnalyticsService  │
                       │                                          │       │ (reading offset 5)│
                       └─────────────────────────────────────────┘       └──────────────────┘
                                                                          ┌──────────────────┐
                                                                    ◀──read│ FraudCheckService│
                                                                          │ (reading offset 0) │
                                                                          └──────────────────┘
```

What this buys you:

- **Decoupling** — `OrderService` writes once. Zero, one, or ten consumers can read it; `OrderService` doesn't change either way.
- **Independent speeds** — `EmailService` at offset 2 and `AnalyticsService` at offset 5 are both fine. Each consumer just remembers "my last read position" (its offset) and moves forward at its own pace.
- **Replay** — `FraudCheckService` deploys a week later and starts at offset 0. All history is still there. Try that with an HTTP call that already returned.
- **Failure isolation** — If `EmailService` crashes for an hour, the log just keeps growing. When `EmailService` comes back, it resumes exactly where it left off. `OrderService` never even knew.

### 1.4 Why append-only?

"Append-only" means the log only supports one write operation: add to the end. No `UPDATE`, no `DELETE` of a specific record, no inserting in the middle.

- **Sequential disk I/O is fast.** Appending to the end of a file is a sequential write — the disk head (or SSD's linear write path) never has to jump around. Random writes (like updating record #4,502 out of a million) are 50-500x slower on spinning disks, and still meaningfully slower on SSDs due to how flash pages/erase blocks work. We lean on this hard in Section 3.
- **Immutability = simplicity.** Once written, a record never changes. That means multiple consumers can safely read the same record concurrently — there's no "what if someone edits it while I'm reading it" race condition to worry about, no locks needed for reads.
- **Natural audit trail.** You always know exactly what happened and in what order, because nothing is ever rewritten.

### 1.5 Why ordered?

Order is guaranteed *within* the log (specifically, within a partition — see Section 2). If `OrderService` writes "reserve item" then "charge card" for the same order, every consumer sees them in that same sequence. This matters enormously: if "charge card" were somehow processed before "reserve item," you could charge a customer for an item you don't have.

Kafka gives you this ordering guarantee for free, as a side effect of being append-only: record N was written before record N+1, and every reader walks the log front-to-back (or from wherever they left off), so they all observe the same sequence.

### 1.6 Why doesn't the log copy data per consumer?

This is the detail that trips people up coming from traditional message queues (think RabbitMQ / JMS queues). In many queue systems, when a consumer reads a message, the message is *removed* from the queue — it's a "destroy on read" model. If you want 3 consumers to each see every message, you need 3 separate queues, and the producer (or a fan-out layer) copies the message into all 3.

Kafka's log does not do this. The log stores each record exactly once, physically, on disk. Every consumer (or consumer group) simply keeps its own independent "bookmark" — an offset — into that single shared log. Reading does not mutate the log; it's just "go to byte position X and read." Ten consumers can all read record #42 without any of them affecting the others, because reading is non-destructive.

```
                     ONE physical copy of the data on disk
                     ┌────┬────┬────┬────┬────┬────┐
                     │ 0  │ 1  │ 2  │ 3  │ 4  │ 5  │   <- the log itself
                     └────┴────┴────┴────┴────┴────┘
                       ▲          ▲              ▲
                       │          │              │
                 bookmark=0  bookmark=2     bookmark=5
                 (FraudCheck) (Email)       (Analytics)
```

This is *why* retention (Section 2/3) has to be time/size-based rather than "delete after everyone's read it" — the log has no idea who has or hasn't read a given record, by design. That's the tradeoff: massively simpler and more scalable, at the cost of needing an explicit retention policy.

### Understanding Check — Section 1

1. In the restaurant analogy, what plays the role of an "offset"? What breaks if a cook forgets their bookmark and starts pulling from a random spot on the rail?
2. If Kafka copied every message into a separate queue per consumer, what would happen to disk usage as you added a 10th, then 20th consumer of the same topic? Why doesn't the shared-log design have this problem?
3. Why is "the log is ordered" a stronger guarantee than "the log is append-only"? Could you have append-only without ordering? (Hint: think about what would happen with concurrent writers and no partitioning.)

---

## Section 2: Core Concepts

### 2.1 Topics — named channels

A **topic** is just a name for a category of messages, e.g. `"orders"` or `"clicks"`. It's the Kafka equivalent of a table name in a database, or a named queue in JMS — a label producers write to and consumers subscribe to.

```go
b.CreateTopic("orders", 3)   // topic "orders" with 3 partitions
b.CreateTopic("clicks", 6)   // topic "clicks" with 6 partitions
```

On disk in this project, a topic becomes a directory:

```
data/
├── orders/
│   ├── partition-0/
│   ├── partition-1/
│   └── partition-2/
└── clicks/
    ├── partition-0/
    ├── ... (6 total)
```

Java equivalent mental model: `Map<String, List<Log>> topics` — exactly what `broker.go` uses (`topics map[string][]*Log`).

### 2.2 Partitions — parallel lanes within a topic

A topic isn't one log — it's **N** logs, called partitions, numbered `0..N-1`. Why split a topic into multiple logs at all?

**The core reason: parallelism.** A single append-only log, by definition, is written and read sequentially by nature — but *nothing* stops you from having several independent logs and spreading work across them. If topic `"orders"` has 1 partition, only one consumer can be actively pulling from it at a time in a consumer group (see 2.4) — everyone else in that group sits idle. If it has 3 partitions, 3 consumers can each work one partition in parallel — 3x the throughput.

**The rule that falls out of this: max useful parallelism for one consumer group = number of partitions.** A 3-partition topic can keep at most 3 consumers (in the same group) busy. A 4th consumer in that group would have nothing to do — there's no 4th lane to hand it.

How does a message get assigned to a partition? Look at `Broker.Publish` in `broker.go`:

- **With a key** (e.g. `key = "customer-42"`): hash the key with FNV-1a, mod by partition count → `hashKey(key, len(partitions))`. The *same key always lands in the same partition*, every time. This is crucial: it means all messages about `customer-42` land in the same partition, in the same relative order they were produced (partition order is guaranteed; cross-partition order is not).
- **Without a key**: round-robin based on total messages produced so far, spreading load evenly with no ordering guarantee tying related messages together.

```
Topic "orders", 3 partitions

produce(key="customer-42", ...) ──hash──▶ always partition 1
produce(key="customer-7",  ...) ──hash──▶ always partition 0
produce(key="customer-42", ...) ──hash──▶ always partition 1   (same key, same partition again)

partition 0: [cust-7 msg A]
partition 1: [cust-42 msg A][cust-42 msg B]   <- both customer-42 messages, in order
partition 2: [cust-19 msg A]
```

Java equivalent for the hash routing:
```java
int partition = Math.abs(key.hashCode()) % numPartitions;  // simplified; real Kafka/FNV differ in hash fn
```

### 2.3 Offsets — the message's permanent address

An **offset** is a message's sequence number *within its partition*, starting at 0 and incrementing by exactly 1 for every message ever appended to that partition. Once assigned, an offset never changes and is never reused — even if the underlying data is later deleted by retention, the numbering doesn't shift.

The cleanest mental model: **a line number in an append-only notebook.** If you write "line 47" in a notebook, that's forever line 47. You can rip out old pages later (retention), but you don't renumber the remaining lines — line 47 stays "line 47" (or becomes unavailable if that page was torn out), it never becomes line 12 just because earlier pages were removed.

```
Partition 1 (topic "orders"):

offset:   0        1        2        3        4        5
        ┌────┐   ┌────┐   ┌────┐   ┌────┐   ┌────┐   ┌────┐
        │msgA│   │msgB│   │msgC│   │msgD│   │msgE│   │msgF│
        └────┘   └────┘   └────┘   └────┘   └────┘   └────┘
                                                          ▲
                                              "give me offset 4" → msgE
```

Note offsets are **per partition**, not per topic. `partition 0` and `partition 1` both have their own independent `offset 0`. So a full message address is really the triple `(topic, partition, offset)` — that's exactly the signature of `Broker.Consume(topic string, partition int, offset uint64)` in `broker.go`.

### 2.4 Consumer Groups — workers splitting partitions

A **consumer group** is a named team of consumers that cooperatively divide up a topic's partitions so that the work (and the messages) get spread across them rather than duplicated.

**The golden rule: within one consumer group, one partition is read by exactly one consumer at a time.** Never two. This is what makes consumer groups a *scale-out* mechanism rather than a *broadcast* mechanism.

```
Topic "orders" — 6 partitions [0,1,2,3,4,5]
Consumer group "processors" — 2 members: worker-A, worker-B

  worker-A reads: [0, 1, 2]
  worker-B reads: [3, 4, 5]

  ✔ Every partition has exactly one reader.
  ✘ Never: worker-A AND worker-B both reading partition 2.
```

If `worker-C` joins the same group, the coordinator **rebalances**: partitions get redistributed so all 3 workers have ~2 each. This project uses **sticky assignment** (`coordinator.go`) rather than naive round-robin — sticky assignment tries to leave existing assignments alone and only moves the minimum number of partitions needed, because reassigning a partition means the new owner has to "warm up" (fetch its committed offset, etc.) — moving fewer partitions on every rebalance is strictly better.

```
Before:  A=[0,1,2], B=[3,4,5]     (2 consumers, 6 partitions)
C joins: A=[0,1],   B=[3,4], C=[2,5]

A kept 2/3 original partitions. Only excess moved to C.
(Naive round-robin would reshuffle ALL assignments, e.g. A=[0,2,4], B=[1,3,5], C=... )
```

Two *different* groups reading the same topic are fully independent — group "processors" and group "analytics" can both read every partition of "orders", each at their own pace, because the "one partition, one consumer" rule only applies *within* a group. This is how Kafka supports both "queue-like" behavior (spread load within a group) and "pub/sub-like" behavior (broadcast across groups) with the same primitive.

Java mental model:
```java
class ConsumerGroup {
    Map<String, List<Integer>> memberToPartitions; // "worker-A" -> [0,1,2]
}
```

### 2.5 Segments — physical files that make up one partition's log

A partition's log isn't one giant file that grows forever — it's split into **segments**, each a separate file on disk holding a contiguous range of offsets.

```
data/orders/partition-0/
├── segment-00000000000000000000.log   (offsets 0-4999,   closed, 10 MB)
├── segment-00000000000000005000.log   (offsets 5000-9999, closed, 10 MB)
└── segment-00000000000000010000.log   (offsets 10000+,    ACTIVE — new writes go here)
```

The filename literally encodes the **base offset** — the offset of the first message in that file (zero-padded so alphabetical sort = numerical sort; see `Log.newSegment` in `log.go`). Only one segment at a time is "active" (the last one) — that's the only file being written to. Older segments are closed/read-only.

Why segments instead of one huge file? Covered in depth in Section 3, but the short version: a single unbounded file makes retention (deleting old data) and crash recovery (rebuilding an index after a crash) both expensive; many small files make both operations cheap and localized.

### 2.6 Retention — deleting old segments

**Retention** is the policy for how much history to keep before discarding the oldest data. In this project (`Log.enforceRetention` in `log.go`), the rule is: if total size across all segments in a partition exceeds `MaxLogBytes`, delete the *oldest* segment file (never the active one), and repeat until under the cap.

```
Before retention (partition total = 110 MB, cap = 100 MB):
  [seg-0000 (10MB)] [seg-5000 (10MB)] [seg-10000 (90MB, active)]

enforceRetention() runs:
  → total 110MB > 100MB cap → delete oldest segment (seg-0000)

After:
  [seg-5000 (10MB)] [seg-10000 (90MB, active)]     total = 100MB, done.
```

### 2.7 Critical distinction: partitions never get deleted, segments do

This trips people up, so say it plainly:

| Never deleted | Deleted by retention |
|---|---|
| **Partitions** — a topic's partition count is fixed at creation (well, technically can only grow in real Kafka, never shrink) and is a structural/logical decision | **Segments** — individual files *within* a partition, deleted once they age out or the partition grows past its size cap |

A partition is a *logical* concept (a numbered lane, `partition-0`, `partition-1`, ...). A segment is a *physical* concept (one file among possibly many that together make up that lane's data). Retention operates at the segment (file) level — "delete this one old file" — never at the partition level. The partition itself, as an addressable lane, persists for the topic's entire lifetime; only how much history it *retains* shrinks over time.

### Understanding Check — Section 2

1. A topic `"clicks"` has 4 partitions. A consumer group has 6 members. What happens to the 2 extra members? What would happen to throughput if you instead had 4 partitions and only 2 consumers?
2. Why does hashing on the message *key* (rather than round-robin) matter for ordering guarantees? Give an example of a bug that would appear if `customer-42`'s messages were spread randomly across partitions.
3. Segment `segment-00000000000000005000.log` is deleted by retention. What is the *offset numbering* of the next message written to the active segment — does it "reuse" offset 5000, or does numbering continue as if nothing happened? Why?

---

## Section 3: The Storage Engine

### 3.1 The on-disk format: `[4-byte size][body]`

Every message written to a segment file is preceded by a 4-byte integer giving the length, in bytes, of what follows:

```
┌─────────────┬──────────────────────────────┐
│  size (4B)  │           body                │
│  uint32,    │   (however many bytes         │
│  big-endian │    `size` says)                │
└─────────────┴──────────────────────────────┘
```

This is called **length-prefixing**, and it's implemented in `segment.go`'s `Append`:
```go
sizeBuf := make([]byte, 4)
binary.BigEndian.PutUint32(sizeBuf, uint32(len(payload)))
s.file.WriteAt(sizeBuf, pos)
s.file.WriteAt(payload, pos+4)
```

Java equivalent: think `DataOutputStream.writeInt(payload.length); out.write(payload);` — same idea, big-endian by default in Java's `DataOutputStream`, which is why this project also chose big-endian (`binary.BigEndian`) for consistency/familiarity.

### 3.2 Why length-prefixed instead of a delimiter (like `\n`)?

An alternative design: separate messages with a delimiter character, like newline-delimited JSON (`\n`-separated). This is simpler to eyeball in a text editor, but it has a fatal flaw for a general-purpose broker: **messages are arbitrary bytes**, not necessarily text. If a message's *payload* happens to contain the byte `0x0A` (`\n`) as legitimate binary data (e.g., it's a serialized image, a protobuf blob, or literally any binary content), a delimiter-based reader would incorrectly think the message ended early, right in the middle of the payload.

Length-prefixing sidesteps this entirely: the reader doesn't scan for a stop character at all — it reads exactly `size` bytes, no matter what's inside them, then knows the next `size` header starts right after. Binary-safe, unambiguous, and — bonus — you can seek directly to the next record without scanning byte-by-byte for a delimiter.

| | Delimiter-based (`\n`) | Length-prefixed (`[size][body]`) |
|---|---|---|
| Binary-safe | No — payload can't contain the delimiter | Yes — any bytes allowed |
| Must scan to find next record | Yes, byte by byte | No — jump `4 + size` bytes ahead |
| Extra overhead per record | 1 byte (the delimiter) | 4 bytes (the size header) |

### 3.3 Concrete example: encoding "hello world" and "a"

Let's encode the string `"hello world"` (11 bytes) using the length-prefixed scheme (ignoring the CRC/timestamp fields covered in Section 4 for a moment — just the raw `[size][payload]` shape from `Segment.Append`):

```
"hello world" is 11 bytes: 68 65 6c 6c 6f 20 77 6f 72 6c 64

size header (4 bytes, big-endian uint32 for value 11):
  00 00 00 0b

Full on-disk bytes (15 bytes total):
  00 00 00 0b | 68 65 6c 6c 6f 20 77 6f 72 6c 64
  └─ size=11 ─┘└──────────── "hello world" ──────────────┘
```

Now `"a"` (1 byte):

```
size header (4 bytes, big-endian uint32 for value 1):
  00 00 00 01

Full on-disk bytes (5 bytes total):
  00 00 00 01 | 61
  └─ size=1 ─┘  └'a'┘
```

If both were appended back to back in the same segment file, the file would literally contain, in order:
```
00 00 00 0b 68 65 6c 6c 6f 20 77 6f 72 6c 64 00 00 00 01 61
└────── record 1 (15 bytes) ─────────────────┘└─ record 2 (5 bytes) ─┘
```
A reader at position 0 reads 4 bytes → sees `11` → reads the next 11 bytes as the payload → now at position 15 → reads the next 4 bytes → sees `1` → reads 1 more byte. No scanning, no ambiguity, no delimiter needed.

### 3.4 The in-memory index: offset → byte position

Reading a message by offset would be painfully slow if you had to scan the file from byte 0 every single time. So each `Segment` keeps an **in-memory index**: a simple slice (array) where `index[i]` is the byte position in the file where the *i*-th message (relative to this segment) begins.

```go
type Segment struct {
    index      []int64 // index[i] = byte position of message (baseOffset + i)
    baseOffset uint64
    ...
}
```

Concrete numbers. Suppose a segment has `baseOffset = 5000` and these three messages were appended, of sizes 11, 1, and 20 bytes (payload sizes; ignoring the 4-byte header math for the size increments):

```
index[0] = 0     (message at LOCAL index 0, i.e. GLOBAL offset 5000, starts at byte 0)
index[1] = 15    (0 + 4-byte header + 11-byte payload = 15; message at global offset 5001)
index[2] = 20    (15 + 4-byte header + 1-byte payload = 20; message at global offset 5002)

To read global offset 5002:
  localIndex = 5002 - baseOffset(5000) = 2
  bytePosition = index[2] = 20
  seek to byte 20, read 4-byte size header, then read that many bytes.
```

This is `O(1)` — no scanning, no binary search even, just a direct array lookup by local index. (Note: this project's index is *dense* — one entry per message. Real Kafka uses a *sparse* index — one entry every 4KB — plus binary search, trading a tiny bit of read cost for far less memory. That's a noted future upgrade for this project; see the memory notes.)

### 3.5 Memory calculation: 10 million messages × 8 bytes = 80 MB

Each index entry here is an `int64` (a byte offset), which is 8 bytes. If a partition accumulates 10 million messages:

```
10,000,000 messages × 8 bytes/entry = 80,000,000 bytes = ~80 MB of RAM
                                                            (just for the index!)
```

That's *entirely separate* from the actual message data on disk — this 80 MB lives in memory (RAM) for as long as the process is running, because it's rebuilt from the file at startup and kept as a Go slice. This is exactly why real Kafka does NOT keep one index entry per message — at 10M+ messages per partition (very normal for a busy topic), a dense index becomes a meaningful memory tax across potentially thousands of partitions on one broker. A sparse index (every 4KB of file, not every message) cuts this by roughly the average-message-size/4096 ratio — e.g., for 256-byte messages, that's a ~16x reduction in index memory, because instead of 1 index entry per message, you get roughly 1 entry per 16 messages.

### 3.6 fsync: what it is, and what happens without it

When your program calls `file.Write(...)`, the bytes do **not** necessarily land on physical disk immediately. The OS typically buffers writes in memory (the "page cache") and flushes them to the physical disk device later, on its own schedule, for performance. From your program's point of view the write "succeeded" the moment it returned — but the data might still only exist in RAM.

`fsync` (in Go: `file.Sync()`, used in `Segment.Append` and `Segment.AppendBatch`) is the system call that says: "don't return until these bytes are physically durable on the storage device, not just sitting in an OS buffer." Without it:

```
1. Program writes bytes  → OS buffer (RAM) ✓        [fast, ~microseconds]
2. Program says "done, message is safely stored"    [but it ISN'T yet!]
3. *** Power loss / kernel panic / kill -9 the machine (not just the process) ***
4. OS buffer contents (RAM) are gone. Never made it to disk.
5. On reboot: the message you said was "stored" simply never existed.
```

This is exactly the failure mode the crash-recovery tests in this project guard against (see `crash_demo.sh`), and it's precisely why `Segment.Append` calls `s.file.Sync()` before returning — it guarantees "if I told you this offset was assigned, that data is physically on disk," even if the machine loses power one instruction later.

### 3.7 Throughput math: fsync takes ~4ms → max 250 writes/sec

`fsync` is not free — it has to wait for the physical storage medium to confirm the write, and that has real latency, commonly a few milliseconds even on a fast SSD (spinning disks are far worse). This project's own benchmark shows this concretely: single-message, fsync-per-write mode achieves **270 msg/s**, aligning almost exactly with the "~4ms per fsync" figure:

```
1 fsync ≈ 4 ms

messages per second = 1000 ms / 4 ms per fsync ≈ 250 msg/sec  (measured: 270 msg/s)
```

If you need higher write throughput, and you're calling `fsync` once per message, you are structurally capped at ~250-270 msg/sec no matter how fast your CPU or network is — the bottleneck is entirely the disk's fsync latency, once per message.

**This is exactly why batching matters.** `Segment.AppendBatch` (see `segment.go`) writes N messages to the file but calls `file.Sync()` only **once**, after all N are written:

```
Single Append × N messages:  N writes + N fsyncs  = N × 4ms          (N=1000 → 4 seconds)
AppendBatch(N messages):     N writes + 1 fsync   = 1 × 4ms (+ N×tiny writes)
```

The project's real measured numbers back this up:

```
Write (fsync/msg):      270 msg/s
Write (batched):    133,009 msg/s
Batching speedup:       493× faster
```

The tradeoff, stated plainly (per this project's README, and it matches what real Kafka does): batching amortizes the fsync cost across many messages, but if the process crashes *mid-batch*, you can lose up to one whole batch's worth of *unacknowledged* messages (not previously-acknowledged ones — those were already fsynced in an earlier batch). Real Kafka's answer to this exact tradeoff is replication — durability comes not from every single broker fsyncing every single message, but from multiple brokers holding copies, so a single machine's un-fsynced buffer loss doesn't lose the message globally. This project is single-node, so it discloses the batched-fsync risk explicitly rather than hiding it.

### 3.8 Segments: why one big file is bad, and how segments fix it

Imagine one unbounded `partition-0.log` file that just grows forever. Two operations become painful:

1. **Retention (deleting old data).** To reclaim space from old messages in one giant file, you'd have to rewrite the entire file *minus* the old prefix (there's no OS call to "delete the first N bytes of a file" — you can only truncate from the *end*, or delete the whole file). For a 500 GB file, that's an enormous, slow, I/O-heavy rewrite just to free up 10 GB of old data.
2. **Crash recovery.** After an unclean shutdown, you have to scan the file from the beginning to verify which records are intact and rebuild the index (Section 3.10). For one giant file, that's a full scan of everything ever written — potentially hours for a large partition.

Segments fix both by bounding each file's size (this project defaults to 10 MB via `MaxSegmentBytes`, real Kafka commonly uses ~1 GB). When the active segment crosses that size, `Log.Append` rotates to a brand new file (`Log.newSegment`) rather than letting the current one grow unbounded.

```
Growth over time:

t=0:   [seg-0 (growing...)]
t=1:   [seg-0 (10MB, now closed)] [seg-5000 (growing...)]
t=2:   [seg-0 (10MB, closed)] [seg-5000 (10MB, closed)] [seg-10000 (growing...)]
```

Now retention only has to delete `seg-0` — a single, complete, closed file — and crash recovery only has to re-scan the *active* segment (the last one), not the closed ones behind it (closed segments are known-good and don't need re-verification on every restart in a real implementation; this project's `rebuildIndex` does re-scan every segment on open for simplicity, but the *concept* — that closed segments don't need rewriting for retention — still holds).

### 3.9 Retention: delete the oldest segment file — one OS call, instant

Because segments are complete, independent files, `Log.enforceRetention` implements deletion as literally one filesystem call: `os.Remove(path)` (wrapped in `Segment.Remove`). Compare:

| Approach | Cost to delete old data |
|---|---|
| One giant file, need to drop the oldest 10 MB | Rewrite the entire remaining file contents (I/O proportional to file size) |
| Segmented, oldest segment = 10 MB file | `os.Remove("segment-0000....log")` — O(1), a single directory-entry removal |

That's the whole point of choosing the segment boundary to align with the retention unit: deleting old data becomes "unlink one file," which is about as cheap as a disk operation can be — no data movement, no rewriting, just removing a directory entry (the underlying disk blocks are reclaimed by the filesystem asynchronously).

### 3.10 Crash recovery: scan file, rebuild index, truncate torn writes

When a segment file is opened (`OpenSegment` in `segment.go`), it doesn't trust any cached index — it re-derives the index by scanning the raw bytes of the file itself, via `rebuildIndex`. Why re-derive from scratch rather than persist the index separately? Because after a crash, you need a way to detect *and repair* a "torn write" — a write that started but wasn't fully completed when the process died — and the safest source of truth for that is the raw bytes actually present on disk right now.

The scan logic (`Segment.rebuildIndex`):
```go
for pos < fileSize {
    if pos+4 > fileSize {              // not even a full 4-byte size header present
        s.file.Truncate(pos); break
    }
    read 4 bytes at pos → size
    if pos+4+size > fileSize {         // size header claims more payload than actually exists
        s.file.Truncate(pos); break
    }
    index = append(index, pos)          // this record is intact — keep it
    pos += 4 + size
}
```

### 3.11 Step-by-step trace: crash recovery with a corrupted trailing record

Suppose the process was in the middle of `Segment.Append` for a 3rd message when the machine lost power. The file on disk now looks like this (2 complete records, then a torn 3rd):

```
Byte layout on disk after the crash:

  [0]  size=11 (4 bytes: 00 00 00 0b)
  [4]  "hello world" (11 bytes)                          <- record 1: bytes 0-14, COMPLETE
  [15] size=5  (4 bytes: 00 00 00 05)
  [19] "howdy" (5 bytes)                                  <- record 2: bytes 15-23, COMPLETE
  [24] size=20 (4 bytes: 00 00 00 14)                     <- claims a 20-byte payload follows...
  [28] "trunc" (only 5 bytes actually got written before the crash!)
  [file ends at byte 33]                                  <- but only 5 of the claimed 20 bytes exist
```

Trace through `rebuildIndex` step by step:

| Step | `pos` | Check | Result |
|---|---|---|---|
| 1 | 0 | `pos+4 (4) > fileSize(33)`? No. Read size=11. `pos+4+11=15 > 33`? No. | Valid → `index=[0]`, `pos = 0+4+11 = 15` |
| 2 | 15 | `pos+4 (19) > 33`? No. Read size=5. `pos+4+5=24 > 33`? No. | Valid → `index=[0,15]`, `pos = 15+4+5 = 24` |
| 3 | 24 | `pos+4 (28) > 33`? No. Read size=20. `pos+4+20=48 > 33`? **Yes!** | **Torn write detected** → `s.file.Truncate(24)`, loop breaks |

Final state: `index = [0, 15]` (2 valid records), and the file is physically truncated back to 24 bytes — the torn 3rd record's garbage bytes are deleted from disk entirely. `s.size = 24`. The next `Append` call will write cleanly starting at byte 24, as if the torn write never happened.

This is the concrete mechanism behind the claim "zero data loss on `kill -9` mid-write" — precisely because it's *mid-write* data (never acknowledged as durably stored) that's discarded, not previously-completed, previously-fsynced records. Anything that was fully written and fsynced *before* the crash survives, byte for byte; anything torn mid-write is cleanly discarded rather than left as corrupt, unindexed garbage sitting at the end of the file.

### Understanding Check — Section 3

1. Walk through what `rebuildIndex` would do if the crash happened *between* writing the 4-byte size header and writing the payload (i.e., the size header says `size=20` but zero payload bytes made it to disk — the file ends exactly at byte 28 in the trace above). Does the logic still handle it correctly?
2. Why is `os.Remove()` on a segment file O(1) while "delete the oldest 10MB from a 500GB single file" is not? What's fundamentally different about the two operations at the filesystem level?
3. If `MaxSegmentBytes` were set to 1 byte (absurdly small), what would happen to the number of files created, and to crash-recovery cost per restart? What would happen if it were set to `MaxInt64` (never rotate)? What does this tell you about how to pick a real segment size?

### Interview Questions — Section 3

1. **"Why does Kafka use fsync, and why doesn't it fsync on every single message in production?"** — Explain the buffered-write-loss risk, the ~4ms fsync latency number, and how batching (and ultimately replication) resolves the throughput/durability tension.
2. **"How would you design a system that needs to delete only the oldest 10% of data cheaply, without scanning or rewriting the rest?"** — Expect: chunk data into bounded-size, independently deletable files (the segment pattern), rather than one unbounded file or a database row-level delete.
3. **"A process crashes mid-write to an append-only file. How do you detect and recover from a partially-written record on restart, without an external write-ahead log?"** — Expect: length-prefixed records let you scan and validate self-consistency (does the claimed length fit within the actual file size?); anything that fails validation gets truncated, because by definition it was never a complete, acknowledged write.
4. **"Why is a dense (per-message) in-memory index a scalability problem, and what's the alternative?"** — Expect: 8 bytes × message count adds up (80MB per 10M messages per partition, multiplied across many partitions); the fix is a sparse index (e.g. one entry per 4KB) plus a linear scan or binary search within that last small window.

---

## Section 4: The Record Format

### 4.1 The old format's problems: `[size][payload]`

Section 3 covered the framing format `[4-byte size][body]`. But in the *earliest* version of this project, `payload` was just raw opaque bytes — whatever the caller passed in, with nothing else. That's simple, but it's missing several things a real message broker needs:

| Missing feature | Why it matters |
|---|---|
| **No corruption detection** | If a disk silently flips a bit (bit rot, a bad sector, a buggy RAID controller) inside the payload, nothing notices. The consumer just reads corrupted bytes and has zero indication anything is wrong. |
| **No timestamp** | You can't answer "give me all messages produced in the last 5 minutes" — there's no time information stored per record at all, only implicit arrival order via offset. |
| **No key** | Without a key travelling *with* the record itself, you can't do key-based partition routing consistently after the fact, and — critically for Section-2-adjacent features — you can't do **log compaction** (keep only the latest value per key), because compaction needs to know each record's key to decide what to discard. |

### 4.2 The new format: `[size][CRC32][timestamp][key_len][key][value]`

This project's current format (`record.go`) fixes all three gaps:

```
┌──────────┬───────────┬────────────┬────────────┬──────────┬───────────┐
│ size(4B) │ CRC32(4B) │ timestamp  │ key_len    │  key     │  value    │
│ uint32   │ uint32    │ (8B) int64 │ (2B) uint16│ (key_len │ (remaining│
│          │           │  epoch ms  │            │  bytes)  │  bytes)   │
└──────────┴───────────┴────────────┴────────────┴──────────┴───────────┘
   framing  └──────────────── this is the "body", covered by size ────────────────┘
                └──────────────── CRC32 covers everything from here on ───────────┘
```

In Go source (`record.go`):
```go
const RecordHeaderSize = 4 + 8 + 2 // CRC(4) + timestamp(8) + keyLen(2) = 14
// Total on-disk size = 4 (size header) + RecordHeaderSize + len(key) + len(value)
```

### 4.3 What is CRC32? A fingerprint for your data

CRC32 (Cyclic Redundancy Check, 32-bit) is a checksum algorithm: feed it any sequence of bytes, and it deterministically produces a 4-byte (32-bit) number that acts like a fingerprint of that exact data. The key properties:

- **Deterministic:** the same input bytes *always* produce the same CRC32 value.
- **Sensitive to any change:** flip even a single bit anywhere in the input, and the output CRC32 is essentially unrelated to the original — it doesn't degrade gracefully, it just becomes a completely different number.
- **Not cryptographically secure** (an attacker *could* engineer a collision on purpose) — but that's not the threat model here. CRC32 protects against *accidental* corruption (disk errors, bit rot, cosmic rays flipping a bit in RAM), not malicious tampering.

Concrete example — computing CRC32 of `"hello"` versus `"helLo"` (only the 4th letter's case changed):

```
CRC32("hello") = 0x3610a686
CRC32("helLo") = 0xa3948224
```

One lowercase letter became uppercase — a change of a single bit in a single byte — and the fingerprint is completely different, not "close." That's exactly the property you want: any corruption, however tiny, is detectable, because there's no scenario where "slightly corrupted data" produces a "slightly different, still-plausible" checksum.

### 4.4 Why CRC32 matters: bit rot, disk errors, silent corruption

Storage media aren't perfect. Over long periods, disk sectors can develop errors, magnetic/flash storage can experience "bit rot" (a stored bit spontaneously flips value), and controllers, cables, or firmware occasionally have bugs. Without a checksum, a broker has *no way* to know this happened — it will happily hand a consumer a corrupted record, and the consumer might crash on malformed data, silently process wrong data, or (worst case) never notice at all if the corruption happens to still look superficially valid.

With CRC32 (`DecodeRecord` in `record.go`):
```go
storedCRC := binary.BigEndian.Uint32(body[0:4])
actualCRC := crc32.ChecksumIEEE(body[4:])
if storedCRC != actualCRC {
    return nil, fmt.Errorf("CRC mismatch: stored=%08X computed=%08X (data corrupted)", storedCRC, actualCRC)
}
```
The CRC was computed *once*, at write time, from the true original bytes, and stored alongside them. At read time, the CRC is recomputed from whatever bytes are *actually* on disk right now. If even one bit differs from what was originally written, the two CRCs won't match, and the broker can raise a loud, explicit error — "this record is corrupted" — instead of silently serving bad data.

### 4.5 Timestamps: milliseconds since epoch

The 8-byte timestamp field stores the number of milliseconds elapsed since the Unix epoch (midnight, January 1, 1970, UTC) — exactly what Go's `time.Now().UnixMilli()` returns, and the same convention Java's `System.currentTimeMillis()` uses.

```go
func NewRecord(key, value []byte) *Record {
    return &Record{Timestamp: time.Now().UnixMilli(), Key: key, Value: value}
}
```

Storing this *per record* (rather than only implicitly via arrival order/offset) enables time-based queries that offsets alone can't answer, such as "give me every message from the last 5 minutes" or "replay everything starting from timestamp X" — you'd otherwise have no way to correlate an offset number with wall-clock time without this field.

### 4.6 Key/value separation: enables log compaction

Splitting the body into an explicit `key_len | key | value` (rather than one opaque blob) does two things:

1. **Consistent partition routing** — `Broker.Publish` needs the key *specifically* (not the whole payload) to hash-route messages, so that all messages for `customer-42` land in the same partition (Section 2.2). If the key weren't a distinguishable field, you couldn't reliably extract "the routing key" from an arbitrary blob.
2. **Log compaction** — a feature where, instead of keeping every historical message, the log keeps only the **most recent value for each key** (see `compactor.go`). This turns a topic into something like a changelog / state snapshot — e.g. topic `"user-profiles"` where key=`user123` might be written 50 times over a year as the profile changes, but a compacted view only needs the latest one. None of this is possible unless the key travels *with* the record in a well-defined, extractable field — you can't compact "by key" if you can't reliably identify what part of the payload *is* the key.

```
Before compaction: [key=A,v=1] [key=B,v=1] [key=A,v=2] [key=B,v=2] [key=A,v=3]
After compaction:  [key=A,v=3] [key=B,v=2]   <- only latest value per key survives
```

### 4.7 Exact byte layout: a concrete message

Let's encode a real message: `key = "customer-42"`, `value = "order placed"`, `timestamp = 1720350000000` (milliseconds since epoch — this corresponds to July 7, 2024, 12:20:00 UTC).

First, the sizes:
- `key = "customer-42"` → 11 bytes
- `value = "order placed"` → 12 bytes
- `RecordHeaderSize = 4 (CRC) + 8 (timestamp) + 2 (keyLen) = 14 bytes`
- Total **body** size = `14 + 11 + 12 = 37 bytes` (this is what the outer 4-byte size header will contain)
- Total **on-disk** size (including the outer size header) = `4 + 37 = 41 bytes`

Now the exact bytes, field by field:

| Field | Byte range (within body) | Bytes (hex) | Human meaning |
|---|---|---|---|
| outer size header | (before body, 4 bytes) | `00 00 00 25` | 0x25 = 37 → "the body is 37 bytes" |
| CRC32 | `body[0:4]` | `48 c2 50 8d` | checksum of `body[4:]` (timestamp+keyLen+key+value) |
| timestamp | `body[4:12]` | `00 00 01 90 8c d9 c3 80` | 1,720,350,000,000 ms since epoch, big-endian int64 |
| key_len | `body[12:14]` | `00 0b` | 0x0b = 11 → key is 11 bytes |
| key | `body[14:25]` | `63 75 73 74 6f 6d 65 72 2d 34 32` | ASCII for `"customer-42"` |
| value | `body[25:37]` | `6f 72 64 65 72 20 70 6c 61 63 65 64` | ASCII for `"order placed"` |

Laid out as one continuous stream of 41 bytes as it would actually sit on disk:

```
offset:  0        4        8        12       14                            25                           37
         │size    │CRC32   │timestamp        │klen│      key (11B)         │       value (12B)          │
bytes:  00000025 48c2508d 000001908cd9c380  000b 637573746f6d65722d3432    6f7264657220706c61636564
         └──4B───┘└──4B──┘└───────8B────────┘└2B─┘└───────11B─────────────┘└───────12B──────────────────┘
```

**Counting every byte**: `4 (size) + 4 (CRC) + 8 (timestamp) + 2 (keyLen) + 11 (key) + 12 (value) = 41 bytes total`, matching the calculation above (`4` outer header + `37` body). Note the CRC32 itself (`48 c2 50 8d`) is computed over *everything from the timestamp field onward* — i.e., over `body[4:]`, which is `timestamp + keyLen + key + value` — deliberately excluding the CRC field itself (you can't checksum a field to detect corruption *in that same field* using itself) and excluding the outer size field (which belongs to the framing layer, not the record's content).

### Understanding Check — Section 4

1. If a single byte inside the `value` field got corrupted on disk (say, `"order placed"` becomes `"order pmaced"` due to bit rot), would `DecodeRecord` catch it? Walk through *why*, referencing exactly which bytes the CRC32 was computed over.
2. Why does the CRC32 field itself get excluded from the checksum computation (i.e., why is it `crc32.ChecksumIEEE(body[4:])` and not `body[0:]`)?
3. Suppose you wanted to add a "producer ID" field (for deduplication / idempotent producers, a real Kafka feature) to this record format. Where would you insert it, and what would happen to `RecordHeaderSize`, and to every offset calculation in `EncodeRecord`/`DecodeRecord` that currently hardcodes `12`, `14`, etc.?

---

## Section 5: Networking — TCP, Framing, and Concurrency

### 5.1 What TCP actually is

TCP (Transmission Control Protocol) is a **reliable, ordered byte pipe** between two programs. That's it — conceptually it's nothing more than a pair of file descriptors, one on each machine, connected such that bytes written into one end come out the other end **in the same order**, with the OS retransmitting silently underneath if packets get lost or corrupted on the wire.

Two guarantees matter for everything that follows:

1. **Ordering** — if you write `A` then `B`, the other side reads `A` then `B`. Never `B` then `A`.
2. **Reliability** — if `Write` returns successfully, the OS guarantees (via acknowledgments and retransmission) that the bytes *will* arrive, or the connection will eventually report an error. You don't get silent byte loss.

What TCP does **not** give you: any notion of "messages." It's a stream of bytes, not a stream of records. If the sender calls `Write([]byte("hello"))` and then `Write([]byte("world"))`, the receiver might see one `Read()` return `"hello"`, or `"hellowor"` and then `"ld"`, or `"helloworld"` all at once — TCP is free to chop up and recombine your writes into whatever packets are convenient. This is called **stream semantics**, and it's the entire reason Section 5.3 (framing) has to exist at all.

Java equivalent: `Socket` / `ServerSocket` give you exactly this — an `InputStream`/`OutputStream` pair with the same "ordered bytes, no message boundaries" contract. Go's `net.Conn` is the direct analog: it implements `io.Reader` and `io.Writer`, i.e. `Read([]byte) (int, error)` and `Write([]byte) (int, error)`.

### 5.2 Why TCP and not HTTP?

Real Kafka, and this project, talk raw TCP rather than HTTP. Three concrete reasons:

1. **Lower per-message overhead.** An HTTP request carries a method line, a URL, a version, headers (`Host`, `Content-Type`, `Content-Length`, `User-Agent`, cookies, etc.) — typically 200+ bytes of text *before* your actual payload. Our raw-TCP produce request for a tiny message is 27 bytes total (worked out in Section 6.3). At millions of messages/second, that HTTP overhead alone would dwarf the actual data.
2. **Persistent connections, not request/response-per-call.** A Kafka producer opens **one** TCP connection to the broker and keeps it open for the life of the process, pushing thousands of produce requests down it back-to-back. HTTP/1.1 *can* keep connections alive too, but it's built around a request→response, request→response rhythm with text parsing overhead on every cycle; a raw socket lets both sides just exchange binary frames with no protocol renegotiation per call.
3. **Binary-friendly.** HTTP headers are text and assume mostly-textual bodies (even though bodies can be binary, the surrounding envelope is line-oriented text that has to be parsed character by character, looking for `\r\n`). Our protocol is binary from byte 0 — fixed-width integer fields read directly into `uint32`/`uint64` with `binary.BigEndian`, no string parsing, no escaping.

The trade-off, honestly: you give up all the free tooling HTTP has (curl, browsers, load balancers, proxies, standard auth). Real Kafka accepts this trade-off because the client is always a purpose-built Kafka client library, never a generic HTTP tool — the same is true here (`cmd/mini-kafka/main.go` is *our* purpose-built client).

### 5.3 Length-prefixed framing over the network — same pattern as disk

Section 3.1 covered the on-disk format `[4-byte size][body]` and explained *why*: without a length prefix, you can't know where one record ends and the next begins when scanning a stream of bytes. TCP has exactly the same problem, for exactly the same reason — Section 5.1 just established that TCP is a stream with no built-in message boundaries.

`protocol.go`'s `WriteFrame` / `ReadFrame` are the network version of the same trick:

```go
// protocol.go — WriteFrame
func WriteFrame(w io.Writer, data []byte) error {
    lenBuf := make([]byte, 4)
    binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
    w.Write(lenBuf)   // Layer 1: how many bytes follow
    w.Write(data)     // Layer 2: the actual body
    return nil
}
```

```go
// protocol.go — ReadFrame
func ReadFrame(r io.Reader) ([]byte, error) {
    lenBuf := make([]byte, 4)
    io.ReadFull(r, lenBuf)                       // block until exactly 4 bytes arrive
    size := binary.BigEndian.Uint32(lenBuf)
    if size > 64*1024*1024 { return nil, fmt.Errorf(...) }  // 64MB sanity cap
    data := make([]byte, size)
    io.ReadFull(r, data)                          // block until exactly `size` bytes arrive
    return data, nil
}
```

The critical detail, and the reason this *works despite* TCP's stream semantics from Section 5.1: `io.ReadFull` doesn't return after one `Read()` syscall — it loops internally, calling `Read()` repeatedly, until it has accumulated **exactly** the requested number of bytes (or hits an error/EOF). So even if the OS delivers the 4-byte length header split across two separate TCP segments (2 bytes in one packet, 2 in the next), `io.ReadFull(r, lenBuf)` blocks and waits for both before returning. This is what re-imposes message boundaries on top of a boundary-less stream.

The 64MB cap on `size` is a deliberate defensive check: without it, a malicious or buggy client could send a length header claiming `size = 4,000,000,000` and the server would try to `make([]byte, 4_000_000_000)` — allocating 4GB — before even validating anything else. Rejecting absurd sizes up front is cheap insurance against memory-exhaustion attacks or simply garbled data.

### 5.4 Two layers of framing: frame vs. body

It's worth being explicit that there are **two nested layers** here, easy to conflate:

```
┌─────────────────────────── Layer 1: FRAME ───────────────────────────┐
│  [4-byte total length]  [                body                     ]  │
│                          └──────────────┬──────────────────────────┘ │
└──────────────────────────────────────────┼────────────────────────────┘
                                           │
                          ┌────────────────▼────────────────────────┐
                          │   Layer 2: BODY (request-type specific)  │
                          │   [1-byte type] [ type-specific fields ] │
                          └───────────────────────────────────────────┘
```

**Layer 1 (framing)** is generic and doesn't know or care what's inside — `ReadFrame`/`WriteFrame` work identically whether the body is a produce request, a heartbeat, or garbage. Its only job is "tell me how many bytes to read before I can safely start interpreting them."

**Layer 2 (body)** is where meaning starts: the first byte says *which* request type this is (`RequestProduce = 1`, `RequestConsume = 2`, etc. — full list in Section 6.1), and everything after that is fields specific to that request type, decoded by functions like `DecodeProduceRequest`.

This separation matters because it means `handleConnection` in `server.go` can read a frame **without knowing what's in it yet**, and only dispatch to a type-specific decoder once it has the whole thing safely in memory:

```go
// server.go — handleConnection (the dispatch loop)
data, err := ReadFrame(conn)      // Layer 1: get the raw body bytes
...
switch data[0] {                  // Layer 2: peek at the type byte
case RequestProduce:
    response = s.handleProduce(data)
case RequestConsume:
    response = s.handleConsume(data)
// ...
}
```

### 5.5 Tracing a full PRODUCE request end-to-end

Let's follow every byte for a real produce call, tying together framing (this section) and encoding (Section 6). Client wants to produce `key="c42", value="hello"` to topic `"orders"`.

```
CLIENT SIDE                                          NETWORK                    SERVER SIDE
────────────                                        ─────────                  ────────────

1. EncodeProduceRequest(req)
   → 23-byte body:
     [01][00 06 6f7264657273][00 03 633432][00 00 00 05 68656c6c6f]

2. WriteFrame(conn, body)
   → writes 4-byte length (00 00 00 17)
   → writes the 23-byte body
                                            ── 27 bytes total on the wire ──►

                                                                              3. Accept() already returned
                                                                                 this conn to a goroutine
                                                                                 (Section 5.6); it's blocked
                                                                                 inside ReadFrame(conn)

                                                                              4. ReadFrame(conn):
                                                                                 io.ReadFull reads 4 bytes
                                                                                 → size = 23
                                                                                 io.ReadFull reads 23 bytes
                                                                                 → data = the body

                                                                              5. data[0] == RequestProduce (1)
                                                                                 → handleProduce(data)

                                                                              6. DecodeProduceRequest(data)
                                                                                 → req.Topic  = "orders"
                                                                                 → req.Key    = []byte("c42")
                                                                                 → req.Value  = []byte("hello")

                                                                              7. s.broker.Publish(topic, key, value)
                                                                                 → hash("c42") % numPartitions
                                                                                   picks a partition
                                                                                 → broker takes its RWMutex
                                                                                   (see Section 5.8)
                                                                                 → segment.Append(record)
                                                                                   writes + fsyncs to disk
                                                                                   (Section 3)
                                                                                 → returns (partition=1, offset=99)

                                                                              8. EncodeProduceResponse(1, 99)
                                                                                 → 12-byte body
                                                                              9. EncodeResponse(StatusOK, body)
                                                                                 → [00][00 00 00 0c][...12 bytes...]
                                                                                    = 17 bytes
                                                                             10. WriteFrame(conn, responseBytes)
                                                                                 → 4-byte length (00 00 00 11)
                                                                                 → 17-byte response body
                                           ◄── 21 bytes total on the wire ───

11. ReadFrame(conn) returns the
    21-byte response body
12. DecodeResponse(respData)
    → status = 0 (OK)
    → body   = 12 bytes
13. DecodeProduceResponse(body)
    → partition = 1, offset = 99
    Producer now knows exactly
    where its message landed.
```

Notice the request travels as **27 bytes** (4-byte frame length + 23-byte body) and the response as **21 bytes** (4-byte frame length + 17-byte response) — for a 5-byte payload (`"hello"`), that's real overhead, but it's fixed, small, and entirely binary; compare to the 200+ bytes of text an equivalent HTTP POST would cost before you even get to the body.

### 5.6 Goroutines: lightweight threads

A **goroutine** is a function running concurrently, scheduled by the Go runtime rather than the OS. The `go` keyword in front of any function call starts it running as a new goroutine and returns immediately — it does *not* wait for that function to finish.

```go
go s.handleConnection(conn)   // fire off handleConnection, keep looping immediately
```

Java equivalent, field by field:

| Go | Java | Notes |
|---|---|---|
| `go f()` | `new Thread(() -> f()).start()` | starts concurrent execution |
| goroutine | `Thread` | the unit of concurrency |
| Go runtime scheduler (M:N, multiplexes goroutines onto OS threads) | JVM thread = 1:1 OS thread | Go can run thousands of goroutines on a handful of OS threads |
| initial stack ~2KB, **grows dynamically** as needed | fixed stack, typically 512KB–1MB per thread (platform/JVM dependent) | this is the "4KB vs 1MB" comparison, more precisely: Go goroutines start *far* smaller (~2KB) and grow on demand, while a Java `Thread`'s stack size is fixed up front and can't shrink |
| `sync.Mutex` / `sync.RWMutex` | `synchronized` block / `ReentrantReadWriteLock` | mutual exclusion |
| `net.Listener` | `ServerSocket` | accepts incoming connections |
| `net.Conn` | `Socket` | one connection's read/write stream |

*Disclosure on the stack-size number*: "4KB vs 1MB" is the commonly quoted comparison and directionally correct, but the precise Go number is an **initial** stack of about 2KB (as of modern Go releases) that the runtime grows (and can shrink) automatically as a goroutine's call depth changes — it's not a fixed 4KB allocation. A Java `Thread`'s stack size, by contrast, is fixed at creation (default varies by platform/JVM, commonly 512KB–1MB) and never grows — if you recurse too deep, you get `StackOverflowError` with no recovery. The practical consequence is the same either way: you can comfortably run tens of thousands of goroutines in a process, where the equivalent number of Java threads would exhaust memory (or at least be a lot more expensive) well before that.

Why this matters for a broker: **one goroutine per client connection** is the entire concurrency model in `server.go`. Because goroutines are so cheap, "spawn a new one per connection and let it block on `ReadFrame` all day" is a perfectly reasonable design — it would *not* be reasonable with OS threads at scale (thousands of clients = thousands of 1MB-stack threads = gigabytes of stack memory just sitting idle).

### 5.7 The accept loop

```go
// server.go — Start()
for {
    conn, err := s.listener.Accept()   // blocks until a client connects
    if err != nil {
        select {
        case <-s.quit:
            return nil               // Stop() was called — clean shutdown
        default:
            log.Printf("accept error: %v", err)
            continue
        }
    }
    s.wg.Add(1)
    go s.handleConnection(conn)        // hand this connection to its own goroutine
}
```

Walking through this line by line, Java-style:

- `s.listener.Accept()` — Java: `Socket conn = serverSocket.accept();`. Both block the calling goroutine/thread until a new TCP connection arrives.
- `s.wg.Add(1)` — increments a `sync.WaitGroup` counter. This is Go's version of "keep a list of running worker threads so I can `join()` them all later." `Stop()` calls `s.wg.Wait()`, which blocks until every `handleConnection` goroutine has called `s.wg.Done()` (via its `defer`) — i.e., until every client has actually finished.
- `go s.handleConnection(conn)` — spawn the worker, immediately loop back to `Accept()` for the *next* client. The accept loop's only job is accepting; it never touches the actual request-handling logic itself.

The `select { case <-s.quit: ... default: ... }` pattern is how `Accept()` erroring out gets disambiguated: `Stop()` closes `s.listener`, which makes the blocked `Accept()` call return an error immediately. But a *closed-listener* error and a *transient* accept error (e.g. "too many open files") look the same to `Accept()` — both are just `error`. Checking `s.quit` (a channel that `Stop()` closes) distinguishes "we were told to shut down" from "something unexpected went wrong, log it and keep serving other clients."

### 5.8 How many goroutines are running with 3 connected clients?

**Four.** Concretely:

1. The goroutine executing `Start()`'s `for` loop — this is whichever goroutine called `server.Start()` (often `main()` itself, or a goroutine spawned to run the server). It's parked inside `Accept()` waiting for the *next* (4th) client.
2. Client A's `handleConnection(connA)` goroutine — blocked inside `ReadFrame(connA)`, waiting for A's next request.
3. Client B's `handleConnection(connB)` goroutine — same, waiting on B.
4. Client C's `handleConnection(connC)` goroutine — same, waiting on C.

None of these are burning CPU while idle — they're all parked in a blocking syscall (`Accept()` or `Read()` under the hood), which the OS wakes them from only when there's actual data or a new connection. This is exactly why "one goroutine per connection, block-and-wait" scales to thousands of idle connections without pegging the CPU: idle goroutines cost memory (a small, growable stack) but essentially zero CPU.

### 5.9 Where the mutex comes in

`server.go` itself holds no locks — it has no shared mutable state beyond the `sync.WaitGroup` (which is concurrency-safe by design) and `s.quit` (a channel, also safe for concurrent use). All 3 client goroutines call into the **same** `*Broker`, though, and `broker.go` protects its shared state (the topic/partition map, consumer group state, etc.) with a `sync.RWMutex`:

```go
// broker.go
type Broker struct {
    mu sync.RWMutex   // allows multiple readers OR one writer, never both
    ...
}
```

So if clients A and B both call `Publish` (which takes `b.mu.Lock()`, a full write lock) at the same time, one of their goroutines will actually block for a moment inside the broker, waiting for the other's lock to release — this is the real point of concurrency contention in the system, not the TCP layer. Reads (like `Consume`, which takes `b.mu.RLock()`) can run concurrently with each other, just not concurrently with a write. Java equivalent: `sync.RWMutex` ≈ `java.util.concurrent.locks.ReentrantReadWriteLock`; a plain `sync.Mutex` (used nowhere in `Broker` here, but common elsewhere) ≈ a `synchronized` block or `ReentrantLock`.

### Understanding Check — Section 5

1. `ReadFrame` calls `io.ReadFull(r, lenBuf)` rather than a single `r.Read(lenBuf)`. Construct a concrete scenario (in terms of how many bytes the OS delivers per `Read()` call) where using plain `Read()` instead of `ReadFull` would silently corrupt the parsed frame length, and explain why `ReadFull`'s internal loop avoids it.
2. If `Stop()` is called while 2 of the 3 client goroutines are in the middle of a slow `s.broker.Publish()` call (say, waiting on `fsync`), walk through exactly what happens to `s.wg.Wait()`, and why `Stop()` doesn't return until those in-flight calls finish.
3. Suppose 100 clients connect simultaneously. Immediately after all 100 `Accept()` calls have returned, how many goroutines exist that are part of this connection-handling machinery, and how much total *stack* memory would that use very roughly, contrasted with the same design implemented with one Java `Thread` per client?

### Interview Questions — Section 5

1. **"Why can't you just read a fixed number of bytes off a TCP socket and assume that's exactly one message?"** — Expect: TCP has stream semantics, not message semantics; the OS is free to fragment or coalesce writes into arbitrary packet boundaries, so the *application* has to impose its own framing (length-prefix, delimiter, or fixed-size records) to know where messages start and end.
2. **"You have a server handling 10,000 concurrent slow client connections. Would you use one thread per connection, and why might the answer differ between Java and Go?"** — Expect: in Java, 10,000 threads × ~1MB stack ≈ 10GB just in stacks (which is why Java servers historically move to thread pools, NIO/epoll-based event loops, or now virtual threads); in Go, 10,000 goroutines with small growable stacks is a normal, idiomatic design precisely because the per-goroutine overhead is so much smaller.

---

## Section 6: The Wire Protocol — Every Request Type

### 6.1 All 9 request types

Every request body's first byte is one of these type constants, defined in `protocol.go`:

| Constant | Value (dec) | Value (hex) | Meaning |
|---|---|---|---|
| `RequestProduce` | 1 | `0x01` | Write one message to a topic |
| `RequestConsume` | 2 | `0x02` | Read one message at a specific topic/partition/offset |
| `RequestCreateTopic` | 3 | `0x03` | Create a topic with N partitions |
| `RequestCommit` | 4 | `0x04` | Consumer group: save "I'm done up to offset X" |
| `RequestFetchOffset` | 5 | `0x05` | Consumer group: "where should I resume reading?" |
| `RequestJoinGroup` | 6 | `0x06` | Consumer joins a group, gets partition assignment back |
| `RequestHeartbeat` | 7 | `0x07` | Consumer says "I'm alive," gets current assignment back |
| `RequestLeaveGroup` | 8 | `0x08` | Consumer gracefully leaves a group |
| `RequestProduceBatch` | 9 | `0x09` | Write multiple messages in one call, one fsync |

Every one of these type bytes is `data[0]` of the frame body — this is exactly the "Layer 2" byte from Section 5.4's diagram, and it's what `handleConnection`'s `switch data[0]` (server.go) dispatches on.

### 6.2 PRODUCE — byte layout

```go
// EncodeProduceRequest format (protocol.go):
//   [1 byte:  type = 1]
//   [2 bytes: topic length][topic bytes]
//   [2 bytes: key length][key bytes]
//   [4 bytes: value length][value bytes]
```

```
┌────┬─────┬──────────┬─────┬─────────┬───────────┬─────────────┐
│type│tlen │  topic    │klen │   key   │  vlen (4B) │    value    │
│ 1B │ 2B  │ (tlen B)  │ 2B  │(klen B) │            │  (vlen B)   │
└────┴─────┴──────────┴─────┴─────────┴───────────┴─────────────┘
```

Note the asymmetry: topic length and key length are **2-byte** fields (max 65,535 bytes — plenty for a topic name or a routing key), but value length is a **4-byte** field (max ~4GB) because message payloads can legitimately be large (a serialized JSON blob, a file chunk, etc.), while topic names and keys are always short strings by convention.

### 6.3 Hand-encoding a real ProduceRequest

Concrete example: `topic = "orders"`, `key = "c42"`, `value = "hello"`.

**Step 1 — sizes:**
- `topic = "orders"` → 6 bytes
- `key = "c42"` → 3 bytes
- `value = "hello"` → 5 bytes
- body size = `1 (type) + 2 (tlen) + 6 (topic) + 2 (klen) + 3 (key) + 4 (vlen) + 5 (value) = 23 bytes`
- frame size (with outer 4-byte length header) = `4 + 23 = 27 bytes`

**Step 2 — every byte, named:**

| Field | Bytes (hex) | Value |
|---|---|---|
| frame length | `00 00 00 17` | 0x17 = 23 → "body is 23 bytes" |
| type | `01` | `RequestProduce` |
| topic length | `00 06` | 6 |
| topic | `6f 72 64 65 72 73` | ASCII `"orders"` (o=6f, r=72, d=64, e=65, r=72, s=73) |
| key length | `00 03` | 3 |
| key | `63 34 32` | ASCII `"c42"` (c=63, 4=34, 2=32) |
| value length | `00 00 00 05` | 5 |
| value | `68 65 6c 6c 6f` | ASCII `"hello"` (h=68, e=65, l=6c, l=6c, o=6f) |

**Step 3 — as one continuous stream (27 bytes), exactly as it appears on the wire:**

```
offset: 0        4  5     7           13   14         17                  21                26
        │flen    │ty│tlen │  topic     │klen│   key    │      vlen         │      value       │
bytes: 0000 0017 01 0006 6f7264657273  0003 633432    0000 0005            68656c6c6f
        └──4B───┘└1┘└2B─┘└────6B──────┘└2B─┘└──3B─────┘└──────4B──────────┘└──────5B─────────┘
```

Total: `4 + 1 + 2 + 6 + 2 + 3 + 4 + 5 = 27 bytes` — matches Step 1's `4 + 23`.

### 6.4 CONSUME — byte layout and hand-encoded example

```go
// EncodeConsumeRequest format (protocol.go):
//   [1 byte:  type = 2]
//   [2 bytes: topic length][topic bytes]
//   [4 bytes: partition]
//   [8 bytes: offset]
```

```
┌────┬─────┬──────────┬───────────┬───────────────────┐
│type│tlen │  topic    │ partition │       offset        │
│ 1B │ 2B  │ (tlen B)  │    4B     │        8B           │
└────┴─────┴──────────┴───────────┴───────────────────┘
```

Concrete example: `topic = "orders"`, `partition = 2`, `offset = 99`.

**Sizes:** body = `1 + 2 + 6 + 4 + 8 = 21 bytes`; frame = `4 + 21 = 25 bytes`.

| Field | Bytes (hex) | Value |
|---|---|---|
| frame length | `00 00 00 15` | 0x15 = 21 |
| type | `02` | `RequestConsume` |
| topic length | `00 06` | 6 |
| topic | `6f 72 64 65 72 73` | `"orders"` |
| partition | `00 00 00 02` | 2, big-endian `uint32` |
| offset | `00 00 00 00 00 00 00 63` | 99 (0x63), big-endian `uint64` |

As one stream (25 bytes):

```
offset: 0        4  5     7          13         17                          25
        │flen    │ty│tlen│  topic     │partition │           offset            │
bytes: 0000 0015 02 0006 6f7264657273 0000 0002  0000 0000 0000 0063
        └──4B───┘└1┘└2B┘└────6B──────┘└──4B─────┘└──────────8B─────────────────┘
```

Note `offset = 99` still costs the full 8 bytes even though the value is tiny — offsets are `uint64` unconditionally, because a long-lived, high-throughput partition can accumulate offsets well past what a 32-bit integer could hold (`uint32` maxes out at ~4.29 billion; a busy partition can blow past that in weeks).

### 6.5 JOIN_GROUP — byte layout with example

```go
// EncodeJoinGroupRequest format (protocol.go):
//   [1 byte:  type = 6]
//   [2 bytes: group length][group bytes]
//   [2 bytes: memberID length][memberID bytes]
//   [2 bytes: topic length][topic bytes]
```

```
┌────┬─────┬───────┬─────┬──────────┬─────┬────────┐
│type│glen │ group  │mlen │ memberID  │tlen │ topic  │
│ 1B │ 2B  │(glen B)│ 2B  │(mlen B)  │ 2B  │(tlen B)│
└────┴─────┴───────┴─────┴──────────┴─────┴────────┘
```

Concrete example: `group = "orders-cg"`, `memberID = "worker-1"`, `topic = "orders"`.

- `group` → 9 bytes: `6f 72 64 65 72 73 2d 63 67` (`"orders-cg"`)
- `memberID` → 8 bytes: `77 6f 72 6b 65 72 2d 31` (`"worker-1"`)
- `topic` → 6 bytes: `6f 72 64 65 72 73` (`"orders"`)
- body size = `1 + 2+9 + 2+8 + 2+6 = 30 bytes`; frame = `4 + 30 = 34 bytes`

```
frame len:  00 00 00 1e                                       (30 = 0x1e)
type:       06
group:      00 09  6f 72 64 65 72 73 2d 63 67                 (len=9, "orders-cg")
memberID:   00 08  77 6f 72 6b 65 72 2d 31                    (len=8, "worker-1")
topic:      00 06  6f 72 64 65 72 73                          (len=6, "orders")
```

The response body for `JoinGroup` (and `Heartbeat`, which shares the format) is `EncodeAssignmentResponse(partitions)` — `[4-byte count][4-byte partition]...`. If the broker assigns partitions `[0, 2]` to this member: `00 00 00 02` (count=2) `00 00 00 00` (partition 0) `00 00 00 02` (partition 2) — 12 bytes total.

### 6.6 Response format: status + length-prefixed body

Every response — regardless of which request produced it — is wrapped the same way by `EncodeResponse`:

```go
// EncodeResponse format (protocol.go):
//   [1 byte:  status (0 = OK, 1 = Error)]
//   [4 bytes: body length]
//   [body]
```

```
┌────────┬───────────┬──────────────┐
│ status  │ body len   │     body      │
│   1B    │    4B      │  (bodylen B)  │
└────────┴───────────┴──────────────┘
```

This is itself then wrapped in the *outer* frame (Section 5.4's Layer 1) before going on the wire — so a full response is actually `[4-byte frame length][1-byte status][4-byte body length][body]`, i.e. **two separate length fields**: the frame length describes the whole `status+bodylen+body` blob, and the body length describes just `body`. This looks redundant but isn't: the frame length lets `ReadFrame` slurp the whole response off the socket without knowing anything about status codes, and the inner body length lets `DecodeResponse` then slice out exactly `body` from whatever `ReadFrame` handed back — two independent concerns (network framing vs. response-body demarcation), so it's correct for them to be independently expressed even though in practice `bodylen == framelen - 5`.

**Concrete example**: a successful produce lands at `partition = 1, offset = 99`.

- `EncodeProduceResponse(1, 99)` → 12 bytes: `00 00 00 01` (partition, `uint32`) `00 00 00 00 00 00 00 63` (offset, `uint64`)
- `EncodeResponse(StatusOK, body)`:

```
status:    00                         (StatusOK)
body len:  00 00 00 0c                (12)
body:      00 00 00 01  00 00 00 00 00 00 00 63
```

- Total response body = `1 + 4 + 12 = 17 bytes`; wrapped in the outer frame: `4 + 17 = 21 bytes` total on the wire — matching the trace in Section 5.5.

An **error** response looks the same shape, just with `status = 0x01` and `body` being the UTF-8 bytes of the error message string (e.g. `s.broker.Publish` returning `err`, then `[]byte(err.Error())` becomes the body verbatim — no special error struct, just raw text).

### 6.7 PRODUCE_BATCH — sending 3 messages in one call

```go
// EncodeProduceBatchRequest format (protocol.go):
//   [1 byte:  type = 9]
//   [2 bytes: topic length][topic bytes]
//   [4 bytes: message count]
//   for each message:
//     [2 bytes: key length][key bytes]
//     [4 bytes: value length][value bytes]
```

```
┌────┬─────┬───────┬───────┬──────────────────┬──────────────────┬──────────────────┐
│type│tlen │ topic  │ count  │     message 0      │     message 1      │     message 2      │
│ 1B │ 2B  │(tlenB) │  4B    │ [klen|key|vlen|val] │ [klen|key|vlen|val] │ [klen|key|vlen|val] │
└────┴─────┴───────┴───────┴──────────────────┴──────────────────┴──────────────────┘
```

Concrete example: `topic = "orders"`, 3 messages: `(key="c1", value="hi")`, `(key="c2", value="yo")`, `(key="c3", value="hey")`.

**Per-message sizes:**
- msg0: `2 + 2 (key "c1") + 4 + 2 (value "hi") = 10 bytes`
- msg1: `2 + 2 (key "c2") + 4 + 2 (value "yo") = 10 bytes`
- msg2: `2 + 2 (key "c3") + 4 + 3 (value "hey") = 11 bytes`

**Total body:** `1 (type) + 2+6 (topic) + 4 (count) + 10 + 10 + 11 = 44 bytes`. Frame: `4 + 44 = 48 bytes`.

```
frame len:   00 00 00 2c                              (44)
type:        09
topic:       00 06  6f 72 64 65 72 73                 ("orders")
count:       00 00 00 03                              (3 messages)

msg0:  klen 00 02   key 63 31 ("c1")   vlen 00 00 00 02   val 68 69       ("hi")
msg1:  klen 00 02   key 63 32 ("c2")   vlen 00 00 00 02   val 79 6f       ("yo")
msg2:  klen 00 02   key 63 33 ("c3")   vlen 00 00 00 03   val 68 65 79    ("hey")
```

This is the mechanism behind "one fsync per partition instead of one fsync per message" mentioned in the project plan (Section 3.7's throughput math: ~4ms per fsync caps you at ~250 single-message writes/sec). `PublishBatch` (in `broker.go`, called from `handleProduceBatch` in `server.go`) groups these 3 messages by partition and calls `segment.Append` + a single `fsync` per partition touched, rather than 3 separate fsyncs — turning a ~12ms round trip (3 × 4ms) for 3 messages into roughly ~4ms total whenever they land on the same partition. The response, `EncodeBatchProduceResponse`, mirrors this: `[4-byte count][12-byte partition+offset]...` per message, so the producer learns exactly where each of its 3 messages landed, individually, in one response.

### Understanding Check — Section 6

1. `readString16` and `readBytes32` (the shared decode helpers in `protocol.go`) both check `pos+N > len(data)` before reading, where `N` depends on the field. Using the `ProduceRequest` decode path, trace what happens if a malicious client sends a frame claiming `topic length = 60000` but only actually includes 6 bytes of topic data after the length field — which check catches it, and what error comes back?
2. Why does `ConsumeRequest`'s `offset` field need 8 bytes (`uint64`) while its `partition` field only needs 4 bytes (`uint32`)? Think about the realistic maximum value each field could take on a long-running, high-throughput system.
3. The response envelope has two separate length fields — the outer frame length (`WriteFrame`'s 4 bytes) and the inner body length (`EncodeResponse`'s 4 bytes) — and in every case here `bodylen == framelen - 5`. Since one is always derivable from the other, why isn't this actually redundant/wasteful? (Hint: think about which function needs to know which length, and whether either one could do its job with only the *other* length available.)

### Interview Questions — Section 6

1. **"Design a binary wire protocol for a request that can carry either a small routing key or a large arbitrary payload. What size would you make each length-prefix field, and why?"** — Expect: match the length-prefix field's byte-width to the field's realistic maximum size — short, bounded identifiers (topic names, keys) get smaller prefixes (e.g. 2 bytes, 64KB ceiling), while unbounded payload fields get wider prefixes (4 bytes, ~4GB ceiling) — plus a sanity cap somewhere (like the 64MB frame cap in `ReadFrame`) so a corrupted or malicious length field can't trigger a huge allocation.
2. **"Walk me through how you'd add a 10th request type to this protocol without breaking existing clients already talking to the server."** — Expect: pick an unused type byte (10), add `Encode`/`Decode` functions following the existing `[type][fields...]` convention, add a `case RequestX:` branch in the server's dispatch switch — and because the type byte is the very first thing read, and each request type's decoder is self-contained, existing request types are completely unaffected; the risk is only in *changing* an existing type's format, which would need a version negotiation or a new type byte instead of mutating type 1..9 in place.

---

## Section 7: Consumer Groups — Full Deep Dive

### 7.1 The problem: manual partition assignment doesn't scale

Section 2 introduced partitions as the unit of parallelism — a topic with N partitions can be read by up to N consumers in parallel, since each partition is only ever read by one consumer in a group at a time (this is what guarantees ordering *within* a partition while still allowing concurrency *across* partitions).

The naive way to build this: have a human, or a static config file, decide "consumer-1 reads partitions 0-2, consumer-2 reads partitions 3-5." That works exactly until:

- A consumer crashes. Now partitions 0-2 stop being read at all, and nobody is watching for that — the operator has to notice, edit the config, and restart something.
- You want to scale out (add a 3rd consumer to speed up processing). Someone has to recompute the split by hand and redeploy every consumer with new config.
- Two consumers accidentally get assigned the same partition (a typo, a stale config) — now you have two readers racing on one partition, silently double-processing or interleaving in confusing ways.

None of this is acceptable for a system meant to run unattended for months. The fix is to make partition assignment a *service* the broker provides: consumers don't declare "I read partitions X, Y, Z" — they declare "I want to join group G, reading topic T," and the broker's `Coordinator` (`coordinator.go`) computes and hands back the assignment, automatically reacting to consumers joining, leaving, or dying. This is exactly what real Kafka's group coordinator does (the mini-kafka version is a deliberately simplified single-node version of the same idea — no partition leader election for the coordinator itself, no JoinGroup/SyncGroup two-phase protocol, just a direct compute-and-return, covered in 7.3's disclosure).

### 7.2 The lifecycle, step by step, with a timeline

Here's the full sequence using the scenario from the project's own tests (`coordinator_test.go`): a topic `orders` with 4 partitions, group `processors`.

```
t=0.0s   Consumer A starts.
         → sends JoinGroupRequest{Group:"processors", MemberID:"A", Topic:"orders"}
         → server.go's handleJoinGroup → broker.JoinGroup → coordinator.JoinGroup
         → group "processors" doesn't exist yet → created
         → rebalance() runs: 1 member, 4 partitions → A gets ALL of them
         → A's JoinGroup response: assignment = [0,1,2,3]

t=0.5s   Consumer A's poll loop (runConsumer, cmd/mini-kafka/main.go) sends
         HeartbeatRequest{Group:"processors", MemberID:"A"} — this is iteration 1
         of A's `for { ... time.Sleep(500ms) }` loop. Response: assignment
         unchanged, [0,1,2,3]. A reads from all 4 partitions this iteration.

t=1.0s   Consumer B starts.
         → JoinGroupRequest{Group:"processors", MemberID:"B", Topic:"orders"}
         → coordinator.JoinGroup adds B to group.Members, calls rebalance()
         → rebalance sees 2 members, 4 partitions → target = 2 each
         → A had [0,1,2,3] (over target by 2) → excess [2,3] taken from A,
           orphaned = [2,3]
         → B is new, starts empty, gets the 2 orphaned partitions
         → B's JoinGroup response: assignment = [2,3]
         → NOTE: A is NOT notified yet. A is still happily reading partitions
           2 and 3 as if it still owned them — the broker changed A's
           assignment map, but A only *learns* about it on its next Heartbeat.

t=1.5s   A's poll loop hits its next heartbeat (iteration 3, ~500ms after the
         last one). HeartbeatRequest → coordinator.Heartbeat returns A's
         *current* map entry, which rebalance already shrank to [0,1].
         → runConsumer's sameAssignment(old=[0,1,2,3], new=[0,1]) is false
         → prints "REBALANCE! new assignment: [0,1] (was [0,1,2,3])"
         → A stops reading partitions 2 and 3.

t=Ns     Consumer B is killed with SIGKILL (crash — no graceful shutdown,
         so no LeaveGroupRequest is ever sent). B simply stops heartbeating.

t=N+1s to N+10s   Nothing happens yet. B's Member.LastHeartbeat timestamp is
         frozen at whenever its last heartbeat was. The coordinator's
         reapLoop background goroutine (started in NewCoordinator) ticks
         every 1 second, calling reapDeadMembers(), but the elapsed time
         hasn't crossed the 10-second heartbeatTimeout yet
         (broker.go: NewCoordinator(10 * time.Second)).

t=~N+10s (up to N+11s, since the check is on a 1s tick, not instant)
         reapDeadMembers() finds now.Sub(B.LastHeartbeat) > 10s → deletes B
         from group.Members → calls rebalance() → A (the only member left)
         gets targetFloor=4/1=4 → all 4 partitions back: [0,1,2,3].

t=next A heartbeat (within 500ms of the reap)
         A's heartbeat response shows [0,1,2,3] again → "REBALANCE!" printed
         → A resumes reading all 4 partitions.
```

Two things worth calling out explicitly, because they're easy to get wrong when reasoning about this system:

1. **Rebalance is computed the instant membership changes** (inside `JoinGroup`/`LeaveGroup`/`reapDeadMembers`, all of which call `rebalance()` synchronously while holding `Coordinator.mu`) — but a *given* consumer only *finds out* about a rebalance the next time it calls `Heartbeat`. There's no push notification; it's a pull/polling model. That means the maximum delay between "the broker decided your assignment changed" and "you found out" is bounded by how often your poll loop heartbeats — 500ms in this implementation's `runConsumer`, not the 3-second interval real Kafka clients default to (`heartbeat.interval.ms=3000`). This project chose a faster loop, trading more network chatter for lower rebalance-detection latency.
2. **Detecting a dead consumer is not instant.** It takes up to `heartbeatTimeout` (10s) *plus* up to 1 extra second (the `reapLoop` tick granularity) before anyone notices B is gone. During that whole window, B's partitions are simply not being read by anyone — this is the real, unavoidable cost of heartbeat-based failure detection: you cannot distinguish "dead" from "briefly slow" without waiting long enough that a live-but-slow member would have had a chance to check in.

### 7.3 The StickyAssignor algorithm

`rebalance()` (`coordinator.go` lines 171-253) has two goals, and the comment in the source states their priority explicitly:

```go
// Goals (in priority order):
//   1. BALANCE: every member gets within 1 partition of the same count
//   2. STICKY: minimize partition movements from previous assignment
```

Balance comes first: nobody should be starved while someone else hoards partitions. Stickiness comes second: *given* a balanced outcome, prefer the one that moves the fewest partitions between members, because every partition that changes owner has a cost — the new owner has to fetch its committed offset and start consuming from possibly-cold state, and if you're mid-processing a message when your partition gets ripped away, that work may need to be redone by whoever picks it up.

The five steps, mapped directly onto the code:

| # | Step | Code |
|---|---|---|
| 1 | Keep existing assignments for live members only | lines 189-200: for each *currently alive* member (sorted), if it had a previous entry in `group.Assignments`, keep it verbatim; brand-new members start with `[]int{}` |
| 2 | Collect orphaned partitions | lines 202-208: any partition number `0..NumPartitions-1` not marked `assigned[p]=true` in step 1 (i.e., it belonged to a member who just left/died, or was never assigned) |
| 3 | Calculate target floor/ceil/numCeil | lines 210-225: `targetFloor = NumPartitions / len(members)`, `numCeil = NumPartitions % len(members)` — that remainder is *how many* members get one extra partition (`targetFloor+1`); the first `numCeil` members in **sorted ID order** get the ceiling, everyone else gets the floor |
| 4 | Steal excess from over-target members | lines 227-237: for each member (sorted), if they're holding more than their target, chop their slice down to `target` and throw the tail (`current[target:]`) into the orphaned pile |
| 5 | Distribute orphaned to under-target members | lines 239-250: sort the orphaned list ascending, then walk members in sorted order, topping each one up to its target from the front of the orphaned list |

A few implementation details worth being precise about, since they affect the exact output in specific scenarios:

- **"Excess" is always taken from the end of a member's current slice** (`current[target:]`), not chosen by any notion of "which partition has been held longest." Since new orphans get appended to the end of a slice during a *previous* rebalance (step 5), this happens to mean freshly-acquired partitions are the first to be taken away again in a subsequent rebalance — but that's an artifact of slice order, not a deliberate LRU policy.
- **Ties are broken by sorted member ID and ascending partition number**, both explicitly (`sortStrings`, `sortInts` — simple insertion sorts, fine for the small N this toy system expects). This makes the algorithm fully deterministic given the same sequence of joins/leaves, which is exactly what the test suite (`coordinator_test.go`) relies on to make assertions.
- **This is a full recompute every time, not incremental/cooperative rebalancing.** Real Kafka (since KIP-429's `CooperativeStickyAssignor`) can rebalance *without* first revoking every partition from every member — only the partitions that actually need to move are paused. This implementation's `rebalance()` is simpler: it recomputes the entire assignment map from scratch on every join/leave/reap, but because step 1 preserves live members' existing entries verbatim before touching anything, the *practical effect* for unaffected members is identical to cooperative rebalancing (they never lose partitions they're entitled to keep) — it's just implemented as "keep what's still valid" rather than "diff old vs. new and only touch the delta."

### 7.4 Full Trace #1 — a member dies

Setup: topic with 6 partitions, group has three members with this *starting* assignment (already balanced and stable from some earlier rebalance):

```
A = [0, 1]
B = [3, 4]
C = [2, 5]
```

**B dies** (or calls `LeaveGroup` — the rebalance math is identical either way; only the *trigger* differs, per 7.5). `coordinator.LeaveGroup("group", "B")` deletes B from `group.Members`, then calls `rebalance(group)`.

**Step 1 — keep live members' existing assignments:**

```
memberIDs (sorted, live only) = [A, C]     ← B is gone, excluded entirely

newAssignments["A"] = [0, 1]   (kept from group.Assignments["A"])
newAssignments["C"] = [2, 5]   (kept from group.Assignments["C"])

assigned = {0:true, 1:true, 2:true, 5:true}
```

Notice B's old entry `group.Assignments["B"] = [3, 4]` is never even consulted — B isn't in `memberIDs` anymore, so step 1's loop just never visits it. Its partitions become orphaned by *omission*, not by explicit removal.

**Step 2 — collect orphaned partitions:**

```
for p := 0 to 5:
  p=0 → assigned[0]=true → skip
  p=1 → assigned[1]=true → skip
  p=2 → assigned[2]=true → skip
  p=3 → assigned[3] is unset → orphaned
  p=4 → assigned[4] is unset → orphaned
  p=5 → assigned[5]=true → skip

orphaned = [3, 4]
```

**Step 3 — target per member:**

```
targetFloor = 6 / 2 = 3
numCeil     = 6 % 2 = 0    ← divides evenly, nobody needs the +1
targetFor("A") = 3
targetFor("C") = 3
```

**Step 4 — steal excess from over-target members:**

```
A: len([0,1]) = 2, target = 3 → 2 is NOT > 3 → no excess taken
C: len([2,5]) = 2, target = 3 → 2 is NOT > 3 → no excess taken

orphaned unchanged: [3, 4]
```

Neither survivor is *over* target — makes sense, since with 2 partitions each and a target of 3, they're actually *under*.

**Step 5 — distribute orphaned to under-target members:**

```
sorted orphaned = [3, 4]
orphanIdx = 0

member "A" (first in sorted order):
  len(newAssignments["A"])=2 < target=3 → take orphaned[0]=3
    newAssignments["A"] = [0, 1, 3]     orphanIdx → 1
  len=3, not < 3 → stop for A

member "C":
  len(newAssignments["C"])=2 < target=3 → take orphaned[1]=4
    newAssignments["C"] = [2, 5, 4]     orphanIdx → 2
  len=3, not < 3 → stop for C
```

**Final result:**

```
A = [0, 1, 3]      (kept its original 2, gained partition 3)
C = [2, 5, 4]      (kept its original 2, gained partition 4)
```

Both A and C kept 100% of what they had before — exactly what `TestStickyMemberDies` in `coordinator_test.go` asserts (`aKept != len(aBefore)` and `cKept != len(cBefore)` are both failure conditions, meaning full retention is required to pass). B's two orphaned partitions were split one-each to the two survivors, in ascending partition-number order, to whichever survivor was earlier alphabetically first.

### 7.5 Full Trace #2 — a new member joins

Setup: topic with 6 partitions, group has two members, already balanced:

```
A = [0, 1, 2]
B = [3, 4, 5]
```

**D joins** (`coordinator.JoinGroup` adds D with no prior entry, calls `rebalance`).

**Step 1:**

```
memberIDs (sorted) = [A, B, D]

newAssignments["A"] = [0, 1, 2]   (kept)
newAssignments["B"] = [3, 4, 5]   (kept)
newAssignments["D"] = []          (brand new — no previous entry exists)

assigned = {0,1,2,3,4,5} all true
```

**Step 2:** every partition 0-5 is already in `assigned` → `orphaned = []` (nothing unassigned yet — the "orphaning" here is going to come entirely from step 4, not step 2).

**Step 3:**

```
targetFloor = 6 / 3 = 2
numCeil     = 6 % 3 = 0
targetFor(A) = targetFor(B) = targetFor(D) = 2
```

**Step 4 — steal excess:**

```
A: len([0,1,2])=3, target=2 → 3 > 2 → excess = current[2:] = [2]
   newAssignments["A"] = current[:2] = [0, 1]
   orphaned = [] + [2] = [2]

B: len([3,4,5])=3, target=2 → 3 > 2 → excess = current[2:] = [5]
   newAssignments["B"] = current[:2] = [3, 4]
   orphaned = [2] + [5] = [2, 5]

D: len([])=0, target=2 → 0 is NOT > 2 → nothing to steal from D
```

**Step 5 — distribute:**

```
sorted orphaned = [2, 5]
orphanIdx = 0

A: len([0,1])=2, target=2 → already at target, loop body never runs
B: len([3,4])=2, target=2 → already at target, loop body never runs
D: len([])=0 < target=2 → take orphaned[0]=2 → D=[2], orphanIdx→1
   len([2])=1 < target=2 → take orphaned[1]=5 → D=[2,5], orphanIdx→2
   len([2,5])=2, not < 2 → stop
```

**Final result:**

```
A = [0, 1]
B = [3, 4]
D = [2, 5]
```

Exactly what `TestStickyMinimalMoves` checks for (each original member must keep at least 2 of its original 3 partitions — here both A and B keep exactly 2, the minimum required and the maximum possible under a balance constraint of 2-each).

### 7.6 Comparing to round-robin: how many partitions actually moved?

Take trace #2's before/after and compare against what a naive **round-robin** reassignment (partition `p` → member `p mod 3`, over sorted members `[A, B, D]`) would produce instead, recomputing from a blank slate with no memory of who had what before:

```
Round-robin (no memory of prior state):
  partition 0 → member index 0 → A
  partition 1 → member index 1 → B
  partition 2 → member index 2 → D
  partition 3 → member index 0 → A
  partition 4 → member index 1 → B
  partition 5 → member index 2 → D

  → A=[0,3]  B=[1,4]  D=[2,5]
```

| Partition | Owner before | Sticky owner after | Round-robin owner after | Sticky move? | Round-robin move? |
|---|---|---|---|---|---|
| 0 | A | A | A | no | no |
| 1 | A | A | B | no | **yes** |
| 2 | A | D | D | **yes** | **yes** |
| 3 | B | B | A | no | **yes** |
| 4 | B | B | B | no | no |
| 5 | B | D | D | **yes** | **yes** |

**Sticky: 2 partitions moved (2 and 5). Round-robin: 4 partitions moved (1, 2, 3, and 5).** Both algorithms are equally "balanced" (2 partitions per member in both outcomes) — balance alone doesn't distinguish them. The difference is entirely in the second goal: sticky assignment recognizes that A and B don't need to be touched at all beyond shedding their one excess partition each, while a memoryless round-robin recompute treats every rebalance like day one and reshuffles almost everything, even partitions that didn't need to move for the sake of balance. In a real system, every one of those extra 2 moves in round-robin is a consumer that has to stop reading a partition it was actively, correctly processing, fetch a possibly-stale committed offset, and start over — pure waste that stickiness avoids.

### 7.7 Heartbeat-based failure detection

The failure-detection half of this system is entirely separate machinery from the assignment algorithm above — it just decides *when* to remove a member and trigger the rebalance that `rebalance()` computes.

**`reapLoop`** (`coordinator.go` lines 264-273) is a goroutine started once, in `NewCoordinator`, that lives for the life of the broker:

```go
func (c *Coordinator) reapLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.reapDeadMembers()
	}
}
```

Every second, it calls `reapDeadMembers()`, which — under the same `Coordinator.mu` lock used by `JoinGroup`/`Heartbeat`/`LeaveGroup` — walks every group, checks every member's `LastHeartbeat` against `now`, and if `now.Sub(member.LastHeartbeat) > c.heartbeatTimeout` (10 seconds, set once in `broker.go`'s `NewCoordinator(10 * time.Second)` call), deletes that member and rebalances the group (or deletes the whole group if it's now empty).

This gives two very different paths to "a member is gone," with very different latency:

| Path | Trigger | Latency until rebalance |
|---|---|---|
| **Graceful `LeaveGroup`** | Consumer catches SIGINT/SIGTERM (`runConsumer`'s signal handler, `cmd/mini-kafka/main.go`) and explicitly sends `LeaveGroupRequest` before exiting | Immediate — `coordinator.LeaveGroup` calls `rebalance()` synchronously as part of handling the request |
| **Timeout (crash, network partition, SIGKILL)** | No more heartbeats arrive; `LastHeartbeat` goes stale | Up to `heartbeatTimeout` (10s) + up to 1 extra second (the reap tick granularity) — worst case ~11 seconds of "nobody is reading these partitions" |

This is exactly the same tradeoff real Kafka makes with `session.timeout.ms` (member presumed dead) and its own background expiration checking — heartbeat timeouts are fundamentally a bet on how long you're willing to wait before treating "haven't heard from you" as "you're dead," and there's no way to make that instant without false-positives on a merely slow-but-alive consumer (covered in the interview questions below).

### 7.8 The poll-based consumer loop

`runConsumer` (`cmd/mini-kafka/main.go`, starting around line 380) is a single long-lived TCP connection running this loop forever:

```
┌─────────────────────────────────────────────────────────────┐
│  for {                                                        │
│    3a. Heartbeat  → send HeartbeatRequest, get assignment     │
│         if assignment changed from what we had:               │
│           print "REBALANCE!", fetch offsets for any NEW       │
│           partitions we didn't have before                    │
│    3b. Read       → for each assigned partition, ConsumeRequest│
│                      at our tracked offset; advance offset+1  │
│                      on success, otherwise leave offset alone │
│                      (means "caught up, nothing new yet")      │
│    3d. Commit     → every 10th iteration, CommitRequest for   │
│                      every assigned partition's current offset│
│    3e. Sleep       → 500ms, then repeat                       │
│  }                                                             │
└─────────────────────────────────────────────────────────────┘
```

**How rebalance is detected** — there is no push mechanism; the *only* signal a running consumer has that its assignment changed is comparing the assignment returned by its *own* heartbeat call to what it remembered before (`sameAssignment(assignment, newAssignment)`, a simple slice-equality helper at the bottom of `main.go`). If they differ, that means the coordinator's map changed underneath it — because another member joined, left, or was reaped — sometime between the last heartbeat and this one. On a rebalance, the loop must also fetch the committed offset for any *newly* assigned partition it didn't already have an entry for (`offsets[p]`), so it knows where to resume reading rather than starting from offset 0.

One subtlety already visible in 7.2's timeline: because heartbeats and reads happen in the *same* loop iteration, a consumer that just had partitions taken away from it (by a rebalance the coordinator already computed) will keep reading those old partitions for **up to one more full loop iteration (≤500ms)** after the rebalance actually happened server-side, simply because it hasn't called `Heartbeat` again yet to find out. This is generally harmless here (worst case: a brief overlap where two consumers both read the same partition for under half a second) but is worth naming explicitly as a real consequence of the pull-based design.

### Understanding Check — Section 7

1. In trace #1 (6 partitions, `A=[0,1]`, `B=[3,4]`, `C=[2,5]`, B dies), suppose instead **A** died (not B), leaving `B=[3,4]` and `C=[2,5]` alive. Walk through all 5 steps of `rebalance()` yourself and determine the final assignment. (Hint: A's orphaned partitions are `[0,1]` — which of B and C is "first" in sorted order to receive them?)
2. `targetFor` in step 3 gives the *first* `numCeil` members (in sorted ID order) the ceiling value and everyone else the floor. For 7 partitions and 3 members (`A`, `B`, `C`), what are `targetFloor`, `numCeil`, and the exact target for each of A, B, and C? Which member(s) end up with the "extra" partition?
3. Section 7.7 says graceful `LeaveGroup` triggers an immediate rebalance while a timeout takes up to ~11 seconds. If a consumer's process hangs (e.g., stuck in an infinite loop, not crashed, not exited) but its TCP connection is still technically open and it simply stops calling `Heartbeat`, which path does the coordinator take to eventually reclaim its partitions — and how would you observe this happening by watching `coordinator_test.go`-style logs?

### Interview Questions — Section 7

1. **"What if the network between the consumer and broker is slow, but the consumer itself is alive and healthy?"** — Expect: the coordinator has no way to distinguish "genuinely dead" from "alive but its heartbeats keep arriving late/getting dropped" — both look identical from the coordinator's side (no heartbeat received within the timeout window). A consumer on a flaky network can get spuriously reaped, lose its partitions to a rebalance, and then have its *next* heartbeat rejected (since `coordinator.Heartbeat` returns an error for a member no longer in `group.Members` — it would need to re-`JoinGroup` from scratch). The only lever is tuning `heartbeatTimeout` up (fewer false positives, but slower real-failure detection) or down (opposite tradeoff) — there's no way to get both simultaneously with a fixed timeout.
2. **"What's the point of adding more consumers than there are partitions?"** — Expect: none — `rebalance()`'s `targetFor` computation means once `len(memberIDs) > NumPartitions`, `targetFloor` becomes 0 and only `numCeil` (= `NumPartitions`) members get exactly 1 partition; every additional consumer beyond the partition count gets `target=0`, i.e., an empty assignment, permanently idle unless another member later dies. The maximum *useful* number of consumers in a group equals the partition count of the topic — this is a hard ceiling on read parallelism for that topic, and it's why choosing partition count at topic-creation time matters (Section 2's tradeoff: more partitions = more parallelism headroom, but also more file handles, more per-partition overhead, and messier ordering guarantees since ordering is only per-partition).
3. **"Why sticky assignment instead of plain round-robin?"** — Expect the 7.6 comparison directly: round-robin recomputes the entire mapping from scratch on every membership change and is memoryless, so it can move partitions that had absolutely no reason to move (as shown: 4 moves vs. 2 for an equally "balanced" outcome). Every unnecessary move costs a consumer its in-flight processing state and forces a re-fetch of the committed offset before it can resume — real cost with zero benefit, since round-robin's final balance is no better than sticky's. Sticky assignment gets identical balance with strictly fewer disruptions by explicitly preserving valid prior assignments (step 1) before considering any reshuffling at all.

---

## Section 8: Log Compaction

### 8.1 The problem: retention deletes everything, including current state

Section 3 covered *retention* — deleting old segments once they age out (time-based) or once the log grows past a size limit. That's the right policy for an event stream where old events genuinely stop mattering (yesterday's page-view events are useless once you've computed yesterday's aggregate). But retention is the *wrong* policy for a different, equally common use of a log: using it as a changelog of the *current state* of a set of keyed entities.

Concretely: if topic `user-profiles` uses `key = userID` and a message `value = {the profile}` every time a profile changes, then time-based retention would eventually delete *every* update for a given user, including the most recent one — leaving nothing. But the whole point of that topic might be "at any time, replaying it from the start reconstructs the current state of every user's profile." Deleting old messages breaks that guarantee entirely, because you can't tell which deleted message was the *last* one for a given key.

**Log compaction** solves this by using a completely different retention rule: instead of "delete anything older than X," it's "for every key, keep only the most recent message with that key — everything else can go." The log gets smaller not because time passed, but because it's redundant: an older value for a key that's since been overwritten is, by definition, no longer needed to reconstruct current state.

### 8.2 Real-world use cases

| Use case | Key | Value | Why compaction fits |
|---|---|---|---|
| User profile store | `userID` | serialized profile | Only the latest profile per user matters; history of edits is noise for this purpose |
| Application config | config key name | config value | Config topics are read entirely on startup to rebuild current config state — old values are dead weight |
| Inventory/stock levels | `productID` | current quantity | Only the current count matters for "how many do we have right now" |
| Kafka's own `__consumer_offsets` topic | `(group, topic, partition)` | committed offset | This is *exactly* how real Kafka stores consumer group offsets internally — the offsets topic is itself a compacted topic, so it doesn't grow forever even though offsets are committed continuously |

Note the common shape: all of these are really "a log used as a table" — a stream of updates that's meant to be collapsed into a map of `key → latest value`, which is precisely what compaction produces mechanically.

### 8.3 Before vs. after, a concrete example

Eight messages produced, in order, to a topic using keys `A`, `B`, `C`:

```
Before compaction (8 messages, offsets 0-7):
  offset 0: key=A  value="v1"
  offset 1: key=B  value="v1"
  offset 2: key=A  value="v2"
  offset 3: key=C  value="v1"
  offset 4: key=B  value="v2"
  offset 5: key=A  value="v3"
  offset 6: key=C  value="v2"
  offset 7: key=B  value="v3"

After compaction (3 messages remain, relative order preserved):
  key=C  value="v2"     ← was offset 6, the last write to C
  key=B  value="v3"     ← was offset 7, the last write to B
  key=A  value="v3"     ← was offset 5, the last write to A

  Wait — order preserved means by ORIGINAL position, not by key:
  the surviving records stay in their original relative order:
    offset 5 (A, v3), offset 6 (C, v2), offset 7 (B, v3)
```

8 messages become 3 — the other 5 (every earlier write to A, B, or C) are gone, because a strictly later message with the same key made them obsolete.

### 8.4 The algorithm: two passes over the segment

`CompactSegment` (`compactor.go`) works entirely on one already-*closed* segment at a time, and is a textbook two-pass algorithm — read the whole thing once to learn what to keep, then read it again to actually keep it:

```go
// Pass 1: Find the latest offset for each key
latestForKey := make(map[string]int) // key string → local index

for localIdx := 0; localIdx < totalRecords; localIdx++ {
    key, err := readKeyAtIndex(seg, localIdx)
    if err != nil {
        continue // corrupted record — keep it to be safe, don't touch it
    }
    latestForKey[string(key)] = localIdx  // always overwrite — later wins
}
```

Because `localIdx` walks forward from 0, and a later index always *overwrites* the map entry for that key, `latestForKey[key]` ends up holding the *highest* index seen for that key by the time the loop finishes — no explicit "is this newer" comparison is needed, just "overwrite unconditionally, in order."

```go
// Build the "keep" set from the survivors of pass 1
keepSet := make(map[int]bool)
for _, idx := range latestForKey {
    keepSet[idx] = true
}
removed := totalRecords - len(keepSet)
```

```go
// Pass 2: walk the segment again, keep only indices in keepSet,
// in their ORIGINAL relative order (the loop is still 0..totalRecords-1
// ascending — we're filtering, not reordering)
for localIdx := 0; localIdx < totalRecords; localIdx++ {
    if !keepSet[localIdx] {
        continue // superseded by a later message with the same key
    }
    // ...read the full raw record (size header + body) from the OLD file...
    // ...write it, byte-for-byte, to the compacted file...
}
```

The write side reads the **full raw bytes** of each surviving record — `[4-byte size header][CRC][timestamp][keyLen][key][value]`, the exact on-disk format from Section 4 — and copies them verbatim into a new file, `<original-path>.compacted`, tracking a fresh `newIndex` of byte offsets as it goes (positions shift once earlier records are dropped, so the index can't just be reused).

Finally, the atomic swap:

```go
seg.file.Close()
os.Rename(compactedPath, origPath)   // atomic on POSIX filesystems
f, _ := os.OpenFile(origPath, os.O_RDWR, 0644)
seg.file = f
seg.index = newIndex
seg.size = newSize
```

`os.Rename` on the same filesystem is atomic at the OS level — the directory entry for `origPath` either still points to the *old* inode (if the process crashes before rename) or points to the *fully-written, synced* new file (if it crashes after) — there is no observable intermediate state where `origPath` is half-old, half-new. That's why the compacted file is written under a different name (`.compacted`) first, fully flushed to disk (`compactedFile.Sync()`), and *then* swapped into place, rather than truncating and rewriting `origPath` directly (which absolutely would risk a corrupted half-written file if the process died mid-write).

### 8.5 Step-by-step trace with actual index values

Take 6 messages landing in one segment, local indices 0 through 5:

```
localIdx:  0        1        2        3        4        5
key:       A        B        A        C        B        A
value:     "1"      "1"      "2"      "1"      "2"      "3"
```

**Pass 1 — building `latestForKey`, one index at a time:**

```
localIdx=0, key="A" → latestForKey = {A:0}
localIdx=1, key="B" → latestForKey = {A:0, B:1}
localIdx=2, key="A" → latestForKey = {A:2, B:1}        (A overwritten: 0 → 2)
localIdx=3, key="C" → latestForKey = {A:2, B:1, C:3}
localIdx=4, key="B" → latestForKey = {A:2, B:4, C:3}   (B overwritten: 1 → 4)
localIdx=5, key="A" → latestForKey = {A:5, B:4, C:3}   (A overwritten: 2 → 5)

Final: latestForKey = {A:5, B:4, C:3}
```

**Building `keepSet` from the map's values:**

```
keepSet = {5:true, 4:true, 3:true}
removed = totalRecords(6) - len(keepSet)(3) = 3
```

**Pass 2 — filtering, in original ascending order:**

```
localIdx=0 (A,"1") → keepSet[0]? no  → SKIP
localIdx=1 (B,"1") → keepSet[1]? no  → SKIP
localIdx=2 (A,"2") → keepSet[2]? no  → SKIP
localIdx=3 (C,"1") → keepSet[3]? YES → WRITE (new file position 0)
localIdx=4 (B,"2") → keepSet[4]? YES → WRITE (new file position 1)
localIdx=5 (A,"3") → keepSet[5]? YES → WRITE (new file position 2)
```

**Result: the compacted segment holds exactly 3 records, in this order:**

```
new index 0: key=C value="1"   (the only C ever written — survives untouched)
new index 1: key=B value="2"   (superseded B's earlier value="1")
new index 2: key=A value="3"   (superseded A's earlier values "1" and "2")
```

Every discarded record (indices 0, 1, 2) was discarded *specifically* because a later index in the same map bucket beat it — index 0 (A,"1") lost to index 2 (A,"2") which then lost to index 5 (A,"3"); index 1 (B,"1") lost to index 4 (B,"2"); index 3 (C,"1") had no competitor and survived by default.

### 8.6 Why only compact CLOSED segments

`CompactLog` (`compactor.go`) is explicit about this:

```go
// Compact every segment EXCEPT the last one (active)
for i := 0; i < len(l.segments)-1; i++ {
    removed, err := CompactSegment(l.segments[i])
    ...
}
```

and `CompactDir` mirrors it (skip the last `.log` file, "assumed active"). The reason is that `CompactSegment` fundamentally assumes the record count (`len(seg.index)`) is *fixed* for the duration of both passes — pass 1 counts `totalRecords` once at the top and both loops iterate `0..totalRecords`. If new records were being appended to the *active* segment concurrently by `Log.Append` while compaction ran, at minimum the two passes would disagree about what `totalRecords` even means, and worse, `CompactSegment` takes `seg.mu.Lock()` for its *entire* duration — which would simply block all new writes to that partition until compaction finished, defeating the purpose of only compacting in the background. Restricting compaction to already-closed, immutable segments sidesteps the whole problem: a closed segment's contents are never going to change again, so there's no concurrent-writer hazard to reason about at all.

One thing to flag plainly, since it's a real gap versus this being fully production-safe: **`CompactSegment` itself has no internal check that the segment it's given actually is closed** — that invariant is enforced entirely by the *callers* (`CompactLog` and `CompactDir` both explicitly skip `len(segments)-1`, the last/active one). If some other code path called `CompactSegment` directly on an active segment, nothing in `CompactSegment` would stop it, and the file would be actively growing under a lock that's also blocking writers — this is a documented convention, not a compiler- or runtime-enforced guarantee.

### 8.7 CRC validity after compaction

Recall from Section 4 that each record's CRC32 is computed once, at write time, over `body[4:]` (timestamp + keyLen + key + value), and stored in `body[0:4]`. Compaction's pass 2 copies `fullRecord := [size header][body]` **byte-for-byte** from the old file to the new one:

```go
fullRecord := make([]byte, 4+int(bodySize))
seg.file.ReadAt(fullRecord, pos)          // read the exact original bytes
compactedFile.WriteAt(fullRecord, newSize) // write those exact bytes, unmodified
```

No field is recomputed, reinterpreted, or altered — only the record's *position* in the file changes (it moves to `newSize`, wherever that happens to land after earlier discarded records shrank the file). Since the CRC32 was a checksum of the record's *content*, not its *location*, and the content didn't change even by one bit, the stored CRC remains exactly as valid after compaction as it was before — `DecodeRecord`'s `storedCRC != actualCRC` check on a compacted record will find a match, just as it would have on the original file, because compaction is purely a content-*preserving*, position-*changing* operation for anything that survives it.

### Understanding Check — Section 8

1. In the trace in 8.5, what would change if a 7th message arrived with `key=""` (empty string, no key at all)? Trace through `readKeyAtIndex`'s `keyLen == 0` branch (`compactor.go`) and `latestForKey[string(key)]` — what happens to any *other* keyless messages that might already be in the segment?
2. `removed := totalRecords - len(keepSet)` is computed once, right after pass 1, before pass 2 even runs. Why is this safe — i.e., why is it guaranteed that pass 2 will keep *exactly* `len(keepSet)` records, no more and no fewer?
3. Suppose compaction crashes (process killed) after `compactedFile.Sync()` succeeds but before `os.Rename` executes. What is the on-disk state? What if it crashes *during* `os.Rename` itself (assume rename is atomic at the OS level)? In both cases, does the segment lose any data that a consumer had already committed against?

### Interview Questions — Section 8

1. **"How does compaction interact with consumers actively reading from the segment being compacted?"** — Expect: this implementation sidesteps the hardest version of this question by only ever compacting *closed* segments while holding `seg.mu.Lock()` for the whole operation — any reader calling into that segment (e.g., via `Log.Read`, which also needs `seg.mu`) simply blocks until compaction finishes, then reads from the now-swapped-in compacted file. The subtler issue this doesn't fully solve: a consumer mid-read that's tracking a specific **offset** may find that offset no longer exists after compaction (its record was superseded and dropped) — real Kafka handles this by guaranteeing that a compacted-away offset "fast-forwards" the consumer to the next surviving offset ≥ the one it asked for, rather than erroring; this implementation doesn't remap offsets after compaction, which is a simplification worth calling out explicitly.
2. **"What about tombstones — deleting a key entirely, not just updating it?"** — Expect: real Kafka's compaction supports a special **null value** as a tombstone marker — `key=X, value=null` means "delete key X's state entirely," and after a configurable retention window, even the tombstone itself gets removed, letting a key vanish from the compacted view completely. This implementation's `CompactSegment` has **no tombstone concept at all** — every key that's ever been written keeps its *latest non-deleted* value forever; there is no way to signal "and now forget this key," which means the "user profile store" use case in 8.2 can update or overwrite a user's data via compaction, but can never truly delete one. This is a real, disclosed gap versus production Kafka, not an oversight this deep dive is glossing over.

---

## Section 9: Batch Writes — The Performance Story

### 9.1 The problem, stated as a number

Section 3.7 already introduced the raw fact: `fsync` — the system call that forces the OS to actually push written bytes out to the physical disk, rather than leaving them sitting in a volatile write-back cache — costs a real, measurable amount of wall-clock time. On this project's own test hardware (Apple Silicon SSD), that cost is **~4 milliseconds per call**. That number isn't a rounding error or a rare tail latency — it's the *typical*, every-single-time cost, because `fsync` has to wait for the storage medium to confirm the write landed, and confirming a write is fundamentally slower than issuing one.

Do the arithmetic on what that means if you fsync once per message, which is exactly what `Segment.Append` (`segment.go`, lines 63-96) does:

```
1 message → 1 write → 1 fsync (~4ms)
1000 milliseconds ÷ 4ms per message = 250 messages/second, MAXIMUM
```

That ceiling doesn't move no matter how fast your CPU is, how much RAM you have, or how efficient your code is elsewhere — you are bottlenecked entirely on the disk's confirmation latency, once per message, full stop. At 256-byte messages (this project's benchmark default), 250 msg/s works out to:

```
250 msg/s × 256 bytes = 64,000 bytes/s = 62.5 KB/s ≈ 0.06 MB/s
```

0.06 MB/s is not a typo. A system that can theoretically write hundreds of megabytes per second to the same SSD sequentially is being throttled down to *kilobytes* per second purely by calling `fsync` too often. This is the single biggest performance cliff in the entire codebase, and it exists for a genuinely good reason (durability — Section 3.6's crash-recovery guarantee depends on fsync actually having happened before you tell a producer "yes, your message is safely written"), which is exactly why the fix has to be smarter than "just don't fsync."

### 9.2 The solution: pay the fsync cost once per N messages, not once per message

The fix does not remove `fsync`. It amortizes its cost across a batch. Instead of:

```
write(msg1) → fsync → write(msg2) → fsync → write(msg3) → fsync → ...
```

do:

```
write(msg1) → write(msg2) → write(msg3) → ... → write(msgN) → ONE fsync
```

The disk-confirmation cost — that ~4ms — gets paid exactly once, no matter whether the batch holds 1 message or 10,000. The individual `write` calls (via `WriteAt`, no `Sync`) are themselves fast — they're just handing bytes to the OS's in-memory page cache, which is a memory-speed operation, not a disk-speed one. The *only* slow step was ever the fsync, and batching means you do that slow step 1000× less often for the same 1000 messages.

### 9.3 The math, worked through at three scales

**Single-message fsync (the baseline):**
```
1 message  × 1 fsync (4ms)  = 4ms for 1 message   → 250 msg/s
```

**Batched, 1000 messages per fsync (this project's default batch size, `bench.go` line 128):**
```
1000 messages × 1 fsync (4ms) = 4ms for 1000 messages → 250,000 msg/s (theoretical ceiling)
```
That's the theoretical ceiling *if fsync were the only cost at all*. In practice the 1000 individual `WriteAt` calls aren't free — each one is a real syscall with its own (much smaller) overhead — so real measured throughput lands well below that theoretical number but still enormously above the single-fsync baseline. This project's own benchmark run (see 9.5) measures roughly **116,000–133,000 msg/s** batched, comfortably in "tens of megabytes per second" territory (28-32+ MB/s at 256-byte messages) rather than the single-message mode's kilobytes per second.

**Why the theoretical number (250,000) and the measured number (~133,000) differ:** the theoretical calculation only accounts for the one fsync; it ignores the cost of 1000 separate `WriteAt` syscalls (two per message — one for the 4-byte size header, one for the payload, see 9.4), Go's own function-call and slice-append overhead in the loop, and OS-level scheduling. None of that is a bug — it's simply the difference between "the dominant cost, fsync, amortized to near-zero" and "every cost, including the now-secondary ones, still present." The headline point survives either way: fsync went from the *only* cost that mattered (100% of the 4ms) to a *negligible* fraction of the total batch time.

### 9.4 How `AppendBatch` actually works (`segment.go`, lines 154-189)

```go
func (s *Segment) AppendBatch(payloads [][]byte) ([]uint64, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    offsets := make([]uint64, len(payloads))
    pos := s.size

    for i, payload := range payloads {
        offsets[i] = s.baseOffset + uint64(len(s.index))

        sizeBuf := make([]byte, 4)
        binary.BigEndian.PutUint32(sizeBuf, uint32(len(payload)))
        s.file.WriteAt(sizeBuf, pos)   // write size header — NO fsync
        s.file.WriteAt(payload, pos+4) // write payload      — NO fsync

        s.index = append(s.index, pos) // update in-memory index
        pos += 4 + int64(len(payload))
    }

    // ONE fsync for the entire batch — this is where the speed comes from
    if err := s.file.Sync(); err != nil {
        return nil, err
    }

    s.size = pos
    return offsets, nil
}
```

Walk through this against `Segment.Append` (the single-message version, lines 63-96) and the structural difference is exactly one line: `Append` calls `s.file.Sync()` *inside* the (implicit, single-iteration) write; `AppendBatch` calls it *once*, *after* the `for` loop has written every message in the batch. Everything else — writing the 4-byte big-endian size header, writing the payload immediately after it, appending the message's starting byte position (`pos`) to `s.index` so it can be looked up later by local index — is identical per-message work, just repeated `len(payloads)` times before the single `Sync()` call at the end. The offset returned for each message (`s.baseOffset + uint64(len(s.index))`, captured *before* that message's index entry is appended) is computed exactly the same way it would be for a series of individual `Append` calls — batching changes *when* you fsync, not what offset each message gets or where it physically lands in the file.

### 9.5 How `PublishBatch` uses `AppendBatch` (`broker.go`, lines 256-316)

`AppendBatch` operates on one `Segment` — but a real produce call from a client sends messages destined for potentially *different* partitions in the same batch (e.g., a batch with messages keyed `customer-42`, `customer-7`, `customer-42` again — Section 2.2's routing rule sends the two `customer-42` messages to the same partition, but `customer-7`'s message might land on a different one). `Broker.PublishBatch` handles this by grouping first, then batching per group:

1. **Group messages by destination partition.** For each message in the incoming batch, compute its partition the same way `Publish` (single-message) does — hash the key with FNV-1a if present (`hashKey(msg.Key, numPartitions)`), or round-robin by running partition-size totals if there's no key (lines 276-285). Messages headed for the same partition get collected into the same group (`groups := make(map[int][]indexedPayload)`, line 273), preserving each message's `originalIdx` so results can be reassembled in the caller's original order afterward.
2. **Call `AppendBatch` once per group, not once per message.** Line 303: `offsets, err := partitions[pIdx].AppendBatch(payloads)`. If a 3-message batch has 2 messages for partition 1 and 1 for partition 0, that's exactly 2 `AppendBatch` calls total — one per partition actually touched — not 3 individual `Append` calls and not a single global fsync across partitions (partitions are independent files; there is no way to fsync two different files with one syscall).
3. **Reassemble results in the caller's original order.** Line 309-314 walks each group's `items`, using the stashed `originalIdx` to place each `PublishResult{Partition, Offset}` back at the position the corresponding message held in the *original* input slice — so the caller's response ordering matches their request ordering even though internally the messages were shuffled into per-partition groups and back out again.

Net effect: **one fsync per partition actually touched by the batch**, regardless of how many messages in the batch went to that partition. A 1000-message batch that's entirely one key (one partition) costs exactly 1 fsync total. A 1000-message batch spread evenly across 4 partitions costs 4 fsyncs total (250 messages each) — still a massive win over 1000 individual fsyncs, just not quite as dramatic as the single-partition case.

### 9.6 The tradeoff: what you lose by not fsyncing every message

This is not a free lunch, and the codebase (and this deep dive) says so explicitly rather than hiding it.

**Per-message fsync (`Append`):** every message is durably on disk, confirmed, before the caller gets a success response. If the process is killed (`kill -9`) one nanosecond after that response was sent, the message is still there on restart — it was already fsynced. Maximum messages lost on crash: **0 acknowledged messages** (Section 3.6's crash-recovery guarantee covers exactly this case — any message that was *never* acknowledged, i.e., was mid-write, is discarded on restart via truncation, but nothing previously confirmed is ever lost).

**Batched fsync (`AppendBatch`):** messages 1 through 999 of a 1000-message batch are sitting in the OS's page cache — written to the file's in-memory representation, index-tracked, believed durable by nothing and no one — until message 1000 triggers the batch's single `Sync()` call. If the process crashes at message 500 (killed, power loss, OS panic — anything that doesn't let `Sync()` run), **all 500 of those written-but-unsynced messages can vanish** on restart, because the OS never actually pushed them to physical storage; on a clean restart, crash recovery (Section 3.6) will find whatever subset of the batch happened to have been physically flushed by the OS's own background writeback (which is not guaranteed, not ordered by application semantics, and not something you can rely on) and discard/never-see the rest. Maximum messages lost on crash: **up to N** (the configured batch size — 1000 in this project's default).

```
Per-message fsync:  lose 0 acknowledged messages     (safest, slowest — 250 msg/s)
Batched (N=1000):   lose up to 1000 unacked messages (fastest, riskiest — ~130K msg/s)
```

**Real Kafka's actual answer to this tradeoff is not "pick a smaller batch size and hope."** It's **replication**: a production Kafka partition has multiple broker replicas (commonly 3), and a producer can request `acks=all`, meaning the leader broker doesn't tell the producer "success" until a configurable quorum of *replica* brokers have all received the message — not necessarily fsynced to disk on any single one of them, but held independently, on independent machines, with independent failure domains. If one broker's OS cache loses an unsynced batch to a crash, the message still exists on the other replicas' memory/disk, and the cluster's replication protocol (out of scope for this single-node project, but this is the real answer) promotes one of the surviving replicas and the data is never actually lost cluster-wide — even though it might have been "unsynced" on any one individual machine at the moment of that machine's crash. This is why real Kafka can run with relatively aggressive batching *and* strong durability guarantees simultaneously: durability comes from **replica count**, not from every single machine paying the full fsync-per-message tax. This project is single-node, so it cannot make that trade — which is exactly why this deep dive states the batched-fsync risk plainly rather than presenting batching as a strict improvement with no cost.

### 9.7 The benchmark output (actual measured numbers, `cmd/mini-kafka/bench.go`)

Running `mini-kafka bench --messages 100000 --size 256` (100,000 messages, 256 bytes each) on this project's own test hardware produces (per `README.md`'s recorded benchmark):

```
============================================
   SUMMARY
============================================
  Write (fsync/msg):      0.07 MB/s  |      270 msg/s  |  p99 = 5.0ms
  Write (batched):       32.47 MB/s  |  133,009 msg/s
  Read (sequential):    310.94 MB/s  |  1,273,629 msg/s

  Batching speedup: 493× faster writes

  Tradeoff: batched writes are faster but if crash occurs mid-batch,
  the entire unfsynced batch is lost (up to 1000 messages).
  Per-message fsync loses at most 0 acknowledged messages.
```

**270 msg/s → 133,009 msg/s is a 493× improvement** — read that number twice, because it's not an exaggeration or a cherry-picked run: it's the direct, mechanical consequence of paying a ~4ms tax once per 1000 messages instead of once per message. The read benchmark (sequential, no fsync involved at all — reads don't write anything) is faster still, at over 1.27 million messages/second, because sequential reads only have to contend with the OS's read-ahead and page cache, never with a disk-confirmation round trip — this is the same "sequential I/O beats random I/O, and beats write-with-fsync" theme from Section 3's storage engine discussion, just visible here as a hard number.

### Understanding Check — Section 9

1. If `fsync` cost 1ms instead of 4ms (a faster SSD), what would single-message throughput become? What would the *batched* throughput become, and would the *speedup ratio* (batched ÷ single) change, stay the same, or is there not enough information to tell? (Hint: think about which parts of the batched cost scale with fsync latency and which don't.)
2. `PublishBatch` groups messages by partition before calling `AppendBatch` per group. If a 100-message batch has 97 messages for partition 0 and 1 message each for partitions 1, 2, and 3, how many total fsync calls does this batch trigger? Compare that to what it would cost if `PublishBatch` didn't group at all and just called `Append` (single-message, fsync-per-call) on each of the 100 messages individually.
3. Section 9.6 says batched writes can lose "up to N" messages on a mid-batch crash. Is it possible for a batch to lose data even *without* a crash — i.e., is there any scenario where `AppendBatch` returns successfully (no error) but some messages in the batch aren't actually durable? Look again at where `s.file.Sync()` is called relative to the `for` loop, and what happens if `Sync()` itself returns an error partway through — does the function tell the caller which specific messages, if any, were affected?

### Interview Questions — Section 9

1. **"What batch size would you choose in production?"** — Expect: there's no universally correct number — it's a direct tradeoff between throughput (bigger batches amortize fsync cost further) and blast radius on crash (bigger batches risk losing more unacknowledged messages, and also increase producer-side latency, since a producer waiting for a response has to wait for the *whole* batch to fill or a timeout to fire before it gets sent at all). A reasonable production answer references real Kafka's own defaults and knobs: `batch.size` (bytes, not message count) and `linger.ms` (how long to wait for a batch to fill before sending anyway), tuned against measured latency/throughput requirements for the specific workload — for a latency-sensitive workload, small batches or `linger.ms=0`; for a throughput-maximizing bulk-ingest pipeline, large batches and a nonzero linger. The 1000-message batch size in this project is a fixed default chosen for a clear benchmark demonstration, not a tuned production value.
2. **"How does replication make batched fsync safe?"** — Expect the 9.6 answer directly: durability stops being "did *this* machine's disk confirm the write" and becomes "did a quorum of independent replica machines *receive* the write" — with `acks=all` and multiple replicas, a single broker's unsynced-batch loss on crash doesn't lose the message cluster-wide, because the message is independently held by other replicas that didn't crash. This decouples the throughput question (how aggressively can one broker batch its local fsyncs) from the durability question (how many independent copies of the data exist) — a single-node system like this project can't make that separation, which is exactly why it has to choose a point on the fsync-per-message ↔ fsync-per-batch spectrum and disclose the resulting risk, rather than getting to have both maximum throughput and zero-loss durability at once.

---

## Section 10: Go Language Guide — Every Concept Used

This section explains, from scratch, every Go language feature this codebase relies on — assuming zero prior Go knowledge but fluency in Java. For each concept: what it is, a real snippet from this codebase, and the closest Java equivalent.

### 10.1 Packages

A Go **package** is the unit of code organization and visibility — closely analogous to a Java package, except a Go package is usually just "everything in this one directory," not a nested namespace hierarchy. Every `.go` file starts with a `package` declaration; every file in the same directory must declare the *same* package name to belong together.

```go
// segment.go, log.go, broker.go — all start with:
package minikafka
```

**Java equivalent:**
```java
package com.example.minikafka;
```

The critical difference from Java is **capitalization-based visibility**, not an `public`/`private`/`protected` keyword: an identifier (function, type, field, variable) starting with a **capital letter** is exported (visible outside the package, like Java `public`); starting with a **lowercase** letter, it's package-private (like Java default/package-private access, roughly). `func (s *Segment) AppendBatch(...)` is capitalized → callable from `broker.go` (different file, same package) *and* from an importing package like `cmd/mini-kafka/bench.go` (via `minikafka.NewLog`, capitalized). `func hashKey(...)` (lowercase) is only callable from within package `minikafka` itself.

### 10.2 Variables

Go has two main ways to declare a variable, and — unlike Java — type inference is idiomatic and common, not a "modern convenience" bolted on later.

```go
numMessages := 100000        // := infers the type (int) from the value
var messageSize int = 256    // explicit type, explicit declaration
var dataDir string           // explicit type, zero value ("" for string)
```

**Java equivalent:**
```java
var numMessages = 100000;    // Java 10+ var: infers int
int messageSize = 256;       // explicit type
String dataDir;              // explicit type, null (not a true "zero value" like Go's "")
```

The `:=` operator can *only* be used when declaring a new variable (it both declares and assigns in one step); `=` alone is assignment to an already-declared variable. Go's zero values are a real language guarantee (every type has a well-defined default: `0` for numbers, `""` for strings, `nil` for pointers/slices/maps) — there's no equivalent of Java's `null` being the default for every uninitialized reference type combined with "uninitialized primitive is a compile error"; Go's rule is uniform and every declared-but-unassigned variable is immediately usable at its zero value.

### 10.3 Structs

A Go `struct` is a plain data container with named, typed fields — the closest analogue to a Java class, but with **no inheritance whatsoever**, no constructors (in the Java sense — you write a plain function like `NewLog` that returns a struct literal instead), and no access-modifier keywords on individual fields (visibility is the same capital/lowercase rule as 10.1, applied per-field).

```go
// log.go
type LogConfig struct {
    MaxSegmentBytes int64 // start a new segment when the active one exceeds this size
    MaxLogBytes     int64 // delete oldest segments when total log size exceeds this
}
```

**Java equivalent:**
```java
public class LogConfig {
    public long maxSegmentBytes; // start a new segment when the active one exceeds this size
    public long maxLogBytes;     // delete oldest segments when total log size exceeds this
}
```

There's no `extends` in Go — code reuse across structs is done via **composition** (embedding one struct inside another) rather than inheritance, and via interfaces (10.13) for polymorphism, not a class hierarchy.

### 10.4 Methods

A Go **method** is a function with a special "receiver" parameter written *before* the function name, in its own parentheses — this is what attaches a function to a specific struct type, playing the same role as an instance method on a Java class, where the receiver name (commonly a short abbreviation like `s` or `l`) plays the role of Java's implicit `this`.

```go
// segment.go
func (s *Segment) Append(payload []byte) (uint64, error) {
    // `s` refers to the specific Segment this method was called on — like `this` in Java
    s.mu.Lock()
    defer s.mu.Unlock()
    ...
}
```

**Java equivalent:**
```java
public class Segment {
    public long append(byte[] payload) throws IOException {
        // `this` is implicit — you'd write `this.mu.lock()` or just `mu.lock()`
        mu.lock();
        try {
            ...
        } finally {
            mu.unlock();
        }
    }
}
```

You call it the same way you'd call a Java instance method: `segment.Append(payload)` — Go just makes the receiver explicit and named in the function signature, rather than implicit like Java's `this`.

### 10.5 Pointers

A Go pointer (`*Segment`) holds the *memory address* of a value, letting you refer to and mutate the original object rather than a copy — this concept doesn't map onto a Java keyword directly, because **in Java, every object variable already behaves like a pointer/reference** (assigning or passing a Java object never copies its fields; it copies the reference to the same object). Go, by contrast, has *both* value types (structs passed by copy, by default) and pointer types (explicit `*T`, passed as a reference) — you have to choose.

```go
// segment.go
func (s *Segment) Append(payload []byte) (uint64, error) { ... }
//         ^ *Segment — a pointer receiver. Mutating s.index or s.size here
//           mutates the REAL Segment, not a throwaway copy.
```

If `Append`'s receiver were `(s Segment)` (no `*`), Go would copy the entire `Segment` struct into `s` on every call, and any mutation (`s.size = pos + ...`) would be invisible to the caller the instant the method returned — which is why every method in this codebase that needs to mutate state uses a pointer receiver (`*Segment`, `*Log`, `*Broker`).

**Java equivalent:** there isn't a direct syntactic equivalent, because Java doesn't let you choose value-vs-reference semantics for your own classes — every Java object reference *is* effectively what Go calls a pointer. The closest mental model: Go's `*Segment` behaves exactly like a normal Java `Segment` variable (mutations visible everywhere); Go's non-pointer `Segment` (rare in this codebase, used mainly for small immutable-ish data) behaves like Java's primitive `int`/`long` — copied on every assignment or parameter pass.

### 10.6 Error handling

Go has no exceptions and no `try`/`catch`. Instead, any function that can fail returns an **extra `error` value** alongside its normal result, and the caller is expected to check it immediately, every time, explicitly.

```go
// segment.go
func (s *Segment) Append(payload []byte) (uint64, error) {
    ...
    if _, err := s.file.WriteAt(sizeBuf, pos); err != nil {
        return 0, err   // propagate the error up; return a zero-value offset alongside it
    }
    ...
}

// broker.go, calling it:
offsets, err := partitions[pIdx].AppendBatch(payloads)
if err != nil {
    return nil, fmt.Errorf("batch write to partition %d failed: %w", pIdx, err)
}
```

**Java equivalent:**
```java
public long append(byte[] payload) throws IOException {
    try {
        file.write(sizeBuf, pos);
    } catch (IOException e) {
        throw e; // or wrap it
    }
    ...
}

// caller:
try {
    long[] offsets = partitions[pIdx].appendBatch(payloads);
} catch (IOException e) {
    throw new RuntimeException("batch write to partition " + pIdx + " failed", e);
}
```

The Go convention `if err != nil { return ..., err }`, repeated after almost every fallible call, is doing the exact job of Java's `catch`/`throws` propagation — just visibly, inline, every single time, rather than implicitly via stack unwinding. There's no equivalent of an uncaught exception silently aborting a whole call stack; if you don't check `err`, Go will happily let you ignore it and continue with a garbage/zero-value result, which is a real Go footgun (unlike Java, where an unchecked exception is at least loud).

### 10.7 Slices

A Go **slice** (`[]int64`, `[][]byte`) is a growable, dynamically-sized view over an array — functionally the closest thing to Java's `ArrayList<T>`, though implemented very differently under the hood (a slice is a small struct of `{pointer, length, capacity}` pointing at a backing array, not a full object).

```go
// segment.go
s.index = append(s.index, pos)      // like list.add(pos) — but append returns a NEW slice header
                                     // (may or may not share the same backing array)

// broker.go
payloads := make([][]byte, len(items))  // like new ArrayList<byte[]>(items.size())
for i, item := range items {
    payloads[i] = item.payload
}
```

**Java equivalent:**
```java
List<Long> index = new ArrayList<>();
index.add(pos);                       // list.add — mutates in place, no reassignment needed

List<byte[]> payloads = new ArrayList<>(items.size());
for (int i = 0; i < items.size(); i++) {
    payloads.add(items.get(i).payload);
}
```

The one genuinely tricky bit for a Java developer: `append(slice, item)` **returns a new slice value** that you must assign back (`s.index = append(s.index, pos)`), because if the backing array is full, Go allocates a bigger one and copies — the old slice variable wouldn't see the new element. This is unlike `ArrayList.add`, which mutates the same object in place and never requires reassignment. `make([]int64, 0)` creates an empty slice (like `new ArrayList<Long>()`); `len(slice)` gives the current element count (like `list.size()`); `slice[i]` indexes exactly like `list.get(i)`/`list.set(i, v)` depending on context.

### 10.8 Maps

A Go **map** (`map[string][]*Log`) is a hash table — directly analogous to Java's `HashMap<K, V>`, with one distinctive Go idiom: a lookup returns *two* values, the value and a boolean indicating whether the key existed.

```go
// broker.go
topics map[string][]*Log   // topic name → its list of partition Logs

partitions, exists := b.topics[topic]
if !exists {
    return nil, fmt.Errorf("topic %q does not exist", topic)
}
```

**Java equivalent:**
```java
Map<String, List<Log>> topics; // topic name -> its list of partition Logs

List<Log> partitions = topics.get(topic);
if (partitions == null) {
    throw new RuntimeException("topic " + topic + " does not exist");
}
```

Go's `value, exists := m[key]` pattern is strictly better than Java's `null`-checking idiom for one subtle reason: it distinguishes "key present with a zero-value value" from "key absent entirely," which a plain `map.get(key) == null` check in Java cannot do if `null` is itself a legitimate stored value. `make(map[string][]*Log)` creates an empty map (like `new HashMap<>()`); a `nil` (uninitialized) Go map can be *read* from safely (returns zero-value, `exists=false`) but panics if you try to *write* to it without initializing first via `make` — Java's uninitialized `Map` reference (`null`) throws `NullPointerException` on both read and write, which is a meaningfully different failure mode to watch for.

### 10.9 Goroutines

A **goroutine** (`go someFunc()`) is a lightweight, concurrently-executing function — conceptually identical to `new Thread(() -> someFunc()).start()` in Java, but with a dramatically smaller footprint: a goroutine starts with roughly a **2KB** stack that grows as needed, versus a Java thread's roughly **1MB** fixed stack. That 500x difference is *why* spawning tens of thousands of goroutines is completely normal, idiomatic Go, while spawning tens of thousands of Java threads would exhaust memory or the OS's thread limits long before that.

```go
// server.go
s.wg.Add(1)
go s.handleConnection(conn)   // spawn a goroutine — one per client connection
```

**Java equivalent:**
```java
new Thread(() -> handleConnection(conn)).start();
// or, more realistically at scale: submit to a thread pool / use virtual threads (Java 21+)
```

The `1000 connections × ~1MB Java threads ≈ 1GB just in stacks` versus `1000 goroutines × ~2KB ≈ 2MB` comparison from Section 5 is the concrete number behind this — this project's one-goroutine-per-TCP-connection design (`server.go`'s `Start` loop) would be a genuinely questionable choice in classic (pre-virtual-threads) Java at scale, but is a completely unremarkable, idiomatic pattern in Go.

### 10.10 Channels

A Go **channel** (`chan struct{}`) is a typed conduit for passing values (or, as used here, pure signals) between goroutines — there's no single one-line Java equivalent, but the closest analogues are `BlockingQueue` (for passing values) or a `CountDownLatch`/manually-managed flag with `wait()`/`notifyAll()` (for pure signaling, which is exactly this project's usage).

```go
// server.go
quit chan struct{}   // signal to stop accepting connections
...
quit: make(chan struct{}),
...
func (s *Server) Stop() {
    close(s.quit)   // closing broadcasts to EVERY goroutine currently doing <-s.quit
    ...
}
...
select {
case <-s.quit:
    return nil // clean shutdown
default:
    log.Printf("accept error: %v", err)
    continue
}
```

**Java equivalent (closest available idiom, not a direct translation):**
```java
private final AtomicBoolean quit = new AtomicBoolean(false);
// or a CountDownLatch(1) if you want blocking-wait semantics:
private final CountDownLatch quitLatch = new CountDownLatch(1);

public void stop() {
    quitLatch.countDown(); // signals ALL waiters, similar to close()
}
// elsewhere:
if (quitLatch.getCount() == 0) {
    return; // clean shutdown
}
```

`chan struct{}` specifically (as opposed to, say, `chan int`) is an idiom meaning "this channel carries no actual data — its only purpose is the *event* of a value arriving (or the channel being closed)," chosen because `struct{}` (the empty struct) takes zero bytes of memory, making it the cheapest possible "something happened" signal. `close(ch)` is special: every goroutine blocked on `<-ch`, present and future, immediately receives the zero value and unblocks — it's a one-shot broadcast, not a single-consumer message, which is exactly why `Stop()` uses `close(s.quit)` rather than sending a single value (a sent value would only unblock *one* waiting receiver, not all of them). `select` lets a goroutine wait on multiple channel operations simultaneously and proceed with whichever one is ready first — here, "is a shutdown signal available on `s.quit`, or not" — with `default` making the whole `select` non-blocking (check and fall through immediately if nothing's ready, rather than waiting).

### 10.11 Mutex

`sync.Mutex` and `sync.RWMutex` are Go's locking primitives — `sync.Mutex` is a straightforward mutual-exclusion lock, exactly like Java's `ReentrantLock` (or a plain `synchronized` block) — one lock, one owner at a time, everyone else waits. `sync.RWMutex` is a **read-write lock**, exactly matching Java's `ReentrantReadWriteLock`: any number of readers can hold the read lock simultaneously (they don't block each other, since reads don't mutate anything), but a writer needs exclusive access — no readers, no other writers.

```go
// log.go — plain mutex, one Log's segments mutated by one writer path
type Log struct {
    mu sync.Mutex
    ...
}
func (l *Log) Append(payload []byte) (uint64, error) {
    l.mu.Lock()
    defer l.mu.Unlock()
    ...
}

// broker.go — RWMutex, because many goroutines can safely READ b.topics
// concurrently (e.g. concurrent Consume calls), but CreateTopic needs exclusive access
type Broker struct {
    mu sync.RWMutex
    ...
}
func (b *Broker) Consume(topic string, partition int, offset uint64) ([]byte, error) {
    b.mu.RLock()          // shared read lock — many goroutines can hold this at once
    defer b.mu.RUnlock()
    ...
}
func (b *Broker) CreateTopic(name string, numPartitions int) error {
    b.mu.Lock()           // exclusive write lock — blocks everyone else, readers included
    defer b.mu.Unlock()
    ...
}
```

**Java equivalent:**
```java
public class Log {
    private final ReentrantLock mu = new ReentrantLock();
    public long append(byte[] payload) throws IOException {
        mu.lock();
        try {
            ...
        } finally {
            mu.unlock();
        }
    }
}

public class Broker {
    private final ReentrantReadWriteLock mu = new ReentrantReadWriteLock();
    public byte[] consume(String topic, int partition, long offset) {
        mu.readLock().lock();
        try {
            ...
        } finally {
            mu.readLock().unlock();
        }
    }
    public void createTopic(String name, int numPartitions) {
        mu.writeLock().lock();
        try {
            ...
        } finally {
            mu.writeLock().unlock();
        }
    }
}
```

`Lock()`/`Unlock()` map directly to `lock()`/`unlock()`; `RLock()`/`RUnlock()` map directly to `readLock().lock()`/`readLock().unlock()`. The choice between plain `Mutex` (`log.go`) and `RWMutex` (`broker.go`) tracks the actual read/write ratio of each struct's usage: `Log`'s methods are almost all mutating (appends, retention enforcement), so there's little benefit to distinguishing readers from writers; `Broker` fields, especially `topics`, get read constantly (every `Consume`, every `Publish`) but written rarely (only `CreateTopic`), which is exactly the profile where `RWMutex` earns its extra complexity by letting concurrent reads proceed without contending with each other.

### 10.12 Defer

`defer` schedules a function call to run when the *enclosing function* returns — no matter whether it returns normally, via an early `return`, or (rarer in Go, since there's no exception mechanism) via a panic. It is Go's direct equivalent of Java's `finally` block, but attached to an arbitrary statement rather than requiring a `try { ... } finally { ... }` wrapper around the whole function body.

```go
// segment.go
func (s *Segment) Append(payload []byte) (uint64, error) {
    s.mu.Lock()
    defer s.mu.Unlock()   // GUARANTEED to run when Append returns, from ANY return point below
    ...
    if _, err := s.file.WriteAt(sizeBuf, pos); err != nil {
        return 0, err     // s.mu.Unlock() still runs here, automatically
    }
    ...
    return globalOffset, nil  // and here too
}
```

**Java equivalent:**
```java
public long append(byte[] payload) throws IOException {
    mu.lock();
    try {
        if (!writeAt(sizeBuf, pos)) {
            return 0; // mu.unlock() still runs, because of the finally block below
        }
        ...
        return globalOffset;
    } finally {
        mu.unlock();
    }
}
```

The huge practical win, in both languages: you cannot forget to release the lock on one of several early-return paths, because the release is declared *once*, right next to where the lock was acquired, rather than needing to be duplicated (or wrapped in `finally`) at every exit point. Go's `defer` is more general than just lock-release — it's used identically for `defer f.Close()` (closing a file no matter how the function exits), `defer os.RemoveAll(dir)` (cleanup, as seen in `bench.go`), etc. — anywhere Java would reach for a `finally` block or a try-with-resources.

### 10.13 Interfaces

A Go **interface** (`io.Reader`, `io.Writer`) is a set of method signatures — structurally identical in purpose to a Java interface — but Go interfaces are satisfied **implicitly**: there is no `implements` keyword. Any type that happens to define all the methods an interface requires automatically satisfies that interface, with zero declaration required.

```go
// protocol.go
func WriteFrame(w io.Writer, data []byte) error { ... }
func ReadFrame(r io.Reader) ([]byte, error) { ... }
```

`io.Writer` is just: "anything with a method `Write(p []byte) (n int, err error)`." A `net.Conn` (a TCP connection) has that method, so you can pass a `net.Conn` directly to `WriteFrame` — nobody had to write `class Conn implements Writer`, it just *is* one, structurally, by having the right method shape. This is sometimes called "duck typing," though it's fully statically checked at compile time, unlike Python's runtime duck typing.

**Java equivalent:**
```java
public interface Writer {
    int write(byte[] p) throws IOException;
}
// Java REQUIRES an explicit declaration:
public class Connection implements Writer {
    public int write(byte[] p) throws IOException { ... }
}
```

The practical consequence for this codebase: `WriteFrame`/`ReadFrame` work on *any* type with the right `Read`/`Write` method shape — a real `net.Conn`, an in-memory `bytes.Buffer` (handy for tests, no real socket needed), a file — without any of those types needing to know `io.Writer`/`io.Reader` exist, let alone declare conformance to them.

### 10.14 for loop

Go has exactly **one** looping keyword, `for` — no separate `while`, no separate `do-while` — and it's used in four distinct shapes depending on what you omit.

```go
// classic C-style, all three clauses present (bench.go)
for i := 0; i < *numMessages; i++ {
    ...
}

// "while" — condition only, no init/increment clauses (server.go's Start, conceptually)
for {
    conn, err := s.listener.Accept()
    ...
}

// for-each over a slice, with index and value (broker.go)
for i, msg := range messages {
    ...
}

// for-each, discarding the index (broker.go)
for _, p := range partitions {
    total += p.Offset()
}
```

**Java equivalent:**
```java
for (int i = 0; i < numMessages; i++) { ... }     // classic
while (true) { Socket conn = server.accept(); ... }  // Java's separate while(true)
for (int i = 0; i < messages.length; i++) { var msg = messages[i]; ... } // indexed for-each
for (Log p : partitions) { total += p.getOffset(); } // Java's for-each (no index)
```

`for {}` (no clauses at all) is Go's `while (true)` — there is no separate keyword, `for` alone with an empty header just loops forever until an explicit `break` or `return`. `for i, msg := range messages` gives both index and value on every iteration (closest to a classic indexed Java `for`); `for _, p := range partitions` uses the blank identifier `_` to explicitly discard the index when you only need the value (closest to Java's `for (Log p : partitions)`, which never exposes an index at all).

### 10.15 Multiple return values

Go functions can return more than one value directly, as a first-class language feature — no wrapper class, no `Pair<A,B>`, no `Optional`, no output parameter needed.

```go
// segment.go — returns BOTH a uint64 offset AND an error, always, every call
func (s *Segment) Append(payload []byte) (uint64, error) {
    ...
    return globalOffset, nil   // success: real offset, nil error
    ...
    return 0, err              // failure: zero-value offset, real error
}

// caller — the underscore discards a value you don't need
_, err := log.Append(payload)   // I don't care about the offset here, just did it fail?
offset, err := log.Append(payload) // I need both
```

**Java equivalent (there genuinely is no direct equivalent — you need one of these workarounds):**
```java
// Option 1: exceptions carry the "error" channel implicitly, return type carries only the value
public long append(byte[] payload) throws IOException {
    return globalOffset; // "error" is out-of-band, via throws
}

// Option 2: a wrapper/record type, if you insist on Go-style explicit dual return
public record AppendResult(long offset, IOException error) {}
public AppendResult append(byte[] payload) {
    return new AppendResult(globalOffset, null);
}
```

This is arguably the single most pervasive Go idiom in the entire codebase: essentially *every* function that can fail returns `(result, error)`, and the blank identifier `_` is the standard way to explicitly say "I am choosing to ignore this particular return value" — which, unlike simply not catching a Java exception, is visible right there in the call site, not an invisible omission.

---

## Section 11: Interview Questions & Answers

### Design Decisions

**1. Why append-only? Why not a database?**
An append-only log is the simplest possible data structure that gives you two things a database's general-purpose B-tree/LSM storage doesn't optimize for by default: strictly ordered, immutable history, and pure sequential I/O for every write. A database is built to answer "what is the *current* value of row X" efficiently, with random-access updates and deletes as first-class operations; Kafka's job is "what is the *sequence* of everything that happened," where updates and deletes never really occur — you only ever add new facts to the end. Sequential-only writes are also why Kafka can saturate disk throughput in a way that random-write-optimized databases generally can't; Section 3 measured this directly with fsync math (270 msg/s single-write vs. 133,009 msg/s batched, both on the same append-only file).

**2. Why binary protocol instead of HTTP/JSON?**
JSON is human-readable but expensive to produce and parse relative to a fixed binary layout — every field name is repeated as text on the wire, numbers get parsed from decimal strings, and there's no way to know a message's length without scanning for a delimiter or fully parsing the structure. This project's protocol (`protocol.go`) uses length-prefixed binary framing — `[4-byte size][body]` — so a reader knows exactly how many bytes to read before even looking at the content, and every field (topic length, partition, offset) is a fixed-width integer, not a variable-length text token. HTTP adds further overhead per request (headers, connection semantics not built for this project's persistent-connection, many-small-messages pattern) that a raw TCP socket with custom framing avoids entirely.

**3. Why per-partition ordering instead of global ordering?**
Global ordering across an entire topic would require every producer to coordinate through a single serialization point — effectively giving up all write parallelism, since only one writer could safely append "the next" message at any instant. Per-partition ordering (Section 2.2/2.3) gets you a strong, useful guarantee — all messages sharing a key land in the same partition, in the order they were produced — while still letting N partitions accept writes and serve reads fully independently and in parallel. It's a deliberate trade: you give up "message 500 across the whole topic globally happened before message 501," which most real applications never actually needed in the first place, in exchange for horizontal write/read scalability.

**4. Why sticky assignment instead of round-robin?**
Section 7.6 measured this concretely: for one realistic membership change (a 3-member, 6-partition group with one member replaced), sticky assignment moved only 2 partitions while a memoryless round-robin recompute moved 4 — both achieved identical balance (2 partitions per member), so round-robin's extra moves bought nothing. Every partition that changes owner costs the new owner a cold start (fetch its committed offset, warm up any in-memory state) and potentially discards in-flight processing work on the old owner — sticky assignment's rule ("keep what's already valid, only reassign what's actually orphaned") minimizes that real, unavoidable cost.

**5. Why CRC32 and not SHA-256?**
CRC32 is a checksum designed to catch accidental corruption (bit flips, truncated writes, disk errors) cheaply and fast — it's not cryptographically secure and was never meant to be; a CRC32 collision can be found or engineered trivially, but that's irrelevant for its actual job here. SHA-256 is a cryptographic hash, meant to resist deliberate, malicious tampering by an adversary who controls the input — vastly more computationally expensive (tens of times slower) for a guarantee this project doesn't need, since the on-disk record format isn't defending against an attacker forging records, only against random hardware/OS-level corruption. Real Kafka makes the same choice for the same reason: CRC32 (specifically CRC32C in modern versions) on every record, not a cryptographic hash.

### Performance

**6. What's your write throughput? What limits it?**
Single-message, fsync-per-write mode measures 270 msg/s (0.07 MB/s at 256-byte messages) on this project's benchmark, hard-capped by fsync's ~4ms confirmation latency per call — 1000ms ÷ 4ms ≈ 250, matching the measured number almost exactly. Batched writes (1 fsync per 1000 messages) reach 133,009 msg/s (32.47 MB/s) — a 493× improvement — because the fsync cost gets amortized across 1000 messages instead of paid once per message; what's left limiting batched throughput is the per-message `WriteAt` syscall overhead and Go-level loop/allocation cost, not fsync anymore.

**7. How would you improve read throughput?**
This project's own read benchmark already hits 1,273,629 msg/s (310.94 MB/s) sequential — far faster than any write path, precisely because reads never touch fsync at all and benefit from the OS page cache and read-ahead. The next lever, not yet implemented here, would be a sparse index (Section 3, one entry per ~4KB rather than per message) to keep the in-memory index smaller across many partitions, plus memory-mapping segment files (`mmap`) to let the OS manage paging instead of explicit `ReadAt` calls, and possibly zero-copy sends (`sendfile`) on the network path so read data goes straight from page cache to socket without an extra userspace copy.

**8. What's the cost of adding a consumer? (Almost zero — just one more offset)**
A new consumer joining an *existing* consumer group just needs the coordinator's `rebalance()` to hand it a subset of already-existing partitions (Section 7) — no new storage, no new segment files, nothing written to disk on the producer/broker side at all. The only new persistent state is the new consumer's committed offset per partition it's assigned (`OffsetStore.Commit`, `offsets.go`) — a tiny file-based bookkeeping entry, not a duplication of any message data; N independent consumer groups can all read the exact same partition's messages independently, each paying only for its own offset bookkeeping.

**9. Why is sequential I/O so much faster than random I/O?**
On spinning disks, a random write means physically moving the read/write head to a new location before writing — a mechanical operation costing milliseconds, versus a sequential write where the head is already positioned correctly and just keeps streaming; that's the 50-500x gap Section 1 cites. SSDs don't have a physical head to move, but they have their own analogous penalty: flash is organized into large erase blocks, and a small random write can force a read-modify-erase-rewrite of an entire block, while sequential writes let the SSD's controller batch writes efficiently into fresh blocks. Kafka's append-only design (Section 3) means every single write, always, is the sequential case — you never pay the random-I/O penalty at all, on either kind of disk.

**10. What would you change to handle 1M messages/sec?**
First, batch aggressively and tune batch size against acceptable producer-side latency (Section 9's `linger.ms`/`batch.size` discussion) — the 493× measured speedup from batching alone gets a single partition into six-figure msg/s territory. Second, add more partitions, since partitions are this system's actual unit of write parallelism — one partition's throughput ceiling doesn't rise just by adding more consumers, but 10 partitions each doing 100K msg/s does reach 1M aggregate. Third, replication (not present in this single-node project) so that durability doesn't have to come from every message's own fsync — letting individual brokers batch even more aggressively because a crash on any one machine doesn't lose data cluster-wide.

### Failure Handling

**11. What happens if the broker crashes mid-write?**
Section 3.6's crash recovery scans each segment file from the beginning on restart, validating each length-prefixed record against the file's actual remaining size — any record whose claimed length would run past the end of the file (a torn, mid-write record) gets detected and the file is truncated back to the last fully-valid record boundary. Anything that was fully written *and* fsynced before the crash survives byte-for-byte; anything genuinely mid-write, by definition never acknowledged to a caller as durable, is discarded — this is the "zero data loss on `kill -9`" guarantee, but it only covers already-fsynced data (Section 9.6's batching tradeoff is the explicit exception: unsynced batched writes can still be lost).

**12. What happens if a consumer crashes mid-processing?**
Nothing happens to the broker or the data at all — the consumer's committed offset (`OffsetStore`, `offsets.go`) simply stops advancing at whatever it last successfully committed. If the crashed consumer was part of a consumer group, the coordinator's heartbeat-timeout mechanism (Section 7.2) eventually notices (`up to heartbeatTimeout=10s + up to 1s reap-tick granularity` ≈ 11 seconds worst case) and reassigns its partitions to a surviving member via `rebalance()`, who resumes from that same last-committed offset — meaning any message processed but not yet committed before the crash gets reprocessed by whoever picks up the partition next.

**13. How does a consumer know where to resume after a crash?**
It calls `OffsetStore.Fetch(group, topic, partition)` (`offsets.go`) to read back the last value it (or, after a rebalance, its predecessor) committed via `OffsetStore.Commit` — a small persisted `(group, topic, partition) → offset` record, conceptually identical to real Kafka's internal `__consumer_offsets` topic. On a totally fresh consumer/group with no prior commit, it starts from offset 0 (or, in a production system, a configurable "earliest" vs. "latest" policy); on a resumed one, it starts exactly one past wherever it last confirmed processing completed.

**14. What's at-least-once delivery? How do you achieve exactly-once?**
At-least-once means a message is guaranteed to be delivered to a consumer eventually, but may be delivered more than once — e.g., a consumer processes a message, crashes *before* committing its offset, and on restart re-reads and reprocesses that same message, because the broker has no idea it was already handled. This project's offset-commit-after-processing pattern is exactly at-least-once. True exactly-once delivery requires either idempotent consumer-side processing (reprocessing the same message twice produces the same end result, so duplication is harmless — e.g., "set user's balance to $50" rather than "add $10 to balance") or a transactional producer/consumer protocol (real Kafka's actual exactly-once semantics use producer idempotence plus transactional offset commits, tying the offset commit and the processing side-effect into one atomic unit) — this project implements neither, and that's a disclosed simplification, not a subtle bug.

**15. What if the disk is silently corrupting data?**
This is exactly what the CRC32 checksum (Section 4, computed over timestamp+key+value at write time, stored alongside the record, re-verified on every read via `DecodeRecord`) is designed to catch — a corrupted byte anywhere in the record's body changes its computed CRC32, so `storedCRC != actualCRC` flags it as corrupt on read rather than silently returning garbage bytes to the caller as if they were a valid message. What this project's CRC32-only approach does *not* do is repair the corruption (there's no replica to fall back to, since it's single-node) — it can only detect and refuse to serve bad data, which is strictly better than serving it unknowingly, but real production Kafka's actual fix for surviving corruption (not just detecting it) is, again, replication across independent disks/machines.

### Architecture

**16. How do you scale beyond one broker? (Replication — explain)**
Real Kafka replicates each partition across multiple brokers (commonly 3): one broker is the elected **leader** for that partition, handling all reads and writes, while the others are **followers**, continuously pulling and replicating the leader's log. If the leader dies, one in-sync follower is promoted to take over — clients get redirected to the new leader, and no data is lost as long as at least one replica had the message. This project is explicitly single-node/single-broker — there is no leader election, no follower replication, no ISR (in-sync-replica) tracking — which is precisely why its durability story leans entirely on local fsync (Section 3) and why batching (Section 9) has to be disclosed as a real risk rather than resolved by "another copy exists elsewhere."

**17. What's the difference between retention and compaction?**
Retention (Section 2.6, `Log.enforceRetention`) deletes entire old *segment files* once a partition's total size exceeds a configured cap, oldest segment first — it doesn't look at message content at all, it's purely time/size-based bulk deletion, appropriate for event-stream data where old events genuinely stop mattering. Compaction (Section 8, `CompactSegment`/`CompactLog`) is content-aware: it keeps only the *latest* value for each distinct key within a segment and discards all earlier, superseded values for that same key — appropriate for changelog/state-snapshot data (e.g., "user123's current profile") where you want the current state to survive forever even as the log itself shrinks. A topic can use one, the other, or (in real Kafka) both simultaneously, since they operate at different granularities (whole segments vs. individual keyed records).

**18. Why do consumer groups exist? Max consumers = num partitions?**
Consumer groups exist to let multiple consumer processes cooperatively divide a topic's partitions — spreading read/processing load across machines while guaranteeing each partition is still read by exactly one consumer within that group at a time, preserving per-partition ordering (Section 2.4) even under parallel consumption. Yes — max *useful* consumers in one group equals the topic's partition count; Section 7's `rebalance()` math (`targetFloor = NumPartitions / len(members)`) means once member count exceeds partition count, `targetFloor` hits 0 and the excess members get an empty assignment, permanently idle unless another member later dies and frees up a partition.

**19. If you had to add authentication, where would you put it?**
The natural point is right at connection/handshake time in `server.go`'s `handleConnection`, before any request is dispatched to the `Broker` — e.g., requiring a first "auth" frame (username/token/mTLS client cert) on every new TCP connection, validating it, and either closing the connection immediately on failure or attaching an authenticated-identity context that every subsequent request on that connection carries. Authorization (which topics/operations a given identity may access) would layer on top of that identity, checked inside `Broker`'s methods (`Publish`, `Consume`, `CreateTopic`) before they touch any state — keeping the wire protocol itself (`protocol.go`) unaware of *who* is asking, just *what* they're asking for, with the permission check as a separate concern at the broker layer.

**20. What would you do differently if starting over?**
Design the on-disk record format (Section 4) with CRC32 and a timestamp from the very first version rather than retrofitting them later — both are cheap to add early and painful to add after data already exists in an older format without them. Build the sparse index (Section 3) from day one instead of the dense per-message index, since the dense-index memory cost (80MB per 10M messages per partition, Section 3) only gets discovered as a real problem once you're already running at a scale where migrating the index format is disruptive. And build the benchmark suite (Section 9) alongside the very first storage engine, not after several iterations — every throughput claim in this whole deep dive (270 → 133,009 msg/s, 493×) only exists because the benchmark was eventually written; having it from the start would have made every subsequent design decision (dense vs. sparse index, batch size, fsync strategy) something that could be measured immediately rather than argued about.

**Bonus — 21. What's the actual bottleneck once you've batched writes?**
Once fsync is amortized away (Section 9), the next-most-expensive things are per-message syscall overhead (`WriteAt` called twice per message — size header, then payload — even inside a batch), the in-memory index growing unboundedly for a dense per-message index (Section 3's 80MB-per-10M-messages number), and eventually network/framing overhead on the produce path itself (`protocol.go`'s per-request framing cost) once storage stops being the limiting factor. This is exactly why real Kafka's actual performance engineering goes well beyond "batch the fsyncs" — page-cache-aware zero-copy sends, sparse indexing, and compression are all targeting these *next* bottlenecks, once the obvious 493×-style win from batching has already been captured.

---

## Section 12: Real Kafka vs Our Implementation

This section is the honest ledger. Every simplification this project makes is listed here, next to what real Kafka actually does, with the interview-safe way to explain the gap. The rule for this whole project has always been: **build the real algorithm where it's feasible in a weekend project (sticky assignment, CRC32, crash recovery, compaction), and explicitly disclose the shortcut where it isn't (replication, exactly-once, security).** Nothing below is hidden — if an interviewer asks "why didn't you build X," the honest answer is always some version of "it's a multi-week distributed-systems problem in its own right, orthogonal to the storage/protocol/consumer-group concepts I was trying to master, and real Kafka's own solution is documented below."

| # | Feature | Real Kafka | Our Implementation | Gap |
|---|---|---|---|---|
| 1 | **Storage format** | `RecordBatch` wrapper: magic byte (format version), attributes byte (compression codec + timestamp type), a batch-level CRC, a base offset + base timestamp, and a *varint-delta-encoded* array of records (each record only stores its *offset delta* and *timestamp delta* from the batch base, not full 8-byte values) | `[4-byte size][4-byte CRC32][8-byte timestamp][2-byte keyLen][key][value]` per record — no batch wrapper, no delta encoding, full-width fields (`record.go`) | Real Kafka's format is a batch-of-records container purpose-built to make compression and delta-encoding possible; ours encodes and CRCs one record at a time. The gap is entirely about wire efficiency (bytes-per-message), not correctness — our CRC32-per-record is actually *stronger* per-message integrity than Kafka's batch-level CRC, just less space-efficient at scale. |
| 2 | **Replication** | Each partition has one **leader** broker (handles all reads/writes) and N **follower** brokers that continuously fetch and replicate the leader's log; an **ISR** (in-sync replica set) tracks which followers are caught up enough to be promoted; `acks=all` means the producer waits for every ISR member to confirm before the write is considered durable | Single-node: one broker, one copy of every log, durability comes entirely from `Segment.Append`'s `file.Sync()` (`segment.go` line 85) | This is a full distributed-consensus problem — leader election, follower catch-up protocol, ISR shrink/expand logic, and handling network partitions between leader and followers are each their own multi-week subsystem. **Interview answer:** "Single-node durability was the right scope for learning the storage engine and protocol; replication is a separate problem (leader election + follower replication protocol), and I'd reach for Raft (see Section 13's closing) or study Kafka's own ISR design before building it." |
| 3 | **Exactly-once** | Idempotent producer: each producer gets a broker-assigned PID + per-partition sequence number; the broker rejects/dedupes any produce request whose sequence number it's already seen. Transactions: a producer can atomically write to multiple partitions *and* commit consumer offsets in one transaction, visible to consumers only on commit | At-least-once only: `Log.Append` has no producer identity or sequence number at all — a retried produce request is indistinguishable from a new one and gets a new offset | Idempotent producer requires broker-side PID allocation and per-partition sequence tracking; transactions require a transaction coordinator, markers written into the log, and consumer-side filtering of uncommitted transactional records. **Interview answer:** "I implement and can explain exactly-once's actual mechanism (PID+sequence dedup, transactional markers) but didn't build it — it's layered on top of the at-least-once foundation I did build, and the honest tradeoff is: idempotent consumer processing (Section 11 Q14) gets you the same practical outcome for a fraction of the complexity." |
| 4 | **Consumer groups — rebalancing** | Cooperative (incremental) rebalancing: during a rebalance, members keep processing their *unaffected* partitions and only pause/reassign the partitions that actually need to move; generation IDs stamp every heartbeat/assignment so a broker can detect and reject a heartbeat from a consumer still running an old, stale generation | Eager rebalance with no generation ID: `Coordinator.rebalance()` (`coordinator.go`) recomputes the whole group's assignment synchronously on every join/leave; nothing in `Heartbeat()` distinguishes "current generation" from "one rebalance ago" | Our `rebalance()` *is* sticky (it minimizes partition movement, item 5 below) — the gap is only that we don't stop-the-world less. Real Kafka's cooperative protocol needs a two-phase revoke/assign handshake with each member so partitions that don't need to move are never paused at all, plus a generation counter to fence stale requests. **Interview answer:** "My rebalance is sticky (same movement-minimization goal as Kafka's `StickyAssignor`) but eager, not cooperative — cooperative rebalancing needs a two-phase protocol between coordinator and members that's a meaningful chunk of additional coordination logic beyond what a single `rebalance()` function can do." |
| 5 | **Assignment strategies** | Pluggable: `RangeAssignor` (contiguous partition ranges per member, can be very unbalanced), `RoundRobinAssignor` (deals partitions like cards, no stickiness), `StickyAssignor` (balanced + minimal movement, eager), `CooperativeStickyAssignor` (balanced + minimal movement + no unnecessary pausing) | Only one strategy, hand-rolled sticky (`coordinator.go` `rebalance()`): keep-live → collect-orphaned → target-floor/ceil split → steal-excess → redistribute | We deliberately built the *best* single-node strategy (sticky) rather than all four — Range and RoundRobin are strictly worse on movement, and Cooperative-Sticky's extra value (no pausing of unaffected partitions) requires the two-phase protocol from item 4. **Interview answer:** "I chose to implement Sticky directly since it's strictly better than Range/RoundRobin on the metric that matters (movement count, measured in Section 7.6) — pluggability across all four is a real Kafka feature, but building just one and building it well demonstrates the algorithm, not just wiring in a Strategy pattern." |
| 6 | **Segment indexing** | A separate sparse `.index` file per segment, mapping *every ~4KB of log data* to `(relative offset → byte position)` — one lookup entry per several dozen/hundred messages, not per message; reading requires a binary search over the sparse index, then a linear scan forward from that point to the exact record | Full dense in-memory index: `Segment.index []int64` (`segment.go`) has one entry *per message*, rebuilt on startup by `rebuildIndex()` scanning the whole file | Dense indexing is simpler code (`s.index[localIndex]` is O(1) exact lookup, no scan-forward step) but costs real memory: Section 3 measured ~80MB of index for 10M messages in one partition — that cost scales linearly and unboundedly with message count, forever, per partition. **Interview answer:** "I used a dense index because it's the simplest correct thing and this project runs on one machine's RAM for a bounded number of partitions; the fix if this needed to scale is the sparse index + binary search Kafka uses, trading a small amount of read latency (bounded linear scan within one 4KB block) for index memory that's flat regardless of log size." |
| 7 | **Log compaction** | A background **log cleaner thread pool** runs continuously, picks the topic/segment with the worst `dirty ratio` (bytes since last cleaned ÷ total bytes) that also exceeds `min.cleanable.dirty.ratio`, and compacts it — plus **tombstones**: a record with a null value marks a key as deleted, and survives for `delete.retention.ms` before being removed entirely (so consumers that were behind still see the deletion) | On-demand only: `CompactSegment`/`CompactLog` (`compactor.go`) run when explicitly called — no background scheduler, no dirty-ratio heuristic, and no tombstone/null-value handling (`readKeyAtIndex` treats a zero-length key, not a zero-length *value*, as "skip") | The two-pass algorithm itself (build `latestForKey` map, filter, atomic rename) is the real Kafka algorithm — what's missing is the *scheduling* policy (when to run it, on which segment, automatically) and *tombstones* (how deletes propagate to slow consumers instead of just vanishing). **Interview answer:** "The compaction algorithm is the real one — same two-pass, same atomic-rename-for-crash-safety approach Kafka uses — but I run it on demand rather than building the background cleaner's dirty-ratio scheduling heuristic, and I don't yet support null-value tombstones for actual delete semantics." |
| 8 | **Batching** | Producer-side `RecordAccumulator`: application calls `send()` per message, and a client-side buffer batches messages *per partition* in the background, flushing when either `batch.size` bytes accumulate or `linger.ms` elapses (whichever first) — batching is transparent to the caller | Explicit batch API: caller must call `PublishBatch`/`AppendBatch` directly with a pre-built list of messages (`broker.go`, `segment.go`) — there's no background accumulator or timer; `Publish` (singular) is unbatched | Real Kafka's batching is invisible to the app developer (call `send()` N times, the client batches for you); ours makes the caller choose the batched API explicitly. **Interview answer:** "I built the batching mechanism (`AppendBatch`'s single fsync, measured at 493× in Section 9) but not the client-side accumulator + `linger.ms` timer that makes batching transparent — that's a client-library concern layered on top of the same core insight, not a different algorithm." |
| 9 | **Zero-copy** | Consumer fetch uses the `sendfile()` syscall: the OS moves bytes directly from the page cache to the network socket buffer, in kernel space, with **zero** copies into the Java/broker process's userspace memory | `Segment.Read`/`ReadAt` (`segment.go`) copies bytes into a Go `[]byte` in the broker process, which then get copied again into the TCP write buffer — at least two userspace copies | This is an OS-syscall-level optimization (`sendfile`, or Go's `io.Copy` with a `*os.File` source doing the same under the hood) that bypasses the broker process's own memory entirely for the bulk data path. **Interview answer:** "I understand why this matters — CPU cycles and memory bandwidth spent copying bytes the broker process never actually looks at — but exploiting it needs `sendfile`-style OS integration on the read path, which I didn't wire up since our reads (Section 9) were already disk/syscall-bound at a much lower absolute throughput than would make copy overhead the bottleneck." |
| 10 | **Compression** | Per-`RecordBatch` compression with snappy, lz4, zstd, or gzip — the whole batch is compressed once, dramatically shrinking both disk usage and network bytes for repetitive payloads (JSON, logs) | None — every record is stored and sent as raw, uncompressed bytes (`record.go`, `protocol.go`) | Compression trades CPU (compress on produce, decompress on consume) for disk and network bytes — for typical semi-structured payloads (JSON, protobuf) the ratio is often 3-10x. **Interview answer:** "Not implemented — it's a pure engineering add-on (wrap `EncodeRecord`'s output in a codec before the CRC, same idea as gzip-ing an HTTP body) rather than a new distributed-systems concept, so I prioritized the algorithms that actually change system *behavior* (compaction, sticky assignment, crash recovery) over this one that just changes byte *efficiency*." |
| 11 | **Schema/serialization** | A separate **Schema Registry** service stores versioned Avro/Protobuf/JSON schemas; producers/consumers reference a schema ID (a few bytes) instead of embedding field names, and the registry enforces compatibility rules (a new schema version can't break old consumers) | Raw `[]byte` value with no schema awareness at all — `Record.Value` (`record.go`) is opaque bytes; the broker never looks inside it | Schema management is an entirely separate concern from message transport — Kafka the broker doesn't know or care about schemas either, it's a client-side/registry-side layer above the wire protocol. **Interview answer:** "Kafka core doesn't handle this either — it's explicitly a client-and-registry-level concern (Confluent Schema Registry, or hand-rolled protobuf), so treating `Value` as opaque bytes here matches how the *actual* broker treats it; schema evolution is a real problem but it's one layer up from what this project scopes to." |
| 12 | **Security** | SASL (SCRAM, GSSAPI/Kerberos, OAUTHBEARER) for authentication, TLS for encryption-in-transit, and ACLs (per-principal, per-resource, per-operation permission rules) enforced by the broker on every request | None — any TCP client that can reach the port can produce/consume/create topics/join any group (`server.go`'s `handleConnection` dispatches every request type with zero identity check) | This is a deliberately deferred, not-yet-solved layer discussed directly in Section 11 Q19 — the natural insertion point (an auth frame at connection time in `handleConnection`, an identity check in `Broker`'s methods) is understood, just not built. **Interview answer:** "No security today — I know exactly where it plugs in (connection-time handshake in `server.go`, permission checks inside `Broker` before touching state, per Section 11 Q19) but building real SASL/TLS/ACL support is its own large surface area I scoped out of a project focused on the storage/replication/consumer-group internals." |
| 13 | **Monitoring** | JMX metrics for essentially everything (request rates, fsync latency percentiles, ISR shrink/expand events, under-replicated-partition counts) plus **consumer lag** — the gap between a partition's latest offset and a group's committed offset, the single most important operational number in any real Kafka deployment | A WebSocket event bus (`ws.go`, `EventBus.Emit` in `broker.go`) that streams produce/consume/commit/join/leave events live to a browser dashboard — real-time visibility, but no historical metrics, no percentiles, and no lag calculation | Our dashboard shows *what's happening right now*; it doesn't compute or expose consumer lag (`latest offset − committed offset` per partition, trivially derivable from `Log.Offset()` and `OffsetStore.Fetch()`, but not wired up anywhere), and nothing persists metrics history for percentile/trend analysis. **Interview answer:** "The event bus proves I understand observability matters and built a real-time hook for it — lag tracking specifically is a small addition (subtract two already-available numbers) I'd add next; full JMX-style metrics-with-history is a bigger, separate concern (a metrics registry + time-series storage) than the event-streaming I built." |
| 14 | **Controller** | ZooKeeper (older versions) or KRaft (Kafka's own built-in Raft implementation, current versions) elects a **controller** broker that owns cluster metadata — which broker leads which partition, ISR membership, topic configs — and handles leader failover when a broker dies | None — a single broker has no metadata to coordinate about (there's only one broker, so "who's the leader of this partition" has exactly one always-correct answer: this process) | A controller and its consensus protocol (Raft, in KRaft's case) only becomes necessary the moment you have *multiple* brokers that need to agree on shared cluster state — it's a direct consequence of item 2 (replication) needing multiple brokers in the first place, not an independent gap. **Interview answer:** "No controller needed — it's solving a coordination problem (which of N brokers is authoritative for what) that only exists once you have N>1 brokers; the moment replication (item 2) gets added, a controller/consensus layer becomes necessary too, and I'd study Raft specifically (see Section 13's closing) before implementing one." |

**What's IDENTICAL between real Kafka and us:**
- Append-only segmented log
- Offset-based consumption (one log, multiple readers with bookmarks)
- Key-based partition routing (hash % numPartitions)
- Consumer groups (partition assignment, one consumer per partition per group)
- Committed offsets (per group/topic/partition)
- Heartbeat-based failure detection
- At-least-once delivery via commit-after-processing
- Length-prefixed binary protocol over TCP

That list is the actual point of this project: every item on it is a real, load-bearing piece of how Kafka works, not a simplification of it. The gaps in the table above are almost entirely about *scale* (replication, security, monitoring depth) or *efficiency* (compression, zero-copy, batch accumulation) — not about *correctness* of the core log/offset/consumer-group model, which this project implements the same way Kafka does.

---

## Section 13: The Full Code Walkthrough

This section walks through the seven functions that carry the most conceptual weight in the codebase — read them once here, and you can explain (and defend, in an interview) every line.

### 13.1 `Segment.Append` (segment.go) — the fundamental write operation

```go
func (s *Segment) Append(payload []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos := s.size // current end of file = where this message starts

	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(payload)))

	if _, err := s.file.WriteAt(sizeBuf, pos); err != nil {
		return 0, err
	}
	if _, err := s.file.WriteAt(payload, pos+4); err != nil {
		return 0, err
	}

	if err := s.file.Sync(); err != nil { // force to disk
		return 0, err
	}

	s.index = append(s.index, pos)
	s.size = pos + 4 + int64(len(payload))

	globalOffset := s.baseOffset + uint64(len(s.index)) - 1
	return globalOffset, nil
}
```

Line by line:
1. `s.mu.Lock()` / `defer s.mu.Unlock()` — one writer at a time per segment; Go's `defer` guarantees the unlock runs even if a `return` happens early on an error path.
2. `pos := s.size` — every write always lands at the current end-of-file. This is the entire "append-only" guarantee in one line: there is no code path anywhere in `Append` that writes at any position other than the current end.
3. `sizeBuf` + `binary.BigEndian.PutUint32` — build the 4-byte length header as raw bytes, big-endian (most-significant byte first — matters only for cross-machine consistency, not for correctness on one machine).
4. First `WriteAt(sizeBuf, pos)` — write the length header at the current end.
5. Second `WriteAt(payload, pos+4)` — write the actual message bytes immediately after the header.
6. `s.file.Sync()` — the fsync. This is the one syscall in the whole function that's slow (~4ms, per Section 3/9) and the one that actually makes the preceding two writes *durable* rather than just sitting in an OS buffer that a `kill -9` or power loss could still lose.
7. `s.index = append(...)` and `s.size = ...` — update in-memory bookkeeping *after* the fsync succeeds, never before — if `Sync()` had failed and returned early, these lines never run, so the in-memory state can't drift ahead of what's actually durable on disk.
8. `globalOffset := s.baseOffset + uint64(len(s.index)) - 1` — the offset returned to the caller is this segment's base plus "how many messages this segment now holds, minus one" (since we just appended the len(index)-th message, 1-indexed, so its 0-indexed position is len-1).

**Why `WriteAt` instead of `Write`?** `Write` writes at the file's *current cursor position* and then advances that cursor — it's stateful, and if two goroutines called `Write` on the same `*os.File` without a mutex, they could interleave in ways that corrupt the file (goroutine A's write partially overwritten by goroutine B seeking elsewhere first). `WriteAt(buf, pos)` is *positional and stateless*: you say exactly which byte offset to write to, every time, with no shared cursor state to get out of sync. Given that `s.mu` already serializes access, plain `Write` would technically work too here — but `WriteAt` makes the "always write exactly here" invariant explicit in the function signature itself, rather than relying on the file's cursor having been left in the right place by whoever called last.

**What an interviewer would ask about this code:** *"Why is `s.file.Sync()` called after both writes instead of after each one individually?"* — Because a single `fsync` call flushes the *entire* OS write buffer for that file descriptor to disk, not just the bytes from one specific `WriteAt` call; calling it once after both writes durably persists both the size header and the payload in one syscall, and calling it twice (once per `WriteAt`) would double the fsync cost for zero additional durability benefit, since neither write is useful without the other (a header with no payload, or a payload with no header, are both unreadable).

### 13.2 `Segment.AppendBatch` (segment.go) — the batched version

```go
func (s *Segment) AppendBatch(payloads [][]byte) ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offsets := make([]uint64, len(payloads))
	pos := s.size

	for i, payload := range payloads {
		offsets[i] = s.baseOffset + uint64(len(s.index))

		sizeBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(sizeBuf, uint32(len(payload)))
		s.file.WriteAt(sizeBuf, pos)
		s.file.WriteAt(payload, pos+4)

		s.index = append(s.index, pos)
		pos += 4 + int64(len(payload))
	}

	if err := s.file.Sync(); err != nil { // ONE fsync for the entire batch
		return nil, err
	}

	s.size = pos
	return offsets, nil
}
```

The structure is identical to `Append` — same size-header-then-payload write pattern, same index bookkeeping — with exactly one difference that matters: **the `Sync()` call moved outside the `for` loop.** In `Append`, N messages means N calls to `Sync()`, each ~4ms, for N×4ms of unavoidable latency. In `AppendBatch`, N messages inside one call means N `WriteAt` pairs (cheap, no disk-flush guarantee) followed by exactly one `Sync()` (the expensive part) — the fsync cost gets amortized across however many messages are in the batch. This single structural change is what Section 9 measured as a 493× throughput improvement (270 msg/s → 133,009 msg/s at batch size 1000): fsync latency didn't get faster, it just got paid once instead of a thousand times.

**What an interviewer would ask about this code:** *"What happens if the process crashes halfway through the `for` loop, after writing message 500 of 1000 but before the `Sync()` call?"* — Nothing in the batch is guaranteed durable, because durability in this design is entirely defined by "survived a completed `Sync()` call" — messages 1 through 500 are sitting in the OS write buffer, unflushed, and on restart `rebuildIndex()` (`segment.go`) will scan the file, find some prefix of those writes may or may not have actually made it to the physical disk (OS buffering behavior here is not guaranteed message-by-message), and truncate at the last fully-valid record boundary it can verify. This is the explicit, disclosed tradeoff Section 9.6 calls out: batching trades "at most (batch size − 1) messages of exposure on crash" for the 493× throughput win, and the batch size is the dial that trades one against the other.

### 13.3 `Log.Read` (log.go) — finding which segment has an offset

```go
func (l *Log) findSegment(offset uint64) *Segment {
	for i := len(l.segments) - 1; i >= 0; i-- {
		if offset >= l.segments[i].baseOffset {
			return l.segments[i]
		}
	}
	return nil
}

func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	seg := l.findSegment(offset)
	if seg == nil {
		return nil, fmt.Errorf("offset %d not found (may have been deleted by retention)", offset)
	}

	localIndex := offset - seg.baseOffset
	return seg.Read(localIndex)
}
```

`findSegment` walks `l.segments` **from the end, backwards** (`i := len(l.segments) - 1; i >= 0; i--`), not from the start. Segments are stored sorted ascending by `baseOffset` ([0, 5000, 10000, ...]), and the first segment (scanning from the end) whose `baseOffset` is `<= offset` is the one that contains it — because segment `i`'s valid offset range is `[segments[i].baseOffset, segments[i+1].baseOffset)`. Scanning backwards is a deliberate micro-optimization: most reads in a real workload target recent data (the tail of the log), so checking the newest segments first means the common case returns after 1 iteration instead of scanning the entire (potentially long) list of old segments first.

`Read` then converts the *global* offset the caller asked for into a *local* index within that specific segment: `localIndex := offset - seg.baseOffset`. Concretely: if segment 1 has `baseOffset=5000` and the caller wants global offset 5003, `localIndex = 5003 - 5000 = 3` — the 4th message (0-indexed) written to that specific file, which `Segment.Read` then looks up directly via `s.index[3]` to get its exact byte position.

**What an interviewer would ask about this code:** *"Why is this a linear scan instead of a binary search, given segments are sorted?"* — With a typical `MaxSegmentBytes` of 10MB and realistic message sizes, one partition might have single or low-double-digit numbers of segments even after gigabytes of total data — a linear scan over say 20 items is faster in practice than the overhead of a binary search (`sort.Search`'s comparator-callback indirection) for a list this small, and the backwards-scan-from-newest heuristic already makes the common case O(1). Binary search would only start winning once a single partition accumulates hundreds of segments, which isn't this project's operating regime.

### 13.4 `Broker.Publish` (broker.go) — the routing layer

```go
func (b *Broker) Publish(topic string, key []byte, value []byte) (int, uint64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	partitions, exists := b.topics[topic]
	if !exists {
		return 0, 0, fmt.Errorf("topic %q does not exist", topic)
	}

	var partitionIdx int
	if len(key) > 0 {
		partitionIdx = hashKey(key, len(partitions))
	} else {
		total := uint64(0)
		for _, p := range partitions {
			total += p.Offset()
		}
		partitionIdx = int(total % uint64(len(partitions)))
	}

	offset, err := partitions[partitionIdx].Append(value)
	if err != nil {
		return 0, 0, err
	}

	b.Events.Emit("produce", map[string]interface{}{
		"topic": topic, "partition": partitionIdx, "offset": offset, "key": string(key),
	})

	return partitionIdx, offset, nil
}
```

`hashKey` (also in `broker.go`) is `int(fnv.New32a().Sum32() % uint32(numPartitions))` — FNV-1a is a fast, deterministic, non-cryptographic hash (same "not defending against an attacker" reasoning as CRC32 in Section 11 Q5), and the same key bytes always produce the same hash, always `% numPartitions` to the same partition index — this is the entire mechanism behind Kafka's "same key → same partition → preserved order" guarantee from Section 2. When there's no key, the code falls back to a sum of every partition's current `Offset()` mod partition count — a crude round-robin that spreads load without needing to track a separate counter, though it re-sums all partitions' offsets on every single unkeyed publish (an O(numPartitions) cost per call, acceptable at this project's partition counts).

After the write succeeds, `b.Events.Emit("produce", ...)` pushes a structured event into the `EventBus` (`events.go`), which the WebSocket dashboard (`ws.go`) is subscribed to — this is a fire-and-forget notification, decoupled from the actual write path; a slow or disconnected dashboard client never blocks or slows down a producer, because `Emit` is not on the critical path of returning `(partitionIdx, offset, nil)` to the caller.

**What an interviewer would ask about this code:** *"Why `RLock` here instead of a full `Lock`?"* — `Publish` only *reads* the `b.topics` map (to look up which partitions exist) — it never adds or removes a topic — so multiple `Publish`/`Consume` calls across different goroutines can safely run concurrently under a shared read lock; `RLock` only blocks if some *other* goroutine is holding the exclusive write lock (e.g., `CreateTopic`, which does mutate the map). Using `RLock` instead of `Lock` here is what lets many producers publish to different (or even the same) topic in parallel without serializing on the broker's map access.

### 13.5 `Coordinator.rebalance` (coordinator.go) — sticky assignment

```go
func (c *Coordinator) rebalance(group *ConsumerGroup) {
	memberIDs := make([]string, 0, len(group.Members))
	for id := range group.Members {
		memberIDs = append(memberIDs, id)
	}
	sortStrings(memberIDs)

	if len(memberIDs) == 0 {
		group.Assignments = make(map[string][]int)
		return
	}

	// Step 1: keep existing assignments for live members
	newAssignments := make(map[string][]int)
	assigned := make(map[int]bool)
	for _, id := range memberIDs {
		if prev, exists := group.Assignments[id]; exists {
			newAssignments[id] = prev
			for _, p := range prev {
				assigned[p] = true
			}
		} else {
			newAssignments[id] = []int{}
		}
	}

	// Step 2: collect orphaned partitions
	orphaned := []int{}
	for p := 0; p < group.NumPartitions; p++ {
		if !assigned[p] {
			orphaned = append(orphaned, p)
		}
	}

	// Step 3: target count per member (floor/ceil split)
	targetFloor := group.NumPartitions / len(memberIDs)
	numCeil := group.NumPartitions % len(memberIDs)
	targetFor := make(map[string]int)
	for i, id := range memberIDs {
		if i < numCeil {
			targetFor[id] = targetFloor + 1
		} else {
			targetFor[id] = targetFloor
		}
	}

	// Step 4: steal excess from over-target members
	for _, id := range memberIDs {
		target := targetFor[id]
		current := newAssignments[id]
		if len(current) > target {
			excess := current[target:]
			newAssignments[id] = current[:target]
			orphaned = append(orphaned, excess...)
		}
	}

	// Step 5: distribute orphaned to under-target members
	sortInts(orphaned)
	orphanIdx := 0
	for _, id := range memberIDs {
		target := targetFor[id]
		for len(newAssignments[id]) < target && orphanIdx < len(orphaned) {
			newAssignments[id] = append(newAssignments[id], orphaned[orphanIdx])
			orphanIdx++
		}
	}

	group.Assignments = newAssignments
}
```

Annotated by step:
1. **Keep live** — `sortStrings(memberIDs)` makes iteration order deterministic (Go map iteration order is intentionally randomized, so without this, two runs with identical input could disagree on tie-breaking). The loop then copies forward every *currently alive* member's *previous* assignment unchanged — this is the entire "sticky" property: nobody loses a partition just because *someone else* joined or left.
2. **Collect orphaned** — any partition number `0..NumPartitions-1` not marked in the `assigned` map belongs to nobody right now — either its old owner just left the group, or (on first-ever join) nobody has ever owned it.
3. **Calculate target** — integer division gives the floor (`6 partitions / 4 members = 1` with remainder `2`); the remainder (`numCeil`) is how many members need one *extra* partition to make the total add up — those extra slots go to the first `numCeil` members in sorted order, so exactly `NumPartitions` partitions are targeted in total, never more or fewer.
4. **Steal excess** — a member who's currently *above* their new target (because the group just shrank, so the per-member floor rose) gives back their partitions from the end of their list — `current[target:]` — back into the orphan pool. Only the minimum necessary partitions move; anyone within their target keeps everything.
5. **Distribute** — `sortInts(orphaned)` again ensures determinism, then orphaned partitions get handed out to whichever members are still under their target, lowest-numbered orphan first, until every member is exactly at target (or the orphan pool runs out, in the perfectly-balanced case).

**What an interviewer would ask about this code:** *"Is this assignment guaranteed to be balanced? How unbalanced can it get?"* — Yes, provably: every member ends at either `targetFloor` or `targetFloor+1` partitions by construction (step 3 assigns every member one of exactly those two numbers as their target, and steps 4-5 drive every member's actual count to their target), so the maximum spread between any two members is exactly 1 partition — the same balance guarantee Kafka's real `StickyAssignor` provides, verified concretely in Section 7.6's worked example (2 partitions per member, zero variance, after a membership change).

### 13.6 `CompactSegment` (compactor.go) — log compaction

```go
// Pass 1: find latest offset for each key
latestForKey := make(map[string]int)
for localIdx := 0; localIdx < totalRecords; localIdx++ {
	key, err := readKeyAtIndex(seg, localIdx)
	if err != nil {
		continue // corruption? keep it to be safe
	}
	latestForKey[string(key)] = localIdx // always overwrite — higher index wins
}

keepSet := make(map[int]bool)
for _, idx := range latestForKey {
	keepSet[idx] = true
}
```

Pass 1 scans every record once, reading *only* its key (`readKeyAtIndex` reads the 14-byte header plus the key bytes, never the value — cheap even for large values), and for each distinct key string, keeps overwriting `latestForKey[key]` with the current `localIdx`. Because the loop runs in increasing index order (oldest to newest), whatever index a key is mapped to when the loop finishes is, by construction, the *highest* (most recent) index that key ever appeared at — no explicit "is this newer" comparison needed, just "always overwrite, then whoever's still standing at the end is the newest."

```go
// Pass 2: write only kept records to a temp file, atomic rename
compactedFile, _ := os.Create(compactedPath) // origPath + ".compacted"
for localIdx := 0; localIdx < totalRecords; localIdx++ {
	if !keepSet[localIdx] {
		continue
	}
	// ... read full raw record (size header + body) from seg.file at seg.index[localIdx] ...
	newIndex = append(newIndex, newSize)
	compactedFile.WriteAt(fullRecord, newSize)
	newSize += int64(len(fullRecord))
}
compactedFile.Sync()
compactedFile.Close()

seg.file.Close()
os.Rename(compactedPath, origPath) // atomic
```

Pass 2 scans again, this time skipping every index *not* in `keepSet` (i.e., every record that's a superseded old value for some key), and writes only the survivors — sequentially, so the new file has no gaps — into a brand-new `.compacted` file, tracking a fresh `newIndex` of byte positions as it goes. Only after that entire file is written and `Sync()`ed does the code close the *original* segment file and call `os.Rename(compactedPath, origPath)`.

The rename is the safety-critical line: `os.Rename` on the same filesystem is atomic at the OS level — at any instant, `origPath` either points to the fully-old file or the fully-new file, *never* a half-written mix of the two. If the process crashes between "finish writing `.compacted`" and "rename," the original untouched file is still sitting at `origPath` and nothing is lost; if it crashes *during* the rename itself, the OS guarantees the rename either completed or didn't — there's no observable in-between state.

**What an interviewer would ask about this code:** *"Why two passes instead of one? Couldn't you decide keep-vs-discard while writing, in a single scan?"* — No, because you can't know whether index 3 (an early write for key "user1") is going to be superseded until you've seen *every later* record — a hypothetical single pass would have to write index 3's record speculatively, then potentially need to "un-write" it later when index 47 turns out to have the same key, which a sequential append-only output file structurally cannot do. Two passes trade a second full read of the segment (cheap — sequential disk reads, per Section 3/11's numbers, are extremely fast) for the ability to know the complete answer ("is this the final value for this key?") before writing anything.

### 13.7 `Server.handleConnection` (server.go) — the request loop

```go
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	for {
		data, err := ReadFrame(conn)
		if err != nil {
			return // client disconnected
		}

		if len(data) == 0 {
			s.sendError(conn, "empty request")
			continue
		}

		var response []byte
		switch data[0] {
		case RequestProduce:
			response = s.handleProduce(data)
		case RequestConsume:
			response = s.handleConsume(data)
		case RequestCreateTopic:
			response = s.handleCreateTopic(data)
		// ... RequestCommit, RequestFetchOffset, RequestJoinGroup,
		//     RequestHeartbeat, RequestLeaveGroup, RequestProduceBatch ...
		default:
			response = EncodeResponse(StatusError, []byte(fmt.Sprintf("unknown request type: %d", data[0])))
		}

		if err := WriteFrame(conn, response); err != nil {
			return // client disconnected mid-response
		}
	}
}
```

This is a classic **request-response loop over a persistent connection**, one goroutine per client (spawned by `Start()`'s `go s.handleConnection(conn)`, one per accepted `net.Conn`). The loop body is four steps repeated forever until the client disconnects: (1) `ReadFrame(conn)` blocks until a complete, length-prefixed frame has arrived (`protocol.go`'s `[4-byte size][body]` framing means the reader always knows exactly how many bytes to wait for — no ambiguity about "is this message done yet"); (2) `data[0]` — the very first byte of every request — is a request-type tag, and the `switch` dispatches purely on that one byte to the matching `handleXxx` method; (3) each `handleXxx` decodes the rest of `data` into a typed request struct, calls the matching `Broker` method, and encodes a response; (4) `WriteFrame(conn, response)` sends the length-prefixed response back, and the loop immediately goes back to step 1 to wait for this same client's *next* request — the connection is reused, not torn down per request (this is the "persistent connections" upgrade noted in the project's fix list, replacing a hypothetical reconnect-per-command design).

Both `return` statements (on `ReadFrame` error, or `WriteFrame` error) are the *only* ways this loop ends — a client's TCP disconnect (or any I/O error) simply surfaces as an error from one of those two calls, at which point `defer conn.Close()` and `defer s.wg.Done()` run automatically, cleaning up the connection and telling the server's `WaitGroup` (used by `Stop()` for graceful shutdown) that one fewer goroutine is active.

**What an interviewer would ask about this code:** *"What happens if two clients send requests to the same topic/partition at the exact same instant?"* — Each client has its *own* goroutine running its *own* copy of this loop, so both goroutines can call into `Broker` methods concurrently — safety at that point falls entirely on `Broker`'s own locking (`b.mu.RLock()` in `Publish`, per Section 13.4, plus each `Segment`'s own `s.mu` in `Append`), not on anything in `handleConnection` itself; the per-connection loop's only job is framing and dispatch, and it deliberately has zero cross-client coordination logic — that's the broker/segment layer's responsibility, one level down.

---

## Conclusion

### What you built

Across thirteen sections, this project builds a single-node message broker that implements the *real* mechanisms behind Kafka's core guarantees, not simplified stand-ins for them:

- A **segmented, append-only, crash-recoverable log** (`segment.go`, `log.go`) with real fsync-based durability, real corruption detection (CRC32, `record.go`), and real crash recovery (truncate-to-last-valid-record on restart).
- A **length-prefixed binary wire protocol** (`protocol.go`) and a **persistent-connection TCP server** (`server.go`) handling nine distinct request types over long-lived, per-client goroutines.
- **Key-hashed partition routing** (`broker.go`) giving real per-key ordering guarantees, with both single-message and batched produce paths — the batched path delivering a measured 493× throughput improvement by amortizing fsync (Section 9).
- **Consumer groups with real sticky rebalancing** (`coordinator.go`) — provably balanced (±1 partition per member) *and* movement-minimizing, with heartbeat-based failure detection reassigning orphaned partitions automatically.
- **Real two-pass log compaction** (`compactor.go`) with crash-safe atomic rename, and **file-based, atomically-written offset persistence** (`offsets.go`) giving at-least-once delivery with correct crash-resume behavior.
- A **live WebSocket event dashboard** (`ws.go`, `events.go`) streaming every produce/consume/commit/join/leave event in real time.
- Every simplification against real, production, multi-broker Kafka — replication, exactly-once, security, compression, schema management — is disclosed and explained in Section 12, with the specific reason it was scoped out and what the correct fix actually is.

### Three resume bullets (defensible)

1. *"Built a Kafka-compatible message broker in Go from first principles — segmented append-only log storage with CRC32 corruption detection and crash recovery, a custom binary wire protocol over TCP, and a sticky consumer-group rebalancing algorithm that provably minimizes partition movement while maintaining ±1 partition balance across members."*
2. *"Measured and closed a 493× write-throughput gap (270 → 133,009 msg/s) by redesigning the fsync strategy from per-message to per-batch durability, quantifying the exact tradeoff between crash-safety exposure and throughput as a function of batch size."*
3. *"Implemented real Kafka-style log compaction (two-pass latest-value-per-key scan with atomic file rename for crash safety) and file-based consumer offset persistence, enabling at-least-once delivery semantics with correct resume-after-crash behavior for both producers and consumers."*

Every clause in those three bullets maps to a specific, working function in this codebase and a specific measured number — nothing there is aspirational, and every one of them survives the follow-up question "walk me through how that actually works" using Section 13 as the answer key.

### What to study next, if you want to go deeper

- **Raft consensus** — the algorithm underneath both etcd/Consul-style systems *and* Kafka's own KRaft controller (Section 12, item 14). Understanding Raft's leader election and log-replication protocol is the direct next step toward actually building the replication (item 2) this project explicitly doesn't have — a single-node log with an fsync is a *component* of a Raft-replicated log, not a replacement for one.
- **Exactly-once semantics** — specifically, how Kafka's idempotent producer (PID + per-partition sequence number, rejecting duplicate sequence numbers at the broker) and transactions (atomic multi-partition writes + offset commits, visible only on commit via transaction markers written directly into the log) actually work end-to-end; Section 12 item 3 sketches the mechanism, but implementing a toy version of PID+sequence deduplication on top of this project's existing `Log.Append` would be a natural, scoped next exercise.
- **Zero-copy I/O** — `sendfile()` and `mmap()` at the OS syscall level, and how Go's standard library exposes (or doesn't directly expose) them; this is the natural next lever once storage/fsync stops being the bottleneck (Section 11 Q21), and a good way to learn the boundary between "userspace program" and "what the OS kernel can do on your behalf without you ever seeing the bytes."
