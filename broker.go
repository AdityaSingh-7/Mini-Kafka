package minikafka

// broker.go — The central broker that holds topics and partitions.
// Each topic is a name, each partition is one of our Log structs.

import (
	"fmt"
	"hash/fnv"
	"path/filepath"
	"sync"
	"time"
)

// BrokerConfig holds settings for the broker.
type BrokerConfig struct {
	DataDir         string   // where to store all data (e.g. "data/")
	DefaultPartitions int    // how many partitions when creating a new topic
	SegmentBytes    int64    // per-segment size limit (passed to each Log)
	RetentionBytes  int64    // per-partition total size limit
}

// DefaultBrokerConfig returns sensible defaults for development.
func DefaultBrokerConfig() BrokerConfig {
	return BrokerConfig{
		DataDir:           "data",
		DefaultPartitions: 3,
		SegmentBytes:      10 * 1024 * 1024,  // 10 MB
		RetentionBytes:    100 * 1024 * 1024,  // 100 MB
	}
}

// Broker holds topics and their partitions.
//
// Java equivalent:
//   public class Broker {
//       private Map<String, List<Log>> topics;
//       private OffsetStore offsets;
//       private Coordinator coordinator;
//       private BrokerConfig config;
//   }
type Broker struct {
	topics      map[string][]*Log  // topic name → list of partition Logs
	offsets     *OffsetStore       // persisted consumer group offsets
	coordinator *Coordinator       // manages consumer groups
	config      BrokerConfig
	mu          sync.RWMutex       // allows multiple readers OR one writer
}

// NewBroker creates a broker with the given config.
func NewBroker(config BrokerConfig) (*Broker, error) {
	// Create the offset store
	offsetDir := filepath.Join(config.DataDir, "offsets")
	offsets, err := NewOffsetStore(offsetDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create offset store: %w", err)
	}

	b := &Broker{
		topics:      make(map[string][]*Log),
		offsets:     offsets,
		coordinator: NewCoordinator(10 * time.Second), // member dead after 10s no heartbeat
		config:      config,
	}
	return b, nil
}

// JoinGroup adds a consumer to a group and returns its partition assignment.
// This is called when a consumer starts and says "I want to be in group X, reading topic Y."
func (b *Broker) JoinGroup(group, memberID, topic string) ([]int, error) {
	b.mu.RLock()
	partitions, exists := b.topics[topic]
	b.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("topic %q does not exist", topic)
	}

	return b.coordinator.JoinGroup(group, memberID, topic, len(partitions))
}

// LeaveGroup removes a consumer from a group. Triggers rebalance.
func (b *Broker) LeaveGroup(group, memberID string) error {
	return b.coordinator.LeaveGroup(group, memberID)
}

// Heartbeat updates a member's liveness. Returns their current partition assignment.
// If a rebalance happened, the new assignment is returned here — that's how
// the consumer LEARNS about rebalances.
func (b *Broker) Heartbeat(group, memberID string) ([]int, error) {
	return b.coordinator.Heartbeat(group, memberID)
}

// CommitOffset saves a consumer group's position.
// "Group X has processed everything before this offset on topic/partition."
func (b *Broker) CommitOffset(group, topic string, partition int, offset uint64) error {
	return b.offsets.Commit(group, topic, partition, offset)
}

// FetchOffset returns where a consumer group should resume reading.
// Returns 0 if they've never committed (start from the beginning).
func (b *Broker) FetchOffset(group, topic string, partition int) (uint64, error) {
	return b.offsets.Fetch(group, topic, partition)
}

// CreateTopic creates a new topic with N partitions.
// Each partition is a separate Log in its own directory.
//
// On disk it looks like:
//   data/orders/partition-0/segment-*.log
//   data/orders/partition-1/segment-*.log
//   data/orders/partition-2/segment-*.log
func (b *Broker) CreateTopic(name string, numPartitions int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Check if topic already exists
	// Java: if (this.topics.containsKey(name)) { throw ... }
	if _, exists := b.topics[name]; exists {
		return fmt.Errorf("topic %q already exists", name)
	}

	// Create each partition (each one is a Log in its own directory)
	partitions := make([]*Log, numPartitions)
	for i := 0; i < numPartitions; i++ {
		// Directory: data/orders/partition-0/
		dir := filepath.Join(b.config.DataDir, name, fmt.Sprintf("partition-%d", i))

		logConfig := LogConfig{
			MaxSegmentBytes: b.config.SegmentBytes,
			MaxLogBytes:     b.config.RetentionBytes,
		}

		log, err := NewLog(dir, logConfig)
		if err != nil {
			// Clean up any partitions we already created
			for j := 0; j < i; j++ {
				partitions[j].Close()
			}
			return fmt.Errorf("failed to create partition %d for topic %q: %w", i, name, err)
		}
		partitions[i] = log
	}

	b.topics[name] = partitions
	return nil
}

// Publish writes a message to a topic.
// The partition is chosen by hashing the key.
// If key is empty, it uses round-robin (based on current offset).
//
// Returns (partition number, offset within that partition).
func (b *Broker) Publish(topic string, key []byte, value []byte) (int, uint64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Find the topic
	// Java: List<Log> partitions = this.topics.get(topic);
	partitions, exists := b.topics[topic]
	if !exists {
		return 0, 0, fmt.Errorf("topic %q does not exist", topic)
	}

	// Pick a partition
	var partitionIdx int
	if len(key) > 0 {
		// Hash the key to pick a partition
		// Same key → always same partition → messages stay in order
		partitionIdx = hashKey(key, len(partitions))
	} else {
		// No key — use a simple round-robin based on total messages
		// (not perfect, but simple for now)
		total := uint64(0)
		for _, p := range partitions {
			total += p.Offset()
		}
		partitionIdx = int(total % uint64(len(partitions)))
	}

	// Append to the chosen partition
	offset, err := partitions[partitionIdx].Append(value)
	if err != nil {
		return 0, 0, err
	}

	return partitionIdx, offset, nil
}

// MessageEntry is one message in a batch (key + value).
type MessageEntry struct {
	Key   []byte
	Value []byte
}

// PublishResult is the result for one message in a batch.
type PublishResult struct {
	Partition int
	Offset    uint64
}

// PublishBatch writes multiple messages to a topic with ONE fsync per partition.
// Messages are grouped by partition (based on key hash), then each group is
// written in a single batch to that partition's log.
//
// This is the key performance API:
//   Single Publish × 1000: 1000 fsyncs = ~4 seconds
//   PublishBatch(1000):     N fsyncs (one per partition hit) = ~4ms × N partitions
func (b *Broker) PublishBatch(topic string, messages []MessageEntry) ([]PublishResult, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	partitions, exists := b.topics[topic]
	if !exists {
		return nil, fmt.Errorf("topic %q does not exist", topic)
	}

	numPartitions := len(partitions)

	// Group messages by partition
	// Java: Map<Integer, List<byte[]>> groups = new HashMap<>();
	type indexedPayload struct {
		originalIdx int    // position in the input slice (for building results)
		payload     []byte // the value to write
	}
	groups := make(map[int][]indexedPayload)

	for i, msg := range messages {
		var pIdx int
		if len(msg.Key) > 0 {
			pIdx = hashKey(msg.Key, numPartitions)
		} else {
			total := uint64(0)
			for _, p := range partitions {
				total += p.Offset()
			}
			pIdx = int((total + uint64(i)) % uint64(numPartitions))
		}
		groups[pIdx] = append(groups[pIdx], indexedPayload{
			originalIdx: i,
			payload:     msg.Value,
		})
	}

	// Write each group as a batch (one fsync per partition)
	results := make([]PublishResult, len(messages))

	for pIdx, items := range groups {
		// Collect payloads for this partition
		payloads := make([][]byte, len(items))
		for i, item := range items {
			payloads[i] = item.payload
		}

		// Batch write — ONE fsync for all messages in this partition
		offsets, err := partitions[pIdx].AppendBatch(payloads)
		if err != nil {
			return nil, fmt.Errorf("batch write to partition %d failed: %w", pIdx, err)
		}

		// Fill in results at the correct positions
		for i, item := range items {
			results[item.originalIdx] = PublishResult{
				Partition: pIdx,
				Offset:    offsets[i],
			}
		}
	}

	return results, nil
}

// Consume reads a message from a specific topic/partition/offset.
// Returns the message bytes.
func (b *Broker) Consume(topic string, partition int, offset uint64) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Find the topic
	partitions, exists := b.topics[topic]
	if !exists {
		return nil, fmt.Errorf("topic %q does not exist", topic)
	}

	// Check partition number is valid
	if partition < 0 || partition >= len(partitions) {
		return nil, fmt.Errorf("partition %d does not exist (topic %q has %d partitions)",
			partition, topic, len(partitions))
	}

	// Read from that partition's Log
	return partitions[partition].Read(offset)
}

// Close closes all partition logs.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, partitions := range b.topics {
		for _, log := range partitions {
			log.Close()
		}
	}
	return nil
}

// --- Internal helpers ---

// hashKey maps a key to a partition index (0 to numPartitions-1).
// Uses FNV-1a hash — fast, good distribution, deterministic.
//
// Same key + same numPartitions → ALWAYS same partition.
// This guarantees ordering for messages with the same key.
func hashKey(key []byte, numPartitions int) int {
	// FNV-1a is a simple hash function.
	// Java equivalent: Math.abs(key.hashCode()) % numPartitions
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(numPartitions))
}
