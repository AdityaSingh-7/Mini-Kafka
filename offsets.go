package minikafka

// offsets.go — Stores consumer group offsets on disk.
// Each consumer group/topic/partition combination gets one small file
// containing a single number: the next offset to read.
//
// On disk:
//   data/offsets/{group}/{topic}/partition-{N}
//   Contents: "42" (just the number as text)

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// OffsetStore manages committed offsets for consumer groups.
//
// Java equivalent:
//   public class OffsetStore {
//       private String dir;   // "data/offsets/"
//       // get(group, topic, partition) → offset
//       // set(group, topic, partition, offset)
//   }
type OffsetStore struct {
	dir string
	mu  sync.Mutex
}

// NewOffsetStore creates an offset store in the given directory.
func NewOffsetStore(dir string) (*OffsetStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &OffsetStore{dir: dir}, nil
}

// Commit saves the offset for a consumer group/topic/partition.
// This means: "this consumer group has processed everything up to (but not including) this offset."
//
// Example: Commit("checkout", "orders", 1, 5)
//   → The file data/offsets/checkout/orders/partition-1 now contains "5"
//   → Next time this consumer asks, we'll tell it to read from offset 5
func (s *OffsetStore) Commit(group, topic string, partition int, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Build the file path: data/offsets/{group}/{topic}/partition-{N}
	dir := filepath.Join(s.dir, group, topic)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	path := filepath.Join(dir, fmt.Sprintf("partition-%d", partition))

	// Write the offset as a text number
	// We write to a temp file then rename — this makes the operation ATOMIC
	// (either the full new value is there, or the old value is there — never half-written)
	tmpPath := path + ".tmp"
	data := []byte(strconv.FormatUint(offset, 10))

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	// Rename is atomic on most filesystems
	// Java equivalent: Files.move(tmp, target, ATOMIC_MOVE)
	return os.Rename(tmpPath, path)
}

// Fetch returns the committed offset for a consumer group/topic/partition.
// Returns 0 if no offset has been committed (brand new consumer).
func (s *OffsetStore) Fetch(group, topic string, partition int) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.dir, group, topic, fmt.Sprintf("partition-%d", partition))

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// No offset committed yet — start from 0
			return 0, nil
		}
		return 0, err
	}

	// Parse the number from the file
	offset, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("corrupt offset file %s: %v", path, err)
	}

	return offset, nil
}
