package minikafka

// log.go — The main Log that manages multiple segments.
// Handles segment rotation (creating new files) and retention (deleting old ones).

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// LogConfig holds the settings for segment rotation and retention.
//
// Java equivalent:
//   public class LogConfig {
//       long maxSegmentBytes;   // when to start a new segment file
//       long maxLogBytes;       // when to delete old segments
//   }
type LogConfig struct {
	MaxSegmentBytes int64 // start a new segment when the active one exceeds this size
	MaxLogBytes     int64 // delete oldest segments when total log size exceeds this
}

// DefaultConfig returns sensible defaults for testing.
// In production you'd want much larger values.
func DefaultConfig() LogConfig {
	return LogConfig{
		MaxSegmentBytes: 10 * 1024 * 1024, // 10 MB per segment
		MaxLogBytes:     100 * 1024 * 1024, // 100 MB total before deleting old segments
	}
}

// Log manages multiple segments in a directory.
//
// Java equivalent:
//   public class Log {
//       private String dir;
//       private List<Segment> segments;    // ordered by baseOffset
//       private Segment activeSegment;     // last one, accepts writes
//       private LogConfig config;
//   }
type Log struct {
	dir      string     // directory where segment files live
	segments []*Segment // ordered by baseOffset, last one is active
	config   LogConfig
	mu       sync.Mutex
}

// NewLog opens or creates a log in the given directory.
// It scans for existing segment files and reopens them.
func NewLog(dir string, config LogConfig) (*Log, error) {
	// Create the directory if it doesn't exist
	// Java equivalent: new File(dir).mkdirs();
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	l := &Log{
		dir:      dir,
		segments: make([]*Segment, 0),
		config:   config,
	}

	// Load existing segments from disk
	if err := l.loadSegments(); err != nil {
		return nil, err
	}

	// If no segments exist (brand new log), create the first one
	if len(l.segments) == 0 {
		if err := l.newSegment(0); err != nil {
			return nil, err
		}
	}

	return l, nil
}

// Append writes a message to the active segment.
// If the active segment is full, it creates a new one first.
// Then enforces retention (deletes old segments if total is too big).
func (l *Log) Append(payload []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Get the active segment (the last one in the list)
	active := l.segments[len(l.segments)-1]

	// Is the active segment full? If so, start a new one.
	// Java: if (activeSegment.getSize() >= config.maxSegmentBytes)
	if active.Size() >= l.config.MaxSegmentBytes {
		// New segment's baseOffset = current active's baseOffset + how many messages it has
		nextBase := active.baseOffset + active.Count()
		if err := l.newSegment(nextBase); err != nil {
			return 0, err
		}
		active = l.segments[len(l.segments)-1] // now points to the new segment
	}

	// Write the message
	offset, err := active.Append(payload)
	if err != nil {
		return 0, err
	}

	// Enforce retention — delete old segments if we're over the cap
	l.enforceRetention()

	return offset, nil
}

// Read returns the message at the given global offset.
// It finds which segment holds that offset, then reads from it.
func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Find which segment contains this offset
	seg := l.findSegment(offset)
	if seg == nil {
		return nil, fmt.Errorf("offset %d not found (may have been deleted by retention)", offset)
	}

	// Convert global offset to local index within the segment
	// Example: global offset 5003, segment baseOffset 5000 → local index 3
	localIndex := offset - seg.baseOffset
	return seg.Read(localIndex)
}

// AppendRecord writes a Record (with CRC, timestamp, key/value) to the active segment.
func (l *Log) AppendRecord(rec *Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	active := l.segments[len(l.segments)-1]

	if active.Size() >= l.config.MaxSegmentBytes {
		nextBase := active.baseOffset + active.Count()
		if err := l.newSegment(nextBase); err != nil {
			return 0, err
		}
		active = l.segments[len(l.segments)-1]
	}

	offset, err := active.AppendRecord(rec)
	if err != nil {
		return 0, err
	}

	l.enforceRetention()
	return offset, nil
}

// AppendBatch writes multiple messages with one fsync.
// If the active segment would overflow, rotates first.
func (l *Log) AppendBatch(payloads [][]byte) ([]uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	active := l.segments[len(l.segments)-1]

	// If active segment is full, rotate before writing the batch
	if active.Size() >= l.config.MaxSegmentBytes {
		nextBase := active.baseOffset + active.Count()
		if err := l.newSegment(nextBase); err != nil {
			return nil, err
		}
		active = l.segments[len(l.segments)-1]
	}

	// Write all messages with one fsync
	offsets, err := active.AppendBatch(payloads)
	if err != nil {
		return nil, err
	}

	l.enforceRetention()
	return offsets, nil
}

// ReadRecord reads and decodes a Record at the given offset (with CRC validation).
func (l *Log) ReadRecord(offset uint64) (*Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	seg := l.findSegment(offset)
	if seg == nil {
		return nil, fmt.Errorf("offset %d not found (may have been deleted by retention)", offset)
	}

	localIndex := offset - seg.baseOffset
	return seg.ReadRecord(localIndex)
}

// Offset returns the next offset that will be assigned.
// (i.e., the total number of messages ever written)
func (l *Log) Offset() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	active := l.segments[len(l.segments)-1]
	return active.baseOffset + active.Count()
}

// Close closes all segments.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, seg := range l.segments {
		seg.Close()
	}
	return nil
}

// --- Internal helpers ---

// findSegment returns the segment that contains the given global offset.
// Uses the fact that segments are sorted by baseOffset.
//
// Example: segments have baseOffsets [0, 5000, 10000]
//   findSegment(7500) → returns segment with baseOffset 5000
//   findSegment(0)    → returns segment with baseOffset 0
//   findSegment(15000)→ returns segment with baseOffset 10000
func (l *Log) findSegment(offset uint64) *Segment {
	// Search from the end (most recent segments are most likely to be queried)
	for i := len(l.segments) - 1; i >= 0; i-- {
		if offset >= l.segments[i].baseOffset {
			return l.segments[i]
		}
	}
	return nil // offset is before our earliest segment (deleted by retention)
}

// newSegment creates a new segment file and adds it to our list.
func (l *Log) newSegment(baseOffset uint64) error {
	// File name encodes the base offset (zero-padded for sorting)
	// Example: segment-0000005000.log
	name := fmt.Sprintf("segment-%020d.log", baseOffset)
	path := filepath.Join(l.dir, name)

	seg, err := OpenSegment(path, baseOffset)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, seg)
	return nil
}

// enforceRetention deletes the oldest segments until total size is under the cap.
// Never deletes the active (last) segment.
func (l *Log) enforceRetention() {
	for len(l.segments) > 1 { // never delete the active segment
		totalSize := l.totalSize()
		if totalSize <= l.config.MaxLogBytes {
			break // we're within the cap
		}

		// Delete the oldest segment
		oldest := l.segments[0]
		oldest.Remove()
		l.segments = l.segments[1:] // remove from our list
		// Java equivalent: this.segments.remove(0);
	}
}

// totalSize returns the sum of all segment file sizes.
func (l *Log) totalSize() int64 {
	var total int64
	for _, seg := range l.segments {
		total += seg.Size()
	}
	return total
}

// loadSegments reads the directory, finds existing segment files,
// and opens each one in order.
func (l *Log) loadSegments() error {
	// List all files in the directory
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}

	// Filter for segment files and sort by name (which sorts by base offset
	// because we zero-pad the number)
	var segFiles []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "segment-") && strings.HasSuffix(entry.Name(), ".log") {
			segFiles = append(segFiles, entry.Name())
		}
	}
	sort.Strings(segFiles) // alphabetical = numerical due to zero-padding

	// Open each segment
	for _, name := range segFiles {
		// Extract base offset from filename: "segment-0000005000.log" → 5000
		numStr := strings.TrimPrefix(name, "segment-")
		numStr = strings.TrimSuffix(numStr, ".log")
		baseOffset, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			continue // skip files we don't understand
		}

		path := filepath.Join(l.dir, name)
		seg, err := OpenSegment(path, baseOffset)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, seg)
	}

	return nil
}
