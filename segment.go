package minikafka

// segment.go — A single log segment file.
// Each segment holds a range of messages in [size][payload] format.
// It's basically our original Log struct, but now it knows its "base offset"
// (the offset of the first message it contains).

import (
	"encoding/binary"
	"os"
	"sync"
)

// Segment is one file in the log. It holds messages starting at baseOffset.
//
// Java equivalent:
//   public class Segment {
//       private File file;
//       private List<Long> index;   // byte positions within THIS file
//       private long baseOffset;    // first message's offset in the overall log
//       private long size;          // current file size in bytes
//   }
type Segment struct {
	file       *os.File
	index      []int64 // index[i] = byte position of message (baseOffset + i)
	baseOffset uint64  // the global offset of the first message in this segment
	size       int64   // current file size in bytes
	mu         sync.Mutex
}

// OpenSegment opens (or creates) a segment file.
// baseOffset = the offset number of the first message that will go in this segment.
//
// Example: OpenSegment("data/segment-0000005000.log", 5000)
//   → This segment holds messages starting at offset 5000
func OpenSegment(path string, baseOffset uint64) (*Segment, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	s := &Segment{
		file:       f,
		index:      make([]int64, 0),
		baseOffset: baseOffset,
		size:       0,
	}

	// Rebuild index by scanning existing data (crash recovery)
	if err := s.rebuildIndex(); err != nil {
		f.Close()
		return nil, err
	}

	return s, nil
}

// Append writes a message to this segment.
// Returns the GLOBAL offset (baseOffset + position within this segment).
//
// Example: if baseOffset=5000 and this is the 3rd message in this segment,
//          returns 5003.
func (s *Segment) Append(payload []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Current end of file = where this message starts
	pos := s.size

	// Build the 4-byte size header
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(payload)))

	// Write size header
	if _, err := s.file.WriteAt(sizeBuf, pos); err != nil {
		return 0, err
	}

	// Write payload
	if _, err := s.file.WriteAt(payload, pos+4); err != nil {
		return 0, err
	}

	// Force to disk
	if err := s.file.Sync(); err != nil {
		return 0, err
	}

	// Update our tracking
	s.index = append(s.index, pos)
	s.size = pos + 4 + int64(len(payload))

	// Global offset = baseOffset + how many messages are in this segment - 1
	globalOffset := s.baseOffset + uint64(len(s.index)) - 1
	return globalOffset, nil
}

// Read returns the message at the given LOCAL index (0-based within this segment).
//
// Example: if this segment has baseOffset=5000 and you want global offset 5003,
//          the caller passes localIndex=3.
func (s *Segment) Read(localIndex uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if localIndex >= uint64(len(s.index)) {
		return nil, os.ErrNotExist
	}

	pos := s.index[localIndex]

	// Read size header
	sizeBuf := make([]byte, 4)
	if _, err := s.file.ReadAt(sizeBuf, pos); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(sizeBuf)

	// Read payload
	payload := make([]byte, size)
	if _, err := s.file.ReadAt(payload, pos+4); err != nil {
		return nil, err
	}

	return payload, nil
}

// AppendRecord writes a Record to this segment (new format with CRC + timestamp + key/value).
// Returns the global offset.
func (s *Segment) AppendRecord(rec *Record) (uint64, error) {
	body := EncodeRecord(rec)
	return s.Append(body)
}

// ReadRecord reads and decodes a Record at the given local index.
// Validates the CRC32 checksum — returns error if data is corrupted.
func (s *Segment) ReadRecord(localIndex uint64) (*Record, error) {
	body, err := s.Read(localIndex)
	if err != nil {
		return nil, err
	}
	return DecodeRecord(body) // this validates CRC
}

// AppendBatch writes multiple messages with ONE fsync at the end.
// This is the key performance optimization: amortizes the ~4ms fsync cost
// across all messages in the batch.
//
// Returns the global offsets for all messages in order.
//
// Comparison:
//   Single Append (N messages): N writes + N fsyncs = N × 4ms = slow
//   AppendBatch (N messages):   N writes + 1 fsync  = 1 × 4ms = fast!
func (s *Segment) AppendBatch(payloads [][]byte) ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offsets := make([]uint64, len(payloads))
	pos := s.size

	for i, payload := range payloads {
		// Record this message's position BEFORE writing
		offsets[i] = s.baseOffset + uint64(len(s.index))

		// Write size header
		sizeBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(sizeBuf, uint32(len(payload)))
		if _, err := s.file.WriteAt(sizeBuf, pos); err != nil {
			return nil, err
		}

		// Write payload
		if _, err := s.file.WriteAt(payload, pos+4); err != nil {
			return nil, err
		}

		// Update index (in memory)
		s.index = append(s.index, pos)
		pos += 4 + int64(len(payload))
	}

	// ONE fsync for the entire batch — this is where the speed comes from
	if err := s.file.Sync(); err != nil {
		return nil, err
	}

	s.size = pos
	return offsets, nil
}

// Count returns how many messages this segment holds.
func (s *Segment) Count() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.index))
}

// Size returns the current file size in bytes.
func (s *Segment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// Close closes the segment file.
func (s *Segment) Close() error {
	return s.file.Close()
}

// Remove closes and deletes the segment file from disk.
// Used for retention — deleting old segments to free space.
func (s *Segment) Remove() error {
	path := s.file.Name() // get the file path before closing
	s.file.Close()
	return os.Remove(path)
}

// rebuildIndex scans the file from the start, reading each [size][payload]
// record to rebuild the in-memory index. Truncates any incomplete trailing record.
func (s *Segment) rebuildIndex() error {
	// Get file size to know how much data exists
	info, err := s.file.Stat()
	if err != nil {
		return err
	}
	fileSize := info.Size()
	s.size = 0

	pos := int64(0)
	sizeBuf := make([]byte, 4)

	for pos < fileSize {
		// Need at least 4 bytes for a size header
		if pos+4 > fileSize {
			// Partial size header — truncate
			s.file.Truncate(pos)
			break
		}

		// Read size header
		if _, err := s.file.ReadAt(sizeBuf, pos); err != nil {
			s.file.Truncate(pos)
			break
		}
		size := int64(binary.BigEndian.Uint32(sizeBuf))

		// Check if full payload exists
		if pos+4+size > fileSize {
			// Partial payload — truncate
			s.file.Truncate(pos)
			break
		}

		// Valid record
		s.index = append(s.index, pos)
		pos += 4 + size
	}

	s.size = pos
	return nil
}
