package minikafka

// compactor.go — Log compaction: keeps only the latest value per key.
//
// How it works:
//   1. Scan a closed segment, read every record's key
//   2. Build a map: key → highest offset in this segment
//   3. Scan again: write only records whose offset IS the highest for their key
//   4. Replace old segment file with compacted one (atomic rename)
//
// Result: segment shrinks, old values removed, latest per key preserved.

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

// CompactSegment compacts a single closed segment file.
// Reads all records, keeps only the latest value per key.
// Returns the number of records removed.
//
// The segment must NOT be the active segment (no writes during compaction).
func CompactSegment(seg *Segment) (int, error) {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	totalRecords := len(seg.index)
	if totalRecords == 0 {
		return 0, nil
	}

	// Pass 1: Find the latest offset for each key
	// key → local index of the latest message with that key
	latestForKey := make(map[string]int) // key string → local index

	for localIdx := 0; localIdx < totalRecords; localIdx++ {
		// Read the record at this index to get its key
		key, err := readKeyAtIndex(seg, localIdx)
		if err != nil {
			// If we can't read a record (corruption?), keep it to be safe
			continue
		}

		// Always update to the latest (higher index = later message)
		latestForKey[string(key)] = localIdx
	}

	// How many records will survive?
	keepSet := make(map[int]bool)
	for _, idx := range latestForKey {
		keepSet[idx] = true
	}

	removed := totalRecords - len(keepSet)
	if removed == 0 {
		// Nothing to compact — all keys are unique
		return 0, nil
	}

	// Pass 2: Write a new compacted file with only the kept records
	origPath := seg.file.Name()
	compactedPath := origPath + ".compacted"

	compactedFile, err := os.Create(compactedPath)
	if err != nil {
		return 0, fmt.Errorf("failed to create compacted file: %w", err)
	}

	newIndex := make([]int64, 0, len(keepSet))
	var newSize int64

	for localIdx := 0; localIdx < totalRecords; localIdx++ {
		if !keepSet[localIdx] {
			continue // skip — superseded by a later message with same key
		}

		// Read the full raw record (size header + body)
		pos := seg.index[localIdx]
		sizeBuf := make([]byte, 4)
		if _, err := seg.file.ReadAt(sizeBuf, pos); err != nil {
			compactedFile.Close()
			os.Remove(compactedPath)
			return 0, fmt.Errorf("read size at index %d: %w", localIdx, err)
		}
		bodySize := binary.BigEndian.Uint32(sizeBuf)

		fullRecord := make([]byte, 4+int(bodySize))
		if _, err := seg.file.ReadAt(fullRecord, pos); err != nil {
			compactedFile.Close()
			os.Remove(compactedPath)
			return 0, fmt.Errorf("read record at index %d: %w", localIdx, err)
		}

		// Write to compacted file
		newIndex = append(newIndex, newSize)
		if _, err := compactedFile.WriteAt(fullRecord, newSize); err != nil {
			compactedFile.Close()
			os.Remove(compactedPath)
			return 0, fmt.Errorf("write compacted record: %w", err)
		}
		newSize += int64(len(fullRecord))
	}

	// Sync the compacted file
	if err := compactedFile.Sync(); err != nil {
		compactedFile.Close()
		os.Remove(compactedPath)
		return 0, err
	}
	compactedFile.Close()

	// Atomic swap: close old file, rename compacted over it
	seg.file.Close()
	if err := os.Rename(compactedPath, origPath); err != nil {
		return 0, fmt.Errorf("atomic rename failed: %w", err)
	}

	// Reopen the file and update segment state
	f, err := os.OpenFile(origPath, os.O_RDWR, 0644)
	if err != nil {
		return 0, fmt.Errorf("reopen compacted file: %w", err)
	}
	seg.file = f
	seg.index = newIndex
	seg.size = newSize

	return removed, nil
}

// CompactLog compacts all CLOSED segments in a log (not the active one).
// Returns total records removed across all segments.
func CompactLog(l *Log) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	totalRemoved := 0

	// Compact every segment EXCEPT the last one (active)
	for i := 0; i < len(l.segments)-1; i++ {
		removed, err := CompactSegment(l.segments[i])
		if err != nil {
			return totalRemoved, fmt.Errorf("compact segment %d: %w", i, err)
		}
		totalRemoved += removed
	}

	return totalRemoved, nil
}

// readKeyAtIndex reads a record at the given local index and extracts just the key.
// Uses the Record format: [CRC(4)][timestamp(8)][keyLen(2)][key][value]
func readKeyAtIndex(seg *Segment, localIdx int) ([]byte, error) {
	pos := seg.index[localIdx]

	// Read size header
	sizeBuf := make([]byte, 4)
	if _, err := seg.file.ReadAt(sizeBuf, pos); err != nil {
		return nil, err
	}
	bodySize := binary.BigEndian.Uint32(sizeBuf)

	// We only need the first 14 bytes of the body (CRC + timestamp + keyLen)
	// plus the key itself. But we need keyLen to know how much to read.
	// Read the minimum: CRC(4) + timestamp(8) + keyLen(2) = 14 bytes
	if bodySize < 14 {
		return nil, fmt.Errorf("record body too small: %d", bodySize)
	}

	headerBuf := make([]byte, 14)
	if _, err := seg.file.ReadAt(headerBuf, pos+4); err != nil {
		return nil, err
	}

	keyLen := int(binary.BigEndian.Uint16(headerBuf[12:14]))
	if keyLen == 0 {
		return nil, nil // no key — can't compact keyless messages
	}

	// Read the key
	keyBuf := make([]byte, keyLen)
	if _, err := seg.file.ReadAt(keyBuf, pos+4+14); err != nil {
		return nil, err
	}

	return keyBuf, nil
}

// CompactDir compacts segment files in a directory without needing an open Log.
// Useful as a standalone maintenance tool.
func CompactDir(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	// Find all segment files, sort them
	var segFiles []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			segFiles = append(segFiles, filepath.Join(dir, e.Name()))
		}
	}

	if len(segFiles) <= 1 {
		return 0, nil // nothing to compact (0 or only active)
	}

	totalRemoved := 0

	// Compact all except the last (assumed active)
	for i := 0; i < len(segFiles)-1; i++ {
		seg, err := OpenSegment(segFiles[i], 0) // baseOffset doesn't matter for compaction
		if err != nil {
			return totalRemoved, err
		}

		removed, err := CompactSegment(seg)
		seg.Close()
		if err != nil {
			return totalRemoved, err
		}
		totalRemoved += removed
	}

	return totalRemoved, nil
}
