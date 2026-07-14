package minikafka

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestCompactionBasic(t *testing.T) {
	dir := "test_compaction_basic"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Small segments so we get multiple (and can compact closed ones)
	config := LogConfig{
		MaxSegmentBytes: 200, // very small — forces rotation
		MaxLogBytes:     10000,
	}

	log, err := NewLog(dir, config)
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}

	// Write records with repeated keys — using the Record format
	// Same key "user-42" gets updated 5 times
	messages := []struct {
		key   string
		value string
	}{
		{"user-42", "city=NYC"},       // offset 0 — will be superseded
		{"user-99", "city=Tokyo"},     // offset 1 — will be superseded
		{"user-42", "city=LA"},        // offset 2 — will be superseded
		{"user-99", "city=Paris"},     // offset 3 — LATEST for user-99
		{"user-42", "city=London"},    // offset 4 — will be superseded
		{"user-77", "city=Berlin"},    // offset 5 — LATEST for user-77
		{"user-42", "city=Berlin"},    // offset 6 — LATEST for user-42
	}

	for _, msg := range messages {
		rec := &Record{
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte(msg.key),
			Value:     []byte(msg.value),
		}
		_, err := log.AppendRecord(rec)
		if err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	t.Logf("wrote %d messages, log has %d segments", len(messages), len(log.segments))

	// Compact the closed segments
	removed, err := CompactLog(log)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}
	t.Logf("compaction removed %d records", removed)

	if removed == 0 {
		t.Logf("no records removed — all may be in the active segment (too small segments)")
	}

	log.Close()
}

func TestCompactionKeepsLatest(t *testing.T) {
	dir := "test_compaction_latest"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Create a segment directly (not through Log) for precise control
	segPath := dir + "/segment.log"
	os.MkdirAll(dir, 0755)

	seg, err := OpenSegment(segPath, 0)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}

	// Write 10 messages: keys A, B, C repeated
	type msg struct {
		key, value string
	}
	messages := []msg{
		{"A", "A-version-1"},
		{"B", "B-version-1"},
		{"C", "C-version-1"},
		{"A", "A-version-2"},
		{"B", "B-version-2"},
		{"A", "A-version-3"}, // ← latest A
		{"C", "C-version-2"}, // ← latest C
		{"B", "B-version-3"}, // ← latest B
	}

	for _, m := range messages {
		rec := &Record{
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte(m.key),
			Value:     []byte(m.value),
		}
		seg.AppendRecord(rec)
	}

	t.Logf("before compaction: %d records", seg.Count())

	// Compact
	removed, err := CompactSegment(seg)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	t.Logf("after compaction: %d records (removed %d)", seg.Count(), removed)

	// Should have 3 records left (one per unique key)
	if seg.Count() != 3 {
		t.Fatalf("expected 3 records after compaction, got %d", seg.Count())
	}

	// Verify the LATEST value for each key survived
	expectedValues := map[string]string{
		"A": "A-version-3",
		"B": "B-version-3",
		"C": "C-version-2",
	}

	for i := uint64(0); i < seg.Count(); i++ {
		rec, err := seg.ReadRecord(i)
		if err != nil {
			t.Fatalf("read record %d failed: %v", i, err)
		}

		key := string(rec.Key)
		expectedVal, exists := expectedValues[key]
		if !exists {
			t.Fatalf("unexpected key %q in compacted segment", key)
		}
		if string(rec.Value) != expectedVal {
			t.Fatalf("key %q: expected value %q, got %q", key, expectedVal, string(rec.Value))
		}
		delete(expectedValues, key) // mark as found
		t.Logf("  ✓ key=%q value=%q (latest)", key, string(rec.Value))
	}

	// All keys should have been found
	if len(expectedValues) > 0 {
		t.Fatalf("missing keys after compaction: %v", expectedValues)
	}

	seg.Close()
}

func TestCompactionCRCStillValid(t *testing.T) {
	dir := "test_compaction_crc"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)

	seg, err := OpenSegment(dir+"/segment.log", 0)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Write messages
	for i := 0; i < 5; i++ {
		rec := &Record{
			Timestamp: time.Now().UnixMilli(),
			Key:       []byte("same-key"),
			Value:     []byte(fmt.Sprintf("version-%d", i)),
		}
		seg.AppendRecord(rec)
	}

	// Compact (should keep only the last one)
	removed, err := CompactSegment(seg)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}
	if removed != 4 {
		t.Fatalf("expected 4 removed, got %d", removed)
	}

	// Read the surviving record — CRC should still validate
	rec, err := seg.ReadRecord(0)
	if err != nil {
		t.Fatalf("CRC validation failed after compaction: %v", err)
	}

	if string(rec.Value) != "version-4" {
		t.Fatalf("expected 'version-4', got '%s'", string(rec.Value))
	}
	if !bytes.Equal(rec.Key, []byte("same-key")) {
		t.Fatalf("key mismatch after compaction")
	}

	t.Logf("✓ CRC valid after compaction: key=%q value=%q", rec.Key, rec.Value)
	seg.Close()
}
