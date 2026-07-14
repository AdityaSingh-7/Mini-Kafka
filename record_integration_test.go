package minikafka

import (
	"bytes"
	"os"
	"testing"
	"time"
)

func TestLogRecordRoundTrip(t *testing.T) {
	dir := "test_log_record"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	log, err := NewLog(dir, DefaultConfig())
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}

	// Write records with key/value
	records := []*Record{
		{Timestamp: time.Now().UnixMilli(), Key: []byte("customer-42"), Value: []byte("order #1")},
		{Timestamp: time.Now().UnixMilli(), Key: []byte("customer-42"), Value: []byte("order #2")},
		{Timestamp: time.Now().UnixMilli(), Key: nil, Value: []byte("no key message")},
	}

	for i, rec := range records {
		offset, err := log.AppendRecord(rec)
		if err != nil {
			t.Fatalf("append record %d failed: %v", i, err)
		}
		if offset != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, offset)
		}
	}

	// Read them back — CRC is validated on each read
	for i, original := range records {
		rec, err := log.ReadRecord(uint64(i))
		if err != nil {
			t.Fatalf("read record %d failed: %v", i, err)
		}

		if rec.Timestamp != original.Timestamp {
			t.Fatalf("record %d: timestamp mismatch", i)
		}
		if !bytes.Equal(rec.Key, original.Key) {
			t.Fatalf("record %d: key mismatch: %q vs %q", i, rec.Key, original.Key)
		}
		if !bytes.Equal(rec.Value, original.Value) {
			t.Fatalf("record %d: value mismatch: %q vs %q", i, rec.Value, original.Value)
		}
	}

	t.Logf("✓ 3 records written and read back with CRC validation")

	log.Close()

	// Reopen — crash recovery should work with new format
	log2, err := NewLog(dir, DefaultConfig())
	if err != nil {
		t.Fatalf("failed to reopen log: %v", err)
	}
	defer log2.Close()

	// Verify records survived restart
	rec, err := log2.ReadRecord(0)
	if err != nil {
		t.Fatalf("read after restart failed: %v", err)
	}
	if !bytes.Equal(rec.Key, []byte("customer-42")) {
		t.Fatalf("key mismatch after restart: %q", rec.Key)
	}
	if !bytes.Equal(rec.Value, []byte("order #1")) {
		t.Fatalf("value mismatch after restart: %q", rec.Value)
	}

	t.Logf("✓ records survived crash recovery with CRC intact")
}

func TestLogRecordCorruptionDetection(t *testing.T) {
	dir := "test_log_record_corrupt"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	log, err := NewLog(dir, DefaultConfig())
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}

	// Write a record
	rec := &Record{
		Timestamp: time.Now().UnixMilli(),
		Key:       []byte("key"),
		Value:     []byte("very important data"),
	}
	log.AppendRecord(rec)
	log.Close()

	// CORRUPT the file directly — flip a byte in the value area
	// The segment file is at: dir/segment-*.log
	entries, _ := os.ReadDir(dir)
	var segPath string
	for _, e := range entries {
		if !e.IsDir() {
			segPath = dir + "/" + e.Name()
			break
		}
	}

	if segPath == "" {
		t.Fatalf("no segment file found")
	}

	// Read the file, corrupt a byte near the end (in the value), write it back
	data, _ := os.ReadFile(segPath)
	data[len(data)-3] ^= 0xFF // flip bits in the value
	os.WriteFile(segPath, data, 0644)

	// Reopen the log — rebuild index should work (it doesn't check CRC during scan)
	log2, err := NewLog(dir, DefaultConfig())
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer log2.Close()

	// Try to READ — CRC check should CATCH the corruption
	_, err = log2.ReadRecord(0)
	if err == nil {
		t.Fatalf("expected CRC error on corrupted record, but read succeeded!")
	}

	t.Logf("✓ corruption detected on read: %v", err)
}
