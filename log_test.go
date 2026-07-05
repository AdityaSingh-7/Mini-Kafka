package minikafka

import (
	"fmt"
	"os"
	"testing"
)

func TestBasicAppendAndRead(t *testing.T) {
	// Create a temp directory for our log
	dir := "test_log_basic"
	os.RemoveAll(dir) // clean slate
	defer os.RemoveAll(dir)

	// Use default config (10MB segments — we won't hit rotation here)
	log, err := NewLog(dir, DefaultConfig())
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}

	// Append two messages
	offset0, err := log.Append([]byte("hello world"))
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if offset0 != 0 {
		t.Fatalf("expected offset 0, got %d", offset0)
	}

	offset1, err := log.Append([]byte("a"))
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if offset1 != 1 {
		t.Fatalf("expected offset 1, got %d", offset1)
	}

	// Read them back
	data0, err := log.Read(0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(data0) != "hello world" {
		t.Fatalf("expected 'hello world', got '%s'", string(data0))
	}

	data1, err := log.Read(1)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(data1) != "a" {
		t.Fatalf("expected 'a', got '%s'", string(data1))
	}

	log.Close()
}

func TestCrashRecovery(t *testing.T) {
	dir := "test_log_crash"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Write some messages, then close (simulating crash)
	log, err := NewLog(dir, DefaultConfig())
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}

	log.Append([]byte("message zero"))
	log.Append([]byte("message one"))
	log.Append([]byte("message two"))
	log.Close()

	// Reopen (simulating restart) — should recover all 3 messages
	log2, err := NewLog(dir, DefaultConfig())
	if err != nil {
		t.Fatalf("failed to reopen log: %v", err)
	}
	defer log2.Close()

	// Verify all messages survived
	for i, expected := range []string{"message zero", "message one", "message two"} {
		data, err := log2.Read(uint64(i))
		if err != nil {
			t.Fatalf("read offset %d failed: %v", i, err)
		}
		if string(data) != expected {
			t.Fatalf("offset %d: expected '%s', got '%s'", i, expected, string(data))
		}
	}

	// Can still append after recovery
	offset, err := log2.Append([]byte("message three"))
	if err != nil {
		t.Fatalf("append after recovery failed: %v", err)
	}
	if offset != 3 {
		t.Fatalf("expected offset 3, got %d", offset)
	}
}

func TestSegmentRotation(t *testing.T) {
	dir := "test_log_segments"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Tiny segments: 50 bytes max per segment
	// Each message = 4 (size header) + payload bytes
	// A 20-byte payload = 24 bytes per record
	// So 2 messages fit per segment, 3rd triggers rotation
	config := LogConfig{
		MaxSegmentBytes: 50,
		MaxLogBytes:     1000, // high cap so retention doesn't kick in yet
	}

	log, err := NewLog(dir, config)
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}

	// Write 5 messages (should create multiple segments)
	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf("message-%04d-padding!", i) // ~22 bytes each
		_, err := log.Append([]byte(msg))
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Should have more than 1 segment now
	if len(log.segments) < 2 {
		t.Fatalf("expected multiple segments, got %d", len(log.segments))
	}
	t.Logf("created %d segments for 5 messages", len(log.segments))

	// Read ALL messages back — should work across segment boundaries
	for i := 0; i < 5; i++ {
		data, err := log.Read(uint64(i))
		if err != nil {
			t.Fatalf("read offset %d failed: %v", i, err)
		}
		expected := fmt.Sprintf("message-%04d-padding!", i)
		if string(data) != expected {
			t.Fatalf("offset %d: expected '%s', got '%s'", i, expected, string(data))
		}
	}

	log.Close()

	// Reopen — should find and recover all segments
	log2, err := NewLog(dir, config)
	if err != nil {
		t.Fatalf("failed to reopen log: %v", err)
	}
	defer log2.Close()

	// Verify all messages still readable after restart
	for i := 0; i < 5; i++ {
		data, err := log2.Read(uint64(i))
		if err != nil {
			t.Fatalf("after reopen, read offset %d failed: %v", i, err)
		}
		expected := fmt.Sprintf("message-%04d-padding!", i)
		if string(data) != expected {
			t.Fatalf("after reopen, offset %d: expected '%s', got '%s'", i, expected, string(data))
		}
	}
}

func TestRetention(t *testing.T) {
	dir := "test_log_retention"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Tiny config: 50 bytes per segment, 120 bytes total max
	// This means we keep at most ~2-3 segments before old ones get deleted
	config := LogConfig{
		MaxSegmentBytes: 50,
		MaxLogBytes:     120,
	}

	log, err := NewLog(dir, config)
	if err != nil {
		t.Fatalf("failed to create log: %v", err)
	}
	defer log.Close()

	// Write 10 messages — this WILL trigger retention (old segments deleted)
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("message-%04d-padding!", i)
		_, err := log.Append([]byte(msg))
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}

	// Old offsets should be gone (deleted by retention)
	_, err = log.Read(0)
	if err == nil {
		t.Fatalf("expected offset 0 to be deleted by retention, but it's still readable")
	}
	t.Logf("offset 0 correctly deleted: %v", err)

	// Recent offsets should still be readable
	data, err := log.Read(9)
	if err != nil {
		t.Fatalf("latest offset should be readable: %v", err)
	}
	expected := "message-0009-padding!"
	if string(data) != expected {
		t.Fatalf("expected '%s', got '%s'", expected, string(data))
	}

	t.Logf("total size after retention: %d bytes", log.totalSize())
	t.Logf("segments remaining: %d", len(log.segments))
}
