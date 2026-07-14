package minikafka

import (
	"bytes"
	"testing"
	"time"
)

func TestRecordRoundTrip(t *testing.T) {
	// Create a record
	original := &Record{
		Timestamp: time.Now().UnixMilli(),
		Key:       []byte("customer-42"),
		Value:     []byte("order placed"),
	}

	// Encode it
	body := EncodeRecord(original)
	t.Logf("encoded record: %d bytes (key=%d, value=%d, overhead=%d)",
		len(body), len(original.Key), len(original.Value),
		len(body)-len(original.Key)-len(original.Value))

	// Decode it
	decoded, err := DecodeRecord(body)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify all fields match
	if decoded.Timestamp != original.Timestamp {
		t.Fatalf("timestamp mismatch: %d vs %d", decoded.Timestamp, original.Timestamp)
	}
	if !bytes.Equal(decoded.Key, original.Key) {
		t.Fatalf("key mismatch: %q vs %q", decoded.Key, original.Key)
	}
	if !bytes.Equal(decoded.Value, original.Value) {
		t.Fatalf("value mismatch: %q vs %q", decoded.Value, original.Value)
	}

	t.Logf("✓ round-trip: key=%q value=%q timestamp=%d", decoded.Key, decoded.Value, decoded.Timestamp)
}

func TestRecordEmptyKey(t *testing.T) {
	// Record with no key (key = nil)
	original := &Record{
		Timestamp: 1234567890000,
		Key:       nil,
		Value:     []byte("hello world"),
	}

	body := EncodeRecord(original)
	decoded, err := DecodeRecord(body)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(decoded.Key) != 0 {
		t.Fatalf("expected empty key, got %q", decoded.Key)
	}
	if !bytes.Equal(decoded.Value, original.Value) {
		t.Fatalf("value mismatch")
	}

	t.Logf("✓ empty key works: value=%q", decoded.Value)
}

func TestRecordCRCDetectsCorruption(t *testing.T) {
	// Create and encode a valid record
	original := &Record{
		Timestamp: time.Now().UnixMilli(),
		Key:       []byte("key"),
		Value:     []byte("important data that must not be corrupted"),
	}
	body := EncodeRecord(original)

	// Verify it decodes fine before corruption
	_, err := DecodeRecord(body)
	if err != nil {
		t.Fatalf("valid record should decode: %v", err)
	}

	// CORRUPT one byte in the value (flip a bit)
	corruptedBody := make([]byte, len(body))
	copy(corruptedBody, body)
	corruptedBody[len(corruptedBody)-5] ^= 0xFF // flip bits in the value

	// Try to decode — should FAIL with CRC mismatch
	_, err = DecodeRecord(corruptedBody)
	if err == nil {
		t.Fatalf("corrupted record should fail CRC check, but decoded successfully!")
	}

	t.Logf("✓ CRC caught corruption: %v", err)
}

func TestRecordCRCDetectsTimestampCorruption(t *testing.T) {
	original := &Record{
		Timestamp: time.Now().UnixMilli(),
		Key:       []byte("k"),
		Value:     []byte("v"),
	}
	body := EncodeRecord(original)

	// Corrupt the timestamp (byte 5, which is inside the timestamp field)
	corruptedBody := make([]byte, len(body))
	copy(corruptedBody, body)
	corruptedBody[5] ^= 0x01 // flip one bit in timestamp

	_, err := DecodeRecord(corruptedBody)
	if err == nil {
		t.Fatalf("timestamp corruption should be caught by CRC")
	}

	t.Logf("✓ CRC caught timestamp corruption: %v", err)
}
