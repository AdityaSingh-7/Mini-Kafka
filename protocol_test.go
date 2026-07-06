package minikafka

import (
	"bytes"
	"testing"
)

func TestProtocolProduceRoundTrip(t *testing.T) {
	// Create a produce request
	original := &ProduceRequest{
		Topic: "orders",
		Key:   []byte("customer-42"),
		Value: []byte("order placed"),
	}

	// Encode it to bytes
	encoded := EncodeProduceRequest(original)
	t.Logf("encoded produce request: %d bytes", len(encoded))

	// Decode it back
	decoded, err := DecodeProduceRequest(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify all fields match
	if decoded.Topic != original.Topic {
		t.Fatalf("topic: expected %q, got %q", original.Topic, decoded.Topic)
	}
	if !bytes.Equal(decoded.Key, original.Key) {
		t.Fatalf("key: expected %q, got %q", original.Key, decoded.Key)
	}
	if !bytes.Equal(decoded.Value, original.Value) {
		t.Fatalf("value: expected %q, got %q", original.Value, decoded.Value)
	}
}

func TestProtocolConsumeRoundTrip(t *testing.T) {
	original := &ConsumeRequest{
		Topic:     "orders",
		Partition: 2,
		Offset:    12345,
	}

	encoded := EncodeConsumeRequest(original)
	t.Logf("encoded consume request: %d bytes", len(encoded))

	decoded, err := DecodeConsumeRequest(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.Topic != original.Topic {
		t.Fatalf("topic mismatch")
	}
	if decoded.Partition != original.Partition {
		t.Fatalf("partition: expected %d, got %d", original.Partition, decoded.Partition)
	}
	if decoded.Offset != original.Offset {
		t.Fatalf("offset: expected %d, got %d", original.Offset, decoded.Offset)
	}
}

func TestProtocolFrameRoundTrip(t *testing.T) {
	// Simulate sending a frame over a connection (using a buffer as fake network)
	original := []byte("hello this is a test message with some data")

	// Write a frame to a buffer (simulates sending over TCP)
	var buf bytes.Buffer
	err := WriteFrame(&buf, original)
	if err != nil {
		t.Fatalf("write frame failed: %v", err)
	}
	t.Logf("frame on wire: %d bytes (4 header + %d payload)", buf.Len(), len(original))

	// Read the frame back (simulates receiving from TCP)
	received, err := ReadFrame(&buf)
	if err != nil {
		t.Fatalf("read frame failed: %v", err)
	}

	if !bytes.Equal(received, original) {
		t.Fatalf("frame mismatch: expected %q, got %q", original, received)
	}
}
