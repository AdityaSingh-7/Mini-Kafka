package minikafka

import (
	"net"
	"os"
	"testing"
	"time"
)

func TestServerEndToEnd(t *testing.T) {
	dir := "test_server_e2e"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// --- SET UP THE BROKER + SERVER ---

	config := DefaultBrokerConfig()
	config.DataDir = dir

	broker, err := NewBroker(config)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer broker.Close()

	// Use port 0 = "OS, pick any free port for me" (avoids conflicts)
	server := NewServer(":0", broker)

	// Start server in the background (it blocks, so we use a goroutine)
	go server.Start()
	time.Sleep(50 * time.Millisecond) // give it a moment to start listening
	defer server.Stop()

	// --- ACT AS A CLIENT ---

	// Connect to the server (like a producer would)
	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// --- CREATE A TOPIC ---

	createReq := EncodeCreateTopicRequest(&CreateTopicRequest{
		Topic:         "orders",
		NumPartitions: 3,
	})
	if err := WriteFrame(conn, createReq); err != nil {
		t.Fatalf("failed to send create topic: %v", err)
	}

	// Read response
	respData, err := ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read create response: %v", err)
	}
	status, body, err := DecodeResponse(respData)
	if err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if status != StatusOK {
		t.Fatalf("create topic failed: %s", string(body))
	}
	t.Logf("create topic response: %s", string(body))

	// --- PRODUCE A MESSAGE ---

	produceReq := EncodeProduceRequest(&ProduceRequest{
		Topic: "orders",
		Key:   []byte("customer-42"),
		Value: []byte("order placed"),
	})
	if err := WriteFrame(conn, produceReq); err != nil {
		t.Fatalf("failed to send produce: %v", err)
	}

	// Read response
	respData, err = ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read produce response: %v", err)
	}
	status, body, err = DecodeResponse(respData)
	if err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if status != StatusOK {
		t.Fatalf("produce failed: %s", string(body))
	}

	partition, offset, err := DecodeProduceResponse(body)
	if err != nil {
		t.Fatalf("failed to decode produce response body: %v", err)
	}
	t.Logf("produced to partition %d, offset %d", partition, offset)

	// --- CONSUME THE MESSAGE BACK ---

	consumeReq := EncodeConsumeRequest(&ConsumeRequest{
		Topic:     "orders",
		Partition: partition,
		Offset:    offset,
	})
	if err := WriteFrame(conn, consumeReq); err != nil {
		t.Fatalf("failed to send consume: %v", err)
	}

	// Read response
	respData, err = ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read consume response: %v", err)
	}
	status, body, err = DecodeResponse(respData)
	if err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if status != StatusOK {
		t.Fatalf("consume failed: %s", string(body))
	}

	if string(body) != "order placed" {
		t.Fatalf("expected 'order placed', got '%s'", string(body))
	}
	t.Logf("consumed: %q ✓", string(body))
}

func TestServerMultipleClients(t *testing.T) {
	dir := "test_server_multi"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	config := DefaultBrokerConfig()
	config.DataDir = dir

	broker, _ := NewBroker(config)
	defer broker.Close()

	server := NewServer(":0", broker)
	go server.Start()
	time.Sleep(50 * time.Millisecond)
	defer server.Stop()

	// --- Client 1: Create topic ---
	conn1, _ := net.Dial("tcp", server.Addr())
	defer conn1.Close()

	WriteFrame(conn1, EncodeCreateTopicRequest(&CreateTopicRequest{
		Topic: "events", NumPartitions: 2,
	}))
	ReadFrame(conn1) // consume the response

	// --- Client 1: Produce messages ---
	for i := 0; i < 5; i++ {
		WriteFrame(conn1, EncodeProduceRequest(&ProduceRequest{
			Topic: "events",
			Key:   []byte("key"),
			Value: []byte("message from client 1"),
		}))
		ReadFrame(conn1) // consume response
	}

	// --- Client 2: Connects separately and consumes ---
	conn2, _ := net.Dial("tcp", server.Addr())
	defer conn2.Close()

	// Read from partition determined by key hash
	WriteFrame(conn2, EncodeConsumeRequest(&ConsumeRequest{
		Topic:     "events",
		Partition: hashKey([]byte("key"), 2), // same hash as producer used
		Offset:    0,
	}))

	respData, _ := ReadFrame(conn2)
	status, body, _ := DecodeResponse(respData)

	if status != StatusOK {
		t.Fatalf("consume from client 2 failed: %s", string(body))
	}
	if string(body) != "message from client 1" {
		t.Fatalf("expected 'message from client 1', got '%s'", string(body))
	}
	t.Logf("client 2 consumed message written by client 1: %q ✓", string(body))
}
