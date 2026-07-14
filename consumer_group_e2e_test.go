package minikafka

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

// TestConsumerGroupEndToEnd proves the full consumer group lifecycle over TCP:
//   1. Two consumers join the same group
//   2. Partitions are split between them
//   3. One consumer dies (stops heartbeating)
//   4. Surviving consumer gets ALL partitions
//
// This is the test that makes the resume bullet defensible.
func TestConsumerGroupEndToEnd(t *testing.T) {
	dir := "test_consumer_group_e2e"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// --- Setup: broker + server with short heartbeat timeout ---
	config := DefaultBrokerConfig()
	config.DataDir = dir

	broker, err := NewBroker(config)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer broker.Close()

	// Override heartbeat timeout to 2 seconds for fast testing
	broker.coordinator = NewCoordinator(2 * time.Second)

	server := NewServer(":0", broker)
	go server.Start()
	time.Sleep(50 * time.Millisecond)
	defer server.Stop()

	addr := server.Addr()

	// --- Create a topic with 4 partitions ---
	conn := mustConnect(t, addr)
	defer conn.Close()

	sendAndExpectOK(t, conn, EncodeCreateTopicRequest(&CreateTopicRequest{
		Topic: "orders", NumPartitions: 4,
	}))

	// Produce some messages so we can verify consumers can read
	for i := 0; i < 4; i++ {
		sendAndExpectOK(t, conn, EncodeProduceRequest(&ProduceRequest{
			Topic: "orders",
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("message-%d", i)),
		}))
	}

	// --- Consumer A joins ---
	connA := mustConnect(t, addr)
	defer connA.Close()

	WriteFrame(connA, EncodeJoinGroupRequest(&JoinGroupRequest{
		Group: "processors", MemberID: "consumer-A", Topic: "orders",
	}))
	respData, _ := ReadFrame(connA)
	status, body, _ := DecodeResponse(respData)
	if status != StatusOK {
		t.Fatalf("consumer-A join failed: %s", string(body))
	}
	assignA, _ := DecodeAssignmentResponse(body)
	t.Logf("consumer-A joins alone: assigned partitions %v", assignA)

	// A should get ALL 4 partitions (it's alone)
	if len(assignA) != 4 {
		t.Fatalf("expected 4 partitions for lone consumer, got %d", len(assignA))
	}

	// --- Consumer B joins ---
	connB := mustConnect(t, addr)
	defer connB.Close()

	WriteFrame(connB, EncodeJoinGroupRequest(&JoinGroupRequest{
		Group: "processors", MemberID: "consumer-B", Topic: "orders",
	}))
	respData, _ = ReadFrame(connB)
	status, body, _ = DecodeResponse(respData)
	if status != StatusOK {
		t.Fatalf("consumer-B join failed: %s", string(body))
	}
	assignB, _ := DecodeAssignmentResponse(body)
	t.Logf("consumer-B joins: assigned partitions %v", assignB)

	// A's assignment changed — check via heartbeat
	WriteFrame(connA, EncodeHeartbeatRequest(&HeartbeatRequest{
		Group: "processors", MemberID: "consumer-A",
	}))
	respData, _ = ReadFrame(connA)
	_, body, _ = DecodeResponse(respData)
	assignA, _ = DecodeAssignmentResponse(body)
	t.Logf("consumer-A after B joins: assigned partitions %v", assignA)

	// Together they should have all 4 partitions
	totalPartitions := len(assignA) + len(assignB)
	if totalPartitions != 4 {
		t.Fatalf("expected 4 total partitions, got %d (A=%v, B=%v)", totalPartitions, assignA, assignB)
	}
	t.Logf("✓ partitions split: A=%v, B=%v (total=4)", assignA, assignB)

	// --- Consumer B dies (close connection, stop heartbeating) ---
	connB.Close()
	t.Logf("consumer-B disconnected (simulating crash)")

	// Keep A heartbeating while we wait for B to be reaped
	var finalAssignA []int
	for i := 0; i < 15; i++ {
		time.Sleep(300 * time.Millisecond)
		WriteFrame(connA, EncodeHeartbeatRequest(&HeartbeatRequest{
			Group: "processors", MemberID: "consumer-A",
		}))
		respData, err := ReadFrame(connA)
		if err != nil {
			t.Fatalf("heartbeat read failed: %v", err)
		}
		_, body, _ = DecodeResponse(respData)
		finalAssignA, _ = DecodeAssignmentResponse(body)

		if len(finalAssignA) == 4 {
			break // B was reaped, A got everything
		}
	}

	t.Logf("consumer-A after B dies: assigned partitions %v", finalAssignA)

	if len(finalAssignA) != 4 {
		t.Fatalf("after B dies, A should have all 4 partitions, got %d: %v", len(finalAssignA), finalAssignA)
	}

	t.Logf("✓ consumer-B died → consumer-A took over ALL partitions %v", finalAssignA)

	// --- Verify A can still read from the reassigned partitions ---
	WriteFrame(connA, EncodeConsumeRequest(&ConsumeRequest{
		Topic: "orders", Partition: finalAssignA[0], Offset: 0,
	}))
	respData, _ = ReadFrame(connA)
	status, body, _ = DecodeResponse(respData)
	if status != StatusOK {
		t.Fatalf("consume after reassignment failed: %s", string(body))
	}
	t.Logf("✓ consumer-A can read from reassigned partition %d: %q", finalAssignA[0], string(body))
}

// --- Helpers ---

func mustConnect(t *testing.T, addr string) net.Conn {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect to %s: %v", addr, err)
	}
	return conn
}

func sendAndExpectOK(t *testing.T, conn net.Conn, reqData []byte) {
	if err := WriteFrame(conn, reqData); err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
	respData, err := ReadFrame(conn)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}
	status, body, err := DecodeResponse(respData)
	if err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if status != StatusOK {
		t.Fatalf("expected OK, got error: %s", string(body))
	}
}
