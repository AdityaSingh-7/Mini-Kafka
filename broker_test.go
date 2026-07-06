package minikafka

import (
	"fmt"
	"os"
	"testing"
)

func TestBrokerBasic(t *testing.T) {
	dir := "test_broker_basic"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	config := DefaultBrokerConfig()
	config.DataDir = dir

	// Create broker
	broker, err := NewBroker(config)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer broker.Close()

	// Create a topic with 3 partitions
	err = broker.CreateTopic("orders", 3)
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Publish a message with a key
	partition, offset, err := broker.Publish("orders", []byte("customer-42"), []byte("order placed"))
	if err != nil {
		t.Fatalf("publish failed: %v", err)
	}
	t.Logf("published to partition %d, offset %d", partition, offset)

	// Consume it back
	data, err := broker.Consume("orders", partition, offset)
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}
	if string(data) != "order placed" {
		t.Fatalf("expected 'order placed', got '%s'", string(data))
	}
}

func TestBrokerKeyPartitioning(t *testing.T) {
	dir := "test_broker_partitioning"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	config := DefaultBrokerConfig()
	config.DataDir = dir

	broker, err := NewBroker(config)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer broker.Close()

	broker.CreateTopic("orders", 3)

	// Same key should ALWAYS go to the same partition
	p1, _, _ := broker.Publish("orders", []byte("customer-42"), []byte("msg1"))
	p2, _, _ := broker.Publish("orders", []byte("customer-42"), []byte("msg2"))
	p3, _, _ := broker.Publish("orders", []byte("customer-42"), []byte("msg3"))

	if p1 != p2 || p2 != p3 {
		t.Fatalf("same key went to different partitions: %d, %d, %d", p1, p2, p3)
	}
	t.Logf("customer-42 always goes to partition %d ✓", p1)

	// Different keys MIGHT go to different partitions
	partitionsSeen := make(map[int]bool)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("customer-%d", i)
		p, _, _ := broker.Publish("orders", []byte(key), []byte("msg"))
		partitionsSeen[p] = true
	}

	if len(partitionsSeen) < 2 {
		t.Fatalf("expected messages spread across partitions, but all went to one")
	}
	t.Logf("20 different keys spread across %d partitions ✓", len(partitionsSeen))
}

func TestBrokerTopicNotFound(t *testing.T) {
	dir := "test_broker_notfound"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	config := DefaultBrokerConfig()
	config.DataDir = dir

	broker, err := NewBroker(config)
	if err != nil {
		t.Fatalf("failed to create broker: %v", err)
	}
	defer broker.Close()

	// Publish to non-existent topic should fail
	_, _, err = broker.Publish("ghost-topic", []byte("key"), []byte("msg"))
	if err == nil {
		t.Fatalf("expected error for non-existent topic, got nil")
	}
	t.Logf("correctly rejected: %v", err)
}
