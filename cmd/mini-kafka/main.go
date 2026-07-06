package main

// main.go — The command-line interface for mini-kafka.
// One binary, three subcommands: broker, produce, consume, create-topic.
//
// Usage:
//   mini-kafka broker                              (start the server)
//   mini-kafka create-topic --topic orders --partitions 3
//   mini-kafka produce --topic orders --key c42 --value "hello"
//   mini-kafka consume --topic orders --partition 0 --offset 0

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	minikafka "github.com/AdityaSingh-7/mini-kafka"
)

func main() {
	// The first argument after the program name is the subcommand
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Route to the right subcommand
	switch os.Args[1] {
	case "broker":
		runBroker()
	case "create-topic":
		runCreateTopic()
	case "produce":
		runProduce()
	case "consume":
		runConsume()
	case "commit":
		runCommit()
	case "fetch-offset":
		runFetchOffset()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage: mini-kafka <command>

Commands:
  broker                       Start the broker server
  create-topic                 Create a new topic
  produce                      Publish a message
  consume                      Read a message
  commit                       Save consumer group's position
  fetch-offset                 Get consumer group's saved position`)
}

// === BROKER ===

func runBroker() {
	// Parse flags for the broker subcommand
	fs := flag.NewFlagSet("broker", flag.ExitOnError)
	port := fs.String("port", "9092", "port to listen on")
	dataDir := fs.String("data", "data", "directory for log data")
	fs.Parse(os.Args[2:])

	// Create the broker
	config := minikafka.DefaultBrokerConfig()
	config.DataDir = *dataDir

	broker, err := minikafka.NewBroker(config)
	if err != nil {
		log.Fatalf("failed to create broker: %v", err)
	}

	// Create the server
	server := minikafka.NewServer(":"+*port, broker)

	// Handle Ctrl+C gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nshutting down...")
		server.Stop()
		broker.Close()
	}()

	// Start (blocks until stopped)
	if err := server.Start(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

// === CREATE TOPIC ===

func runCreateTopic() {
	fs := flag.NewFlagSet("create-topic", flag.ExitOnError)
	broker := fs.String("broker", "localhost:9092", "broker address")
	topic := fs.String("topic", "", "topic name (required)")
	partitions := fs.Int("partitions", 3, "number of partitions")
	fs.Parse(os.Args[2:])

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --topic is required")
		os.Exit(1)
	}

	// Connect to broker
	conn, err := net.Dial("tcp", *broker)
	if err != nil {
		log.Fatalf("failed to connect to broker at %s: %v", *broker, err)
	}
	defer conn.Close()

	// Send create-topic request
	req := minikafka.EncodeCreateTopicRequest(&minikafka.CreateTopicRequest{
		Topic:         *topic,
		NumPartitions: *partitions,
	})
	if err := minikafka.WriteFrame(conn, req); err != nil {
		log.Fatalf("failed to send request: %v", err)
	}

	// Read response
	respData, err := minikafka.ReadFrame(conn)
	if err != nil {
		log.Fatalf("failed to read response: %v", err)
	}

	status, body, err := minikafka.DecodeResponse(respData)
	if err != nil {
		log.Fatalf("failed to decode response: %v", err)
	}

	if status != minikafka.StatusOK {
		log.Fatalf("error: %s", string(body))
	}

	fmt.Printf("topic %q created with %d partitions\n", *topic, *partitions)
}

// === PRODUCE ===

func runProduce() {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	brokerAddr := fs.String("broker", "localhost:9092", "broker address")
	topic := fs.String("topic", "", "topic name (required)")
	key := fs.String("key", "", "message key (for partition routing)")
	value := fs.String("value", "", "message value (required)")
	fs.Parse(os.Args[2:])

	if *topic == "" || *value == "" {
		fmt.Fprintln(os.Stderr, "error: --topic and --value are required")
		os.Exit(1)
	}

	// Connect to broker
	conn, err := net.Dial("tcp", *brokerAddr)
	if err != nil {
		log.Fatalf("failed to connect to broker at %s: %v", *brokerAddr, err)
	}
	defer conn.Close()

	// Send produce request
	req := minikafka.EncodeProduceRequest(&minikafka.ProduceRequest{
		Topic: *topic,
		Key:   []byte(*key),
		Value: []byte(*value),
	})
	if err := minikafka.WriteFrame(conn, req); err != nil {
		log.Fatalf("failed to send request: %v", err)
	}

	// Read response
	respData, err := minikafka.ReadFrame(conn)
	if err != nil {
		log.Fatalf("failed to read response: %v", err)
	}

	status, body, err := minikafka.DecodeResponse(respData)
	if err != nil {
		log.Fatalf("failed to decode response: %v", err)
	}

	if status != minikafka.StatusOK {
		log.Fatalf("error: %s", string(body))
	}

	partition, offset, err := minikafka.DecodeProduceResponse(body)
	if err != nil {
		log.Fatalf("failed to decode produce response: %v", err)
	}

	fmt.Printf("published to partition %d, offset %d\n", partition, offset)
}

// === CONSUME ===

func runConsume() {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	brokerAddr := fs.String("broker", "localhost:9092", "broker address")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", 0, "partition number")
	offset := fs.Uint64("offset", 0, "offset to read from")
	fs.Parse(os.Args[2:])

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --topic is required")
		os.Exit(1)
	}

	// Connect to broker
	conn, err := net.Dial("tcp", *brokerAddr)
	if err != nil {
		log.Fatalf("failed to connect to broker at %s: %v", *brokerAddr, err)
	}
	defer conn.Close()

	// Send consume request
	req := minikafka.EncodeConsumeRequest(&minikafka.ConsumeRequest{
		Topic:     *topic,
		Partition: *partition,
		Offset:    *offset,
	})
	if err := minikafka.WriteFrame(conn, req); err != nil {
		log.Fatalf("failed to send request: %v", err)
	}

	// Read response
	respData, err := minikafka.ReadFrame(conn)
	if err != nil {
		log.Fatalf("failed to read response: %v", err)
	}

	status, body, err := minikafka.DecodeResponse(respData)
	if err != nil {
		log.Fatalf("failed to decode response: %v", err)
	}

	if status != minikafka.StatusOK {
		log.Fatalf("error: %s", string(body))
	}

	fmt.Printf("%s\n", string(body))
}

// === COMMIT OFFSET ===

func runCommit() {
	fs := flag.NewFlagSet("commit", flag.ExitOnError)
	brokerAddr := fs.String("broker", "localhost:9092", "broker address")
	group := fs.String("group", "", "consumer group name (required)")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", 0, "partition number")
	offset := fs.Uint64("offset", 0, "offset to commit (required)")
	fs.Parse(os.Args[2:])

	if *group == "" || *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --group and --topic are required")
		os.Exit(1)
	}

	conn, err := net.Dial("tcp", *brokerAddr)
	if err != nil {
		log.Fatalf("failed to connect to broker at %s: %v", *brokerAddr, err)
	}
	defer conn.Close()

	req := minikafka.EncodeCommitRequest(&minikafka.CommitRequest{
		Group:     *group,
		Topic:     *topic,
		Partition: *partition,
		Offset:    *offset,
	})
	if err := minikafka.WriteFrame(conn, req); err != nil {
		log.Fatalf("failed to send request: %v", err)
	}

	respData, err := minikafka.ReadFrame(conn)
	if err != nil {
		log.Fatalf("failed to read response: %v", err)
	}

	status, body, err := minikafka.DecodeResponse(respData)
	if err != nil {
		log.Fatalf("failed to decode response: %v", err)
	}

	if status != minikafka.StatusOK {
		log.Fatalf("error: %s", string(body))
	}

	fmt.Printf("committed offset %d for group %q (topic=%s, partition=%d)\n",
		*offset, *group, *topic, *partition)
}

// === FETCH OFFSET ===

func runFetchOffset() {
	fs := flag.NewFlagSet("fetch-offset", flag.ExitOnError)
	brokerAddr := fs.String("broker", "localhost:9092", "broker address")
	group := fs.String("group", "", "consumer group name (required)")
	topic := fs.String("topic", "", "topic name (required)")
	partition := fs.Int("partition", 0, "partition number")
	fs.Parse(os.Args[2:])

	if *group == "" || *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --group and --topic are required")
		os.Exit(1)
	}

	conn, err := net.Dial("tcp", *brokerAddr)
	if err != nil {
		log.Fatalf("failed to connect to broker at %s: %v", *brokerAddr, err)
	}
	defer conn.Close()

	req := minikafka.EncodeFetchOffsetRequest(&minikafka.FetchOffsetRequest{
		Group:     *group,
		Topic:     *topic,
		Partition: *partition,
	})
	if err := minikafka.WriteFrame(conn, req); err != nil {
		log.Fatalf("failed to send request: %v", err)
	}

	respData, err := minikafka.ReadFrame(conn)
	if err != nil {
		log.Fatalf("failed to read response: %v", err)
	}

	status, body, err := minikafka.DecodeResponse(respData)
	if err != nil {
		log.Fatalf("failed to decode response: %v", err)
	}

	if status != minikafka.StatusOK {
		log.Fatalf("error: %s", string(body))
	}

	offset, err := minikafka.DecodeFetchOffsetResponse(body)
	if err != nil {
		log.Fatalf("failed to decode offset: %v", err)
	}

	fmt.Printf("%d\n", offset)
}
