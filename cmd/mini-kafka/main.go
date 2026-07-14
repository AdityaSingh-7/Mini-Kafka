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
	"time"

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
	case "consumer":
		runConsumer() // long-running poll-based consumer
	case "commit":
		runCommit()
	case "fetch-offset":
		runFetchOffset()
	case "bench":
		runBench()
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
  consume                      Read a single message (one-shot)
  consumer                     Long-running poll-based consumer (joins group, heartbeats, reads)
  commit                       Save consumer group's position
  fetch-offset                 Get consumer group's saved position`)
}

// === BROKER ===

func runBroker() {
	// Parse flags for the broker subcommand
	fs := flag.NewFlagSet("broker", flag.ExitOnError)
	port := fs.String("port", "9092", "port to listen on")
	dashPort := fs.String("dash", "8080", "dashboard HTTP port")
	dataDir := fs.String("data", "data", "directory for log data")
	fs.Parse(os.Args[2:])

	// Create the broker
	config := minikafka.DefaultBrokerConfig()
	config.DataDir = *dataDir

	broker, err := minikafka.NewBroker(config)
	if err != nil {
		log.Fatalf("failed to create broker: %v", err)
	}

	// Create the TCP server (for producers/consumers)
	server := minikafka.NewServer(":"+*port, broker)

	// Start the dashboard (WebSocket + HTTP) in background
	dashboard := minikafka.NewDashboardServer(broker)
	go dashboard.Start(":" + *dashPort)

	// Handle Ctrl+C gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nshutting down...")
		server.Stop()
		broker.Close()
	}()

	// Start TCP server (blocks until stopped)
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

// === CONSUMER (long-running poll-based) ===
//
// This is how a REAL Kafka consumer works:
//   1. Join a group → get assigned partitions
//   2. Fetch committed offsets for each partition (resume where we left off)
//   3. Loop forever:
//      a. Heartbeat → check if assignment changed
//      b. For each assigned partition, read next message
//      c. Process it (for us: print it)
//      d. Commit offsets periodically
//      e. Sleep, repeat
//   4. On Ctrl+C → send LeaveGroup → broker rebalances immediately

func runConsumer() {
	fs := flag.NewFlagSet("consumer", flag.ExitOnError)
	brokerAddr := fs.String("broker", "localhost:9092", "broker address")
	group := fs.String("group", "", "consumer group name (required)")
	topic := fs.String("topic", "", "topic name (required)")
	memberID := fs.String("id", "", "unique consumer ID (auto-generated if empty)")
	fs.Parse(os.Args[2:])

	if *group == "" || *topic == "" {
		fmt.Fprintln(os.Stderr, "error: --group and --topic are required")
		os.Exit(1)
	}

	// Auto-generate member ID if not provided
	if *memberID == "" {
		*memberID = fmt.Sprintf("consumer-%d", time.Now().UnixNano()%100000)
	}

	// Connect to broker (one persistent connection)
	conn, err := net.Dial("tcp", *brokerAddr)
	if err != nil {
		log.Fatalf("failed to connect to broker at %s: %v", *brokerAddr, err)
	}
	defer conn.Close()

	// Step 1: Join the group
	fmt.Printf("[%s] joining group %q for topic %q...\n", *memberID, *group, *topic)
	minikafka.WriteFrame(conn, minikafka.EncodeJoinGroupRequest(&minikafka.JoinGroupRequest{
		Group: *group, MemberID: *memberID, Topic: *topic,
	}))
	respData, err := minikafka.ReadFrame(conn)
	if err != nil {
		log.Fatalf("join failed: %v", err)
	}
	status, body, _ := minikafka.DecodeResponse(respData)
	if status != minikafka.StatusOK {
		log.Fatalf("join rejected: %s", string(body))
	}
	assignment, _ := minikafka.DecodeAssignmentResponse(body)
	fmt.Printf("[%s] assigned partitions: %v\n", *memberID, assignment)

	// Step 2: Fetch committed offsets for each assigned partition
	offsets := make(map[int]uint64) // partition → next offset to read
	for _, p := range assignment {
		minikafka.WriteFrame(conn, minikafka.EncodeFetchOffsetRequest(&minikafka.FetchOffsetRequest{
			Group: *group, Topic: *topic, Partition: p,
		}))
		respData, _ := minikafka.ReadFrame(conn)
		_, body, _ := minikafka.DecodeResponse(respData)
		off, _ := minikafka.DecodeFetchOffsetResponse(body)
		offsets[p] = off
	}
	fmt.Printf("[%s] starting offsets: %v\n", *memberID, offsets)

	// Handle Ctrl+C: send LeaveGroup for clean shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Printf("\n[%s] shutting down, leaving group...\n", *memberID)
		minikafka.WriteFrame(conn, minikafka.EncodeLeaveGroupRequest(&minikafka.LeaveGroupRequest{
			Group: *group, MemberID: *memberID,
		}))
		minikafka.ReadFrame(conn) // consume response
		conn.Close()
		os.Exit(0)
	}()

	// Step 3: Poll loop — runs forever
	commitInterval := 10 // commit every 10 iterations
	iteration := 0

	for {
		// 3a. Heartbeat — tells broker "I'm alive" and gets current assignment
		minikafka.WriteFrame(conn, minikafka.EncodeHeartbeatRequest(&minikafka.HeartbeatRequest{
			Group: *group, MemberID: *memberID,
		}))
		respData, err := minikafka.ReadFrame(conn)
		if err != nil {
			log.Fatalf("heartbeat failed (broker down?): %v", err)
		}
		status, body, _ := minikafka.DecodeResponse(respData)
		if status != minikafka.StatusOK {
			log.Fatalf("heartbeat rejected: %s", string(body))
		}
		newAssignment, _ := minikafka.DecodeAssignmentResponse(body)

		// Check if assignment changed (rebalance happened)
		if !sameAssignment(assignment, newAssignment) {
			fmt.Printf("[%s] REBALANCE! new assignment: %v (was %v)\n", *memberID, newAssignment, assignment)
			assignment = newAssignment
			// Fetch offsets for any NEW partitions
			for _, p := range assignment {
				if _, exists := offsets[p]; !exists {
					minikafka.WriteFrame(conn, minikafka.EncodeFetchOffsetRequest(&minikafka.FetchOffsetRequest{
						Group: *group, Topic: *topic, Partition: p,
					}))
					respData, _ := minikafka.ReadFrame(conn)
					_, body, _ := minikafka.DecodeResponse(respData)
					off, _ := minikafka.DecodeFetchOffsetResponse(body)
					offsets[p] = off
					fmt.Printf("[%s] new partition %d, starting at offset %d\n", *memberID, p, off)
				}
			}
		}

		// 3b. Read next message from each assigned partition
		for _, p := range assignment {
			offset := offsets[p]
			minikafka.WriteFrame(conn, minikafka.EncodeConsumeRequest(&minikafka.ConsumeRequest{
				Topic: *topic, Partition: p, Offset: offset,
			}))
			respData, _ := minikafka.ReadFrame(conn)
			status, body, _ := minikafka.DecodeResponse(respData)

			if status == minikafka.StatusOK {
				// 3c. "Process" the message (print it)
				fmt.Printf("[%s] partition=%d offset=%d: %s\n", *memberID, p, offset, string(body))
				offsets[p] = offset + 1 // advance to next
			}
			// If status != OK, partition is caught up (no new messages). That's fine.
		}

		// 3d. Commit offsets periodically
		iteration++
		if iteration%commitInterval == 0 {
			for _, p := range assignment {
				minikafka.WriteFrame(conn, minikafka.EncodeCommitRequest(&minikafka.CommitRequest{
					Group: *group, Topic: *topic, Partition: p, Offset: offsets[p],
				}))
				minikafka.ReadFrame(conn) // consume ack
			}
		}

		// 3e. Sleep before next poll
		time.Sleep(500 * time.Millisecond)
	}
}

// sameAssignment checks if two partition assignments are equal.
func sameAssignment(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	setA := make(map[int]bool)
	for _, v := range a {
		setA[v] = true
	}
	for _, v := range b {
		if !setA[v] {
			return false
		}
	}
	return true
}
