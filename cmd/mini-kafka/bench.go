package main

// bench.go — Benchmarks for mini-kafka.
// Measures write throughput, read throughput, and write latency.
//
// Usage:
//   mini-kafka bench [--messages 100000] [--size 256] [--dir bench_data]

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	minikafka "github.com/AdityaSingh-7/mini-kafka"
)

func runBench() {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	numMessages := fs.Int("messages", 100000, "number of messages to write")
	messageSize := fs.Int("size", 256, "size of each message in bytes")
	dataDir := fs.String("dir", "bench_data", "directory for benchmark data")
	fs.Parse(os.Args[2:])

	// Clean slate
	os.RemoveAll(*dataDir)
	defer os.RemoveAll(*dataDir)

	fmt.Println("============================================")
	fmt.Println("   MINI-KAFKA BENCHMARK")
	fmt.Println("============================================")
	fmt.Printf("  Messages:     %d\n", *numMessages)
	fmt.Printf("  Message size: %d bytes\n", *messageSize)
	fmt.Printf("  Fsync:        every write (maximum durability)\n")
	fmt.Println()

	// Create a single-partition log (raw storage benchmark, no network)
	config := minikafka.LogConfig{
		MaxSegmentBytes: 64 * 1024 * 1024, // 64 MB segments
		MaxLogBytes:     1024 * 1024 * 1024, // 1 GB max (won't hit during bench)
	}

	log, err := minikafka.NewLog(*dataDir, config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log: %v\n", err)
		os.Exit(1)
	}
	defer log.Close()

	// Generate random payload (same for all messages)
	payload := make([]byte, *messageSize)
	rand.Read(payload)

	// === WRITE BENCHMARK ===
	fmt.Println("▶ Write benchmark (fsync every message)...")

	latencies := make([]time.Duration, *numMessages)
	startWrite := time.Now()

	for i := 0; i < *numMessages; i++ {
		msgStart := time.Now()
		_, err := log.Append(payload)
		latencies[i] = time.Since(msgStart)
		if err != nil {
			fmt.Fprintf(os.Stderr, "write failed at message %d: %v\n", i, err)
			os.Exit(1)
		}
	}

	writeTime := time.Since(startWrite)
	totalBytes := int64(*numMessages) * int64(*messageSize)
	writeMBps := float64(totalBytes) / writeTime.Seconds() / 1024 / 1024
	writeMsgPerSec := float64(*numMessages) / writeTime.Seconds()

	fmt.Printf("  Time:       %v\n", writeTime.Round(time.Millisecond))
	fmt.Printf("  Throughput: %.2f MB/s\n", writeMBps)
	fmt.Printf("  Messages:   %.0f msg/s\n", writeMsgPerSec)

	// Latency percentiles
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]
	p999 := latencies[len(latencies)*999/1000]

	fmt.Printf("  Latency p50:  %v\n", p50)
	fmt.Printf("  Latency p99:  %v\n", p99)
	fmt.Printf("  Latency p999: %v\n", p999)
	fmt.Println()

	// === READ BENCHMARK ===
	fmt.Println("▶ Read benchmark (sequential reads)...")

	startRead := time.Now()

	for i := 0; i < *numMessages; i++ {
		_, err := log.Read(uint64(i))
		if err != nil {
			fmt.Fprintf(os.Stderr, "read failed at offset %d: %v\n", i, err)
			os.Exit(1)
		}
	}

	readTime := time.Since(startRead)
	readMBps := float64(totalBytes) / readTime.Seconds() / 1024 / 1024
	readMsgPerSec := float64(*numMessages) / readTime.Seconds()

	fmt.Printf("  Time:       %v\n", readTime.Round(time.Millisecond))
	fmt.Printf("  Throughput: %.2f MB/s\n", readMBps)
	fmt.Printf("  Messages:   %.0f msg/s\n", readMsgPerSec)
	fmt.Println()

	// === BATCHED WRITE BENCHMARK ===
	fmt.Println("▶ Write benchmark (BATCHED — 1 fsync per 1000 messages)...")

	batchDir := *dataDir + "_batch"
	os.RemoveAll(batchDir)
	defer os.RemoveAll(batchDir)

	logBatch, err := minikafka.NewLog(batchDir, config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create batch log: %v\n", err)
		os.Exit(1)
	}
	defer logBatch.Close()

	batchSize := 1000
	numBatches := *numMessages / batchSize
	batch := make([][]byte, batchSize)
	for i := range batch {
		batch[i] = payload
	}

	startBatch := time.Now()

	for b := 0; b < numBatches; b++ {
		_, err := logBatch.AppendBatch(batch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "batch write failed: %v\n", err)
			os.Exit(1)
		}
	}

	batchTime := time.Since(startBatch)
	batchTotalBytes := int64(numBatches*batchSize) * int64(*messageSize)
	batchMBps := float64(batchTotalBytes) / batchTime.Seconds() / 1024 / 1024
	batchMsgPerSec := float64(numBatches*batchSize) / batchTime.Seconds()

	fmt.Printf("  Time:       %v\n", batchTime.Round(time.Millisecond))
	fmt.Printf("  Throughput: %.2f MB/s\n", batchMBps)
	fmt.Printf("  Messages:   %.0f msg/s\n", batchMsgPerSec)
	fmt.Printf("  Batch size: %d messages per fsync\n", batchSize)
	fmt.Println()

	// === SUMMARY ===
	fmt.Println("============================================")
	fmt.Println("   SUMMARY")
	fmt.Println("============================================")
	fmt.Printf("  Write (fsync/msg):   %7.2f MB/s  |  %7.0f msg/s  |  p99 = %v\n", writeMBps, writeMsgPerSec, p99)
	fmt.Printf("  Write (batched):     %7.2f MB/s  |  %7.0f msg/s\n", batchMBps, batchMsgPerSec)
	fmt.Printf("  Read (sequential):   %7.2f MB/s  |  %7.0f msg/s\n", readMBps, readMsgPerSec)
	fmt.Println()
	speedup := batchMsgPerSec / writeMsgPerSec
	fmt.Printf("  Batching speedup: %.0f× faster writes\n", speedup)
	fmt.Println()
	fmt.Println("  Tradeoff: batched writes are faster but if crash occurs mid-batch,")
	fmt.Println("  the entire unfsynced batch is lost (up to 1000 messages).")
	fmt.Println("  Per-message fsync loses at most 0 acknowledged messages.")
}
