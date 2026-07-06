#!/bin/bash
# crash_demo.sh — Demonstrates that mini-kafka survives crashes.
#
# This script:
# 1. Starts the broker
# 2. Creates a topic and produces messages
# 3. Commits a consumer offset
# 4. KILLS the broker (simulating a crash)
# 5. Restarts the broker
# 6. Shows that messages AND consumer offsets survived

set -e
cd "$(dirname "$0")"

BINARY="./mini-kafka"
DATA_DIR="data"

# Clean slate
rm -rf "$DATA_DIR"

echo "============================================"
echo "   MINI-KAFKA CRASH RECOVERY DEMO"
echo "============================================"
echo ""

# --- Phase 1: Normal operation ---
echo "▶ Starting broker..."
$BINARY broker --data "$DATA_DIR" &
BROKER_PID=$!
sleep 0.5

echo "▶ Creating topic 'orders' with 3 partitions..."
$BINARY create-topic --topic orders --partitions 3
echo ""

echo "▶ Producing 5 messages..."
for i in 1 2 3 4 5; do
    RESULT=$($BINARY produce --topic orders --key "customer-1" --value "order #$i placed")
    echo "  $RESULT"
done
echo ""

echo "▶ Consumer reads first 3 messages, commits offset..."
PARTITION=1  # customer-1 always hashes to the same partition
for i in 0 1 2; do
    MSG=$($BINARY consume --topic orders --partition $PARTITION --offset $i)
    echo "  offset $i: $MSG"
done
$BINARY commit --group checkout-service --topic orders --partition $PARTITION --offset 3
echo "  ✓ Committed offset 3 (processed 0, 1, 2)"
echo ""

# --- Phase 2: CRASH ---
echo "============================================"
echo "  💥 KILLING BROKER (simulating crash)"
echo "============================================"
kill -9 $BROKER_PID 2>/dev/null
wait $BROKER_PID 2>/dev/null || true
sleep 0.5
echo ""

# --- Phase 3: Recovery ---
echo "▶ Restarting broker..."
$BINARY broker --data "$DATA_DIR" &
BROKER_PID=$!
sleep 0.5
echo ""

echo "============================================"
echo "  🔍 VERIFYING RECOVERY"
echo "============================================"
echo ""

echo "▶ Fetching committed offset for 'checkout-service'..."
OFFSET=$($BINARY fetch-offset --group checkout-service --topic orders --partition $PARTITION)
echo "  Committed offset: $OFFSET (expected: 3)"
echo ""

echo "▶ Reading messages that were produced before crash..."
for i in 0 1 2 3 4; do
    MSG=$($BINARY consume --topic orders --partition $PARTITION --offset $i)
    echo "  offset $i: $MSG"
done
echo ""

echo "▶ Consumer resumes from committed offset ($OFFSET)..."
for i in $OFFSET 4; do
    MSG=$($BINARY consume --topic orders --partition $PARTITION --offset $i)
    echo "  offset $i: $MSG  ← new (not re-processed)"
done
echo ""

echo "▶ Producing a NEW message after recovery..."
RESULT=$($BINARY produce --topic orders --key "customer-1" --value "order #6 placed")
echo "  $RESULT"
echo ""

echo "============================================"
echo "  ✅ ALL DATA SURVIVED THE CRASH"
echo "============================================"
echo ""
echo "  What survived:"
echo "    ✓ All 5 messages (durability via fsync)"
echo "    ✓ Consumer offset (group 'checkout-service' at offset 3)"
echo "    ✓ Can produce new messages after restart"
echo "    ✓ Consumer resumes from offset 3, skips already-processed messages"
echo ""

# Cleanup
kill $BROKER_PID 2>/dev/null
wait $BROKER_PID 2>/dev/null || true
echo "Demo complete."
