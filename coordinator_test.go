package minikafka

import (
	"testing"
	"time"
)

func TestCoordinatorJoinAndAssign(t *testing.T) {
	coord := NewCoordinator(10 * time.Second)

	// First consumer joins — gets ALL partitions
	assignment, err := coord.JoinGroup("processors", "consumer-A", "orders", 4)
	if err != nil {
		t.Fatalf("join failed: %v", err)
	}
	if len(assignment) != 4 {
		t.Fatalf("expected 4 partitions for lone consumer, got %d: %v", len(assignment), assignment)
	}
	t.Logf("consumer-A alone: %v", assignment)

	// Second consumer joins — partitions are split
	assignmentB, err := coord.JoinGroup("processors", "consumer-B", "orders", 4)
	if err != nil {
		t.Fatalf("join failed: %v", err)
	}

	// Get A's updated assignment
	assignmentA, _ := coord.GetAssignment("processors", "consumer-A")

	t.Logf("after B joins: A=%v, B=%v", assignmentA, assignmentB)

	// Together they should cover all 4 partitions
	total := len(assignmentA) + len(assignmentB)
	if total != 4 {
		t.Fatalf("expected 4 total partitions, got %d", total)
	}

	// Each should have 2 (4 partitions / 2 consumers)
	if len(assignmentA) != 2 || len(assignmentB) != 2 {
		t.Fatalf("expected even split (2,2), got (%d,%d)", len(assignmentA), len(assignmentB))
	}
}

func TestCoordinatorLeaveAndRebalance(t *testing.T) {
	coord := NewCoordinator(10 * time.Second)

	// Two consumers join
	coord.JoinGroup("processors", "consumer-A", "orders", 4)
	coord.JoinGroup("processors", "consumer-B", "orders", 4)

	// B leaves
	err := coord.LeaveGroup("processors", "consumer-B")
	if err != nil {
		t.Fatalf("leave failed: %v", err)
	}

	// A should now have ALL partitions
	assignmentA, _ := coord.GetAssignment("processors", "consumer-A")
	if len(assignmentA) != 4 {
		t.Fatalf("after B leaves, A should have all 4 partitions, got %d: %v", len(assignmentA), assignmentA)
	}
	t.Logf("after B leaves: A=%v", assignmentA)
}

func TestCoordinatorHeartbeatTimeout(t *testing.T) {
	// Very short timeout for testing
	coord := NewCoordinator(500 * time.Millisecond)

	// Two consumers join
	coord.JoinGroup("processors", "consumer-A", "orders", 4)
	coord.JoinGroup("processors", "consumer-B", "orders", 4)

	// Only A keeps heartbeating
	go func() {
		for i := 0; i < 15; i++ {
			time.Sleep(200 * time.Millisecond)
			coord.Heartbeat("processors", "consumer-A")
		}
	}()

	// B stops heartbeating — wait for timeout + reap
	time.Sleep(2 * time.Second)

	// A should now have all partitions (B was reaped)
	assignmentA, err := coord.GetAssignment("processors", "consumer-A")
	if err != nil {
		t.Fatalf("get assignment failed: %v", err)
	}
	if len(assignmentA) != 4 {
		t.Fatalf("after B timeout, A should have all 4 partitions, got %d: %v", len(assignmentA), assignmentA)
	}
	t.Logf("after B times out: A=%v ✓", assignmentA)

	// B should no longer exist
	_, err = coord.GetAssignment("processors", "consumer-B")
	if err == nil {
		t.Fatalf("B should be removed, but is still in group")
	}
	t.Logf("B correctly removed from group ✓")
}

func TestCoordinatorThreeConsumers(t *testing.T) {
	coord := NewCoordinator(10 * time.Second)

	// 5 partitions, 3 consumers — uneven split
	coord.JoinGroup("workers", "A", "events", 5)
	coord.JoinGroup("workers", "B", "events", 5)
	coord.JoinGroup("workers", "C", "events", 5)

	a, _ := coord.GetAssignment("workers", "A")
	b, _ := coord.GetAssignment("workers", "B")
	c, _ := coord.GetAssignment("workers", "C")

	t.Logf("5 partitions / 3 consumers: A=%v, B=%v, C=%v", a, b, c)

	// Total should be 5
	total := len(a) + len(b) + len(c)
	if total != 5 {
		t.Fatalf("expected 5 total, got %d", total)
	}

	// No one should have 0 partitions
	if len(a) == 0 || len(b) == 0 || len(c) == 0 {
		t.Fatalf("every consumer should get at least 1 partition")
	}
}

func TestStickyMinimalMoves(t *testing.T) {
	coord := NewCoordinator(10 * time.Second)

	// Start with 2 consumers, 6 partitions
	coord.JoinGroup("sticky-test", "A", "events", 6)
	coord.JoinGroup("sticky-test", "B", "events", 6)

	aBefore, _ := coord.GetAssignment("sticky-test", "A")
	bBefore, _ := coord.GetAssignment("sticky-test", "B")
	t.Logf("before: A=%v, B=%v", aBefore, bBefore)

	// Now C joins — should only move partitions TO C, not shuffle A and B
	coord.JoinGroup("sticky-test", "C", "events", 6)

	aAfter, _ := coord.GetAssignment("sticky-test", "A")
	bAfter, _ := coord.GetAssignment("sticky-test", "B")
	cAfter, _ := coord.GetAssignment("sticky-test", "C")
	t.Logf("after C joins: A=%v, B=%v, C=%v", aAfter, bAfter, cAfter)

	// C should have gotten partitions, not be empty
	if len(cAfter) == 0 {
		t.Fatalf("C should have partitions after joining")
	}

	// Count how many of A's original partitions it KEPT
	aKept := countOverlap(aBefore, aAfter)
	bKept := countOverlap(bBefore, bAfter)
	t.Logf("A kept %d/%d, B kept %d/%d (sticky = minimal moves)",
		aKept, len(aBefore), bKept, len(bBefore))

	// Each had 3 before. Target is now 2. So each should keep exactly 2.
	if aKept < 2 {
		t.Fatalf("sticky failed: A should keep at least 2 partitions, kept %d", aKept)
	}
	if bKept < 2 {
		t.Fatalf("sticky failed: B should keep at least 2 partitions, kept %d", bKept)
	}
}

func TestStickyMemberDies(t *testing.T) {
	coord := NewCoordinator(10 * time.Second)

	// 3 consumers, 6 partitions
	coord.JoinGroup("die-test", "A", "orders", 6)
	coord.JoinGroup("die-test", "B", "orders", 6)
	coord.JoinGroup("die-test", "C", "orders", 6)

	aBefore, _ := coord.GetAssignment("die-test", "A")
	cBefore, _ := coord.GetAssignment("die-test", "C")
	t.Logf("before: A=%v, C=%v", aBefore, cBefore)

	// B dies
	coord.LeaveGroup("die-test", "B")

	aAfter, _ := coord.GetAssignment("die-test", "A")
	cAfter, _ := coord.GetAssignment("die-test", "C")
	t.Logf("after B dies: A=%v, C=%v", aAfter, cAfter)

	// A and C should keep ALL their original partitions
	aKept := countOverlap(aBefore, aAfter)
	cKept := countOverlap(cBefore, cAfter)
	t.Logf("A kept %d/%d, C kept %d/%d", aKept, len(aBefore), cKept, len(cBefore))

	if aKept != len(aBefore) {
		t.Fatalf("sticky failed: A should keep ALL original partitions on B's death")
	}
	if cKept != len(cBefore) {
		t.Fatalf("sticky failed: C should keep ALL original partitions on B's death")
	}

	// Total should still be 6
	if len(aAfter)+len(cAfter) != 6 {
		t.Fatalf("expected 6 total, got %d", len(aAfter)+len(cAfter))
	}
}

// countOverlap returns how many elements from 'before' are still in 'after'.
func countOverlap(before, after []int) int {
	afterSet := make(map[int]bool)
	for _, v := range after {
		afterSet[v] = true
	}
	count := 0
	for _, v := range before {
		if afterSet[v] {
			count++
		}
	}
	return count
}
