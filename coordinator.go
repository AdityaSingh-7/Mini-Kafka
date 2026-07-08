package minikafka

// coordinator.go — Manages consumer groups.
// Tracks group membership, handles joins/leaves, assigns partitions,
// and triggers rebalancing when membership changes.

import (
	"fmt"
	"sync"
	"time"
)

// Member represents one consumer in a group.
type Member struct {
	ID            string    // unique identifier for this consumer
	LastHeartbeat time.Time // last time we heard from them
}

// ConsumerGroup tracks all members of a group and their partition assignments.
//
// Java equivalent:
//   public class ConsumerGroup {
//       Map<String, Member> members;       // who's in the group
//       String topic;                      // what topic they're reading
//       int numPartitions;                 // how many partitions the topic has
//       Map<String, List<Integer>> assignments;  // who reads what
//   }
type ConsumerGroup struct {
	Members       map[string]*Member   // memberID → member
	Topic         string               // topic this group is consuming
	NumPartitions int                  // partition count for that topic
	Assignments   map[string][]int     // memberID → assigned partition numbers
}

// Coordinator manages all consumer groups.
type Coordinator struct {
	groups          map[string]*ConsumerGroup // group name → group
	heartbeatTimeout time.Duration            // how long before we declare a member dead
	mu              sync.Mutex
}

// NewCoordinator creates a coordinator with the given heartbeat timeout.
func NewCoordinator(heartbeatTimeout time.Duration) *Coordinator {
	c := &Coordinator{
		groups:          make(map[string]*ConsumerGroup),
		heartbeatTimeout: heartbeatTimeout,
	}

	// Start background goroutine that checks for dead members
	go c.reapLoop()

	return c
}

// JoinGroup adds a consumer to a group (or creates the group).
// Returns the member's assigned partitions.
//
// This is what happens when a consumer starts up and says:
//   "I'm consumer X, I want to join group Y reading topic Z"
func (c *Coordinator) JoinGroup(groupName, memberID, topic string, numPartitions int) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get or create the group
	group, exists := c.groups[groupName]
	if !exists {
		group = &ConsumerGroup{
			Members:       make(map[string]*Member),
			Topic:         topic,
			NumPartitions: numPartitions,
			Assignments:   make(map[string][]int),
		}
		c.groups[groupName] = group
	}

	// Add the member
	group.Members[memberID] = &Member{
		ID:            memberID,
		LastHeartbeat: time.Now(),
	}

	// Rebalance — recalculate who gets which partitions
	c.rebalance(group)

	// Return this member's assignment
	return group.Assignments[memberID], nil
}

// LeaveGroup removes a consumer from a group.
// Triggers rebalance so remaining members pick up the orphaned partitions.
func (c *Coordinator) LeaveGroup(groupName, memberID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, exists := c.groups[groupName]
	if !exists {
		return fmt.Errorf("group %q does not exist", groupName)
	}

	delete(group.Members, memberID)

	// If group is now empty, remove it entirely
	if len(group.Members) == 0 {
		delete(c.groups, groupName)
		return nil
	}

	// Rebalance remaining members
	c.rebalance(group)
	return nil
}

// Heartbeat updates a member's "last seen" timestamp.
// Returns the member's current assignment (may have changed due to rebalance).
//
// The consumer calls this every few seconds to say "I'm still alive."
// The response tells them their current partition assignment
// (which might have changed if another consumer joined/left).
func (c *Coordinator) Heartbeat(groupName, memberID string) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, exists := c.groups[groupName]
	if !exists {
		return nil, fmt.Errorf("group %q does not exist", groupName)
	}

	member, exists := group.Members[memberID]
	if !exists {
		return nil, fmt.Errorf("member %q not in group %q", memberID, groupName)
	}

	// Update timestamp
	member.LastHeartbeat = time.Now()

	// Return current assignment
	return group.Assignments[memberID], nil
}

// GetAssignment returns a member's currently assigned partitions.
func (c *Coordinator) GetAssignment(groupName, memberID string) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, exists := c.groups[groupName]
	if !exists {
		return nil, fmt.Errorf("group %q does not exist", groupName)
	}

	assignment, exists := group.Assignments[memberID]
	if !exists {
		return nil, fmt.Errorf("member %q not in group %q", memberID, groupName)
	}

	return assignment, nil
}

// rebalance recalculates partition assignments using the Sticky algorithm.
// Called whenever membership changes (join or leave).
//
// Goals (in priority order):
//   1. BALANCE: every member gets within 1 partition of the same count
//   2. STICKY: minimize partition movements from previous assignment
//
// Algorithm:
//   1. Keep existing assignments for live members
//   2. Collect orphaned partitions (from dead/departed members)
//   3. Calculate target per member (total / members)
//   4. Take excess partitions from over-target members
//   5. Assign orphaned + excess to under-target members
func (c *Coordinator) rebalance(group *ConsumerGroup) {
	// Collect live member IDs in sorted order (deterministic)
	memberIDs := make([]string, 0, len(group.Members))
	for id := range group.Members {
		memberIDs = append(memberIDs, id)
	}
	sortStrings(memberIDs)

	if len(memberIDs) == 0 {
		group.Assignments = make(map[string][]int)
		return
	}

	// Step 1: Keep existing assignments for live members only
	// Remove any assignments for members that are no longer alive
	newAssignments := make(map[string][]int)
	assigned := make(map[int]bool) // track which partitions are already assigned

	for _, id := range memberIDs {
		if prev, exists := group.Assignments[id]; exists {
			// Keep this member's previous assignment
			newAssignments[id] = prev
			for _, p := range prev {
				assigned[p] = true
			}
		} else {
			// New member — starts with nothing
			newAssignments[id] = []int{}
		}
	}

	// Step 2: Collect orphaned partitions (not assigned to any live member)
	orphaned := []int{}
	for p := 0; p < group.NumPartitions; p++ {
		if !assigned[p] {
			orphaned = append(orphaned, p)
		}
	}

	// Step 3: Calculate target count per member
	// targetFloor = partitions / members (minimum each should have)
	// targetCeil  = targetFloor + 1 (some members get one extra if it doesn't divide evenly)
	// numCeil     = how many members get the extra one
	targetFloor := group.NumPartitions / len(memberIDs)
	numCeil := group.NumPartitions % len(memberIDs) // this many members get targetFloor+1

	// Which members get ceil vs floor? First numCeil (sorted) get ceil.
	targetFor := make(map[string]int)
	for i, id := range memberIDs {
		if i < numCeil {
			targetFor[id] = targetFloor + 1
		} else {
			targetFor[id] = targetFloor
		}
	}

	// Step 4: Take excess from over-target members
	for _, id := range memberIDs {
		target := targetFor[id]
		current := newAssignments[id]
		if len(current) > target {
			// Take excess partitions from the END (least recently assigned)
			excess := current[target:]
			newAssignments[id] = current[:target]
			orphaned = append(orphaned, excess...)
		}
	}

	// Step 5: Give orphaned partitions to under-target members
	// Sort orphaned for determinism
	sortInts(orphaned)

	orphanIdx := 0
	for _, id := range memberIDs {
		target := targetFor[id]
		for len(newAssignments[id]) < target && orphanIdx < len(orphaned) {
			newAssignments[id] = append(newAssignments[id], orphaned[orphanIdx])
			orphanIdx++
		}
	}

	group.Assignments = newAssignments
}

// sortInts sorts a slice of ints (insertion sort — fine for small lists).
func sortInts(s []int) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// reapLoop runs in the background, checking every second for members
// who haven't heartbeated within the timeout.
func (c *Coordinator) reapLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.reapDeadMembers()
	}
}

// reapDeadMembers removes members who haven't heartbeated recently
// and triggers rebalance for affected groups.
func (c *Coordinator) reapDeadMembers() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()

	for groupName, group := range c.groups {
		var dead []string

		// Find dead members
		for id, member := range group.Members {
			if now.Sub(member.LastHeartbeat) > c.heartbeatTimeout {
				dead = append(dead, id)
			}
		}

		// Remove dead members
		for _, id := range dead {
			delete(group.Members, id)
		}

		// Rebalance if anyone was removed
		if len(dead) > 0 {
			if len(group.Members) == 0 {
				// Group is empty — remove it
				delete(c.groups, groupName)
			} else {
				c.rebalance(group)
			}
		}
	}
}

// sortStrings sorts a slice of strings (simple insertion sort — fine for small lists).
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
