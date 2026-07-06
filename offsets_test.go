package minikafka

import (
	"os"
	"testing"
)

func TestOffsetCommitAndFetch(t *testing.T) {
	dir := "test_offsets"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatalf("failed to create offset store: %v", err)
	}

	// Fetch before any commit — should return 0 (start from beginning)
	offset, err := store.Fetch("checkout-service", "orders", 1)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if offset != 0 {
		t.Fatalf("expected 0 for new consumer, got %d", offset)
	}

	// Commit offset 5
	err = store.Commit("checkout-service", "orders", 1, 5)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Fetch — should now return 5
	offset, err = store.Fetch("checkout-service", "orders", 1)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if offset != 5 {
		t.Fatalf("expected 5, got %d", offset)
	}

	// Commit a higher offset
	store.Commit("checkout-service", "orders", 1, 10)

	// Fetch — should return 10
	offset, _ = store.Fetch("checkout-service", "orders", 1)
	if offset != 10 {
		t.Fatalf("expected 10, got %d", offset)
	}

	// Different group, same topic/partition — independent!
	offset, _ = store.Fetch("analytics-service", "orders", 1)
	if offset != 0 {
		t.Fatalf("different group should start at 0, got %d", offset)
	}

	t.Logf("offset store works: checkout-service at 10, analytics-service at 0 ✓")
}

func TestOffsetSurvivesRestart(t *testing.T) {
	dir := "test_offsets_restart"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Create store, commit, then "crash" (close/discard the object)
	store1, _ := NewOffsetStore(dir)
	store1.Commit("my-group", "events", 2, 42)

	// "Restart" — create a NEW store pointing at the same directory
	store2, _ := NewOffsetStore(dir)
	offset, _ := store2.Fetch("my-group", "events", 2)

	if offset != 42 {
		t.Fatalf("offset didn't survive restart: expected 42, got %d", offset)
	}

	t.Logf("offset survived restart: %d ✓", offset)
}
