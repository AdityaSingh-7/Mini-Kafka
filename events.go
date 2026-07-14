package minikafka

// events.go — Internal pub/sub event bus.
// When anything happens in the broker (produce, consume, rebalance, etc.),
// an event is broadcast to all subscribers (WebSocket clients).

import (
	"sync"
	"time"
)

// Event represents something that happened in the broker.
type Event struct {
	Type      string                 `json:"type"`      // "produce", "consume", "commit", "join", "leave", "rebalance"
	Timestamp int64                  `json:"timestamp"` // Unix milliseconds
	Data      map[string]interface{} `json:"data"`      // flexible payload
}

// EventBus is a simple pub/sub: events are broadcast to all subscribers.
type EventBus struct {
	subscribers []chan Event
	mu          sync.RWMutex
}

// NewEventBus creates an event bus.
func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make([]chan Event, 0),
	}
}

// Subscribe returns a channel that receives all future events.
// The channel has a buffer so slow consumers don't block the broker.
func (eb *EventBus) Subscribe() chan Event {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	ch := make(chan Event, 100) // buffer 100 events before dropping
	eb.subscribers = append(eb.subscribers, ch)
	return ch
}

// Unsubscribe removes a subscriber channel.
func (eb *EventBus) Unsubscribe(ch chan Event) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	for i, sub := range eb.subscribers {
		if sub == ch {
			eb.subscribers = append(eb.subscribers[:i], eb.subscribers[i+1:]...)
			close(ch)
			return
		}
	}
}

// Emit broadcasts an event to all subscribers.
// Non-blocking: if a subscriber's channel is full, the event is dropped for them.
func (eb *EventBus) Emit(eventType string, data map[string]interface{}) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	event := Event{
		Type:      eventType,
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}

	for _, ch := range eb.subscribers {
		select {
		case ch <- event:
			// sent
		default:
			// subscriber is slow, drop the event for them (non-blocking)
		}
	}
}
