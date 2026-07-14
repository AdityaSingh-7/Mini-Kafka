package minikafka

// ws.go — WebSocket server for the real-time dashboard.
// Serves the React frontend and streams broker events over WebSocket.

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/gorilla/websocket"
)

// DashboardServer serves the frontend and WebSocket events.
type DashboardServer struct {
	broker   *Broker
	upgrader websocket.Upgrader
	clients  map[*websocket.Conn]bool
	mu       sync.Mutex
}

// NewDashboardServer creates a dashboard server for the given broker.
func NewDashboardServer(broker *Broker) *DashboardServer {
	return &DashboardServer{
		broker: broker,
		upgrader: websocket.Upgrader{
			// Allow connections from any origin (for development)
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		clients: make(map[*websocket.Conn]bool),
	}
}

// Start begins serving the dashboard on the given address (e.g. ":8080").
// This blocks forever — run it in a goroutine.
func (ds *DashboardServer) Start(addr string) error {
	mux := http.NewServeMux()

	// WebSocket endpoint
	mux.HandleFunc("/ws", ds.handleWS)

	// API endpoints (for the "Produce" button in the UI)
	mux.HandleFunc("/api/produce", ds.handleAPIProduce)
	mux.HandleFunc("/api/create-topic", ds.handleAPICreateTopic)
	mux.HandleFunc("/api/state", ds.handleAPIState)
	mux.HandleFunc("/api/reset", ds.handleAPIReset)

	// Serve static frontend files
	mux.Handle("/", http.FileServer(http.Dir("frontend/dist")))

	log.Printf("dashboard listening on http://localhost%s", addr)

	// Start broadcasting events to WebSocket clients
	go ds.broadcastLoop()

	return http.ListenAndServe(addr, mux)
}

// handleWS upgrades HTTP to WebSocket and registers the client.
func (ds *DashboardServer) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := ds.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed: %v", err)
		return
	}

	// Register client
	ds.mu.Lock()
	ds.clients[conn] = true
	ds.mu.Unlock()

	// Send initial state snapshot
	snapshot := ds.buildSnapshot()
	conn.WriteJSON(snapshot)

	// Keep connection alive — read messages (we don't expect any, but need to detect disconnect)
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			ds.mu.Lock()
			delete(ds.clients, conn)
			ds.mu.Unlock()
			conn.Close()
			return
		}
	}
}

// broadcastLoop subscribes to the broker's event bus and forwards events to all WebSocket clients.
func (ds *DashboardServer) broadcastLoop() {
	ch := ds.broker.Events.Subscribe()

	for event := range ch {
		ds.mu.Lock()
		for conn := range ds.clients {
			err := conn.WriteJSON(event)
			if err != nil {
				conn.Close()
				delete(ds.clients, conn)
			}
		}
		ds.mu.Unlock()
	}
}

// buildSnapshot returns the current state of the broker for new WebSocket clients.
func (ds *DashboardServer) buildSnapshot() map[string]interface{} {
	ds.broker.mu.RLock()
	defer ds.broker.mu.RUnlock()

	topics := make(map[string]interface{})
	for name, partitions := range ds.broker.topics {
		partList := make([]map[string]interface{}, len(partitions))
		for i, p := range partitions {
			partList[i] = map[string]interface{}{
				"id":     i,
				"offset": p.Offset(),
			}
		}
		topics[name] = map[string]interface{}{
			"partitions": partList,
		}
	}

	// Get consumer group info from coordinator
	groups := make(map[string]interface{})
	ds.broker.coordinator.mu.Lock()
	for groupName, group := range ds.broker.coordinator.groups {
		members := make([]map[string]interface{}, 0)
		for id := range group.Members {
			members = append(members, map[string]interface{}{
				"id":         id,
				"assignment": group.Assignments[id],
			})
		}
		groups[groupName] = map[string]interface{}{
			"members": members,
			"topic":   group.Topic,
		}
	}
	ds.broker.coordinator.mu.Unlock()

	return map[string]interface{}{
		"type":   "snapshot",
		"topics": topics,
		"groups": groups,
	}
}

// handleAPIProduce lets the dashboard UI produce messages via HTTP POST.
func (ds *DashboardServer) handleAPIProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Topic string `json:"topic"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	partition, offset, err := ds.broker.Publish(req.Topic, []byte(req.Key), []byte(req.Value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"partition": partition,
		"offset":    offset,
	})
}

// handleAPICreateTopic lets the dashboard UI create topics.
func (ds *DashboardServer) handleAPICreateTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Topic      string `json:"topic"`
		Partitions int    `json:"partitions"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Partitions == 0 {
		req.Partitions = 3
	}

	err := ds.broker.CreateTopic(req.Topic, req.Partitions)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
}

// handleAPIState returns current broker state as JSON (for initial page load without WS).
func (ds *DashboardServer) handleAPIState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ds.buildSnapshot())
}

// handleAPIReset clears all topics and groups (fresh start for demos).
func (ds *DashboardServer) handleAPIReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	ds.broker.mu.Lock()
	// Close all existing logs
	for _, partitions := range ds.broker.topics {
		for _, l := range partitions {
			l.Close()
		}
	}
	// Clear topics
	ds.broker.topics = make(map[string][]*Log)
	ds.broker.mu.Unlock()

	// Clear coordinator groups
	ds.broker.coordinator.mu.Lock()
	ds.broker.coordinator.groups = make(map[string]*ConsumerGroup)
	ds.broker.coordinator.mu.Unlock()

	// Remove data directory and recreate offset store
	os.RemoveAll(ds.broker.config.DataDir)
	offsetDir := filepath.Join(ds.broker.config.DataDir, "offsets")
	ds.broker.offsets, _ = NewOffsetStore(offsetDir)

	// Emit reset event
	ds.broker.Events.Emit("reset", map[string]interface{}{})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "reset"})
}
