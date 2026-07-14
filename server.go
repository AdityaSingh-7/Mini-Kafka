package minikafka

// server.go — TCP server that accepts connections and handles requests.
// Each client gets its own goroutine (background worker).

import (
	"fmt"
	"log"
	"net"
	"sync"
)

// Server listens on a TCP port and routes requests to the Broker.
//
// Java equivalent:
//   public class Server {
//       private ServerSocket listener;
//       private Broker broker;
//       private boolean running;
//   }
type Server struct {
	broker   *Broker
	listener net.Listener // the thing that accepts incoming connections
	addr     string       // e.g. ":9092" (port 9092 on all network interfaces)
	wg       sync.WaitGroup // tracks how many goroutines are still running
	quit     chan struct{}   // signal to stop accepting connections
}

// NewServer creates a server that will listen on the given address.
// addr format: ":9092" means "listen on port 9092"
func NewServer(addr string, broker *Broker) *Server {
	return &Server{
		broker: broker,
		addr:   addr,
		quit:   make(chan struct{}),
	}
}

// Start begins listening for connections.
// This function BLOCKS (runs forever) until Stop() is called.
//
// Java equivalent:
//   public void start() {
//       ServerSocket server = new ServerSocket(9092);
//       while (running) {
//           Socket conn = server.accept();
//           new Thread(() -> handleConnection(conn)).start();
//       }
//   }
func (s *Server) Start() error {
	// Start listening on the port
	// Java: ServerSocket listener = new ServerSocket(9092);
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}

	log.Printf("broker listening on %s", s.listener.Addr().String())

	// Accept connections forever (until Stop is called)
	for {
		// Wait for a client to connect
		// Java: Socket conn = server.accept();
		conn, err := s.listener.Accept()
		if err != nil {
			// Check if we were told to stop
			select {
			case <-s.quit:
				return nil // clean shutdown
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}

		// Handle this connection in the background (a new goroutine)
		// Java: new Thread(() -> handleConnection(conn)).start();
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// Stop gracefully shuts down the server.
func (s *Server) Stop() {
	close(s.quit)          // signal the accept loop to stop
	s.listener.Close()     // unblocks the Accept() call
	s.wg.Wait()            // wait for all active connections to finish
}

// Addr returns the actual address the server is listening on.
// Useful when we use port 0 (OS picks a random available port for testing).
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// handleConnection processes one client's requests until they disconnect.
// Runs in its own goroutine — one per client.
//
// The loop:
//   1. Read a frame (request) from the client
//   2. Decode it (figure out what they want)
//   3. Call the broker (do the work)
//   4. Encode the response
//   5. Send the response frame back
//   6. Repeat until client disconnects
func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	// log.Printf("client connected: %s", conn.RemoteAddr())

	for {
		// Step 1: Read a frame from the client
		data, err := ReadFrame(conn)
		if err != nil {
			// Client disconnected (or network error)
			// This is normal — just stop this goroutine
			return
		}

		// Step 2: Figure out what type of request this is (first byte)
		if len(data) == 0 {
			s.sendError(conn, "empty request")
			continue
		}

		// Step 3: Route to the right handler based on request type
		var response []byte
		switch data[0] {
		case RequestProduce:
			response = s.handleProduce(data)
		case RequestConsume:
			response = s.handleConsume(data)
		case RequestCreateTopic:
			response = s.handleCreateTopic(data)
		case RequestCommit:
			response = s.handleCommit(data)
		case RequestFetchOffset:
			response = s.handleFetchOffset(data)
		case RequestJoinGroup:
			response = s.handleJoinGroup(data)
		case RequestHeartbeat:
			response = s.handleHeartbeat(data)
		case RequestLeaveGroup:
			response = s.handleLeaveGroup(data)
		case RequestProduceBatch:
			response = s.handleProduceBatch(data)
		default:
			response = EncodeResponse(StatusError, []byte(fmt.Sprintf("unknown request type: %d", data[0])))
		}

		// Step 4: Send the response back to the client
		if err := WriteFrame(conn, response); err != nil {
			return // client disconnected mid-response
		}
	}
}

// handleProduce processes a produce request.
func (s *Server) handleProduce(data []byte) []byte {
	// Decode the request bytes into a struct
	req, err := DecodeProduceRequest(data)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	// Call the broker
	partition, offset, err := s.broker.Publish(req.Topic, req.Key, req.Value)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	// Encode success response with partition + offset
	body := EncodeProduceResponse(partition, offset)
	return EncodeResponse(StatusOK, body)
}

// handleConsume processes a consume request.
func (s *Server) handleConsume(data []byte) []byte {
	// Decode
	req, err := DecodeConsumeRequest(data)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	// Call the broker
	value, err := s.broker.Consume(req.Topic, req.Partition, req.Offset)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	// Success — the body IS the message value
	return EncodeResponse(StatusOK, value)
}

// handleCreateTopic processes a create-topic request.
func (s *Server) handleCreateTopic(data []byte) []byte {
	// Decode
	req, err := DecodeCreateTopicRequest(data)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	// Call the broker
	err = s.broker.CreateTopic(req.Topic, req.NumPartitions)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	// Success — empty body (just "OK")
	return EncodeResponse(StatusOK, []byte("created"))
}

// handleCommit processes a commit-offset request.
func (s *Server) handleCommit(data []byte) []byte {
	req, err := DecodeCommitRequest(data)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	err = s.broker.CommitOffset(req.Group, req.Topic, req.Partition, req.Offset)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	return EncodeResponse(StatusOK, []byte("committed"))
}

// handleFetchOffset processes a fetch-offset request.
func (s *Server) handleFetchOffset(data []byte) []byte {
	req, err := DecodeFetchOffsetRequest(data)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	offset, err := s.broker.FetchOffset(req.Group, req.Topic, req.Partition)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	body := EncodeFetchOffsetResponse(offset)
	return EncodeResponse(StatusOK, body)
}

// handleJoinGroup processes a join-group request.
// Consumer says "I'm joining group X to read topic Y."
// Broker adds them, rebalances, returns their assigned partitions.
func (s *Server) handleJoinGroup(data []byte) []byte {
	req, err := DecodeJoinGroupRequest(data)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	partitions, err := s.broker.JoinGroup(req.Group, req.MemberID, req.Topic)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	body := EncodeAssignmentResponse(partitions)
	return EncodeResponse(StatusOK, body)
}

// handleHeartbeat processes a heartbeat request.
// Consumer says "I'm alive." Broker returns their current assignment.
// If a rebalance happened, the new assignment shows up here.
func (s *Server) handleHeartbeat(data []byte) []byte {
	req, err := DecodeHeartbeatRequest(data)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	partitions, err := s.broker.Heartbeat(req.Group, req.MemberID)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	body := EncodeAssignmentResponse(partitions)
	return EncodeResponse(StatusOK, body)
}

// handleLeaveGroup processes a leave-group request.
// Consumer is shutting down gracefully — remove it immediately (don't wait for timeout).
func (s *Server) handleLeaveGroup(data []byte) []byte {
	req, err := DecodeLeaveGroupRequest(data)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	err = s.broker.LeaveGroup(req.Group, req.MemberID)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	return EncodeResponse(StatusOK, []byte("left"))
}

// handleProduceBatch processes a batch produce request.
// Multiple messages in one network call, one fsync per partition.
func (s *Server) handleProduceBatch(data []byte) []byte {
	req, err := DecodeProduceBatchRequest(data)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	results, err := s.broker.PublishBatch(req.Topic, req.Messages)
	if err != nil {
		return EncodeResponse(StatusError, []byte(err.Error()))
	}

	body := EncodeBatchProduceResponse(results)
	return EncodeResponse(StatusOK, body)
}

// sendError is a helper to send an error response frame.
func (s *Server) sendError(conn net.Conn, msg string) {
	response := EncodeResponse(StatusError, []byte(msg))
	WriteFrame(conn, response)
}
