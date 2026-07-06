package minikafka

// protocol.go — Defines how messages look on the wire (TCP).
// Uses length-prefixed framing: [4-byte size][body]
// Same concept as our log file format, but for network communication.

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Request types — the first byte of every request body.
// Java equivalent: public static final int PRODUCE = 1;
const (
	RequestProduce     byte = 1
	RequestConsume     byte = 2
	RequestCreateTopic byte = 3
	RequestCommit      byte = 4
	RequestFetchOffset byte = 5
)

// Response status codes
const (
	StatusOK    byte = 0
	StatusError byte = 1
)

// --- PRODUCE ---

// ProduceRequest is what a producer sends to the broker.
//
// "Put this value into this topic. Use this key to pick the partition."
type ProduceRequest struct {
	Topic string
	Key   []byte
	Value []byte
}

// ProduceResponse is what the broker sends back after a successful produce.
type ProduceResponse struct {
	Partition int
	Offset    uint64
}

// --- CONSUME ---

// ConsumeRequest is what a consumer sends to the broker.
//
// "Give me the message at this topic/partition/offset."
type ConsumeRequest struct {
	Topic     string
	Partition int
	Offset    uint64
}

// --- CREATE TOPIC ---

// CreateTopicRequest asks the broker to create a new topic.
type CreateTopicRequest struct {
	Topic         string
	NumPartitions int
}

// === WIRE FORMAT: Writing bytes to the connection ===

// WriteFrame writes a length-prefixed frame to a writer (TCP connection).
// Format: [4 bytes: length of data][data]
//
// This is the SAME pattern as our log file: [size][payload].
// Just on a network socket instead of a file.
func WriteFrame(w io.Writer, data []byte) error {
	// Write 4-byte length header
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := w.Write(lenBuf); err != nil {
		return err
	}

	// Write the data itself
	if _, err := w.Write(data); err != nil {
		return err
	}

	return nil
}

// ReadFrame reads a length-prefixed frame from a reader (TCP connection).
// Returns the body bytes (without the length header).
func ReadFrame(r io.Reader) ([]byte, error) {
	// Read 4-byte length header
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(lenBuf)

	// Sanity check: reject absurdly large frames (prevents memory exhaustion)
	if size > 64*1024*1024 { // 64 MB max
		return nil, fmt.Errorf("frame too large: %d bytes", size)
	}

	// Read exactly 'size' bytes
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	return data, nil
}

// === ENCODING: Struct → Bytes ===
// These functions turn our nice Go structs into raw bytes for the wire.

// EncodeProduceRequest turns a ProduceRequest into bytes.
//
// Format:
//   [1 byte: type=1]
//   [2 bytes: topic length][topic]
//   [2 bytes: key length][key]
//   [4 bytes: value length][value]
func EncodeProduceRequest(req *ProduceRequest) []byte {
	// Calculate total size
	size := 1 + 2 + len(req.Topic) + 2 + len(req.Key) + 4 + len(req.Value)
	buf := make([]byte, 0, size)

	// Type byte
	buf = append(buf, RequestProduce)

	// Topic: [2-byte length][string bytes]
	buf = appendString16(buf, req.Topic)

	// Key: [2-byte length][bytes]
	buf = appendBytes16(buf, req.Key)

	// Value: [4-byte length][bytes]
	buf = appendBytes32(buf, req.Value)

	return buf
}

// DecodeProduceRequest turns bytes into a ProduceRequest.
func DecodeProduceRequest(data []byte) (*ProduceRequest, error) {
	if len(data) < 1 || data[0] != RequestProduce {
		return nil, fmt.Errorf("not a produce request")
	}

	pos := 1 // skip the type byte
	req := &ProduceRequest{}

	// Read topic
	topic, n, err := readString16(data, pos)
	if err != nil {
		return nil, err
	}
	req.Topic = topic
	pos += n

	// Read key
	key, n, err := readBytes16(data, pos)
	if err != nil {
		return nil, err
	}
	req.Key = key
	pos += n

	// Read value
	value, _, err := readBytes32(data, pos)
	if err != nil {
		return nil, err
	}
	req.Value = value

	return req, nil
}

// EncodeConsumeRequest turns a ConsumeRequest into bytes.
//
// Format:
//   [1 byte: type=2]
//   [2 bytes: topic length][topic]
//   [4 bytes: partition]
//   [8 bytes: offset]
func EncodeConsumeRequest(req *ConsumeRequest) []byte {
	size := 1 + 2 + len(req.Topic) + 4 + 8
	buf := make([]byte, 0, size)

	buf = append(buf, RequestConsume)
	buf = appendString16(buf, req.Topic)

	// Partition as 4 bytes
	partBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(partBuf, uint32(req.Partition))
	buf = append(buf, partBuf...)

	// Offset as 8 bytes
	offBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(offBuf, req.Offset)
	buf = append(buf, offBuf...)

	return buf
}

// DecodeConsumeRequest turns bytes into a ConsumeRequest.
func DecodeConsumeRequest(data []byte) (*ConsumeRequest, error) {
	if len(data) < 1 || data[0] != RequestConsume {
		return nil, fmt.Errorf("not a consume request")
	}

	pos := 1
	req := &ConsumeRequest{}

	// Read topic
	topic, n, err := readString16(data, pos)
	if err != nil {
		return nil, err
	}
	req.Topic = topic
	pos += n

	// Read partition (4 bytes)
	if pos+4 > len(data) {
		return nil, fmt.Errorf("truncated: missing partition")
	}
	req.Partition = int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	// Read offset (8 bytes)
	if pos+8 > len(data) {
		return nil, fmt.Errorf("truncated: missing offset")
	}
	req.Offset = binary.BigEndian.Uint64(data[pos : pos+8])

	return req, nil
}

// EncodeCreateTopicRequest turns a CreateTopicRequest into bytes.
func EncodeCreateTopicRequest(req *CreateTopicRequest) []byte {
	size := 1 + 2 + len(req.Topic) + 4
	buf := make([]byte, 0, size)

	buf = append(buf, RequestCreateTopic)
	buf = appendString16(buf, req.Topic)

	partBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(partBuf, uint32(req.NumPartitions))
	buf = append(buf, partBuf...)

	return buf
}

// DecodeCreateTopicRequest turns bytes into a CreateTopicRequest.
func DecodeCreateTopicRequest(data []byte) (*CreateTopicRequest, error) {
	if len(data) < 1 || data[0] != RequestCreateTopic {
		return nil, fmt.Errorf("not a create topic request")
	}

	pos := 1
	req := &CreateTopicRequest{}

	topic, n, err := readString16(data, pos)
	if err != nil {
		return nil, err
	}
	req.Topic = topic
	pos += n

	if pos+4 > len(data) {
		return nil, fmt.Errorf("truncated: missing num_partitions")
	}
	req.NumPartitions = int(binary.BigEndian.Uint32(data[pos : pos+4]))

	return req, nil
}

// EncodeResponse encodes a success or error response.
//
// Format:
//   [1 byte: status (0=ok, 1=error)]
//   [4 bytes: body length][body]
func EncodeResponse(status byte, body []byte) []byte {
	buf := make([]byte, 0, 1+4+len(body))
	buf = append(buf, status)

	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(body)))
	buf = append(buf, lenBuf...)
	buf = append(buf, body...)

	return buf
}

// DecodeResponse decodes a response into (status, body).
func DecodeResponse(data []byte) (byte, []byte, error) {
	if len(data) < 5 {
		return 0, nil, fmt.Errorf("response too short")
	}

	status := data[0]
	bodyLen := binary.BigEndian.Uint32(data[1:5])

	if int(bodyLen) > len(data)-5 {
		return 0, nil, fmt.Errorf("response body truncated")
	}

	body := data[5 : 5+bodyLen]
	return status, body, nil
}

// EncodeProduceResponse encodes (partition, offset) into bytes.
func EncodeProduceResponse(partition int, offset uint64) []byte {
	buf := make([]byte, 12) // 4 bytes partition + 8 bytes offset
	binary.BigEndian.PutUint32(buf[0:4], uint32(partition))
	binary.BigEndian.PutUint64(buf[4:12], offset)
	return buf
}

// DecodeProduceResponse decodes (partition, offset) from bytes.
func DecodeProduceResponse(data []byte) (int, uint64, error) {
	if len(data) < 12 {
		return 0, 0, fmt.Errorf("produce response too short")
	}
	partition := int(binary.BigEndian.Uint32(data[0:4]))
	offset := binary.BigEndian.Uint64(data[4:12])
	return partition, offset, nil
}

// --- COMMIT OFFSET ---

// CommitRequest is what a consumer sends to save its position.
//
// "I'm in group X, reading topic Y partition Z, and I'm done up to offset W."
type CommitRequest struct {
	Group     string
	Topic     string
	Partition int
	Offset    uint64
}

// EncodeCommitRequest turns a CommitRequest into bytes.
// Format: [type=4][group len][group][topic len][topic][partition: 4][offset: 8]
func EncodeCommitRequest(req *CommitRequest) []byte {
	size := 1 + 2 + len(req.Group) + 2 + len(req.Topic) + 4 + 8
	buf := make([]byte, 0, size)

	buf = append(buf, RequestCommit)
	buf = appendString16(buf, req.Group)
	buf = appendString16(buf, req.Topic)

	partBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(partBuf, uint32(req.Partition))
	buf = append(buf, partBuf...)

	offBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(offBuf, req.Offset)
	buf = append(buf, offBuf...)

	return buf
}

// DecodeCommitRequest turns bytes into a CommitRequest.
func DecodeCommitRequest(data []byte) (*CommitRequest, error) {
	if len(data) < 1 || data[0] != RequestCommit {
		return nil, fmt.Errorf("not a commit request")
	}

	pos := 1
	req := &CommitRequest{}

	group, n, err := readString16(data, pos)
	if err != nil {
		return nil, err
	}
	req.Group = group
	pos += n

	topic, n, err := readString16(data, pos)
	if err != nil {
		return nil, err
	}
	req.Topic = topic
	pos += n

	if pos+4 > len(data) {
		return nil, fmt.Errorf("truncated: missing partition")
	}
	req.Partition = int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	if pos+8 > len(data) {
		return nil, fmt.Errorf("truncated: missing offset")
	}
	req.Offset = binary.BigEndian.Uint64(data[pos : pos+8])

	return req, nil
}

// --- FETCH OFFSET ---

// FetchOffsetRequest asks "where should I resume reading?"
type FetchOffsetRequest struct {
	Group     string
	Topic     string
	Partition int
}

// EncodeFetchOffsetRequest turns a FetchOffsetRequest into bytes.
// Format: [type=5][group len][group][topic len][topic][partition: 4]
func EncodeFetchOffsetRequest(req *FetchOffsetRequest) []byte {
	size := 1 + 2 + len(req.Group) + 2 + len(req.Topic) + 4
	buf := make([]byte, 0, size)

	buf = append(buf, RequestFetchOffset)
	buf = appendString16(buf, req.Group)
	buf = appendString16(buf, req.Topic)

	partBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(partBuf, uint32(req.Partition))
	buf = append(buf, partBuf...)

	return buf
}

// DecodeFetchOffsetRequest turns bytes into a FetchOffsetRequest.
func DecodeFetchOffsetRequest(data []byte) (*FetchOffsetRequest, error) {
	if len(data) < 1 || data[0] != RequestFetchOffset {
		return nil, fmt.Errorf("not a fetch-offset request")
	}

	pos := 1
	req := &FetchOffsetRequest{}

	group, n, err := readString16(data, pos)
	if err != nil {
		return nil, err
	}
	req.Group = group
	pos += n

	topic, n, err := readString16(data, pos)
	if err != nil {
		return nil, err
	}
	req.Topic = topic
	pos += n

	if pos+4 > len(data) {
		return nil, fmt.Errorf("truncated: missing partition")
	}
	req.Partition = int(binary.BigEndian.Uint32(data[pos : pos+4]))

	return req, nil
}

// EncodeFetchOffsetResponse encodes a single offset value.
func EncodeFetchOffsetResponse(offset uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, offset)
	return buf
}

// DecodeFetchOffsetResponse decodes a single offset value.
func DecodeFetchOffsetResponse(data []byte) (uint64, error) {
	if len(data) < 8 {
		return 0, fmt.Errorf("fetch-offset response too short")
	}
	return binary.BigEndian.Uint64(data[0:8]), nil
}

// === Helper functions for reading/writing variable-length fields ===

// appendString16 appends [2-byte length][string bytes] to buf.
func appendString16(buf []byte, s string) []byte {
	lenBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(lenBuf, uint16(len(s)))
	buf = append(buf, lenBuf...)
	buf = append(buf, []byte(s)...)
	return buf
}

// appendBytes16 appends [2-byte length][bytes] to buf.
func appendBytes16(buf []byte, b []byte) []byte {
	lenBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(lenBuf, uint16(len(b)))
	buf = append(buf, lenBuf...)
	buf = append(buf, b...)
	return buf
}

// appendBytes32 appends [4-byte length][bytes] to buf.
func appendBytes32(buf []byte, b []byte) []byte {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(b)))
	buf = append(buf, lenBuf...)
	buf = append(buf, b...)
	return buf
}

// readString16 reads [2-byte length][string bytes] from data at pos.
// Returns (string, bytes consumed, error).
func readString16(data []byte, pos int) (string, int, error) {
	if pos+2 > len(data) {
		return "", 0, fmt.Errorf("truncated: missing string length at pos %d", pos)
	}
	strLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	if pos+2+strLen > len(data) {
		return "", 0, fmt.Errorf("truncated: string data at pos %d", pos)
	}
	s := string(data[pos+2 : pos+2+strLen])
	return s, 2 + strLen, nil
}

// readBytes16 reads [2-byte length][bytes] from data at pos.
func readBytes16(data []byte, pos int) ([]byte, int, error) {
	if pos+2 > len(data) {
		return nil, 0, fmt.Errorf("truncated: missing bytes length at pos %d", pos)
	}
	bLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	if pos+2+bLen > len(data) {
		return nil, 0, fmt.Errorf("truncated: bytes data at pos %d", pos)
	}
	b := data[pos+2 : pos+2+bLen]
	return b, 2 + bLen, nil
}

// readBytes32 reads [4-byte length][bytes] from data at pos.
func readBytes32(data []byte, pos int) ([]byte, int, error) {
	if pos+4 > len(data) {
		return nil, 0, fmt.Errorf("truncated: missing bytes length at pos %d", pos)
	}
	bLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	if pos+4+bLen > len(data) {
		return nil, 0, fmt.Errorf("truncated: bytes data at pos %d", pos)
	}
	b := data[pos+4 : pos+4+bLen]
	return b, 4 + bLen, nil
}
