package minikafka

// record.go — The on-disk message format.
//
// Format:
//   [4-byte size][4-byte CRC32][8-byte timestamp][2-byte key len][key][value]
//
// - size: covers everything after itself (CRC + timestamp + keyLen + key + value)
// - CRC32: checksum of (timestamp + keyLen + key + value). Detects corruption.
// - timestamp: milliseconds since Unix epoch. When the message was produced.
// - key len: how many bytes the key is (0 = no key)
// - key: the partition-routing key
// - value: the actual message content

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

// Record is one message in the log.
type Record struct {
	Timestamp int64  // milliseconds since Unix epoch
	Key       []byte // partition-routing key (can be empty)
	Value     []byte // the actual message
}

// RecordHeaderSize is the fixed overhead per record (CRC + timestamp + key length).
// Total on-disk size = 4 (size header) + RecordHeaderSize + len(key) + len(value)
const RecordHeaderSize = 4 + 8 + 2 // CRC(4) + timestamp(8) + keyLen(2) = 14

// EncodeRecord turns a Record into bytes for writing to disk.
// Returns the complete body (everything AFTER the 4-byte size header).
//
// Layout: [CRC32][timestamp][keyLen][key][value]
func EncodeRecord(r *Record) []byte {
	// Total body size
	bodySize := RecordHeaderSize + len(r.Key) + len(r.Value)
	body := make([]byte, bodySize)

	// Leave first 4 bytes empty for CRC (we'll fill it after computing)
	// Write timestamp at offset 4
	binary.BigEndian.PutUint64(body[4:12], uint64(r.Timestamp))

	// Write key length at offset 12
	binary.BigEndian.PutUint16(body[12:14], uint16(len(r.Key)))

	// Write key at offset 14
	copy(body[14:14+len(r.Key)], r.Key)

	// Write value at offset 14+keyLen
	copy(body[14+len(r.Key):], r.Value)

	// Compute CRC32 of everything AFTER the CRC field (bytes 4 onward)
	checksum := crc32.ChecksumIEEE(body[4:])
	binary.BigEndian.PutUint32(body[0:4], checksum)

	return body
}

// DecodeRecord turns bytes (the body, without the size header) into a Record.
// Also validates the CRC32 checksum.
func DecodeRecord(body []byte) (*Record, error) {
	if len(body) < RecordHeaderSize {
		return nil, fmt.Errorf("record too short: %d bytes (need at least %d)", len(body), RecordHeaderSize)
	}

	// Read stored CRC
	storedCRC := binary.BigEndian.Uint32(body[0:4])

	// Compute actual CRC of everything after the CRC field
	actualCRC := crc32.ChecksumIEEE(body[4:])

	// Compare
	if storedCRC != actualCRC {
		return nil, fmt.Errorf("CRC mismatch: stored=%08X computed=%08X (data corrupted)", storedCRC, actualCRC)
	}

	// Read timestamp
	timestamp := int64(binary.BigEndian.Uint64(body[4:12]))

	// Read key length
	keyLen := int(binary.BigEndian.Uint16(body[12:14]))

	// Bounds check
	if 14+keyLen > len(body) {
		return nil, fmt.Errorf("record truncated: keyLen=%d but body only has %d bytes", keyLen, len(body))
	}

	// Extract key and value
	key := body[14 : 14+keyLen]
	value := body[14+keyLen:]

	return &Record{
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
	}, nil
}

// NewRecord creates a record with the current timestamp.
func NewRecord(key, value []byte) *Record {
	return &Record{
		Timestamp: time.Now().UnixMilli(),
		Key:       key,
		Value:     value,
	}
}

// RecordBodySize returns how many bytes the body will be for given key/value lengths.
func RecordBodySize(keyLen, valueLen int) int {
	return RecordHeaderSize + keyLen + valueLen
}
