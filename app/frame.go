package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

var (
	ErrOutOfBounds         = errors.New("frame: out of bounds")
	ErrInvalidStringLength = errors.New("frame: invalid string length")
	ErrUvarintTooLarge     = errors.New("frame: uvarint too large")
	ErrNullCompactString   = errors.New("frame: compact string cannot be null")
)

// This struct is not thread-safe
type Frame struct {
	buf []byte
	pos int
}

func NewFrame(data []byte) Frame {
	return Frame{
		buf: data,
		pos: 0,
	}
}

func (f *Frame) Remaining() int {
	return len(f.buf) - f.pos
}

// Read a single byte from the frame and advances the current postion by 1
// Also checks for OOB and returns an error
func (f *Frame) ReadByte() (byte, error) {
	if f.Remaining() < 1 {
		return 0, ErrOutOfBounds
	}
	b := f.buf[f.pos]
	f.pos++
	return b, nil
}

func (f *Frame) Skip(n int) error {
	_, err := f.ReadBytes(n)
	return err
}

// Read n bytes from the frame and advances the current postion by n
// Also checks for OOB and returns an error
func (f *Frame) ReadBytes(n int) ([]byte, error) {
	if n < 0 || f.Remaining() < n {
		return nil, ErrOutOfBounds
	}
	out := f.buf[f.pos : f.pos+n]
	f.pos += n
	return out, nil
}

func (f *Frame) ReadInt8() (int8, error) {
	b, err := f.ReadByte()
	if err != nil {
		return 0, err
	}
	return int8(b), nil
}

func (f *Frame) ReadInt16() (int16, error) {
	if f.Remaining() < 2 {
		return 0, ErrOutOfBounds
	}
	v := int16(binary.BigEndian.Uint16(f.buf[f.pos : f.pos+2]))
	f.pos += 2
	return v, nil
}

func (f *Frame) ReadInt32() (int32, error) {
	if f.Remaining() < 4 {
		return 0, ErrOutOfBounds
	}
	v := int32(binary.BigEndian.Uint32(f.buf[f.pos : f.pos+4]))
	f.pos += 4
	return v, nil
}

func (f *Frame) ReadInt64() (int64, error) {
	if f.Remaining() < 8 {
		return 0, ErrOutOfBounds
	}
	v := int64(binary.BigEndian.Uint64(f.buf[f.pos : f.pos+8]))
	f.pos += 8
	return v, nil
}

// See more: https://protobuf.dev/programming-guides/encoding/#varints
func (f *Frame) ReadUvarint() (uint64, error) {
	var result uint64
	var shift uint

	for range 10 { // max 10 bytes for uint64
		b, err := f.ReadByte()
		if err != nil {
			return 0, fmt.Errorf("read uvarint byte: %w", err)
		}

		result |= uint64(b&0x7F) << shift

		if b&0x80 == 0 {
			return result, nil
		}

		shift += 7
	}

	return 0, ErrUvarintTooLarge
}

func (f *Frame) ReadUvarintAsInt16(field string) (int16, error) {
	v, err := f.ReadUvarint()
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", field, err)
	}

	if v > math.MaxInt16 {
		return 0, fmt.Errorf("%s too large for int16: %d", field, v)
	}

	return int16(v), nil
}

// See more: https://protobuf.dev/programming-guides/encoding/#varints
// Kafka signed VARINT uses zig-zag encoding.
// Used by Record.length, timestampDelta, offsetDelta, keyLength, valueLength, headersCount.
func (f *Frame) ReadVarint() (int64, error) {
	u, err := f.ReadUvarint()
	if err != nil {
		return 0, err
	}

	// Zig-zag decode:
	// 0 -> 0
	// 1 -> -1
	// 2 -> 1
	// 3 -> -2
	return int64(u>>1) ^ -int64(u&1), nil
}

func (f *Frame) ReadVarint32() (int32, error) {
	v, err := f.ReadVarint()
	return int32(v), err
}

func (f *Frame) ReadVarint64() (int64, error) {
	return f.ReadVarint()
}

func (f *Frame) SkipTaggedFields() error {
	numTags, err := f.ReadUvarint()
	if err != nil {
		return fmt.Errorf("read tagged fields count: %w", err)
	}

	for range numTags {
		_, err := f.ReadUvarint() // tag id
		if err != nil {
			return fmt.Errorf("read tag id: %w", err)
		}

		size, err := f.ReadUvarint()
		if err != nil {
			return fmt.Errorf("read tag size: %w", err)
		}

		if err := f.Skip(int(size)); err != nil {
			return fmt.Errorf("skip tag payload: size=%d: %w", size, err)
		}
	}

	return nil
}

func (f *Frame) ReadUUID() ([16]byte, error) {
	var id [16]byte

	b, err := f.ReadBytes(16)
	if err != nil {
		return id, fmt.Errorf("read uuid: %w", err)
	}

	copy(id[:], b)
	return id, nil
}

/*
COMPACT_STRING:
Represents a sequence of characters.
First the length N + 1 is given as an UNSIGNED_VARINT.
Then N bytes follow which are the UTF-8 encoding of the character sequence.

This type follows the N+1 syntax (0 for null, 1 for empty length, 2 for length 1, so on).
*/
func (f *Frame) ReadCompactString() (string, error) {
	n, err := f.ReadUvarint()
	if err != nil {
		return "", fmt.Errorf("read compact string length: %w", err)
	}
	if n == 0 {
		return "", ErrNullCompactString
	}

	b, err := f.ReadBytes(int(n - 1))
	if err != nil {
		return "", fmt.Errorf("read compact string payload: length=%d: %w", n, err)
	}
	return string(b), nil
}

/*
NULLABLE_STRING:
Represents a sequence of characters or null.
For non-null strings, first the length N is given as an INT16.
Then N bytes follow which are the UTF-8 encoding of the character sequence.
A null value is encoded with length of -1 and there are no following bytes.

This type follows the N syntax, not the N+1 syntax
*/
func (f *Frame) ReadNullableString() (*string, error) {
	// First 2 bytes is the length
	n, err := f.ReadInt16()
	if err != nil {
		return nil, fmt.Errorf("read nullable string length: %w", err)
	}

	// Null check
	if n == -1 {
		return nil, nil
	}

	// Invalid length check
	if n < 0 {
		return nil, fmt.Errorf("read nullable string: length=%d: %w", n, ErrInvalidStringLength)
	}

	// n >= 0: read n bytes (possibly zero)
	b, err := f.ReadBytes(int(n))
	if err != nil {
		return nil, fmt.Errorf("read nullable string payload: length=%d: %w", n, err)
	}
	s := string(b)
	return &s, nil
}

/*
ReadClientID is just a descriptive wrapper that calls ReadNullableString
since client_id has type NULLABLE_STRING.

See more: https://kafka.apache.org/42/design/protocol/#the-messages
*/
func (f *Frame) ReadClientID() (*string, error) {
	s, err := f.ReadNullableString()
	if err != nil {
		return nil, fmt.Errorf("read client_id: %w", err)
	}
	return s, nil
}

/*
Parsing Request Header v2
Request Header v2 => request_api_key request_api_version correlation_id client_id

request_api_key => INT16 (byte 0 1)
request_api_version => INT16 (byte 2 3)
correlation_id => INT32 (byte 4 5 6 7)
client_id => NULLABLE_STRING

This function is guaranteed to consume all the bytes related to the header version 2.
For now, the last byte (TAG BUFFER) is being read using ReadByte, but we might have to use
ReadUvarint later and part the tagged fields.
*/
func (f *Frame) ReadRequestHeaderV2() (RequestHeaderV2, error) {
	apiKey, err := f.ReadInt16()
	if err != nil {
		return RequestHeaderV2{}, fmt.Errorf("read request_api_key: %w", err)
	}

	apiVersion, err := f.ReadInt16()
	if err != nil {
		return RequestHeaderV2{}, fmt.Errorf("read request_api_version: %w", err)
	}

	correlationID, err := f.ReadInt32()
	if err != nil {
		return RequestHeaderV2{}, fmt.Errorf("read correlation_id: %w", err)
	}

	clientID, err := f.ReadClientID()
	if err != nil {
		return RequestHeaderV2{}, fmt.Errorf("read client_id: %w", err)
	}

	// There is a 0x00 tag buffer field in the header, we'll consume and ignore it for now
	// See https://binspec.org/kafka-describe-topic-partitions-request-v0
	// The visualization shows the tag buffer at the end of the header
	b, err := f.ReadByte()
	if err != nil {
		return RequestHeaderV2{}, fmt.Errorf("read header tagged fields placeholder: %w", err)
	}
	if b != 0x00 {
		return RequestHeaderV2{}, fmt.Errorf("expected empty header tagged fields 0x00, got 0x%02x", b)
	}
	return RequestHeaderV2{
		RequestAPIKey:     apiKey,
		RequestAPIVersion: apiVersion,
		CorrelationID:     correlationID,
		ClientID:          clientID,
	}, nil
}
