package main

import (
	"encoding/binary"
	"errors"
)

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
		return 0, errors.New("out of bound")
	}
	b := f.buf[f.pos]
	f.pos++
	return b, nil
}

// Read n bytes from the frame and advances the current postion by n
// Also checks for OOB and returns an error
func (f *Frame) ReadBytes(n int) ([]byte, error) {
	if n < 0 || f.Remaining() < n {
		return nil, errors.New("out of bounds")
	}
	out := f.buf[f.pos : f.pos+n]
	f.pos += n
	return out, nil
}

func (f *Frame) ReadInt16() (int16, error) {
	if f.Remaining() < 2 {
		return 0, errors.New("out of bounds")
	}
	v := int16(binary.BigEndian.Uint16(f.buf[f.pos : f.pos+2]))
	f.pos += 2
	return v, nil
}

func (f *Frame) ReadInt32() (int32, error) {
	if f.Remaining() < 4 {
		return 0, errors.New("out of bounds")
	}
	v := int32(binary.BigEndian.Uint32(f.buf[f.pos : f.pos+4]))
	f.pos += 4
	return v, nil
}

func createApiVersionsResponse(request_api_version int16, correlation_id int32) (response []byte) {
	// Seems like there's no request body, just the header. So we're just gonna build the response.
	// https://kafka.apache.org/42/design/protocol/#The_Messages_ApiVersions
	/*
		Simple example response:
		00 00 00 13  // message_size:      19 bytes
		ab cd ef 12  // correlation_id:    (matches request)
		00 00        // error_code:        0 (no error)
		02           // api_keys array length:    1 element
		00 12        // api_key:           18 (ApiVersions)
		00 00        // min_version:       0
		00 04        // max_version:       4
		00           // TAG_BUFFER:        empty
		00 00 00 00  // throttle_time_ms:  0
		00           // TAG_BUFFER:        empty

		A bit more complex response, with ApiVersions and DescribeTopicPartitions in the array:
		00 00 00 1d  // message_size:      29 bytes
		ab cd ef 12  // correlation_id:    (matches request)
		00 00        // error_code:        0 (no error)
		03           // api_keys array:    2 elements (compact array encoding)
		00 12        // api_key:           18 (ApiVersions)
		00 00        // min_version:       0
		00 04        // max_version:       4
		00           // TAG_BUFFER:        empty
		00 4b        // api_key:           75 (DescribeTopicPartitions)
		00 00        // min_version:       0
		00 00        // max_version:       0
		00           // TAG_BUFFER:        empty
		00 00 00 00  // throttle_time_ms:  0
		00           // TAG_BUFFER:        empty

		Structually, it's like this:
		error_code
		api_keys_length
			entry1 fields
			entry1 tag section = 00
			entry2 fields
			entry2 tag section = 00
		throttle_time_ms
		top-level tag section = 00
	*/
	/*
		ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms [supported_features]<tag: 0> finalized_features_epoch<tag: 1> [finalized_features]<tag: 2> zk_migration_ready<tag: 3>
		  error_code => INT16
		  api_keys => api_key min_version max_version
		    api_key => INT16
		    min_version => INT16
		    max_version => INT16
		  throttle_time_ms => INT32
		  supported_features<tag: 0> => name min_version max_version
		    name => COMPACT_STRING
		    min_version => INT16
		    max_version => INT16
		  finalized_features_epoch<tag: 1> => INT64
		  finalized_features<tag: 2> => name max_version_level min_version_level
		    name => COMPACT_STRING
		    max_version_level => INT16
		    min_version_level => INT16
		  zk_migration_ready<tag: 3> => BOOLEAN
	*/
	const ApiVersions_MIN_VERSION = 0
	const ApiVersions_MAX_VERSION = 4

	const DescribeTopicPartitions_MIN_VERSION = 0
	const DescribeTopicPartitions_MAX_VERSION = 0

	const ERROR_NONE = 0
	const ERROR_UNSUPPORTED_VERSION = 35

	var error_code uint16 = ERROR_NONE
	if request_api_version < ApiVersions_MIN_VERSION || request_api_version > ApiVersions_MAX_VERSION {
		error_code = ERROR_UNSUPPORTED_VERSION
	}

	body := []byte{}
	body = binary.BigEndian.AppendUint32(body, uint32(correlation_id)) // correlation_id (4 bytes)
	body = binary.BigEndian.AppendUint16(body, uint16(error_code))     // error_code (2 bytes)

	// This field is the length of the [api_keys] array.
	// Since we're working with version 4, a flexible version, this follows the N+1 syntax
	// See here: https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ApiVersionsResponse.json
	// 3 because the array is [ApiVersions, DescribeTopicPartitions]
	body = append(body, 3) // api_keys array length (1 byte)

	// Why is there a TAG_BUFFER after each array entry?
	// https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A%2BThe%2BKafka%2BProtocol%2Bshould%2BSupport%2BOptional%2BTagged%2BFields#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-TagSections
	// "In a flexible version, each structure ends with a tag section."
	// So we will often see a "00" between each entries, but semantically it belongs to the previous entry's tag section.

	// For ApiVersions API (Key: 18)
	body = binary.BigEndian.AppendUint16(body, uint16(18))                      // api_key (2 bytes)
	body = binary.BigEndian.AppendUint16(body, uint16(ApiVersions_MIN_VERSION)) // min_version (2 bytes)
	body = binary.BigEndian.AppendUint16(body, uint16(ApiVersions_MAX_VERSION)) // max_version (2 bytes)
	body = append(body, 0)                                                      // TAG_BUFFER (1 byte)

	// For DescribeTopicPartitions API (Key: 75)
	body = binary.BigEndian.AppendUint16(body, uint16(75))                                  // api_key (2 bytes)
	body = binary.BigEndian.AppendUint16(body, uint16(DescribeTopicPartitions_MIN_VERSION)) // min_version (2 bytes)
	body = binary.BigEndian.AppendUint16(body, uint16(DescribeTopicPartitions_MAX_VERSION)) // max_version (2 bytes)
	body = append(body, 0)                                                                  // TAG_BUFFER (1 byte)

	body = binary.BigEndian.AppendUint32(body, uint32(0)) // throttle_time_ms (4 bytes)

	// This last TAG_BUFFER is for the whole response
	body = append(body, 0) // TAG_BUFFER (1 byte)

	response = binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	response = append(response, body...)
	return
}
