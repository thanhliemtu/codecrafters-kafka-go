package main

import (
	"encoding/binary"
	"fmt"
)

type RequestHeaderV2 struct {
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
	ClientID          *string
}

const (
	ERROR_NONE                 = 0
	UNKNOWN_TOPIC_OR_PARTITION = 3
	ERROR_UNSUPPORTED_VERSION  = 35
)

func handleApiVersions(frame Frame, header RequestHeaderV2) (response []byte, err error) {
	// https://kafka.apache.org/42/design/protocol/#The_Messages_ApiVersions

	/*
		ApiVersions Request (Version: 4) => client_software_name client_software_version
		client_software_name => COMPACT_STRING
		client_software_version => COMPACT_STRING
	*/
	// For now we don't seem to need the request body so we will skip to assembling the response.
	// client_software_name, err := frame.ReadCompactString()
	// client_software_version, err := frame.ReadCompactString()

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

		  Response header version: 0
	*/
	const ApiVersions_MIN_VERSION = 0
	const ApiVersions_MAX_VERSION = 4

	const DescribeTopicPartitions_MIN_VERSION = 0
	const DescribeTopicPartitions_MAX_VERSION = 0

	var error_code uint16 = ERROR_NONE
	if header.RequestAPIVersion < ApiVersions_MIN_VERSION || header.RequestAPIVersion > ApiVersions_MAX_VERSION {
		error_code = ERROR_UNSUPPORTED_VERSION
	}

	body := []byte{}
	body = binary.BigEndian.AppendUint32(body, uint32(header.CorrelationID)) // correlation_id (4 bytes)
	body = binary.BigEndian.AppendUint16(body, uint16(error_code))           // error_code (2 bytes)

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

func handleDescribeTopicPartitions(frame Frame, header RequestHeaderV2) (response []byte, err error) {
	/*
		DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor
		  topics => name
		    name => COMPACT_STRING
		  response_partition_limit => INT32
		  cursor => topic_name partition_index
		    topic_name => COMPACT_STRING
		    partition_index => INT32
	*/
	/*
		https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeTopicPartitionsRequest.json
		The topics array does not have a nullable version, so it can't be null, only empty.

		https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/README.md#nullable-fields
		Kafka’s message-definition README states that arrays may optionally be nullable,
		and to make a field nullable you must set nullableVersions.

		Also topics length follows the N+1 syntax
	*/
	var topics []string
	compact_topics_len, err := frame.ReadUvarint()

	if err != nil {
		return []byte{}, fmt.Errorf("read topics length: %v", err)
	}

	if compact_topics_len == 0 {
		return []byte{}, fmt.Errorf("topics can't be null: %v", err)
	}

	for range compact_topics_len - 1 {
		topic, err := frame.ReadCompactString() // topic_name
		if err != nil {
			return []byte{}, fmt.Errorf("read topic: %v", err)
		}

		_, err = frame.ReadByte() // TAG_BUFFER (1 byte)
		if err != nil {
			return []byte{}, fmt.Errorf("read tag buffer: %v", err)
		}

		topics = append(topics, topic)
	}

	if len(topics) == 0 {
		return []byte{}, fmt.Errorf("empty topic array: %v", err)
	}

	topic := topics[0]

	error_code := ERROR_NONE
	if topic != "foo" {
		error_code = UNKNOWN_TOPIC_OR_PARTITION
	}

	/*
		DescribeTopicPartitions Response (Version: 0) => throttle_time_ms [topics] next_cursor
		  throttle_time_ms => INT32
		  topics => error_code name topic_id is_internal [partitions] topic_authorized_operations
		    error_code => INT16
		    name => COMPACT_NULLABLE_STRING
		    topic_id => UUID
		    is_internal => BOOLEAN
		    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [eligible_leader_replicas] [last_known_elr] [offline_replicas]
		      error_code => INT16
		      partition_index => INT32
		      leader_id => INT32
		      leader_epoch => INT32
		      replica_nodes => INT32
		      isr_nodes => INT32
		      eligible_leader_replicas => INT32
		      last_known_elr => INT32
		      offline_replicas => INT32
		    topic_authorized_operations => INT32
		  next_cursor => topic_name partition_index
		    topic_name => COMPACT_STRING
		    partition_index => INT32

			Response header version: 1

			https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json
	*/
	body := []byte{}

	// Response Header v1 (this one has tag buffer)
	body = binary.BigEndian.AppendUint32(body, uint32(header.CorrelationID)) // correlation_id (4 bytes)
	body = append(body, 0)                                                   // TAG_BUFFER (1 byte)

	// Body
	body = binary.BigEndian.AppendUint32(body, uint32(0)) // throttle_time_ms (4 bytes)

	body = append(body, 2) // topics array length: 1 element (1 byte)

	body = binary.BigEndian.AppendUint16(body, uint16(error_code))           // error_code: 3 (2 bytes)
	body = append(body, 4)                                                   // name length: 3 (compact string) (1 byte)
	body = append(body, topic...)                                            // topic_name: "foo" (3 bytes)
	body = append(body, "0000000000000000"...)                               // topic_id: 0000000000000000 (16 bytes)
	body = append(body, 0)                                                   // is_internal: false (1 byte)
	body = append(body, 1)                                                   // partitions array: 0 element (1 byte)
	body = binary.BigEndian.AppendUint32(body, uint32(header.CorrelationID)) // topic_authorized_operations:  0 (4 bytes)
	body = append(body, 0)                                                   // TAG_BUFFER (1 byte)

	// Structs can be null
	body = append(body, 0xff) // // next_cursor: -1 (null) (1 byte)

	body = append(body, 0) // TAG_BUFFER (1 byte)
	response = binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	response = append(response, body...)
	return
}
