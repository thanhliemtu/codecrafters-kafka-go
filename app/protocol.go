package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

type RequestHeaderV2 struct {
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
	ClientID          *string
}

func handleApiVersions(frame *Frame, header *RequestHeaderV2) (response []byte, err error) {
	if frame == nil || header == nil {
		return []byte{}, errors.New("ApiVersions: received null inputs")
	}
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

	var error_code uint16 = ERROR_NONE
	if header.RequestAPIVersion < int16(ApiKeyAndMinMaxVersions[18].MinVersion) || header.RequestAPIVersion > int16(ApiKeyAndMinMaxVersions[18].MaxVersion) {
		error_code = ERROR_UNSUPPORTED_VERSION
	}

	body := []byte{}
	body = binary.BigEndian.AppendUint32(body, uint32(header.CorrelationID)) // correlation_id (4 bytes)
	body = binary.BigEndian.AppendUint16(body, uint16(error_code))           // error_code (2 bytes)

	// This field is the length of the [api_keys] array.
	// Since we're working with version 4, a flexible version, this follows the N+1 syntax
	// See here: https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ApiVersionsResponse.json
	body = binary.AppendUvarint(body, uint64(len(ApiKeyAndMinMaxVersions)+1)) // api_keys array length (unsigned varint)

	// Why is there a TAG_BUFFER after each array entry?
	// https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A%2BThe%2BKafka%2BProtocol%2Bshould%2BSupport%2BOptional%2BTagged%2BFields#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-TagSections
	// "In a flexible version, each structure ends with a tag section."
	// So we will often see a "00" between each entries, but semantically it belongs to the previous entry's tag section.

	for _, apiKeyAndMinMaxVersion := range ApiKeyAndMinMaxVersions {
		body = binary.BigEndian.AppendUint16(body, uint16(apiKeyAndMinMaxVersion.ApiKey))     // api_key (2 bytes)
		body = binary.BigEndian.AppendUint16(body, uint16(apiKeyAndMinMaxVersion.MinVersion)) // min_version (2 bytes)
		body = binary.BigEndian.AppendUint16(body, uint16(apiKeyAndMinMaxVersion.MaxVersion)) // max_version (2 bytes)
		body = append(body, 0)                                                                // TAG_BUFFER (1 byte)
	}

	body = binary.BigEndian.AppendUint32(body, uint32(0)) // throttle_time_ms (4 bytes)

	// This last TAG_BUFFER is for the whole response
	body = append(body, 0) // TAG_BUFFER (1 byte)

	response = binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	response = append(response, body...)
	return
}

func handleDescribeTopicPartitions(frame *Frame, header *RequestHeaderV2) (response []byte, err error) {
	if frame == nil || header == nil {
		return []byte{}, errors.New("DescribeTopicPartitions: received null inputs")
	}
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
	var topicQueries []string
	compact_topics_len, err := frame.ReadUvarint()

	if err != nil {
		return []byte{}, fmt.Errorf("read topics length: %v", err)
	}

	if compact_topics_len == 0 {
		return []byte{}, fmt.Errorf("topics can't be null: %v", err)
	}

	for range compact_topics_len - 1 {
		topicQuery, err := frame.ReadCompactString() // topic_name
		if err != nil {
			return []byte{}, fmt.Errorf("read topic: %v", err)
		}

		_, err = frame.ReadByte() // TAG_BUFFER (1 byte)
		if err != nil {
			return []byte{}, fmt.Errorf("read tag buffer: %v", err)
		}

		topicQueries = append(topicQueries, topicQuery)
	}

	if len(topicQueries) == 0 {
		return []byte{}, fmt.Errorf("empty topic array: %v", err)
	}

	type topicMetadataOrError struct {
		queryName      string
		queryMetadata  *topicMetadata
		queryErrorCode uint16
	}

	var topicMetadataOrErrors []topicMetadataOrError

	// sorting topicQueries so that the response order stays consistent
	slices.Sort(topicQueries)
	for _, topicQuery := range topicQueries {
		val, ok := metadata[topicQuery]
		if !ok {
			temp := topicMetadataOrError{
				queryName:      topicQuery,
				queryMetadata:  nil,
				queryErrorCode: UNKNOWN_TOPIC_OR_PARTITION,
			}
			topicMetadataOrErrors = append(topicMetadataOrErrors, temp)
		} else {
			temp := topicMetadataOrError{
				queryName:      topicQuery,
				queryMetadata:  &val,
				queryErrorCode: ERROR_NONE,
			}
			topicMetadataOrErrors = append(topicMetadataOrErrors, temp)
		}
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

	body = binary.AppendUvarint(body, uint64(len(topicMetadataOrErrors)+1)) // topics array length (unsigned varint)

	for _, query := range topicMetadataOrErrors { // looping over each topic
		body = binary.BigEndian.AppendUint16(body, uint16(query.queryErrorCode)) // error_code (2 bytes)
		body = binary.AppendUvarint(body, uint64(len(query.queryName)+1))        // name length (unsigned varint)
		body = append(body, query.queryName...)                                  // topic_name
		if query.queryMetadata == nil {
			body = append(body, make([]byte, 16)...) // topic_id(16 bytes)
		} else {
			body = append(body, query.queryMetadata.ID[:]...)
		}
		body = append(body, 0) // is_internal: false (1 byte)

		if query.queryMetadata == nil {
			body = append(body, 1) // partitions array length: 0 element (unsigned varint)
		} else {
			body = binary.AppendUvarint(body, uint64(len(query.queryMetadata.Partitions)+1)) // partitions array length (unsigned varint)
			for _, partitionMetadata := range query.queryMetadata.Partitions {               // looping over partitions
				body = binary.BigEndian.AppendUint16(body, uint16(0))                             // Error Code
				body = binary.BigEndian.AppendUint32(body, uint32(partitionMetadata.ID))          // Partition Index
				body = binary.BigEndian.AppendUint32(body, uint32(partitionMetadata.LeaderID))    // Leader ID
				body = binary.BigEndian.AppendUint32(body, uint32(partitionMetadata.LeaderEpoch)) // Leader Epoch

				body = binary.AppendUvarint(body, uint64(len(partitionMetadata.ReplicaNodes)+1)) // replica nodes array length
				for _, node := range partitionMetadata.ReplicaNodes {
					body = binary.BigEndian.AppendUint32(body, uint32(node))
				}

				body = binary.AppendUvarint(body, uint64(len(partitionMetadata.IsrNodes)+1)) // ISR nodes array length
				for _, node := range partitionMetadata.IsrNodes {
					body = binary.BigEndian.AppendUint32(body, uint32(node))
				}

				body = binary.AppendUvarint(body, uint64(1)) // Eligible Leader Replicas array length
				body = binary.AppendUvarint(body, uint64(1)) // Last Known ELR array length
				body = binary.AppendUvarint(body, uint64(1)) // Offline Replicas array length

				body = append(body, 0) // TAG_BUFFER (1 byte)
			}
		}
		body = binary.BigEndian.AppendUint32(body, uint32(0)) // topic_authorized_operations:  0 (4 bytes)
		body = append(body, 0)                                // TAG_BUFFER (1 byte)

	}

	// Structs can be null
	body = append(body, 0xff) // // next_cursor: -1 (null) (1 byte)

	body = append(body, 0) // TAG_BUFFER (1 byte)
	response = binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	response = append(response, body...)
	return
}

func handleProduce(frame *Frame, header *RequestHeaderV2) (response []byte, err error) {
	return nil, nil
}
