package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	body = appendUint32(body, uint32(header.CorrelationID)) // correlation_id (4 bytes)
	body = appendUint16(body, uint16(error_code))           // error_code (2 bytes)

	// This field is the length of the [api_keys] array.
	// Since we're working with version 4, a flexible version, this follows the N+1 syntax
	// See here: https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ApiVersionsResponse.json
	body = appendUvarint(body, uint64(len(ApiKeyAndMinMaxVersions)+1)) // api_keys array length (unsigned varint)

	// Why is there a TAG_BUFFER after each array entry?
	// https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A%2BThe%2BKafka%2BProtocol%2Bshould%2BSupport%2BOptional%2BTagged%2BFields#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-TagSections
	// "In a flexible version, each structure ends with a tag section."
	// So we will often see a "00" between each entries, but semantically it belongs to the previous entry's tag section.

	for _, apiKeyAndMinMaxVersion := range ApiKeyAndMinMaxVersions {
		body = appendUint16(body, uint16(apiKeyAndMinMaxVersion.ApiKey))     // api_key (2 bytes)
		body = appendUint16(body, uint16(apiKeyAndMinMaxVersion.MinVersion)) // min_version (2 bytes)
		body = appendUint16(body, uint16(apiKeyAndMinMaxVersion.MaxVersion)) // max_version (2 bytes)
		body = append(body, 0)                                               // TAG_BUFFER (1 byte)
	}

	body = appendUint32(body, uint32(0)) // throttle_time_ms (4 bytes)

	// This last TAG_BUFFER is for the whole response
	body = append(body, 0) // TAG_BUFFER (1 byte)

	response = appendUint32(nil, uint32(len(body)))
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
		queryMetadata  *ClusterMetadataLogTopicMetadata
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
				queryErrorCode: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
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
	body = appendUint32(body, uint32(header.CorrelationID)) // correlation_id (4 bytes)
	body = append(body, 0)                                  // TAG_BUFFER (1 byte)

	// Body
	body = appendUint32(body, uint32(0)) // throttle_time_ms (4 bytes)

	body = appendUvarint(body, uint64(len(topicMetadataOrErrors)+1)) // topics array length (unsigned varint)

	for _, query := range topicMetadataOrErrors { // looping over each topic
		body = appendUint16(body, uint16(query.queryErrorCode))    // error_code (2 bytes)
		body = appendUvarint(body, uint64(len(query.queryName)+1)) // name length (unsigned varint)
		body = append(body, query.queryName...)                    // topic_name
		if query.queryMetadata == nil {
			body = append(body, make([]byte, 16)...) // topic_id(16 bytes)
		} else {
			body = append(body, query.queryMetadata.ID[:]...)
		}
		body = append(body, 0) // is_internal: false (1 byte)

		if query.queryMetadata == nil {
			body = append(body, 1) // partitions array length: 0 element (unsigned varint)
		} else {
			body = appendUvarint(body, uint64(len(query.queryMetadata.Partitions)+1)) // partitions array length (unsigned varint)
			for _, partitionMetadata := range query.queryMetadata.Partitions {        // looping over partitions
				body = appendUint16(body, uint16(0))                             // Error Code
				body = appendUint32(body, uint32(partitionMetadata.ID))          // Partition Index
				body = appendUint32(body, uint32(partitionMetadata.LeaderID))    // Leader ID
				body = appendUint32(body, uint32(partitionMetadata.LeaderEpoch)) // Leader Epoch

				body = appendUvarint(body, uint64(len(partitionMetadata.ReplicaNodes)+1)) // replica nodes array length
				for _, node := range partitionMetadata.ReplicaNodes {
					body = appendUint32(body, uint32(node))
				}

				body = appendUvarint(body, uint64(len(partitionMetadata.IsrNodes)+1)) // ISR nodes array length
				for _, node := range partitionMetadata.IsrNodes {
					body = appendUint32(body, uint32(node))
				}

				body = appendUvarint(body, uint64(1)) // Eligible Leader Replicas array length
				body = appendUvarint(body, uint64(1)) // Last Known ELR array length
				body = appendUvarint(body, uint64(1)) // Offline Replicas array length

				body = append(body, 0) // TAG_BUFFER (1 byte)
			}
		}
		body = appendUint32(body, uint32(0)) // topic_authorized_operations:  0 (4 bytes)
		body = append(body, 0)               // TAG_BUFFER (1 byte)

	}

	// Structs can be null
	body = append(body, 0xff) // // next_cursor: -1 (null) (1 byte)

	body = append(body, 0) // TAG_BUFFER (1 byte)
	response = appendUint32(nil, uint32(len(body)))
	response = append(response, body...)
	return
}

// ProduceRequest-specific struct
type ProducePartitionData struct {
	PartitionIndex int32
	RecordBatch    *RecordBatch
	// In ProduceRequest.java validateRecords says EXACTLY 1 RecordBatch per Partition
	// The field can be nil since the records byte blobs is COMPACT_NULLABLE_BYTES
}

type ProduceTopicData struct {
	TopicName       string
	TopicPartitions []ProducePartitionData
}

type ProducePartitionQueryResult struct {
	PartitionData ProducePartitionData
	Metadata      *ClusterMetadataLogPartitionMetadata // nil if partition was not found
	ErrorCode     int16                                // ERROR_NONE or UNKNOWN_TOPIC_OR_PARTITION
}

type ProduceTopicQueryResult struct {
	TopicData        ProduceTopicData
	Metadata         *ClusterMetadataLogTopicMetadata // nil if topic was not found
	ErrorCode        int16                            // ERROR_NONE or UNKNOWN_TOPIC_OR_PARTITION (debug only, does not get serialized)
	PartitionResults []ProducePartitionQueryResult
}

func handleProduce(frame *Frame, header *RequestHeaderV2) (response []byte, err error) {
	/*
		Produce Request (Version: 11) => transactional_id acks timeout_ms [topic_data]
			transactional_id => COMPACT_NULLABLE_STRING
			acks => INT16
			timeout_ms => INT32
			topic_data => name [partition_data]
			name => COMPACT_STRING
			partition_data => index records
				index => INT32
				records => COMPACT_RECORDS

		See more:
			Schema: https://github.com/apache/kafka/blob/22c1e445f17e82ac66800d8150ab46b5547e4035/clients/src/main/resources/common/message/ProduceRequest.json
				This JSON schema is for the bytes that are laid out to get sent over the wires.
				partition_data.records can be null, the byte blobs for the RecordBatch array
				might not exist, hence it's nullable in the schema.
			Source: https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/requests/ProduceRequest.java
				If partition_data.records CAN be parse into an array of RecordBaches, then
				ProduceRequest.validateRecords method confirms that Produce Requests
				are only allowed to contain EXACTLY 1 RecordBatch per partition.

		Produce Response (Version: 11) => [responses] throttle_time_ms [node_endpoints]<tag: 0>
		  responses => name [partition_responses]
		    name => COMPACT_STRING
		    partition_responses => index error_code base_offset log_append_time_ms log_start_offset [record_errors] error_message current_leader<tag: 0>
		      index => INT32
		      error_code => INT16
		      base_offset => INT64
		      log_append_time_ms => INT64
		      log_start_offset => INT64
		      record_errors => batch_index batch_index_error_message
		        batch_index => INT32
		        batch_index_error_message => COMPACT_NULLABLE_STRING
		      error_message => COMPACT_NULLABLE_STRING
		      current_leader<tag: 0> => leader_id leader_epoch
		        leader_id => INT32
		        leader_epoch => INT32
		  throttle_time_ms => INT32
		  node_endpoints<tag: 0> => node_id host port rack
		    node_id => INT32
		    host => COMPACT_STRING
		    port => INT32
		    rack => COMPACT_NULLABLE_STRING
	*/

	// Parsing Request
	_, err = frame.ReadCompactNullableString() // transactional_id (COMPACT_NULLABLE_STRING)
	if err != nil {
		return nil, fmt.Errorf("failed reading transactional id: %v", err)
	}

	_, err = frame.ReadInt16() // acks (INT16)
	if err != nil {
		return nil, fmt.Errorf("failed reading acks: %v", err)
	}

	_, err = frame.ReadInt32() // timeout_ms (INT32)
	if err != nil {
		return nil, fmt.Errorf("failed reading timeout ms: %v", err)
	}

	topics, err := ParseProduceTopics(frame)
	if err != nil {
		return nil, fmt.Errorf("failed parsing topics array: %v", err)
	}

	// fmt.Printf("%+v\n", topics)
	// fmt.Printf("%+v\n", metadata)

	produceTopicQueryResults := QueryProduceTopics(topics, metadata)
	// fmt.Printf("%+v\n", produceTopicQueryResults)

	// Once we have the result, we persist them on the filesystem
	if err := PersistProduceRecords(logDir, produceTopicQueryResults); err != nil {
		return nil, err
	}

	// Building Response
	body := []byte{}

	// Response Header v1 (this one has tag buffer)
	body = appendUint32(body, uint32(header.CorrelationID)) // correlation_id (4 bytes)
	body = append(body, 0)                                  // TAG_BUFFER (1 byte)

	body = appendUvarint(body, uint64(len(produceTopicQueryResults)+1)) // topic query array length + 1 (unsigned varint)
	for _, produceTopicQueryResult := range produceTopicQueryResults {  // looping over each query topic
		body = appendUvarint(body, uint64(len(produceTopicQueryResult.TopicData.TopicName)+1)) // topic_name length + 1 (unsigned varint)
		body = append(body, produceTopicQueryResult.TopicData.TopicName...)                    // topic_name

		body = appendUvarint(body, uint64(len(produceTopicQueryResult.PartitionResults)+1)) // partition query array length + 1 (unsigned varint)
		for _, producePartitionResult := range produceTopicQueryResult.PartitionResults {   // looping over each query partiion for a topic
			body = appendUint32(body, uint32(producePartitionResult.PartitionData.PartitionIndex)) // partition_index (4 bytes)

			body = appendUint16(body, uint16(producePartitionResult.ErrorCode)) // error_code (2 bytes)

			if producePartitionResult.ErrorCode == ERROR_UNKNOWN_TOPIC_OR_PARTITION {
				body = appendUint64(body, 0xffffffffffffffff) // base_offset (8 bytes)
			} else {
				body = appendUint64(body, 0)
			}
			body = appendUint64(body, 0xffffffffffffffff) // log_append_time (8 bytes)

			if producePartitionResult.ErrorCode == ERROR_UNKNOWN_TOPIC_OR_PARTITION {
				body = appendUint64(body, 0xffffffffffffffff) // log_start_offset (8 bytes)
			} else {
				body = appendUint64(body, 0)
			}

			body = appendUvarint(body, uint64(0+1)) // record errors array length + 1 (unsigned varint) empty array, no array blob after

			body = appendUvarint(body, uint64(0)) // error message length (unsigned varint) null string, no blob after this

			body = append(body, 0) // TAG_BUFFER (1 byte)
		}

		body = append(body, 0) // TAG_BUFFER (1 byte)
	}

	body = appendUint32(body, uint32(0)) // throttle_time_ms (4 bytes)

	body = append(body, 0) // TAG_BUFFER (1 byte)
	response = appendUint32(nil, uint32(len(body)))
	response = append(response, body...)
	return
}

func ParseProduceTopics(frame *Frame) ([]ProduceTopicData, error) {
	compact_topic_data_array_length, err := frame.ReadUvarint()
	if err != nil {
		return nil, fmt.Errorf("failed reading topic data array length: %v", err)
	}

	// topic_data array does not have nullableVersion, the bytes blob for it must exist
	if compact_topic_data_array_length == 0 {
		return nil, fmt.Errorf("topic_data array can not be null")
	}

	var topics []ProduceTopicData
	for range compact_topic_data_array_length - 1 {
		topicName, err := frame.ReadCompactString() // topic name
		if err != nil {
			return nil, fmt.Errorf("failed reading topic name: %v", err)
		}

		partitions, err := ParseProducePartitions(frame)
		if err != nil {
			return nil, fmt.Errorf("failed reading topic partition data: %v", err)
		}

		_, err = frame.ReadByte() // TAG_BUFFER for this topic
		if err != nil {
			return nil, fmt.Errorf("read topic tag buffer: %v", err)
		}

		topics = append(topics, ProduceTopicData{
			TopicName:       topicName,
			TopicPartitions: partitions,
		})
	}

	return topics, nil
}

func ParseProducePartitions(frame *Frame) ([]ProducePartitionData, error) {
	compact_partition_data_array_length, err := frame.ReadUvarint()
	if err != nil {
		return nil, fmt.Errorf("failed reading partition data array length: %v", err)
	}

	if compact_partition_data_array_length == 0 {
		return nil, fmt.Errorf("partition data array can not be null")
	}

	var partitions []ProducePartitionData
	for range compact_partition_data_array_length - 1 {
		partitionIndex, err := frame.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading partition index: %v", err)
		}

		compact_record_batch_array_length, err := frame.ReadUvarint()
		if err != nil {
			return nil, fmt.Errorf("failed reading partition data array length: %v", err)
		}

		var recordBatch *RecordBatch = nil

		if compact_record_batch_array_length > 1 {
			record_batches_byte_blob, err := frame.ReadBytes(int(compact_record_batch_array_length - 1))
			if err != nil {
				return nil, fmt.Errorf("failed reading record batches byte blob: %v", err)
			}

			record_batches_frame := NewFrame(record_batches_byte_blob)

			record_batch_array, err := parseRecordBatches(&record_batches_frame)

			if err != nil {
				return nil, fmt.Errorf("failed parsing record batches: %v", err)
			}

			if len(record_batch_array) == 0 {
				return nil, fmt.Errorf("must have at least one record batch for this partition")
			} else if len(record_batch_array) > 1 {
				return nil, fmt.Errorf("must have exactly one record batch for this partition")
			}

			recordBatch = &record_batch_array[0]
		}

		_, err = frame.ReadByte() // TAG_BUFFER for this partition
		if err != nil {
			return nil, fmt.Errorf("read partition tag buffer: %v", err)
		}

		partitions = append(partitions, ProducePartitionData{
			PartitionIndex: partitionIndex,
			RecordBatch:    recordBatch,
		})

	}

	return partitions, nil
}

func QueryProduceTopics(
	topics []ProduceTopicData,
	metadata map[TopicName]ClusterMetadataLogTopicMetadata,
) []ProduceTopicQueryResult {
	results := make([]ProduceTopicQueryResult, 0, len(topics))

	for _, topic := range topics {
		topicResult := ProduceTopicQueryResult{
			TopicData: topic,
			ErrorCode: ERROR_NONE,
			PartitionResults: make(
				[]ProducePartitionQueryResult,
				0,
				len(topic.TopicPartitions),
			),
		}

		topicMeta, topicExists := metadata[topic.TopicName]
		if !topicExists {
			topicResult.ErrorCode = ERROR_UNKNOWN_TOPIC_OR_PARTITION
		} else {
			topicMetaCopy := topicMeta
			topicResult.Metadata = &topicMetaCopy
		}

		for _, partition := range topic.TopicPartitions {
			partitionResult := ProducePartitionQueryResult{
				PartitionData: partition,
				ErrorCode:     ERROR_NONE,
			}

			if !topicExists {
				// Topic missing means this requested topic-partition is invalid.
				partitionResult.ErrorCode = ERROR_UNKNOWN_TOPIC_OR_PARTITION
			} else {
				partitionMeta, ok := findPartitionMetadata(
					topicMeta.Partitions,
					partition.PartitionIndex,
				)
				if !ok {
					partitionResult.ErrorCode = ERROR_UNKNOWN_TOPIC_OR_PARTITION
					topicResult.ErrorCode = ERROR_UNKNOWN_TOPIC_OR_PARTITION
				} else {
					partitionMetaCopy := partitionMeta
					partitionResult.Metadata = &partitionMetaCopy
				}
			}

			topicResult.PartitionResults = append(
				topicResult.PartitionResults,
				partitionResult,
			)
		}

		results = append(results, topicResult)
	}

	return results
}

func findPartitionMetadata(
	partitions []ClusterMetadataLogPartitionMetadata,
	id PartitionID,
) (ClusterMetadataLogPartitionMetadata, bool) {
	for _, partition := range partitions {
		if partition.ID == id {
			return partition, true
		}
	}
	return ClusterMetadataLogPartitionMetadata{}, false
}

func (b RecordBatch) ValidateSerializable() error {
	if b.magic != 2 {
		return fmt.Errorf("record batch magic must be 2, got %d", b.magic)
	}

	if b.attributes&0x07 != 0 {
		return fmt.Errorf("compressed record batches are not supported in this serializer: attributes=%d", b.attributes)
	}

	if len(b.records) == 0 {
		return fmt.Errorf("record batch must contain at least one record")
	}

	return nil
}

func PersistProduceRecords(
	logDir string,
	results []ProduceTopicQueryResult,
) error {
	for _, topicResult := range results {
		topicName := topicResult.TopicData.TopicName

		for _, partitionResult := range topicResult.PartitionResults {
			if partitionResult.ErrorCode != ERROR_NONE {
				continue
			}

			batch := partitionResult.PartitionData.RecordBatch
			if batch == nil {
				return fmt.Errorf(
					"cannot persist nil record batch: topic=%q partition=%d",
					topicName,
					partitionResult.PartitionData.PartitionIndex,
				)
			}

			// Work on a shallow copy so response/query structs stay unchanged.
			batchToWrite := *batch

			if partitionResult.Metadata != nil {
				batchToWrite.partitionLeaderEpoch = partitionResult.Metadata.LeaderEpoch
			}

			if err := batchToWrite.ValidateSerializable(); err != nil {
				return fmt.Errorf(
					"invalid record batch: topic=%q partition=%d: %w",
					topicName,
					partitionResult.PartitionData.PartitionIndex,
					err,
				)
			}

			data := batchToWrite.Bytes()

			partitionDir := filepath.Join(
				logDir,
				fmt.Sprintf("%s-%d", topicName, partitionResult.PartitionData.PartitionIndex),
			)

			if err := os.MkdirAll(partitionDir, 0755); err != nil {
				return fmt.Errorf("create partition log dir %q: %w", partitionDir, err)
			}

			logPath := filepath.Join(partitionDir, "00000000000000000000.log")

			f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				return fmt.Errorf("open log file %q: %w", logPath, err)
			}

			_, writeErr := f.Write(data)
			closeErr := f.Close()

			if writeErr != nil {
				return fmt.Errorf("write record batch to %q: %w", logPath, writeErr)
			}

			if closeErr != nil {
				return fmt.Errorf("close log file %q: %w", logPath, closeErr)
			}
		}
	}

	return nil
}
