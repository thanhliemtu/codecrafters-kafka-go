package main

import (
	"fmt"
	"os"
)

type RecordBatch struct {
	baseOffset           int64
	batchLength          int32
	partitionLeaderEpoch int32
	magic                int8 // current magic value is 2
	crc                  uint32
	attributes           int16
	/*
		bit 0~2:
			0: no compression
			1: gzip
			2: snappy
			3: lz4
			4: zstd
		bit 3: timestampType
		bit 4: isTransactional (0 means not transactional)
		bit 5: isControlBatch (0 means not a control batch)
		bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
		bit 7~15: unused
	*/
	lastOffsetDelta int32
	baseTimestamp   int64
	maxTimestamp    int64
	producerId      int64
	producerEpoch   int16
	baseSequence    int32
	recordsCount    int32
	records         []Record
}

type Record struct {
	length     int32
	attributes int8
	/*
		bit 0~7: unused
	*/
	timestampDelta int64
	offsetDelta    int32
	keyLength      int32
	key            []byte
	valueLength    int32
	value          []byte
	headersCount   int32
	headers        []RecordHeader
}

type RecordHeader struct {
	headerKeyLength   int32
	headerKey         string
	headerValueLength int32
	value             []byte
}

func parseClusterMetadataLog(path string) ([]*RecordBatch, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cluster metadata log: %w", err)
	}

	file := NewFrame(data)

	var recordBatches []*RecordBatch
	for file.Remaining() > 0 {
		recordBatch := &RecordBatch{}

		recordBatch.baseOffset, err = file.ReadInt64()
		if err != nil {
			return nil, fmt.Errorf("failed reading baseOffset: %v", err)
		}

		recordBatch.batchLength, err = file.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading batchLength: %v", err)
		}

		recordBatch.partitionLeaderEpoch, err = file.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading partitionLeaderEpoch: %v", err)
		}

		recordBatch.magic, err = file.ReadInt8()
		if err != nil {
			return nil, fmt.Errorf("failed reading magic: %v", err)
		}
		if recordBatch.magic != 2 {

		}

		recordBatches = append(recordBatches, recordBatch)
	}

	// TODO!
	/*
		Kafka stores metadata about topics in the __cluster_metadata topic.
		This is an internal topic that contains records about topic creation,
		partition assignments, and other cluster configuration.
		To check if a topic exists and get its metadata,
		you'll need to read the cluster metadata log file.

		The log file is located at:
		/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log

		The log file contains Record Batches.
		The documentation for the on-disk format of the RecordBatch and Record structs is here:
		https://kafka.apache.org/42/implementation/message-format/

		RecordBatch:
		baseOffset: int64
		batchLength: int32
		partitionLeaderEpoch: int32
		magic: int8 (current magic value is 2)
		crc: uint32
		attributes: int16
			bit 0~2:
				0: no compression
				1: gzip
				2: snappy
				3: lz4
				4: zstd
			bit 3: timestampType
			bit 4: isTransactional (0 means not transactional)
			bit 5: isControlBatch (0 means not a control batch)
			bit 6: hasDeleteHorizonMs (0 means baseTimestamp is not set as the delete horizon for compaction)
			bit 7~15: unused
		lastOffsetDelta: int32
		baseTimestamp: int64
		maxTimestamp: int64
		producerId: int64
		producerEpoch: int16
		baseSequence: int32
		recordsCount: int32
		records: [Record]

		Record:
		length: varint
		attributes: int8
			bit 0~7: unused
		timestampDelta: varlong
		offsetDelta: varint
		keyLength: varint
		key: byte[]
		valueLength: varint
		value: byte[]
		headersCount: varint
		Headers => [Header]

		Header:
		headerKeyLength: varint
		headerKey: String
		headerValueLength: varint
		Value: byte[]

		VARINT:
		Represents an integer between -231 and 231-1 inclusive.
		Encoding follows the variable-length zig-zag encoding from Google Protocol Buffers.
		https://protobuf.dev/programming-guides/encoding/

		To actually parse a Record, we must look at the Record.value field, which is a byte array.
		To understand how the Record.value field is serialized,
		we look at this Kafka souce code file:
		https://github.com/apache/kafka/blob/5b3027dfcbcb62d169d4b4421260226e620459af/server-common/src/main/java/org/apache/kafka/server/common/serialization/AbstractApiMessageSerde.java

		public void write(ApiMessageAndVersion data,
						ObjectSerializationCache serializationCache,
						Writable out) {
			out.writeUnsignedVarint(DEFAULT_FRAME_VERSION);
			out.writeUnsignedVarint(data.message().apiKey());
			out.writeUnsignedVarint(data.version());
			data.message().write(out, serializationCache, data.version());
		}

		The AbstractApiMessageSerde.write method shows that there will be 3 common fields:
			Frame Version: int8
				Frame Version is a 1-byte integer indicating the version of the format of the record.
				The line "out.writeUnsignedVarint(DEFAULT_FRAME_VERSION);" and
				"private static final short DEFAULT_FRAME_VERSION = 1;" shows that the
				FrameVersion will always be 1.
			Type: int8
				Type is a 1-byte integer indicating the type of the record.
				Also known as apiKey.
			Version: int8
				Version is a 1-byte integer indicating the version of the partition record.
		Note that these are not official field names.

		To see how the bytes are laid out, check out: https://binspec.org/kafka-cluster-metadata

		Since there are many types of Record, they are determined using the Type field, aka apiKey.
		For a list of Record, check out: https://github.com/apache/kafka/tree/5b3027dfcbcb62d169d4b4421260226e620459af/metadata/src/main/resources/common/metadata

		For this task, we are interested in the following metadata Record:
			TopicRecord, PartitionRecord, and FeatureLevelRecord.
		Their schemas can be found in the list of Record link above.

		We will need parse this file and extract the following:
			Topic names and their UUIDs
			Partition IDs for each topic
	*/

	return recordBatches, nil
}
