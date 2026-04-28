package main

import (
	"errors"
	"fmt"
	"os"
)

// a slice or a []byte field inside a struct is just a header (24 bytes: pointer, length, capacity).
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
	records         []Record
}

type Record struct {
	attributes int8
	/*
		bit 0~7: unused
	*/
	timestampDelta int64
	offsetDelta    int32
	key            []byte
	value          []byte // [header[data_frame_version api_key version] message]
	headers        []RecordHeader
}

type RecordHeader struct {
	headerKeyLength   int32
	headerKey         string
	headerValueLength int32
	value             []byte
}

// This is the header of the Record.value field
type RecordValueHeader struct {
	FrameVersion  int16
	RecordType    int16
	RecordVersion int16
}

type partitionID = int32
type topicName = string
type topicID = [16]byte
type topicPartitions = []partitionID // a topic can have mutliple partitions

type topicMetadata struct {
	ID         topicID
	Partitions topicPartitions
}

/*
00000000000000000000.log
└── RecordBatch
    ├── baseOffset
    ├── batchLength
    ├── partitionLeaderEpoch
    ├── magic
    ├── crc
    ├── attributes
    ├── ...
    └── records
        └── Record
            ├── length
            ├── attributes
            ├── timestampDelta
            ├── offsetDelta
            ├── keyLength
            ├── key
            ├── valueLen
            ├── value
            └── headers
*/

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

	Frame Version: UNSIGNED_VARINT
		Frame Version is an UNSIGNED_VARINT indicating the version of the format of the record.
		The line "out.writeUnsignedVarint(DEFAULT_FRAME_VERSION);" and
		"private static final short DEFAULT_FRAME_VERSION = 1;" shows that the
		FrameVersion will always be 1.
	Type: UNSIGNED_VARINT
		Type is an UNSIGNED_VARINT indicating the type of the record.
		Also known as apiKey.
	Version: UNSIGNED_VARINT
		Version is an UNSIGNED_VARINT indicating the version of the partition record.

Note that these are not official field names.

To see how the bytes are laid out, check out: https://binspec.org/kafka-cluster-metadata
There is a discrepancy in binspec, which says they're 1 byte, but the source code uses
Readable.readUnsignedVarint and Writable.writeUnsignedVarint, so they must be UNSIGNED_VARINT.

One thing to note, when parsing, these values are small enough to fit in an int16, or even int8,
but use int16 to be safe since the underlying source code use short (which is int16 in java).

So the picture is this:
On the wire:
frameVersion -> UNSIGNED_VARINT
apiKey       -> UNSIGNED_VARINT
version      -> UNSIGNED_VARINT

In Kafka's Java model:
frameVersion -> short
apiKey       -> short
version      -> short

Which is why AbstractApiMessageSerde does this:
short frameVersion = unsignedIntToShort(input, "frame version");
short apiKey = unsignedIntToShort(input, "type");
short version = unsignedIntToShort(input, "version");

Use UNSIGNED_VARINT encoding because small integers are compact,
but semantically these are short-sized identifiers.

Since there are many types of Record, they are determined using the Type field, aka apiKey.
For a list of Record, check out: https://github.com/apache/kafka/tree/5b3027dfcbcb62d169d4b4421260226e620459af/metadata/src/main/resources/common/metadata

For this task, we are interested in the following metadata Record:

	TopicRecord, PartitionRecord, and FeatureLevelRecord.

Their schemas can be found in the list of Record link above.

We will need parse this file and extract the following:

	Topic names and their UUIDs
	Partition IDs for each topic
*/
func parseClusterMetadataLog(path string) ([]RecordBatch, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cluster metadata log: %w", err)
	}

	file := NewFrame(data)

	var recordBatches []RecordBatch
	for file.Remaining() > 0 {
		recordBatch := RecordBatch{}

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
			return nil, fmt.Errorf("unsupported record batch magic: %d", recordBatch.magic)
		}

		_, err = file.ReadBytes(4) // crc
		if err != nil {
			return nil, fmt.Errorf("failed reading crc: %v", err)
		}

		attributes, err := file.ReadInt16()
		if err != nil {
			return nil, fmt.Errorf("failed reading attributes: %v", err)
		}

		compression := attributes & 0x0007
		if compression != 0 {
			return nil, fmt.Errorf("unsupported compressed record batch, compression type: %d", compression)
		}

		recordBatch.lastOffsetDelta, err = file.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading last offset delta: %v", err)
		}

		recordBatch.baseTimestamp, err = file.ReadInt64()
		if err != nil {
			return nil, fmt.Errorf("failed reading base time stamp: %v", err)
		}

		recordBatch.maxTimestamp, err = file.ReadInt64()
		if err != nil {
			return nil, fmt.Errorf("failed reading max time stamp: %v", err)
		}

		recordBatch.producerId, err = file.ReadInt64()
		if err != nil {
			return nil, fmt.Errorf("failed reading producer id: %v", err)
		}

		recordBatch.producerEpoch, err = file.ReadInt16()
		if err != nil {
			return nil, fmt.Errorf("failed reading producer epoch: %v", err)
		}

		recordBatch.baseSequence, err = file.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading base sequence: %v", err)
		}

		recordsCount, err := file.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading records count: %v", err)
		}

		record := Record{}
		for range recordsCount {
			_, err = file.ReadVarint() // record length, don't think we're gonna need it or store it
			if err != nil {
				return nil, fmt.Errorf("failed reading record length: %v", err)
			}

			record.attributes, err = file.ReadInt8()
			if err != nil {
				return nil, fmt.Errorf("failed reading record attributes: %v", err)
			}

			record.timestampDelta, err = file.ReadVarint()
			if err != nil {
				return nil, fmt.Errorf("failed reading record time stamp delta: %v", err)
			}

			record.offsetDelta, err = file.ReadVarint32()
			if err != nil {
				return nil, fmt.Errorf("failed reading record offset delta: %v", err)
			}

			keyLength, err := file.ReadVarint32()
			if err != nil {
				return nil, fmt.Errorf("failed reading record key length: %v", err)
			}

			if keyLength > 0 { // keylength can be -1, which is null
				record.key, err = file.ReadBytes(int(keyLength))
				if err != nil {
					return nil, fmt.Errorf("failed reading record key: %v", err)
				}
			}

			valueLength, err := file.ReadVarint32()
			if err != nil {
				return nil, fmt.Errorf("failed reading record value length: %v", err)
			}

			record.value, err = file.ReadBytes(int(valueLength))
			if err != nil {
				return nil, fmt.Errorf("failed reading record value: %v", err)
			}

			headersCount, err := file.ReadVarint32()
			if err != nil {
				return nil, fmt.Errorf("failed reading record header count: %v", err)
			}

			if headersCount > 0 { // keylength can be 0, meaning no headers
				return nil, errors.New("Expect zero headers count")
			}

			recordBatch.records = append(recordBatch.records, record)
		}

		recordBatches = append(recordBatches, recordBatch)
	}

	return recordBatches, nil
}

func flattenRecordBatch(recordBatch []RecordBatch) []Record {
	var records []Record
	for _, batch := range recordBatch {
		for _, record := range batch.records {
			records = append(records, record)
		}
	}
	return records
}

func parseRecordValueHeader(value *Frame) (RecordValueHeader, error) {
	frameVersion, err := value.ReadUvarintAsInt16("frame version")
	if err != nil {
		return RecordValueHeader{}, fmt.Errorf("failed reading record frame version: %v", err)
	}
	if frameVersion != 1 {
		return RecordValueHeader{}, fmt.Errorf("expect frame version 1, got: %d", frameVersion)
	}

	recordType, err := value.ReadUvarintAsInt16("type")
	if err != nil {
		return RecordValueHeader{}, fmt.Errorf("failed reading record type: %v", err)
	}

	recordVersion, err := value.ReadUvarintAsInt16("version")
	if err != nil {
		return RecordValueHeader{}, fmt.Errorf("failed reading record version: %v", err)
	}

	ret := RecordValueHeader{
		FrameVersion:  frameVersion,
		RecordType:    recordType,
		RecordVersion: recordVersion,
	}

	return ret, nil
}

func parseRecords(records []Record) (map[topicName]topicMetadata, error) {
	ID2Name := make(map[[16]byte]string)       // UUID -> string
	ID2Partition := make(map[[16]byte][]int32) // UUID -> []int32

	for _, records := range records {
		value := NewFrame(records.value)

		header, err := parseRecordValueHeader(&value) // pass by reference
		if err != nil {
			return nil, fmt.Errorf("failed parsing record value header: %v", err)
		}

		// fmt.Printf("%+v\n", header)

		switch header.RecordType {
		case 2: // Topic Record
			topicName, err := value.ReadCompactString()
			if err != nil {
				return nil, fmt.Errorf("failed parsing topic name: %v", err)
			}

			topicID, err := value.ReadUUID()
			if err != nil {
				return nil, fmt.Errorf("failed parsing topic uuid: %v", err)
			}

			// fmt.Printf("%+v\n", topicName)
			// fmt.Printf("%+v\n", topicID)

			ID2Name[topicID] = topicName
		case 3: // Partition Record
			partitionID, err := value.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("failed parsing partition id: %v", err)
			}

			topicID, err := value.ReadUUID()
			if err != nil {
				return nil, fmt.Errorf("failed parsing topic uuid: %v", err)
			}

			// fmt.Printf("%+v\n", partitionID)
			// fmt.Printf("%+v\n", topicID)

			ID2Partition[topicID] = append(ID2Partition[topicID], partitionID)
		case 12:
		default:
			return nil, fmt.Errorf("unexpected record type, got: %v", header.RecordType)
		}
	}

	ret := make(map[topicName]topicMetadata)
	for id, name := range ID2Name {
		ret[name] = topicMetadata{
			ID:         id,
			Partitions: ID2Partition[id],
		}
	}
	fmt.Println(ret)
	return ret, nil
}
