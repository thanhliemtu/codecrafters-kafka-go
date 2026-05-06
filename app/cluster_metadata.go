package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
)

// This is the header of the Record.value field in the cluster metadata log
type ClusterMetadataLogRecordValueHeader struct {
	FrameVersion  int16
	RecordType    int16
	RecordVersion int16
}

// This is for the parsed partitions in PartitionRecords in the cluster metadata log
type ClusterMetadataLogPartitionMetadata struct {
	ID           PartitionID
	LeaderID     int32
	LeaderEpoch  int32
	ReplicaNodes []int32
	IsrNodes     []int32

	// For the codecrafter challenges, these can probably be empty.
	EligibleLeaderReplicas []int32
	LastKnownElr           []int32
	OfflineReplicas        []int32
}

type PartitionID = int32
type TopicName = string
type topicID = [16]byte

// This is for the topic -> partitions mapping in the cluster metadata log
type ClusterMetadataLogTopicMetadata struct {
	ID         topicID
	Partitions []ClusterMetadataLogPartitionMetadata // a topic can have mutliple partitions
}

var metadata map[TopicName]ClusterMetadataLogTopicMetadata

func dumpClusterMetadataLog(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read cluster metadata log: %w\n", err)
		return
	}

	encoded := base64.StdEncoding.EncodeToString(data)

	fmt.Fprintln(os.Stderr, "----- BEGIN __cluster_metadata LOG BASE64 -----")

	// Print in chunks so terminals/log viewers don't hate one giant line.
	const width = 76
	for i := 0; i < len(encoded); i += width {
		end := i + width
		if end > len(encoded) {
			end = len(encoded)
		}
		fmt.Fprintln(os.Stderr, encoded[i:end])
	}

	fmt.Fprintln(os.Stderr, "----- END __cluster_metadata LOG BASE64 -----")
}

func getMetadataLogFilePath(logDir string) string {
	return filepath.Join(
		logDir,
		"__cluster_metadata-0",
		"00000000000000000000.log",
	)
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
	return parseRecordBatches(&file)
}

// this modifies the original frame
func ParseMetadataLogRecordValueHeader(value *Frame) (ClusterMetadataLogRecordValueHeader, error) {
	frameVersion, err := value.ReadUvarintAsInt16("frame version")
	if err != nil {
		return ClusterMetadataLogRecordValueHeader{}, fmt.Errorf("failed reading record frame version: %w", err)
	}
	if frameVersion != 1 {
		return ClusterMetadataLogRecordValueHeader{}, fmt.Errorf("expect frame version 1, got: %d", frameVersion)
	}

	recordType, err := value.ReadUvarintAsInt16("type")
	if err != nil {
		return ClusterMetadataLogRecordValueHeader{}, fmt.Errorf("failed reading record type: %w", err)
	}

	recordVersion, err := value.ReadUvarintAsInt16("version")
	if err != nil {
		return ClusterMetadataLogRecordValueHeader{}, fmt.Errorf("failed reading record version: %w", err)
	}

	ret := ClusterMetadataLogRecordValueHeader{
		FrameVersion:  frameVersion,
		RecordType:    recordType,
		RecordVersion: recordVersion,
	}

	return ret, nil
}

/*
Note that NOT every Record.value fields are serialized with frame version, type, and version.
In this instance, we are parsing Records that are in the cluster metadata log file.
And the AbstractApiMessageSerde.write function just happens to put the bytes in that manner
into Record.value byte array field.

For other Record types, like the one in Produce API request, this function will not work.
*/
func parseMetadataLogRecords(records []Record) (map[TopicName]ClusterMetadataLogTopicMetadata, error) {
	ID2Name := make(map[[16]byte]string)                                     // UUID -> string
	ID2Partition := make(map[[16]byte][]ClusterMetadataLogPartitionMetadata) // UUID -> []ClusterMetadataLogPartitionMetadata

	for _, records := range records {
		value := NewFrame(records.value)

		header, err := ParseMetadataLogRecordValueHeader(&value) // pass by reference
		if err != nil {
			return nil, fmt.Errorf("failed parsing record value header: %w", err)
		}

		// fmt.Printf("%+v\n", header)

		switch header.RecordType {
		case 2: // Topic Record
			topicName, err := value.ReadCompactString()
			if err != nil {
				return nil, fmt.Errorf("failed parsing topic name: %w", err)
			}

			topicID, err := value.ReadUUID()
			if err != nil {
				return nil, fmt.Errorf("failed parsing topic uuid: %w", err)
			}

			// fmt.Printf("%+v\n", topicName)
			// fmt.Printf("%+v\n", topicID)

			ID2Name[topicID] = topicName
		case 3: // Partition Record
			partitionID, err := value.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("failed parsing partition id: %w", err)
			}

			topicID, err := value.ReadUUID()
			if err != nil {
				return nil, fmt.Errorf("failed parsing topic uuid: %w", err)
			}

			compact_replica_array_length, err := value.ReadUvarint()
			if err != nil {
				return nil, fmt.Errorf("failed parsing replica array length: %w", err)
			}

			var replicaNodes []int32
			if compact_replica_array_length > 0 {
				for range compact_replica_array_length - 1 {
					node, err := value.ReadInt32()
					if err != nil {
						return nil, fmt.Errorf("failed parsing replica array node: %w", err)
					}
					replicaNodes = append(replicaNodes, node)
				}
			}

			compact_in_sync_replica_array_length, err := value.ReadUvarint()
			if err != nil {
				return nil, fmt.Errorf("failed parsing in sync replica array length: %w", err)
			}

			var IsrNodes []int32
			if compact_in_sync_replica_array_length > 0 {
				for range compact_in_sync_replica_array_length - 1 {
					node, err := value.ReadInt32()
					if err != nil {
						return nil, fmt.Errorf("failed parsing in sync replica array node: %w", err)
					}
					IsrNodes = append(IsrNodes, node)
				}
			}

			_, err = value.ReadUvarint() // Length of Removing Replicas array
			if err != nil {
				return nil, fmt.Errorf("failed parsing removing replica array length: %w", err)
			}

			_, err = value.ReadUvarint() // Length of Adding Replicas array
			if err != nil {
				return nil, fmt.Errorf("failed parsing adding replica array length: %w", err)
			}

			leaderID, err := value.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("failed parsing leader id: %w", err)
			}

			leaderEpoch, err := value.ReadInt32()
			if err != nil {
				return nil, fmt.Errorf("failed parsing leader epoch: %w", err)
			}

			ClusterMetadataLogPartitionMetadata := ClusterMetadataLogPartitionMetadata{
				ID:           partitionID,
				LeaderID:     leaderID,
				LeaderEpoch:  leaderEpoch,
				ReplicaNodes: replicaNodes,
				IsrNodes:     IsrNodes,
			}
			// fmt.Printf("%+v\n", partitionID)
			// fmt.Printf("%+v\n", topicID)

			ID2Partition[topicID] = append(ID2Partition[topicID], ClusterMetadataLogPartitionMetadata)
		case 12:
		default:
			return nil, fmt.Errorf("unexpected record type, got: %w", header.RecordType)
		}
	}

	ret := make(map[TopicName]ClusterMetadataLogTopicMetadata)
	for id, name := range ID2Name {
		ret[name] = ClusterMetadataLogTopicMetadata{
			ID:         id,
			Partitions: ID2Partition[id],
		}
	}
	return ret, nil
}
