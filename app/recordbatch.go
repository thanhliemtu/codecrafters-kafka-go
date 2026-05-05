package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// https://kafka.apache.org/42/implementation/message-format
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
	records         []Record // default nil
}

type Record struct {
	attributes int8
	/*
		bit 0~7: unused
	*/
	timestampDelta int64
	offsetDelta    int32
	key            []byte
	value          []byte // opague, can mean differently depending on context
	/*The key of a record header is guaranteed to be non-null,
	while the value of a record header may be null*/
	headers []RecordHeader
}

type RecordHeader struct {
	headerKey string
	value     []byte
}

/*
Parses ALL the bytes the frame and returns a slice of Record Batches.

Input frame must only contain bytes that are relevant to record batches
as this function will read until there are no more bytes left to process.

Input frame should look like this: [batch 1][batch 2]...
*/
func parseRecordBatches(frame *Frame) ([]RecordBatch, error) {
	var recordBatches []RecordBatch
	var err error
	for frame.Remaining() > 0 {
		recordBatch := RecordBatch{}

		recordBatch.baseOffset, err = frame.ReadInt64()
		if err != nil {
			return nil, fmt.Errorf("failed reading baseOffset: %v", err)
		}

		recordBatch.batchLength, err = frame.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading batchLength: %v", err)
		}

		if recordBatch.batchLength < 0 {
			return nil, fmt.Errorf("invalid negative batchLength: %d", recordBatch.batchLength)
		}

		batchBytes, err := frame.ReadBytes(int(recordBatch.batchLength))
		if err != nil {
			return nil, fmt.Errorf("failed reading batch payload: length=%d: %w", recordBatch.batchLength, err)
		}

		batchFrame := NewFrame(batchBytes)

		recordBatch.partitionLeaderEpoch, err = batchFrame.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading partitionLeaderEpoch: %v", err)
		}

		recordBatch.magic, err = batchFrame.ReadInt8()
		if err != nil {
			return nil, fmt.Errorf("failed reading magic: %v", err)
		}
		if recordBatch.magic != 2 {
			return nil, fmt.Errorf("unsupported record batch magic: %d", recordBatch.magic)
		}

		crcBytes, err := batchFrame.ReadBytes(4)
		if err != nil {
			return nil, fmt.Errorf("failed reading crc: %w", err)
		}
		recordBatch.crc = binary.BigEndian.Uint32(crcBytes)

		attributes, err := batchFrame.ReadInt16()
		if err != nil {
			return nil, fmt.Errorf("failed reading attributes: %v", err)
		}

		compression := attributes & 0x0007
		if compression != 0 {
			return nil, fmt.Errorf("unsupported compressed record batch, compression type: %d", compression)
		}

		recordBatch.attributes = attributes

		recordBatch.lastOffsetDelta, err = batchFrame.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading last offset delta: %v", err)
		}

		recordBatch.baseTimestamp, err = batchFrame.ReadInt64()
		if err != nil {
			return nil, fmt.Errorf("failed reading base time stamp: %v", err)
		}

		recordBatch.maxTimestamp, err = batchFrame.ReadInt64()
		if err != nil {
			return nil, fmt.Errorf("failed reading max time stamp: %v", err)
		}

		recordBatch.producerId, err = batchFrame.ReadInt64()
		if err != nil {
			return nil, fmt.Errorf("failed reading producer id: %v", err)
		}

		recordBatch.producerEpoch, err = batchFrame.ReadInt16()
		if err != nil {
			return nil, fmt.Errorf("failed reading producer epoch: %v", err)
		}

		recordBatch.baseSequence, err = batchFrame.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading base sequence: %v", err)
		}

		recordsCount, err := batchFrame.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading records count: %v", err)
		}

		if recordsCount < 0 {
			return nil, fmt.Errorf("invalid negative records count: %d", recordsCount)
		}

		for range recordsCount {
			record := Record{}

			recordLen, err := batchFrame.ReadVarint()
			if err != nil {
				return nil, fmt.Errorf("failed reading record length: %w", err)
			}

			if recordLen < 0 {
				return nil, fmt.Errorf("invalid negative record length: %d", recordLen)
			}

			recordBytes, err := batchFrame.ReadBytes(int(recordLen))
			if err != nil {
				return nil, fmt.Errorf("failed reading record body: length=%d: %w", recordLen, err)
			}

			recordFrame := NewFrame(recordBytes)

			record.attributes, err = recordFrame.ReadInt8()
			if err != nil {
				return nil, fmt.Errorf("failed reading record attributes: %v", err)
			}

			record.timestampDelta, err = recordFrame.ReadVarint()
			if err != nil {
				return nil, fmt.Errorf("failed reading record time stamp delta: %v", err)
			}

			record.offsetDelta, err = recordFrame.ReadVarint32()
			if err != nil {
				return nil, fmt.Errorf("failed reading record offset delta: %v", err)
			}

			record.key, err = readNullableBytesVarint(&recordFrame, "record key")
			if err != nil {
				return nil, err
			}

			record.value, err = readNullableBytesVarint(&recordFrame, "record value")
			if err != nil {
				return nil, err
			}

			headersCount, err := recordFrame.ReadVarint32()
			if err != nil {
				return nil, fmt.Errorf("failed reading record header count: %v", err)
			}

			if headersCount > 0 { // keylength can be 0, meaning no headers
				return nil, errors.New("Expect zero headers count")
			}

			if recordFrame.Remaining() != 0 {
				return nil, fmt.Errorf("trailing bytes in record body: remaining=%d", recordFrame.Remaining())
			}

			recordBatch.records = append(recordBatch.records, record)
		}

		if batchFrame.Remaining() != 0 {
			return nil, fmt.Errorf("trailing bytes in record batch: remaining=%d", batchFrame.Remaining())
		}

		recordBatches = append(recordBatches, recordBatch)

	}

	return recordBatches, nil
}

/*
Extracts records from many record batches.
*/
func flattenRecordBatch(recordBatch []RecordBatch) []Record {
	var records []Record
	for _, batch := range recordBatch {
		for _, record := range batch.records {
			records = append(records, record)
		}
	}
	return records
}

func (h RecordHeader) Bytes() []byte {
	var out []byte

	keyBytes := []byte(h.headerKey)

	// headerKeyLength: varint
	out = appendVarint(out, int64(len(keyBytes)))

	// headerKey: string bytes
	out = append(out, keyBytes...)

	// headerValueLength + value
	out = appendNullableBytesVarint(out, h.value)

	return out
}

func (r Record) Bytes() []byte {
	var body []byte

	// attributes: int8
	body = appendInt8(body, r.attributes)

	// timestampDelta: varlong
	body = appendVarint(body, r.timestampDelta)

	// offsetDelta: varint
	body = appendVarint(body, int64(r.offsetDelta))

	// keyLength + key
	body = appendNullableBytesVarint(body, r.key)

	// valueLength + value
	body = appendNullableBytesVarint(body, r.value)

	// headersCount: varint
	body = appendVarint(body, int64(len(r.headers)))

	// headers
	for _, h := range r.headers {
		body = append(body, h.Bytes()...)
	}

	var out []byte

	// length: varint
	out = appendVarint(out, int64(len(body)))

	// record body
	out = append(out, body...)

	return out
}

func (b RecordBatch) Bytes() []byte {
	var recordsBytes []byte
	for _, record := range b.records {
		recordsBytes = append(recordsBytes, record.Bytes()...)
	}

	recordsCount := int32(len(b.records))

	// The CRC covers the data from the attributes to the end of the batch (i.e. all the bytes that follow the CRC)
	var crcPayload []byte

	crcPayload = appendInt16(crcPayload, b.attributes)
	crcPayload = appendInt32(crcPayload, b.lastOffsetDelta)
	crcPayload = appendInt64(crcPayload, b.baseTimestamp)
	crcPayload = appendInt64(crcPayload, b.maxTimestamp)
	crcPayload = appendInt64(crcPayload, b.producerId)
	crcPayload = appendInt16(crcPayload, b.producerEpoch)
	crcPayload = appendInt32(crcPayload, b.baseSequence)
	crcPayload = appendInt32(crcPayload, recordsCount)
	crcPayload = append(crcPayload, recordsBytes...)

	crc := crc32.Checksum(crcPayload, crc32cTable)

	/*
		batchLength represents the number of bytes from the current position
		(immediately after the batchLength field) to the end of the batch

		That means:
		  partitionLeaderEpoch: 4
		  magic:                1
		  crc:                  4
		  crcPayload:           variable

		Total full batch size is:
		  baseOffset:   8
		  batchLength:  4
		  batchLength bytes...
	*/
	batchLength := int32(4 + 1 + 4 + len(crcPayload))

	var out []byte

	out = appendInt64(out, b.baseOffset)
	out = appendInt32(out, batchLength)
	out = appendInt32(out, b.partitionLeaderEpoch)
	out = appendInt8(out, b.magic)
	out = appendUint32(out, crc)
	out = append(out, crcPayload...)

	return out
}
