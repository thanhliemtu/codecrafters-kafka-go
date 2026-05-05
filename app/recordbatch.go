package main

import (
	"errors"
	"fmt"
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
	value          []byte // opague, can mean differently depending on context
	headers        []RecordHeader
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

		recordBatch.partitionLeaderEpoch, err = frame.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading partitionLeaderEpoch: %v", err)
		}

		recordBatch.magic, err = frame.ReadInt8()
		if err != nil {
			return nil, fmt.Errorf("failed reading magic: %v", err)
		}
		if recordBatch.magic != 2 {
			return nil, fmt.Errorf("unsupported record batch magic: %d", recordBatch.magic)
		}

		_, err = frame.ReadBytes(4) // crc
		if err != nil {
			return nil, fmt.Errorf("failed reading crc: %v", err)
		}

		attributes, err := frame.ReadInt16()
		if err != nil {
			return nil, fmt.Errorf("failed reading attributes: %v", err)
		}

		compression := attributes & 0x0007
		if compression != 0 {
			return nil, fmt.Errorf("unsupported compressed record batch, compression type: %d", compression)
		}

		recordBatch.lastOffsetDelta, err = frame.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading last offset delta: %v", err)
		}

		recordBatch.baseTimestamp, err = frame.ReadInt64()
		if err != nil {
			return nil, fmt.Errorf("failed reading base time stamp: %v", err)
		}

		recordBatch.maxTimestamp, err = frame.ReadInt64()
		if err != nil {
			return nil, fmt.Errorf("failed reading max time stamp: %v", err)
		}

		recordBatch.producerId, err = frame.ReadInt64()
		if err != nil {
			return nil, fmt.Errorf("failed reading producer id: %v", err)
		}

		recordBatch.producerEpoch, err = frame.ReadInt16()
		if err != nil {
			return nil, fmt.Errorf("failed reading producer epoch: %v", err)
		}

		recordBatch.baseSequence, err = frame.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading base sequence: %v", err)
		}

		recordsCount, err := frame.ReadInt32()
		if err != nil {
			return nil, fmt.Errorf("failed reading records count: %v", err)
		}

		for range recordsCount {
			record := Record{}
			_, err = frame.ReadVarint() // record length, don't think we're gonna need it or store it
			if err != nil {
				return nil, fmt.Errorf("failed reading record length: %v", err)
			}

			record.attributes, err = frame.ReadInt8()
			if err != nil {
				return nil, fmt.Errorf("failed reading record attributes: %v", err)
			}

			record.timestampDelta, err = frame.ReadVarint()
			if err != nil {
				return nil, fmt.Errorf("failed reading record time stamp delta: %v", err)
			}

			record.offsetDelta, err = frame.ReadVarint32()
			if err != nil {
				return nil, fmt.Errorf("failed reading record offset delta: %v", err)
			}

			keyLength, err := frame.ReadVarint32()
			if err != nil {
				return nil, fmt.Errorf("failed reading record key length: %v", err)
			}

			if keyLength > 0 { // keylength can be -1, which is null
				record.key, err = frame.ReadBytes(int(keyLength))
				if err != nil {
					return nil, fmt.Errorf("failed reading record key: %v", err)
				}
			}

			valueLength, err := frame.ReadVarint32()
			if err != nil {
				return nil, fmt.Errorf("failed reading record value length: %v", err)
			}

			record.value, err = frame.ReadBytes(int(valueLength))
			if err != nil {
				return nil, fmt.Errorf("failed reading record value: %v", err)
			}

			headersCount, err := frame.ReadVarint32()
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
