package main

import (
	"encoding/binary"
	"hash/crc32"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Signed Integers
func appendInt8(dst []byte, v int8) []byte {
	return append(dst, byte(v))
}

func appendInt16(dst []byte, v int16) []byte {
	return binary.BigEndian.AppendUint16(dst, uint16(v))
}

func appendInt32(dst []byte, v int32) []byte {
	return binary.BigEndian.AppendUint32(dst, uint32(v))
}

func appendInt64(dst []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(dst, uint64(v))
}

// Unsigned Integers
func appendUint16(dst []byte, v uint16) []byte {
	return binary.BigEndian.AppendUint16(dst, v)
}

func appendUint32(dst []byte, v uint32) []byte {
	return binary.BigEndian.AppendUint32(dst, v)
}

func appendUint64(dst []byte, v uint64) []byte {
	return binary.BigEndian.AppendUint64(dst, v)
}

// Variable Length Integers

func appendVarint(dst []byte, v int64) []byte {
	return binary.AppendVarint(dst, v)
}

func appendUvarint(dst []byte, v uint64) []byte {
	return binary.AppendUvarint(dst, v)
}

// Others
func appendNullableBytesVarint(dst []byte, b []byte) []byte {
	if b == nil {
		return appendVarint(dst, -1)
	}

	dst = appendVarint(dst, int64(len(b)))
	dst = append(dst, b...)
	return dst
}
