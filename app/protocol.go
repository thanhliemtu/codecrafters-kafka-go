package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

type Message struct { // all ints are in big endian
	MessageSize int32 // message size of 2 looks like 00 00 00 02
	Header      int32
}

// Marshal converts the Message struct into a Big Endian byte slice
func (m *Message) Marshal() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Message size
	err := binary.Write(buf, binary.BigEndian, m.MessageSize)
	if err != nil {
		return nil, err
	}

	// Header
	err = binary.Write(buf, binary.BigEndian, m.Header)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (m *Message) PrintPreview() {
	data, err := m.Marshal()
	if err != nil {
		log.Printf("Error encoding message: %v\n", err)
		return
	}
	log.Println("--- Message Preview ---")
	log.Printf("Struct Fields: %+v\n", m)
	log.Printf("Network Hex:   [% x]\n", data)
	log.Println("-----------------------")
}

func NewMessageFromBytes(data []byte) (Message, error) {
	if len(data) < 8 { // 4 for size + at least 4 for header
		return Message{}, fmt.Errorf("byte array too short: %d", len(data))
	}

	return Message{
		MessageSize: int32(binary.BigEndian.Uint32(data[:4])),
		Header:      int32(binary.BigEndian.Uint32(data[4:8])),
	}, nil
}
