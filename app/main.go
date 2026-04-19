package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"net"
	"os"
)

// Ensures golog doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

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

func handleConnection(conn net.Conn) {
	defer conn.Close()
	msg := Message{
		MessageSize: 0,
		Header:      7,
	}

	payload, err := msg.Marshal()

	if err != nil {
		log.Println("Marshal msg failed")
	}
	conn.Write(payload)
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	log.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		log.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}
