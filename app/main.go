package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

// Ensures golog doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func handleConnection(ctx context.Context, conn net.Conn) {
	defer func() {
		log.Printf("Closing connection from: %s", conn.RemoteAddr().String())
		conn.Close()
	}()

	msgChan := make(chan []byte)
	errChan := make(chan error)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// The first 4 bytes make up the size of the message frame
				sizeBuf := make([]byte, 4)
				if _, err := io.ReadFull(conn, sizeBuf); err != nil {
					if err != io.EOF && !errors.Is(err, net.ErrClosed) {
						errChan <- fmt.Errorf("failed reading size: %w", err)
					}
					return // other side closed
				}
				size := int32(binary.BigEndian.Uint32(sizeBuf))

				// Read the rest of the message in the frame
				payloadBuf := make([]byte, size)
				if _, err := io.ReadFull(conn, payloadBuf); err != nil {
					errChan <- fmt.Errorf("failed reading payload: %w", err)
					return
				}
				// payload now contains the message frame

				select { // select here because sending to channel might block forever if parent exits first
				case msgChan <- payloadBuf:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case readErr := <-errChan:
			log.Printf("Error occurred: %v", readErr)
			return
		case data := <-msgChan:
			log.Printf("% x\n", data)
			if len(data) >= 8 {
				/*
					Parsing Request Header v2
					Request Header v2 => request_api_key request_api_version correlation_id client_id
						request_api_key => INT16 (byte 0 1)
						request_api_version => INT16 (byte 2 3)
						correlation_id => INT32 (byte 4 5 6 7)
						client_id => NULLABLE_STRING
				*/
				request_api_key := int16(binary.BigEndian.Uint16(data[0:2]))
				request_api_version := int16(binary.BigEndian.Uint16(data[2:4]))
				correlation_id := int32(binary.BigEndian.Uint32(data[4:8]))

				/*
					Assembling the response
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
				*/
				const MIN_VERSION = 0
				const MAX_VERSION = 4

				const ERROR_NONE = 0
				const ERROR_UNSUPPORTED_VERSION = 35

				var error_code uint16 = ERROR_NONE
				if request_api_version < MIN_VERSION || request_api_version > MAX_VERSION {
					error_code = ERROR_UNSUPPORTED_VERSION
				}

				body := []byte{}
				body = binary.BigEndian.AppendUint32(body, uint32(correlation_id))  // correlation_id (4 bytes)
				body = binary.BigEndian.AppendUint16(body, uint16(error_code))      // error_code (2 bytes)
				body = append(body, 2)                                              // api_keys array length (1 byte)
				body = binary.BigEndian.AppendUint16(body, uint16(request_api_key)) // api_key (2 bytes)
				body = binary.BigEndian.AppendUint16(body, uint16(MIN_VERSION))     // min_version (2 bytes)
				body = binary.BigEndian.AppendUint16(body, uint16(MAX_VERSION))     // max_version (2 bytes)
				body = append(body, 0)                                              // TAG_BUFFER (1 byte)
				body = binary.BigEndian.AppendUint32(body, uint32(0))               // throttle_time_ms (4 bytes)
				body = append(body, 0)                                              // TAG_BUFFER (1 byte)

				response := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
				response = append(response, body...)

				conn.Write(response)
			}
			return // this currently kills the connection after 1 message frame
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	log.Println("Logs from your program will appear here!")

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

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

		log.Printf("New connection from: %s", conn.RemoteAddr().String())
		go handleConnection(ctx, conn)
	}
}
