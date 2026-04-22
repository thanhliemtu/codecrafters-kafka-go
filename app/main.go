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
	"time"
)

// Ensures golog doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func handleConnection(ctx context.Context, conn net.Conn) {
	defer func() {
		log.Printf("Closing connection from: %s", conn.RemoteAddr().String())
		conn.Close()
	}()

	msgChan := make(chan Frame)
	errChan := make(chan error)

	go func() {
		for {
			const readTimeout = 60 * time.Second
			const maxFrameSize = 1024 * 1024

			if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
				select {
				case errChan <- fmt.Errorf("failed setting read deadline: %w", err):
				case <-ctx.Done():
				}
				return
			}

			// The first 4 bytes make up the size of the message frame
			sizeBuf := make([]byte, 4)
			if _, err := io.ReadFull(conn, sizeBuf); err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					select {
					case errChan <- fmt.Errorf("read size timeout: %w", err):
					case <-ctx.Done():
					}
					return // timeout
				}
				if err != io.EOF && !errors.Is(err, net.ErrClosed) {
					select {
					case errChan <- fmt.Errorf("failed reading size: %w", err):
					case <-ctx.Done():
					}
					return // read error
				}
				return // other side closed
			}

			size := binary.BigEndian.Uint32(sizeBuf)
			if size > maxFrameSize {
				select {
				case errChan <- fmt.Errorf("frame too large: %d", size):
				case <-ctx.Done():
				}
				return
			}

			// Read the rest of the message in the frame
			payloadBuf := make([]byte, int(size))
			if _, err := io.ReadFull(conn, payloadBuf); err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					select {
					case errChan <- fmt.Errorf("read payload timeout: %w", err):
					case <-ctx.Done():
					}
					return // read error
				}
				select {
				case errChan <- fmt.Errorf("failed reading payload: %w", err):
				case <-ctx.Done():
				}
				return
			}
			// payload now contains the message frame

			select { // select here because sending to channel might block forever if parent exits first
			case msgChan <- NewFrame(payloadBuf):
			case <-ctx.Done():
				return
			}

		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case readErr, ok := <-errChan:
			if !ok {
				return
			}
			log.Printf("Error occurred: %v", readErr)
			return

		case frame, ok := <-msgChan:
			if !ok {
				return
			}

			if frame.Remaining() < 8 {
				log.Printf("frame too short for request header: remaining=%d", frame.Remaining())
				return
			}

			/*
				Parsing Request Header v2
				Request Header v2 => request_api_key request_api_version correlation_id client_id
					request_api_key => INT16 (byte 0 1)
					request_api_version => INT16 (byte 2 3)
					correlation_id => INT32 (byte 4 5 6 7)
					client_id => NULLABLE_STRING
			*/
			request_api_key, err := frame.ReadInt16()
			if err != nil {
				log.Printf("failed to read api key: %v", err)
				return
			}

			request_api_version, err := frame.ReadInt16()
			if err != nil {
				log.Printf("failed to read api version: %v", err)
				return
			}

			correlation_id, err := frame.ReadInt32()
			if err != nil {
				log.Printf("failed to read correlation id: %v", err)
				return
			}

			var response []byte
			switch request_api_key {
			case 18:
				response = createApiVersionsResponse(request_api_version, correlation_id)
			default:
				return // dont know what to do if it's not a known api key so we just return here
			}

			if _, err := conn.Write(response); err != nil {
				log.Printf("failed writing response: %v", err)
				return
			}
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
