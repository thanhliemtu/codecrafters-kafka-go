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

				// The whole frame (size + the rest)
				fullPacket := append(sizeBuf, payloadBuf...)

				select { // select here because sending to channel might block forever if parent exits first
				case msgChan <- fullPacket:
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
