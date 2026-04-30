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

var metadata map[TopicName]ClusterMetadataLogTopicMetadata

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
				log.Printf("message channel closed")
				return
			}

			if frame.Remaining() < 8 {
				log.Printf("frame too short for request header: remaining=%d", frame.Remaining())
				return
			}

			// parsing header
			header, err := frame.ReadRequestHeaderV2()
			if err != nil {
				log.Printf("failed to read request header v2: %v", err)
				return
			}

			var response []byte
			var handleErr error
			switch header.RequestAPIKey {
			case 0:
				response, handleErr = handleProduce(&frame, &header)
			case 18:
				response, handleErr = handleApiVersions(&frame, &header)
			case 75:
				response, handleErr = handleDescribeTopicPartitions(&frame, &header)
			default:
				return // dont know what to do if it's not a known api key so we just return here
			}

			if handleErr != nil {
				log.Printf("failed handling request: %v", handleErr)
				return
			}

			if _, err := conn.Write(response); err != nil {
				log.Printf("failed writing response: %v", err)
				return
			}
		}
	}
}

func loadMetadataFromServerPropertiesArg() error {
	// Earlier Codecrafters stages may not pass /tmp/server.properties.
	if len(os.Args) <= 1 {
		log.Println("no server.properties argument provided; skipping metadata log loading")
		return nil
	}

	serverPropertiesPath := os.Args[1]

	props, err := readServerPropertiesFile(serverPropertiesPath)
	if err != nil {
		return fmt.Errorf("read server.properties %q: %w", serverPropertiesPath, err)
	}

	logDir := firstLogDir(props["log.dirs"])
	if logDir == "" {
		return fmt.Errorf("missing log.dirs in %q", serverPropertiesPath)
	}

	metadataLogFilePath := getMetadataLogFilePath(logDir)

	recordBatches, err := parseClusterMetadataLog(metadataLogFilePath)
	if err != nil {
		return fmt.Errorf("parse metadata log %q: %w", metadataLogFilePath, err)
	}

	records := flattenRecordBatch(recordBatches)

	parsedMetadata, err := parseMetadataLogRecords(records)
	if err != nil {
		return fmt.Errorf("parse metadata records: %w", err)
	}

	metadata = parsedMetadata

	log.Printf("loaded cluster metadata from %s", metadataLogFilePath)
	return nil
}

func serve(ctx context.Context, addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("bind to %s: %w", addr, err)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return fmt.Errorf("accept connection: %w", err)
			}
		}

		log.Printf("New connection from: %s", conn.RemoteAddr().String())
		go handleConnection(ctx, conn)
	}
}

func main() {
	log.SetOutput(os.Stdout) // log defaults to stderr

	if err := loadMetadataFromServerPropertiesArg(); err != nil {
		log.Fatalf("failed to load metadata: %v", err)
	}

	// You can use print statements as follows for debugging, they'll be visible when running tests.
	log.Println("Logs from your program will appear here!")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := serve(ctx, "0.0.0.0:9092"); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
