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

func createApiVersionsResponse(request_api_key int16, request_api_version int16, correlation_id int32) (response []byte) {
	// https://kafka.apache.org/42/design/protocol/#The_Messages_ApiVersions
	/*
		Simple example response:
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

		A bit more complex response, with ApiVersions and DescribeTopicPartitions in the array:
		00 00 00 1d  // message_size:      29 bytes
		ab cd ef 12  // correlation_id:    (matches request)
		00 00        // error_code:        0 (no error)
		03           // api_keys array:    2 elements (compact array encoding)
		00 12        // api_key:           18 (ApiVersions)
		00 00        // min_version:       0
		00 04        // max_version:       4
		00           // TAG_BUFFER:        empty
		00 4b        // api_key:           75 (DescribeTopicPartitions)
		00 00        // min_version:       0
		00 00        // max_version:       0
		00           // TAG_BUFFER:        empty
		00 00 00 00  // throttle_time_ms:  0
		00           // TAG_BUFFER:        empty

		Structually, it's like this:
		error_code
		api_keys_length
			entry1 fields
			entry1 tag section = 00
			entry2 fields
			entry2 tag section = 00
		throttle_time_ms
		top-level tag section = 00
	*/
	/*
		ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms [supported_features]<tag: 0> finalized_features_epoch<tag: 1> [finalized_features]<tag: 2> zk_migration_ready<tag: 3>
		  error_code => INT16
		  api_keys => api_key min_version max_version
		    api_key => INT16
		    min_version => INT16
		    max_version => INT16
		  throttle_time_ms => INT32
		  supported_features<tag: 0> => name min_version max_version
		    name => COMPACT_STRING
		    min_version => INT16
		    max_version => INT16
		  finalized_features_epoch<tag: 1> => INT64
		  finalized_features<tag: 2> => name max_version_level min_version_level
		    name => COMPACT_STRING
		    max_version_level => INT16
		    min_version_level => INT16
		  zk_migration_ready<tag: 3> => BOOLEAN
	*/
	const ApiVersions_MIN_VERSION = 0
	const ApiVersions_MAX_VERSION = 4

	const DescribeTopicPartitions_MIN_VERSION = 0
	const DescribeTopicPartitions_MAX_VERSION = 0

	const ERROR_NONE = 0
	const ERROR_UNSUPPORTED_VERSION = 35

	var error_code uint16 = ERROR_NONE
	if request_api_version < ApiVersions_MIN_VERSION || request_api_version > ApiVersions_MAX_VERSION {
		error_code = ERROR_UNSUPPORTED_VERSION
	}

	body := []byte{}
	body = binary.BigEndian.AppendUint32(body, uint32(correlation_id)) // correlation_id (4 bytes)
	body = binary.BigEndian.AppendUint16(body, uint16(error_code))     // error_code (2 bytes)

	// This field is the length of the [api_keys] array.
	// Since we're working with version 4, a flexible version, this follows the N+1 syntax
	// See here: https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ApiVersionsResponse.json
	// 3 because the array is [ApiVersions, DescribeTopicPartitions]
	body = append(body, 3) // api_keys array length (1 byte)

	// Why is there a TAG_BUFFER after each array entry?
	// https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A%2BThe%2BKafka%2BProtocol%2Bshould%2BSupport%2BOptional%2BTagged%2BFields#KIP482:TheKafkaProtocolshouldSupportOptionalTaggedFields-TagSections
	// "In a flexible version, each structure ends with a tag section."
	// So we will often see a "00" between each entries, but semantically it belongs to the previous entry's tag section.

	// For ApiVersions API (Key: 18)
	body = binary.BigEndian.AppendUint16(body, uint16(18))                      // api_key (2 bytes)
	body = binary.BigEndian.AppendUint16(body, uint16(ApiVersions_MIN_VERSION)) // min_version (2 bytes)
	body = binary.BigEndian.AppendUint16(body, uint16(ApiVersions_MAX_VERSION)) // max_version (2 bytes)
	body = append(body, 0)                                                      // TAG_BUFFER (1 byte)

	// For DescribeTopicPartitions API (Key: 75)
	body = binary.BigEndian.AppendUint16(body, uint16(75))                                  // api_key (2 bytes)
	body = binary.BigEndian.AppendUint16(body, uint16(DescribeTopicPartitions_MIN_VERSION)) // min_version (2 bytes)
	body = binary.BigEndian.AppendUint16(body, uint16(DescribeTopicPartitions_MAX_VERSION)) // max_version (2 bytes)
	body = append(body, 0)                                                                  // TAG_BUFFER (1 byte)

	body = binary.BigEndian.AppendUint32(body, uint32(0)) // throttle_time_ms (4 bytes)

	body = append(body, 0) // TAG_BUFFER (1 byte)

	response = binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	response = append(response, body...)
	return
}

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

				var response []byte
				switch request_api_key {
				case 18:
					response = createApiVersionsResponse(request_api_key, request_api_version, correlation_id)
				default:
					return // dont know what to do if it's not a known api key so we just return here
				}

				conn.Write(response)
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
