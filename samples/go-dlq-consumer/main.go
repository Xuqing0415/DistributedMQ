package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	Magic      = 0xD0
	CmdFetch   = 0x02
	RespSuccess  = 0x00
	RespRedirect = 0x01
	RespError    = 0xFF
)

type Message struct {
	Offset    int64
	Timestamp int64
	Value     []byte
}

func main() {
	brokerAddr := flag.String("broker", "localhost:9092", "Broker TCP address")
	topic := flag.String("topic", "test", "Topic name")
	partition := flag.Int("partition", 0, "Partition ID")
	flag.Parse()

	dlqTopic := *topic + "_DLQ"
	fmt.Printf("DLQ Consumer starting for topic=%s, partition=%d\n", dlqTopic, *partition)

	conn, err := net.Dial("tcp", *brokerAddr)
	if err != nil {
		fmt.Printf("Failed to connect to broker: %v\n", err)
		return
	}
	defer conn.Close()

	offset := int64(0)
	for {
		messages, highWatermark, err := fetch(conn, dlqTopic, *partition, offset, 1024*1024)
		if err != nil {
			fmt.Printf("Fetch failed: %v\n", err)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, msg := range messages {
			fmt.Printf("[DLQ Alert] topic=%s, partition=%d, offset=%d, timestamp=%d, data=%s\n",
				dlqTopic, *partition, msg.Offset, msg.Timestamp, string(msg.Value))
			offset = msg.Offset + 1
		}

		if len(messages) == 0 {
			time.Sleep(1 * time.Second)
		}

		if highWatermark > 0 {
			lag := highWatermark - offset
			if lag > 0 {
				fmt.Printf("[DLQ Lag] %d messages pending\n", lag)
			}
		}
	}
}

func fetch(conn net.Conn, topic string, partition int, offset int64, maxBytes int) ([]Message, int64, error) {
	data := make([]byte, 12)
	binary.BigEndian.PutUint64(data[0:8], uint64(offset))
	binary.BigEndian.PutUint32(data[8:12], uint32(maxBytes))

	header := make([]byte, 12)
	header[0] = Magic
	header[1] = CmdFetch
	binary.BigEndian.PutUint16(header[2:4], uint16(len(topic)))
	binary.BigEndian.PutUint32(header[4:8], uint32(partition))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(data)))

	if _, err := conn.Write(header); err != nil {
		return nil, 0, err
	}
	if _, err := conn.Write([]byte(topic)); err != nil {
		return nil, 0, err
	}
	if _, err := conn.Write(data); err != nil {
		return nil, 0, err
	}

	respHeader := make([]byte, 14)
	if _, err := io.ReadFull(conn, respHeader); err != nil {
		return nil, 0, err
	}

	if respHeader[0] != Magic {
		return nil, 0, fmt.Errorf("invalid magic byte: %x", respHeader[0])
	}

	switch respHeader[1] {
	case RespSuccess:
		highWatermark := int64(binary.BigEndian.Uint64(respHeader[2:10]))
		msgCount := int(binary.BigEndian.Uint32(respHeader[10:14]))
		messages := make([]Message, 0, msgCount)

		for i := 0; i < msgCount; i++ {
			msgHeader := make([]byte, 20)
			if _, err := io.ReadFull(conn, msgHeader); err != nil {
				return messages, highWatermark, err
			}

			msgOffset := int64(binary.BigEndian.Uint64(msgHeader[0:8]))
			timestamp := int64(binary.BigEndian.Uint64(msgHeader[8:16]))
			dataLen := int(binary.BigEndian.Uint32(msgHeader[16:20]))

			msgData := make([]byte, dataLen)
			if _, err := io.ReadFull(conn, msgData); err != nil {
				return messages, highWatermark, err
			}

			messages = append(messages, Message{
				Offset:    msgOffset,
				Timestamp: timestamp,
				Value:     msgData,
			})
		}

		return messages, highWatermark, nil

	case RespRedirect:
		leaderLenHeader := make([]byte, 1)
		if _, err := io.ReadFull(conn, leaderLenHeader); err != nil {
			return nil, 0, fmt.Errorf("redirect: failed to read leader length")
		}
		leaderLen := int(leaderLenHeader[0])
		leaderAddr := make([]byte, leaderLen)
		if _, err := io.ReadFull(conn, leaderAddr); err != nil {
			return nil, 0, fmt.Errorf("redirect: failed to read leader address")
		}
		return nil, 0, fmt.Errorf("redirect:%s", string(leaderAddr))

	case RespError:
		return nil, 0, fmt.Errorf("server error")

	default:
		return nil, 0, fmt.Errorf("unknown response status: %x", respHeader[1])
	}
}