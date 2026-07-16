package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	Magic      = 0xD0
	CmdProduce = 0x01

	RespSuccess  = 0x00
	RespRedirect = 0x01
	RespError    = 0xFF
)

type Response struct {
	success bool
	redirect bool
	leaderAddr string
	err error
}

func main() {
	brokerAddr := flag.String("broker", "localhost:9092", "Broker TCP address")
	topic := flag.String("topic", "test", "Topic name")
	partition := flag.Int("partition", 0, "Partition ID")
	count := flag.Int("count", 1000, "Number of messages")
	acks := flag.Int("acks", 1, "Acks level: 0, 1, or -1 (all)")
	batch := flag.Int("batch", 100, "Batch size for pipelined sends")
	flag.Parse()

	conn, err := connect(*brokerAddr)
	if err != nil {
		fmt.Printf("Failed to connect to broker: %v\n", err)
		return
	}
	defer conn.Close()

	fmt.Printf("Connected to broker: %s\n", *brokerAddr)
	fmt.Printf("Sending %d messages to topic=%s, partition=%d\n", *count, *topic, *partition)
	fmt.Printf("Batch size: %d, Acks: %d\n", *batch, *acks)

	start := time.Now()
	success := 0
	failed := 0
	redirects := 0

	for i := 0; i < *count; i += *batch {
		end := i + *batch
		if end > *count {
			end = *count
		}
		batchSize := end - i

		responses := make([]Response, batchSize)
		var wg sync.WaitGroup

		for j := 0; j < batchSize; j++ {
			wg.Add(1)
			go func(idx int, msgIdx int) {
				defer wg.Done()

				data := []byte(fmt.Sprintf("Message %d: Hello DistributedMQ!", msgIdx))
				err := sendMessage(conn, *topic, int32(*partition), data, int8(*acks))

				if err != nil {
					redirectAddr, isRedirect := isRedirectError(err)
					if isRedirect && redirectAddr != "" {
						responses[idx] = Response{redirect: true, leaderAddr: redirectAddr}
					} else {
						responses[idx] = Response{err: err}
					}
				} else {
					responses[idx] = Response{success: true}
				}
			}(j, i+j)
		}

		wg.Wait()

		needRedirect := false
		var newLeader string
		for _, resp := range responses {
			if resp.redirect {
				needRedirect = true
				newLeader = resp.leaderAddr
				break
			}
		}

		if needRedirect {
			redirects++
			conn.Close()
			conn, err = connect(newLeader)
			if err != nil {
				fmt.Printf("Failed to redirect to %s: %v\n", newLeader, err)
				for _, resp := range responses {
					if !resp.success {
						failed++
					}
				}
				continue
			}
			fmt.Printf("Redirected to leader: %s\n", newLeader)

			for j := 0; j < batchSize; j++ {
				if !responses[j].success {
					data := []byte(fmt.Sprintf("Message %d: Hello DistributedMQ!", i+j))
					err := sendMessage(conn, *topic, int32(*partition), data, int8(*acks))
					if err != nil {
						failed++
					} else {
						success++
					}
				} else {
					success++
				}
			}
		} else {
			for _, resp := range responses {
				if resp.success {
					success++
				} else {
					failed++
				}
			}
		}

		if end > 0 && end%1000 == 0 {
			fmt.Printf("Sent %d/%d messages...\n", end, *count)
		}
	}

	duration := time.Since(start)
	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("Total messages: %d\n", *count)
	fmt.Printf("Success: %d\n", success)
	fmt.Printf("Failed: %d\n", failed)
	fmt.Printf("Redirects: %d\n", redirects)
	fmt.Printf("Duration: %v\n", duration)
	fmt.Printf("Throughput: %.2f msg/s\n", float64(success)/duration.Seconds())
}

func connect(addr string) (net.Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func sendMessage(conn net.Conn, topic string, partition int32, data []byte, acks int8) error {
	topicLen := len(topic)
	dataLen := len(data) + 1

	header := make([]byte, 12)
	header[0] = Magic
	header[1] = CmdProduce
	binary.BigEndian.PutUint16(header[2:4], uint16(topicLen))
	binary.BigEndian.PutUint32(header[4:8], uint32(partition))
	binary.BigEndian.PutUint32(header[8:12], uint32(dataLen))

	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write([]byte(topic)); err != nil {
		return err
	}

	fullData := make([]byte, 1+len(data))
	fullData[0] = byte(acks)
	copy(fullData[1:], data)
	if _, err := conn.Write(fullData); err != nil {
		return err
	}

	respBuf := make([]byte, 1024)
	n, err := conn.Read(respBuf)
	if err != nil {
		return err
	}

	if n < 2 {
		return fmt.Errorf("invalid response")
	}

	if respBuf[0] != Magic {
		return fmt.Errorf("invalid magic byte: %x", respBuf[0])
	}

	switch respBuf[1] {
	case RespSuccess:
		return nil
	case RespRedirect:
		if n < 3 {
			return fmt.Errorf("redirect: no leader address")
		}
		leaderLen := int(respBuf[2])
		if n < 3+leaderLen {
			return fmt.Errorf("redirect: invalid leader address length")
		}
		leaderAddr := string(respBuf[3 : 3+leaderLen])
		return fmt.Errorf("redirect:%s", leaderAddr)
	case RespError:
		return fmt.Errorf("server error")
	default:
		return fmt.Errorf("unknown response status: %x", respBuf[1])
	}
}

func isRedirectError(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	msg := err.Error()
	if len(msg) > 9 && msg[:9] == "redirect:" {
		return msg[9:], true
	}
	return "", false
}