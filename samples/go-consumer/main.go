package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"
)

const (
	Magic          = 0xD0
	CmdFetch       = 0x02
	CmdJoinGroup   = 0x03
	CmdHeartbeat   = 0x04
	CmdCommitOffset = 0x05
	CmdLeaveGroup  = 0x06

	RespSuccess  = 0x00
	RespRedirect = 0x01
	RespError    = 0xFF
)

type JoinResponse struct {
	GroupID     string `json:"group_id"`
	Generation  int    `json:"generation"`
	ClientID    string `json:"client_id"`
	Assignments []int  `json:"assignments"`
	Topic       string `json:"topic"`
}

type Consumer struct {
	nameserverURL string
	groupID       string
	clientID      string
	topic         string
	assignments   []int
	offsets       map[int]int64
	connCache     map[int]net.Conn
	mu            sync.Mutex
	stopCh        chan struct{}
}

func NewConsumer(nameserverURL, groupID, clientID, topic string) *Consumer {
	return &Consumer{
		nameserverURL: nameserverURL,
		groupID:       groupID,
		clientID:      clientID,
		topic:         topic,
		assignments:   make([]int, 0),
		offsets:       make(map[int]int64),
		connCache:     make(map[int]net.Conn),
		stopCh:        make(chan struct{}),
	}
}

func (c *Consumer) JoinGroup() error {
	resp, err := http.Get(fmt.Sprintf("%s/consumer/join?group_id=%s&client_id=%s&topic=%s",
		c.nameserverURL, c.groupID, c.clientID, c.topic))
	if err != nil {
		return fmt.Errorf("join group failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("join group failed: %s", string(body))
	}

	var result JoinResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode join response failed: %v", err)
	}

	c.mu.Lock()
	c.assignments = result.Assignments
	sort.Ints(c.assignments)
	c.mu.Unlock()

	fmt.Printf("[JoinGroup] Group=%s, Generation=%d, Assignments=%v\n",
		c.groupID, result.Generation, c.assignments)

	for _, partition := range c.assignments {
		offset, err := c.getOffset(partition)
		if err != nil {
			fmt.Printf("[Offset] Failed to get offset for partition %d: %v\n", partition, err)
			offset = 0
		}
		c.mu.Lock()
		c.offsets[partition] = offset
		c.mu.Unlock()
		fmt.Printf("[Offset] Partition=%d, Offset=%d\n", partition, offset)
	}

	return nil
}

func (c *Consumer) getOffset(partition int) (int64, error) {
	resp, err := http.Get(fmt.Sprintf("%s/consumer/offset?group_id=%s&topic=%s&partition=%d",
		c.nameserverURL, c.groupID, c.topic, partition))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("get offset failed")
	}

	var result struct {
		Offset int64 `json:"offset"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	return result.Offset, nil
}

func (c *Consumer) commitOffset(partition int, offset int64) error {
	resp, err := http.Get(fmt.Sprintf("%s/consumer/commit_offset?group_id=%s&topic=%s&partition=%d&offset=%d",
		c.nameserverURL, c.groupID, c.topic, partition, offset))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("commit offset failed")
	}

	return nil
}

func (c *Consumer) heartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			resp, err := http.Get(fmt.Sprintf("%s/consumer/heartbeat?group_id=%s&client_id=%s",
				c.nameserverURL, c.groupID, c.clientID))
			if err != nil {
				fmt.Printf("[Heartbeat] Failed: %v\n", err)
				continue
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				fmt.Printf("[Heartbeat] Failed: status=%d\n", resp.StatusCode)
			}
		}
	}
}

func (c *Consumer) getLeader(partition int) (string, error) {
	resp, err := http.Get(fmt.Sprintf("%s/route?topic=%s&partition=%d",
		c.nameserverURL, c.topic, partition))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("get route failed")
	}

	var result struct {
		Leader string `json:"leader"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result.Leader, nil
}

func (c *Consumer) getConn(partition int) (net.Conn, error) {
	c.mu.Lock()
	if conn, ok := c.connCache[partition]; ok {
		c.mu.Unlock()
		return conn, nil
	}
	c.mu.Unlock()

	leader, err := c.getLeader(partition)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", leader)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.connCache[partition] = conn
	c.mu.Unlock()

	return conn, nil
}

func (c *Consumer) resetConn(partition int) {
	c.mu.Lock()
	if conn, ok := c.connCache[partition]; ok {
		conn.Close()
		delete(c.connCache, partition)
	}
	c.mu.Unlock()
}

func (c *Consumer) fetch(partition int, offset int64, maxBytes int) ([]Message, int64, error) {
	conn, err := c.getConn(partition)
	if err != nil {
		return nil, 0, err
	}

	data := make([]byte, 12)
	binary.BigEndian.PutUint64(data[0:8], uint64(offset))
	binary.BigEndian.PutUint32(data[8:12], uint32(maxBytes))

	header := make([]byte, 12)
	header[0] = Magic
	header[1] = CmdFetch
	binary.BigEndian.PutUint16(header[2:4], uint16(len(c.topic)))
	binary.BigEndian.PutUint32(header[4:8], uint32(partition))
	binary.BigEndian.PutUint32(header[8:12], uint32(len(data)))

	if _, err := conn.Write(header); err != nil {
		c.resetConn(partition)
		return nil, 0, err
	}
	if _, err := conn.Write([]byte(c.topic)); err != nil {
		c.resetConn(partition)
		return nil, 0, err
	}
	if _, err := conn.Write(data); err != nil {
		c.resetConn(partition)
		return nil, 0, err
	}

	respHeader := make([]byte, 14)
	if _, err := io.ReadFull(conn, respHeader); err != nil {
		c.resetConn(partition)
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
				c.resetConn(partition)
				return messages, highWatermark, err
			}

			msgOffset := int64(binary.BigEndian.Uint64(msgHeader[0:8]))
			timestamp := int64(binary.BigEndian.Uint64(msgHeader[8:16]))
			dataLen := int(binary.BigEndian.Uint32(msgHeader[16:20]))

			msgData := make([]byte, dataLen)
			if _, err := io.ReadFull(conn, msgData); err != nil {
				c.resetConn(partition)
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
			c.resetConn(partition)
			return nil, 0, fmt.Errorf("redirect: failed to read leader length")
		}
		leaderLen := int(leaderLenHeader[0])
		leaderAddr := make([]byte, leaderLen)
		if _, err := io.ReadFull(conn, leaderAddr); err != nil {
			c.resetConn(partition)
			return nil, 0, fmt.Errorf("redirect: failed to read leader address")
		}
		c.resetConn(partition)
		return nil, 0, fmt.Errorf("redirect:%s", string(leaderAddr))

	case RespError:
		return nil, 0, fmt.Errorf("server error")

	default:
		return nil, 0, fmt.Errorf("unknown response status: %x", header[1])
	}
}

func (c *Consumer) consumePartition(partition int, wg *sync.WaitGroup) {
	defer wg.Done()

	fmt.Printf("[Consumer] Start consuming partition %d\n", partition)

	for {
		select {
		case <-c.stopCh:
			fmt.Printf("[Consumer] Stop consuming partition %d\n", partition)
			return
		default:
		}

		c.mu.Lock()
		offset := c.offsets[partition]
		c.mu.Unlock()

		messages, highWatermark, err := c.fetch(partition, offset, 1024*1024)
		if err != nil {
			if redirectAddr, ok := isRedirectError(err); ok {
				fmt.Printf("[Consumer] Partition %d redirected to %s\n", partition, redirectAddr)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			fmt.Printf("[Consumer] Partition %d fetch failed: %v\n", partition, err)
			time.Sleep(1 * time.Second)
			continue
		}

		if len(messages) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, msg := range messages {
			fmt.Printf("[Consumer] Partition=%d, Offset=%d, Length=%d\n",
				partition, msg.Offset, len(msg.Value))

			c.mu.Lock()
			c.offsets[partition] = msg.Offset + 1
			c.mu.Unlock()
		}

		latestOffset := int64(0)
		c.mu.Lock()
		latestOffset = c.offsets[partition]
		c.mu.Unlock()

		if err := c.commitOffset(partition, latestOffset); err != nil {
			fmt.Printf("[Consumer] Partition %d commit offset failed: %v\n", partition, err)
		}

		lag := highWatermark - latestOffset
		fmt.Printf("[Consumer] Partition=%d, Lag=%d\n", partition, lag)
	}
}

func (c *Consumer) Start() {
	if err := c.JoinGroup(); err != nil {
		fmt.Printf("[Consumer] Failed to join group: %v\n", err)
		return
	}

	go c.heartbeat()

	var wg sync.WaitGroup
	for _, partition := range c.assignments {
		wg.Add(1)
		go c.consumePartition(partition, &wg)
	}

	wg.Wait()
}

func (c *Consumer) Stop() {
	close(c.stopCh)

	c.mu.Lock()
	for _, conn := range c.connCache {
		conn.Close()
	}
	c.connCache = make(map[int]net.Conn)
	c.mu.Unlock()

	resp, err := http.Get(fmt.Sprintf("%s/consumer/leave?group_id=%s&client_id=%s",
		c.nameserverURL, c.groupID, c.clientID))
	if err != nil {
		fmt.Printf("[LeaveGroup] Failed: %v\n", err)
		return
	}
	resp.Body.Close()

	fmt.Printf("[LeaveGroup] Success\n")
}

type Message struct {
	Offset    int64
	Timestamp int64
	Value     []byte
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

func main() {
	nameserverURL := flag.String("nameserver", "http://localhost:9090", "NameServer URL")
	groupID := flag.String("group", "test-group", "Consumer group ID")
	clientID := flag.String("client", "", "Client ID (auto-generated if not provided)")
	topic := flag.String("topic", "test", "Topic name")
	flag.Parse()

	if *clientID == "" {
		*clientID = fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	}

	fmt.Printf("Starting consumer: nameserver=%s, group=%s, client=%s, topic=%s\n",
		*nameserverURL, *groupID, *clientID, *topic)

	consumer := NewConsumer(*nameserverURL, *groupID, *clientID, *topic)
	defer consumer.Stop()

	consumer.Start()
}