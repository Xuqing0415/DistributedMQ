package broker

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"distributedmq/raft"
)

type Message struct {
	Topic     string
	Partition int
	Key       string
	Value     []byte
	Offset    int64
	Timestamp int64
}

type Broker struct {
	mu            sync.Mutex
	brokerID      int
	addr          string
	nameserverURL string
	topics        map[string]*TopicPartition
	raftNode      *raft.RaftNode
	applyCh       chan []byte
	stopCh        chan struct{}
	dataDir       string
}

type TopicPartition struct {
	Topic     string
	Partition int
	commitLog *commitLog
	index     *sparseIndex
}

type commitLog struct {
	path      string
	file      *os.File
	mmapData  []byte
	fileSize  int64
	writePos  int64
}

type sparseIndex struct {
	path    string
	entries []indexEntry
}

type indexEntry struct {
	offset    int64
	size      int32
	timestamp int64
}

func NewBroker(addr string, nameserverURL string, dataDir string, peers []string) *Broker {
	applyCh := make(chan []byte, 100)
	stopCh := make(chan struct{})

	broker := &Broker{
		addr:          addr,
		nameserverURL: nameserverURL,
		topics:        make(map[string]*TopicPartition),
		applyCh:       applyCh,
		stopCh:        stopCh,
		dataDir:       dataDir,
	}

	broker.raftNode = raft.NewRaftNode(addr, peers, applyCh, filepath.Join(dataDir, "raft"))

	go broker.handleApply()
	go broker.registerWithNameServer()

	return broker
}

func (b *Broker) Start() error {
	b.raftNode.Start()

	go b.startHTTPServer()

	return nil
}

func (b *Broker) startHTTPServer() {
	http.HandleFunc("/produce", b.handleProduce)
	http.HandleFunc("/consume", b.handleConsume)
	http.ListenAndServe(b.addr, nil)
}

func (b *Broker) Stop() {
	close(b.stopCh)
	b.raftNode.Stop()
	time.Sleep(100 * time.Millisecond)
	close(b.applyCh)
}

func (b *Broker) registerWithNameServer() {
	for {
		resp, err := http.Get(fmt.Sprintf("%s/register?addr=%s&hostname=localhost", b.nameserverURL, b.addr))
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			var result struct {
				BrokerID int `json:"broker_id"`
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			if err := json.Unmarshal(body, &result); err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			b.brokerID = result.BrokerID
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func (b *Broker) CreateTopic(topic string, partitions int) error {
	resp, err := http.Get(fmt.Sprintf("%s/topic/create?topic=%s&partitions=%d", b.nameserverURL, topic, partitions))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to create topic")
	}

	return nil
}

func (b *Broker) Produce(topic string, key string, value []byte) (*Message, error) {
	route, err := b.getRoute(topic, key)
	if err != nil {
		return nil, err
	}

	if route.Leader != b.addr {
		return nil, fmt.Errorf("not leader, leader is %s", route.Leader)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	tp := b.getOrCreateTopicPartition(topic, route.Partition)

	msg := &Message{
		Topic:     topic,
		Partition: route.Partition,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}

	cmd := &Command{
		Type:      "produce",
		Topic:     topic,
		Partition: route.Partition,
		Key:       key,
		Value:     value,
		Timestamp: msg.Timestamp,
	}

	cmdBytes, _ := json.Marshal(cmd)
	if err := b.raftNode.SubmitCommand(cmdBytes); err != nil {
		return nil, err
	}

	return msg, nil
}

func (b *Broker) Consume(topic string, partition int, offset int64) ([]*Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	tp, exists := b.topics[key]
	if !exists {
		return nil, fmt.Errorf("topic partition not found")
	}

	msgs := make([]*Message, 0)
	entries := tp.index.getEntriesAfter(offset)

	for _, entry := range entries {
		data := make([]byte, entry.size)
		copy(data, tp.commitLog.mmapData[entry.offset:entry.offset+int64(entry.size)])

		msg := &Message{
			Topic:     topic,
			Partition: partition,
			Offset:    entry.offset,
			Timestamp: entry.timestamp,
			Value:     data,
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}

func (b *Broker) getRoute(topic string, key string) (*RouteInfo, error) {
	resp, err := http.Get(fmt.Sprintf("%s/route?topic=%s&key=%s", b.nameserverURL, topic, key))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get route")
	}

	var route RouteInfo
	body, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(body, &route)

	return &route, nil
}

func (b *Broker) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RequestID string `json:"requestId"`
		Type      string `json:"type"`
		Topic     string `json:"topic"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid request"})
		return
	}
	defer r.Body.Close()

	valueBytes, err := decodeBase64(req.Value)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid base64 value"})
		return
	}

	msg, err := b.Produce(req.Topic, req.Key, valueBytes)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"requestId": req.RequestID,
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"key":       msg.Key,
		"value":     encodeBase64(msg.Value),
		"offset":    msg.Offset,
		"timestamp": msg.Timestamp,
	})
}

func (b *Broker) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RequestID string `json:"requestId"`
		Type      string `json:"type"`
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid request"})
		return
	}
	defer r.Body.Close()

	msgs, err := b.Consume(req.Topic, req.Partition, req.Offset)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if len(msgs) == 0 {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"requestId": req.RequestID,
			"messages":  []interface{}{},
		})
		return
	}

	responseMsgs := make([]map[string]interface{}, len(msgs))
	for i, msg := range msgs {
		responseMsgs[i] = map[string]interface{}{
			"topic":     msg.Topic,
			"partition": msg.Partition,
			"key":       msg.Key,
			"value":     encodeBase64(msg.Value),
			"offset":    msg.Offset,
			"timestamp": msg.Timestamp,
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"requestId": req.RequestID,
		"messages":  responseMsgs,
	})
}

func encodeBase64(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func decodeBase64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

func (b *Broker) getOrCreateTopicPartition(topic string, partition int) *TopicPartition {
	key := fmt.Sprintf("%s-%d", topic, partition)
	if tp, exists := b.topics[key]; exists {
		return tp
	}

	cl := b.openCommitLog(topic, partition)
	idx := b.openSparseIndex(topic, partition)

	tp := &TopicPartition{
		Topic:     topic,
		Partition: partition,
		commitLog: cl,
		index:     idx,
	}
	b.topics[key] = tp

	return tp
}

func (b *Broker) openCommitLog(topic string, partition int) *commitLog {
	path := filepath.Join(b.dataDir, "commitlog", fmt.Sprintf("%s-%d.log", topic, partition))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		panic(fmt.Errorf("failed to create commitlog directory: %v", err))
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic(fmt.Errorf("failed to open commitlog file: %v", err))
	}

	fileSize := int64(1024 * 1024 * 1024)
	fi, err := file.Stat()
	if err != nil {
		file.Close()
		panic(fmt.Errorf("failed to stat commitlog file: %v", err))
	}

	if fi.Size() < fileSize {
		if err := file.Truncate(fileSize); err != nil {
			file.Close()
			panic(fmt.Errorf("failed to truncate commitlog file: %v", err))
		}
	}

	mmapData, err := sysMmap(int(file.Fd()), 0, int(fileSize))
	if err != nil {
		file.Close()
		panic(fmt.Errorf("failed to mmap commitlog file: %v", err))
	}

	return &commitLog{
		path:     path,
		file:     file,
		mmapData: mmapData,
		fileSize: fileSize,
		writePos: fi.Size(),
	}
}

func (b *Broker) openSparseIndex(topic string, partition int) *sparseIndex {
	path := filepath.Join(b.dataDir, "index", fmt.Sprintf("%s-%d.idx", topic, partition))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		panic(fmt.Errorf("failed to create index directory: %v", err))
	}

	entries := make([]indexEntry, 0)
	if _, err := os.Stat(path); err == nil {
		data, err := ioutil.ReadFile(path)
		if err != nil {
			panic(fmt.Errorf("failed to read index file: %v", err))
		}
		buf := bytes.NewReader(data)
		var count int64
		if err := binary.Read(buf, binary.LittleEndian, &count); err != nil {
			panic(fmt.Errorf("failed to read index count: %v", err))
		}
		for i := int64(0); i < count; i++ {
			var entry indexEntry
			if err := binary.Read(buf, binary.LittleEndian, &entry.offset); err != nil {
				panic(fmt.Errorf("failed to read index offset: %v", err))
			}
			if err := binary.Read(buf, binary.LittleEndian, &entry.size); err != nil {
				panic(fmt.Errorf("failed to read index size: %v", err))
			}
			if err := binary.Read(buf, binary.LittleEndian, &entry.timestamp); err != nil {
				panic(fmt.Errorf("failed to read index timestamp: %v", err))
			}
			entries = append(entries, entry)
		}
	}

	return &sparseIndex{
		path:    path,
		entries: entries,
	}
}

func (b *Broker) handleApply() {
	for {
		select {
		case <-b.stopCh:
			return
		case cmdBytes, ok := <-b.applyCh:
			if !ok {
				return
			}
			var cmd Command
			if err := json.Unmarshal(cmdBytes, &cmd); err != nil {
				fmt.Printf("failed to unmarshal command: %v\n", err)
				continue
			}

			b.mu.Lock()
			tp := b.getOrCreateTopicPartition(cmd.Topic, cmd.Partition)

			switch cmd.Type {
			case "produce":
				offset := tp.commitLog.writePos
				if offset+int64(len(cmd.Value)) > tp.commitLog.fileSize {
					fmt.Printf("commitlog full for topic %s partition %d\n", cmd.Topic, cmd.Partition)
					b.mu.Unlock()
					continue
				}
				copy(tp.commitLog.mmapData[offset:offset+int64(len(cmd.Value))], cmd.Value)
				tp.commitLog.writePos += int64(len(cmd.Value))

				tp.index.addEntry(offset, int32(len(cmd.Value)), cmd.Timestamp)
				tp.index.flush()
				tp.commitLog.sync()
			}
			b.mu.Unlock()
		}
	}
}

func (cl *commitLog) sync() {
	if err := syscall.Msync(cl.mmapData, syscall.MS_SYNC); err != nil {
		fmt.Printf("failed to sync commitlog: %v\n", err)
	}
}

func (si *sparseIndex) addEntry(offset int64, size int32, timestamp int64) {
	si.entries = append(si.entries, indexEntry{
		offset:    offset,
		size:      size,
		timestamp: timestamp,
	})
}

func (si *sparseIndex) flush() {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, int64(len(si.entries)))
	for _, entry := range si.entries {
		binary.Write(&buf, binary.LittleEndian, entry.offset)
		binary.Write(&buf, binary.LittleEndian, entry.size)
		binary.Write(&buf, binary.LittleEndian, entry.timestamp)
	}
	ioutil.WriteFile(si.path, buf.Bytes(), 0644)
}

func (si *sparseIndex) getEntriesAfter(offset int64) []indexEntry {
	result := make([]indexEntry, 0)
	for _, entry := range si.entries {
		if entry.offset > offset {
			result = append(result, entry)
		}
	}
	return result
}

type Command struct {
	Type      string `json:"type"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	Timestamp int64  `json:"timestamp"`
}

type RouteInfo struct {
	Topic     string   `json:"topic"`
	Partition int      `json:"partition"`
	Leader    string   `json:"leader"`
	Replicas  []string `json:"replicas"`
	ISR       []string `json:"isr"`
}

func sysMmap(fd int, offset int, length int) ([]byte, error) {
	return syscall.Mmap(fd, offset, length, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
}
