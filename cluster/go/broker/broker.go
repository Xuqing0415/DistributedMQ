package broker

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"distributedmq/audit"
	"distributedmq/broker/idempotent"
	"distributedmq/broker/namespace"
	"distributedmq/broker/storage"
	"distributedmq/broker/tracing"
	"distributedmq/broker/transaction"
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

type RetryMessage struct {
	Topic       string
	Partition   int
	Offset      int64
	Data        []byte
	RetryCount  int
	NextRetryAt time.Time
}

type Broker struct {
	mu               sync.Mutex
	brokerID         int
	addr             string
	clientAddr       string
	nameserverURL    string
	topics           map[string]*TopicPartition
	raftNode         *raft.RaftNode
	applyCh          chan *raft.LogEntry
	stopCh           chan struct{}
	dataDir          string
	offsetCache      map[string]int64
	retentionMs      int64
	retentionBytes   int64
	cleanupTicker    *time.Ticker
	retryQueue       []*RetryMessage
	retryTicker      *time.Ticker
	auditLogger      *audit.AuditLogger
	timeWheel        *storage.TimeWheel
	tracer           *tracing.Tracer
	namespaceManager *namespace.NamespaceManager
	idempotentMgr    *idempotent.IdempotentManager
	txnManager       *transaction.TransactionManager
}

type TopicPartition struct {
	Topic     string
	Partition int
	commitLog *storage.CommitLog
	index     *storage.SparseIndex
}

func NewBroker(addr string, clientAddr string, nameserverURL string, dataDir string) *Broker {
	applyCh := make(chan *raft.LogEntry, 100)
	stopCh := make(chan struct{})

	auditLogger, _ := audit.NewAuditLogger(dataDir + "/audit")

	timeWheel := storage.NewTimeWheel(100, 3600)

	tracer := tracing.NewTracer(1000, 5*time.Minute)

	namespaceManager := namespace.NewNamespaceManager()

	idempotentMgr := idempotent.NewIdempotentManager(10000, 10*time.Minute)

	txnManager := transaction.NewTransactionManager(1000, 5*time.Minute)

	broker := &Broker{
		addr:             addr,
		clientAddr:       clientAddr,
		nameserverURL:    nameserverURL,
		topics:           make(map[string]*TopicPartition),
		applyCh:          applyCh,
		stopCh:           stopCh,
		dataDir:          dataDir,
		offsetCache:      make(map[string]int64),
		retentionMs:      7 * 24 * 60 * 60 * 1000,
		retentionBytes:   0,
		retryQueue:       make([]*RetryMessage, 0),
		auditLogger:      auditLogger,
		timeWheel:        timeWheel,
		tracer:           tracer,
		namespaceManager: namespaceManager,
		idempotentMgr:    idempotentMgr,
		txnManager:       txnManager,
	}

	go broker.handleApply()
	broker.registerAndInitializeRaft()

	return broker
}

func (b *Broker) Start() error {
	b.raftNode.Start()

	go b.startHTTPServer()
	go b.startTCPServer()
	go b.startLogCleanup()
	go b.startRetryProcessor()
	go b.startDelayedMessageProcessor()

	<-b.stopCh
	return nil
}

func (b *Broker) startLogCleanup() {
	b.cleanupTicker = time.NewTicker(10 * time.Minute)
	defer b.cleanupTicker.Stop()

	for {
		select {
		case <-b.cleanupTicker.C:
			b.cleanupOldSegments()
		case <-b.stopCh:
			return
		}
	}
}

func (b *Broker) SetRetention(retentionMs int64, retentionBytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.retentionMs = retentionMs
	b.retentionBytes = retentionBytes
}

func (b *Broker) cleanupOldSegments() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, tp := range b.topics {
		segmentDir := filepath.Join(b.dataDir, "commitlog", fmt.Sprintf("%s-%d", tp.Topic, tp.Partition))
		count, err := storage.CleanupOldSegments(segmentDir, b.retentionMs, b.retentionBytes)
		if err != nil {
			fmt.Printf("[Cleanup] Failed to cleanup %s/%d: %v\n", tp.Topic, tp.Partition, err)
			continue
		}
		if count > 0 {
			fmt.Printf("[Cleanup] Cleaned up %d files for %s/%d\n", count, tp.Topic, tp.Partition)
		}
	}
}

func (b *Broker) startRetryProcessor() {
	b.retryTicker = time.NewTicker(1 * time.Second)
	defer b.retryTicker.Stop()

	for {
		select {
		case <-b.retryTicker.C:
			b.processRetryQueue()
		case <-b.stopCh:
			return
		}
	}
}

func (b *Broker) processRetryQueue() {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	newQueue := make([]*RetryMessage, 0)

	for _, msg := range b.retryQueue {
		if msg.NextRetryAt.After(now) {
			newQueue = append(newQueue, msg)
			continue
		}

		retryTopic := msg.Topic + "_RETRY"
		if _, _, err := b.raftNode.SubmitCommand(retryTopic, int32(msg.Partition), msg.Data); err != nil {
			newQueue = append(newQueue, msg)
			fmt.Printf("[Retry] Failed to re-submit message to %s/%d: %v\n", retryTopic, msg.Partition, err)
			continue
		}

		fmt.Printf("[Retry] Message re-submitted to %s/%d, retryCount=%d\n", retryTopic, msg.Partition, msg.RetryCount)
	}

	b.retryQueue = newQueue
}

func (b *Broker) startDelayedMessageProcessor() {
	dispatchCh := b.timeWheel.GetDispatchChannel()

	for {
		select {
		case <-b.stopCh:
			return
		case msg := <-dispatchCh:
			b.mu.Lock()
			if _, ok := b.topics[msg.Topic]; !ok {
				b.mu.Unlock()
				fmt.Printf("[Delayed] Topic %s not found, dropping message\n", msg.Topic)
				continue
			}
			b.mu.Unlock()

			if _, _, err := b.raftNode.SubmitCommand(msg.Topic, int32(msg.Partition), msg.Data); err != nil {
				fmt.Printf("[Delayed] Failed to submit delayed message: %v\n", err)
			} else {
				fmt.Printf("[Delayed] Message dispatched: topic=%s, partition=%d\n", msg.Topic, msg.Partition)
			}
		}
	}
}

func (b *Broker) handleTCPAck(conn net.Conn, topic string, partition int32, data []byte) {
	if len(data) < 9 {
		b.sendResponse(conn, RespError, "", 0)
		return
	}

	offset := int64(binary.BigEndian.Uint64(data[0:8]))
	status := data[8]

	if status == 0 {
		b.sendResponse(conn, RespSuccess, "", 0)
		return
	}

	b.mu.Lock()
	_, ok := b.topics[topic]
	if !ok {
		b.mu.Unlock()
		b.sendResponse(conn, RespError, "", 0)
		return
	}

	retryCount := 0
	if len(data) >= 10 {
		retryCount = int(data[9])
	}

	retryCount++

	if retryCount >= MaxRetryCount {
		dlqTopic := topic + "_DLQ"
		if len(data) < 11 {
			b.mu.Unlock()
			b.sendResponse(conn, RespError, "", 0)
			return
		}
		if _, _, err := b.raftNode.SubmitCommand(dlqTopic, partition, data[10:]); err != nil {
			b.mu.Unlock()
			b.sendResponse(conn, RespError, "", 0)
			return
		}
		fmt.Printf("[DLQ] Message moved to %s/%d, offset=%d\n", dlqTopic, partition, offset)
		b.mu.Unlock()
		b.sendResponse(conn, RespDLQ, "", 0)
		return
	}

	if len(data) < 11 {
		b.mu.Unlock()
		b.sendResponse(conn, RespError, "", 0)
		return
	}

	b.retryQueue = append(b.retryQueue, &RetryMessage{
		Topic:       topic,
		Partition:   int(partition),
		Offset:      offset,
		Data:        data[10:],
		RetryCount:  retryCount,
		NextRetryAt: time.Now().Add(5 * time.Second),
	})

	b.mu.Unlock()
	b.sendResponse(conn, RespRetried, "", 0)
}

func (b *Broker) startHTTPServer() {
	http.HandleFunc("/produce", b.handleProduce)
	http.HandleFunc("/consume", b.handleConsume)
	http.ListenAndServe(":0", nil)
}

func (b *Broker) Stop() {
	close(b.stopCh)
	b.raftNode.Stop()
	time.Sleep(100 * time.Millisecond)
	close(b.applyCh)
}

func (b *Broker) registerAndInitializeRaft() {
	for {
		resp, err := http.Get(fmt.Sprintf("%s/register?addr=%s&client_addr=%s&hostname=localhost", b.nameserverURL, b.addr, b.clientAddr))
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusOK {
			var result struct {
				BrokerID int `json:"broker_id"`
			}
			body, err := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
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
		resp.Body.Close()
		time.Sleep(1 * time.Second)
	}

	time.Sleep(500 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("%s/broker/peers", b.nameserverURL))
	if err != nil {
		b.raftNode = raft.NewRaftNode(b.addr, []string{}, b.applyCh, filepath.Join(b.dataDir, "raft"))
		return
	}
	defer resp.Body.Close()

	var result struct {
		Peers []string `json:"peers"`
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		b.raftNode = raft.NewRaftNode(b.addr, []string{}, b.applyCh, filepath.Join(b.dataDir, "raft"))
		return
	}
	if err := json.Unmarshal(body, &result); err != nil {
		b.raftNode = raft.NewRaftNode(b.addr, []string{}, b.applyCh, filepath.Join(b.dataDir, "raft"))
		return
	}

	peers := make([]string, 0)
	for _, peer := range result.Peers {
		if peer != b.addr {
			peers = append(peers, peer)
		}
	}

	b.raftNode = raft.NewRaftNode(b.addr, peers, b.applyCh, filepath.Join(b.dataDir, "raft"))
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

func (b *Broker) Produce(topic string, key string, value []byte, userID string, producerID string, seqNum int64) (*Message, error) {
	allowed, err := b.namespaceManager.CheckPermission(userID, topic, namespace.ActionProduce)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, fmt.Errorf("permission denied: user %s cannot produce to topic %s", userID, topic)
	}

	route, err := b.getRoute(topic, key)
	if err != nil {
		return nil, err
	}

	if route.Leader != b.clientAddr {
		return nil, fmt.Errorf("not leader, leader is %s", route.Leader)
	}

	if producerID != "" {
		topicPartition := fmt.Sprintf("%s-%d", topic, route.Partition)
		ok, err := b.idempotentMgr.CheckAndRecord(idempotent.ProducerID(producerID), idempotent.SequenceNumber(seqNum), topicPartition)
		if err != nil {
			return nil, err
		}
		if !ok {
			return &Message{
				Topic:     topic,
				Partition: route.Partition,
				Key:       key,
				Value:     value,
				Timestamp: time.Now().UnixNano(),
			}, nil
		}
	}

	traceCtx := b.tracer.StartTrace(topic, route.Partition)
	b.tracer.AddEvent(traceCtx.TraceID, tracing.TraceEventProduce, "broker", b.addr, map[string]interface{}{
		"topic":      topic,
		"partition":  route.Partition,
		"key":        key,
		"size":       len(value),
		"userID":     userID,
		"producerID": producerID,
		"seqNum":     seqNum,
	})

	msg := &Message{
		Topic:     topic,
		Partition: route.Partition,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}

	if _, _, err := b.raftNode.SubmitCommand(topic, int32(route.Partition), value); err != nil {
		b.tracer.AddEvent(traceCtx.TraceID, tracing.TraceEventDLQ, "broker", b.addr, map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	b.tracer.AddEvent(traceCtx.TraceID, tracing.TraceEventReplicate, "broker", b.addr, map[string]interface{}{
		"status": "submitted",
	})

	return msg, nil
}

func (b *Broker) Consume(topic string, partition int, offset int64, userID string) ([]*Message, error) {
	allowed, err := b.namespaceManager.CheckPermission(userID, topic, namespace.ActionConsume)
	if err != nil {
		return nil, err
	}
	if !allowed {
		return nil, fmt.Errorf("permission denied: user %s cannot consume from topic %s", userID, topic)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	tp, exists := b.topics[key]
	if !exists {
		return nil, fmt.Errorf("topic partition not found")
	}

	msgs := make([]*Message, 0)
	entries, err := tp.index.GetEntriesAfter(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get index entries: %v", err)
	}

	for _, entry := range entries {
		data, err := tp.commitLog.Read(entry.Offset, entry.Size)
		if err != nil {
			fmt.Printf("failed to read from commitlog: %v\n", err)
			continue
		}

		msg := &Message{
			Topic:     topic,
			Partition: partition,
			Offset:    entry.Offset,
			Timestamp: entry.Timestamp,
			Value:     data,
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}

func (b *Broker) Fetch(topic string, partition int, offset int64, maxBytes int) ([]*Message, int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	tp, exists := b.topics[key]
	if !exists {
		return nil, 0, fmt.Errorf("topic partition not found")
	}

	msgs := make([]*Message, 0)
	totalBytes := 0

	entries, err := tp.index.GetEntriesAfter(offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get index entries: %v", err)
	}

	highWatermark := int64(0)
	if len(entries) > 0 {
		highWatermark = entries[len(entries)-1].Offset + 1
	}

	for _, entry := range entries {
		if totalBytes+int(entry.Size) > maxBytes && len(msgs) > 0 {
			break
		}

		data, err := tp.commitLog.Read(entry.Offset, entry.Size)
		if err != nil {
			fmt.Printf("failed to read from commitlog: %v\n", err)
			continue
		}

		msg := &Message{
			Topic:     topic,
			Partition: partition,
			Offset:    entry.Offset,
			Timestamp: entry.Timestamp,
			Value:     data,
		}
		msgs = append(msgs, msg)
		totalBytes += int(entry.Size)
	}

	return msgs, highWatermark, nil
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

	msg, err := b.Produce(req.Topic, req.Key, valueBytes, "", "", 0)
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

	msgs, err := b.Consume(req.Topic, req.Partition, req.Offset, "")
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

func (b *Broker) openCommitLog(topic string, partition int) *storage.CommitLog {
	path := filepath.Join(b.dataDir, "commitlog", fmt.Sprintf("%s-%d.log", topic, partition))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		panic(fmt.Errorf("failed to create commitlog directory: %v", err))
	}

	cl, err := storage.OpenCommitLog(path, int64(1024*1024*1024))
	if err != nil {
		panic(fmt.Errorf("failed to open commitlog via C storage: %v", err))
	}

	return cl
}

func (b *Broker) openSparseIndex(topic string, partition int) *storage.SparseIndex {
	path := filepath.Join(b.dataDir, "index", fmt.Sprintf("%s-%d.idx", topic, partition))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		panic(fmt.Errorf("failed to create index directory: %v", err))
	}

	idx, err := storage.OpenSparseIndex(path)
	if err != nil {
		panic(fmt.Errorf("failed to open sparse index via C storage: %v", err))
	}

	return idx
}

func (b *Broker) handleApply() {
	batch := make([]*raft.LogEntry, 0, 100)
	timer := time.NewTimer(5 * time.Millisecond)

	for {
		select {
		case <-b.stopCh:
			if len(batch) > 0 {
				b.applyBatch(batch)
			}
			timer.Stop()
			return
		case entry, ok := <-b.applyCh:
			if !ok {
				if len(batch) > 0 {
					b.applyBatch(batch)
				}
				timer.Stop()
				return
			}

			batch = append(batch, entry)

			if len(batch) >= 100 {
				timer.Stop()
				b.applyBatch(batch)
				batch = batch[:0]
				timer.Reset(5 * time.Millisecond)
			} else if len(batch) == 1 {
				timer.Reset(5 * time.Millisecond)
			}
		case <-timer.C:
			if len(batch) > 0 {
				b.applyBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func (b *Broker) applyBatch(batch []*raft.LogEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, entry := range batch {
		tp := b.getOrCreateTopicPartition(entry.Topic, int(entry.Partition))

		offset, err := tp.commitLog.Append(entry.Data)
		if err != nil {
			fmt.Printf("failed to append to commitlog: %v\n", err)
			continue
		}

		if err := tp.index.Put(offset, int32(len(entry.Data)), time.Now().UnixNano()); err != nil {
			fmt.Printf("failed to put index entry: %v\n", err)
		}
	}

	for _, entry := range batch {
		key := fmt.Sprintf("%s-%d", entry.Topic, entry.Partition)
		if tp, ok := b.topics[key]; ok {
			tp.commitLog.Sync()
		}
	}

	fmt.Printf("applied batch: %d entries\n", len(batch))
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

const (
	Magic          = 0xD0
	CmdProduce     = 0x01
	CmdFetch       = 0x02
	CmdJoinGroup   = 0x03
	CmdHeartbeat   = 0x04
	CmdCommitOffset = 0x05
	CmdLeaveGroup  = 0x06
	CmdAck         = 0x07

	RespSuccess    = 0x00
	RespRedirect   = 0x01
	RespError      = 0xFF
	RespDLQ        = 0x02
	RespRetried    = 0x03

	MaxRetryCount = 3
)

type ProduceRequest struct {
	Topic     string
	Partition int32
	Data      []byte
}

type ProduceResponse struct {
	Status  byte
	Leader  string
	Offset  int64
}

func (b *Broker) startTCPServer() {
	lis, err := net.Listen("tcp", b.clientAddr)
	if err != nil {
		fmt.Printf("failed to start TCP server on %s: %v\n", b.clientAddr, err)
		return
	}
	fmt.Printf("TCP server listening on %s\n", b.clientAddr)

	for {
		conn, err := lis.Accept()
		if err != nil {
			select {
			case <-b.stopCh:
				return
			default:
				fmt.Printf("failed to accept connection: %v\n", err)
				continue
			}
		}
		go b.handleClient(conn)
	}
}

func (b *Broker) handleClient(conn net.Conn) {
	defer conn.Close()

	for {
		header := make([]byte, 12)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}

		if header[0] != Magic {
			fmt.Printf("invalid magic byte: %x\n", header[0])
			return
		}

		cmdID := header[1]
		topicLen := binary.BigEndian.Uint16(header[2:4])
		partition := int32(binary.BigEndian.Uint32(header[4:8]))
		dataLen := binary.BigEndian.Uint32(header[8:12])

		topicBuf := make([]byte, topicLen)
		if _, err := io.ReadFull(conn, topicBuf); err != nil {
			fmt.Println("failed to read topic")
			return
		}
		topic := string(topicBuf)

		data := make([]byte, dataLen)
		if _, err := io.ReadFull(conn, data); err != nil {
			fmt.Println("failed to read data")
			return
		}

		switch cmdID {
		case CmdProduce:
			b.handleTCPProduce(conn, topic, partition, data)
		case CmdFetch:
			b.handleTCPFetch(conn, topic, partition, data)
		case CmdJoinGroup:
			b.handleTCPJoinGroup(conn, topic, partition, data)
		case CmdHeartbeat:
			b.handleTCPHeartbeat(conn, topic, partition, data)
		case CmdCommitOffset:
			b.handleTCPCommitOffset(conn, topic, partition, data)
		case CmdLeaveGroup:
			b.handleTCPLeaveGroup(conn, topic, partition, data)
		case CmdAck:
			b.handleTCPAck(conn, topic, partition, data)
		default:
			fmt.Printf("unknown command: %x\n", cmdID)
		}
	}
}

func (b *Broker) handleTCPProduce(conn net.Conn, topic string, partition int32, data []byte) {
	leader, err := b.GetPartitionLeader(topic, int(partition))
	if err != nil {
		b.sendResponse(conn, RespError, "", 0)
		return
	}

	if leader != b.clientAddr {
		b.sendResponse(conn, RespRedirect, leader, 0)
		return
	}

	if !b.IsPartitionLeader(topic, int(partition)) {
		b.sendResponse(conn, RespRedirect, leader, 0)
		return
	}

	acks := int8(1)
	msgData := data
	if len(data) >= 1 {
		acks = int8(data[0])
		msgData = data[1:]
	}

	_, commitCh, err := b.raftNode.SubmitCommand(topic, partition, msgData)
	if err != nil {
		b.sendResponse(conn, RespError, "", 0)
		return
	}

	if acks == -1 {
		select {
		case <-commitCh:
		case <-time.After(5 * time.Second):
			b.sendResponse(conn, RespError, "", 0)
			return
		}
	}

	if b.auditLogger != nil {
		clientIP := conn.RemoteAddr().String()
		b.auditLogger.LogProduce(clientIP, topic, int(partition), 0, len(msgData))
	}

	b.sendResponse(conn, RespSuccess, "", 0)
}

func (b *Broker) handleTCPFetch(conn net.Conn, topic string, partition int32, data []byte) {
	if len(data) < 8 {
		b.sendResponse(conn, RespError, "", 0)
		return
	}

	offset := int64(binary.BigEndian.Uint64(data[0:8]))
	maxBytes := int32(1024 * 1024)
	if len(data) >= 12 {
		maxBytes = int32(binary.BigEndian.Uint32(data[8:12]))
	}

	msgs, highWatermark, err := b.Fetch(topic, int(partition), offset, int(maxBytes))
	if err != nil {
		b.sendResponse(conn, RespError, "", 0)
		return
	}

	msgCount := len(msgs)
	var responseData []byte
	for _, msg := range msgs {
		entry := struct {
			Offset    int64
			Timestamp int64
			DataLen   int32
			Data      []byte
		}{
			Offset:    msg.Offset,
			Timestamp: msg.Timestamp,
			DataLen:   int32(len(msg.Value)),
			Data:      msg.Value,
		}
		entryBuf := make([]byte, 8+8+4+len(entry.Data))
		binary.BigEndian.PutUint64(entryBuf[0:8], uint64(entry.Offset))
		binary.BigEndian.PutUint64(entryBuf[8:16], uint64(entry.Timestamp))
		binary.BigEndian.PutUint32(entryBuf[16:20], uint32(entry.DataLen))
		copy(entryBuf[20:], entry.Data)
		responseData = append(responseData, entryBuf...)
	}

	if b.auditLogger != nil && msgCount > 0 {
		clientIP := conn.RemoteAddr().String()
		for _, msg := range msgs {
			b.auditLogger.LogConsume(clientIP, topic, int(partition), msg.Offset, "", len(msg.Value))
		}
	}

	respBuf := make([]byte, 1+1+8+4+len(responseData))
	respBuf[0] = Magic
	respBuf[1] = RespSuccess
	binary.BigEndian.PutUint64(respBuf[2:10], uint64(highWatermark))
	binary.BigEndian.PutUint32(respBuf[10:14], uint32(msgCount))
	copy(respBuf[14:], responseData)
	conn.Write(respBuf)
}

func (b *Broker) sendResponse(conn net.Conn, status byte, leader string, offset int64) {
	respBuf := make([]byte, 1+1)
	respBuf[0] = Magic
	respBuf[1] = status

	if status == RespRedirect && leader != "" {
		respBuf = append(respBuf, byte(len(leader)))
		respBuf = append(respBuf, leader...)
	}

	if status == RespSuccess {
		offsetBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(offsetBuf, uint64(offset))
		respBuf = append(respBuf, offsetBuf...)
	}

	conn.Write(respBuf)
}

func (b *Broker) IsPartitionLeader(topic string, partition int) bool {
	if b.raftNode == nil {
		return false
	}
	state, _ := b.raftNode.GetState()
	return state == raft.Leader
}

func (b *Broker) GetPartitionLeader(topic string, partition int) (string, error) {
	route, err := b.getRoute(topic, "")
	if err != nil {
		return "", err
	}
	return route.Leader, nil
}

func (b *Broker) handleTCPJoinGroup(conn net.Conn, topic string, partition int32, data []byte) {
	var request struct {
		GroupID  string `json:"group_id"`
		ClientID string `json:"client_id"`
	}
	if err := json.Unmarshal(data, &request); err != nil {
		b.sendResponse(conn, RespError, "", 0)
		return
	}

	resp, err := http.Get(fmt.Sprintf("%s/consumer/join?group_id=%s&client_id=%s&topic=%s",
		b.nameserverURL, request.GroupID, request.ClientID, topic))
	if err != nil {
		b.sendResponse(conn, RespError, "", 0)
		return
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	respBuf := make([]byte, 1+1+len(body))
	respBuf[0] = Magic
	if resp.StatusCode == http.StatusOK {
		respBuf[1] = RespSuccess
	} else {
		respBuf[1] = RespError
	}
	copy(respBuf[2:], body)
	conn.Write(respBuf)
}

func (b *Broker) handleTCPHeartbeat(conn net.Conn, topic string, partition int32, data []byte) {
	var request struct {
		GroupID  string `json:"group_id"`
		ClientID string `json:"client_id"`
	}
	if err := json.Unmarshal(data, &request); err != nil {
		b.sendResponse(conn, RespError, "", 0)
		return
	}

	resp, err := http.Get(fmt.Sprintf("%s/consumer/heartbeat?group_id=%s&client_id=%s",
		b.nameserverURL, request.GroupID, request.ClientID))
	if err != nil {
		b.sendResponse(conn, RespError, "", 0)
		return
	}
	defer resp.Body.Close()

	respBuf := make([]byte, 2)
	respBuf[0] = Magic
	if resp.StatusCode == http.StatusOK {
		respBuf[1] = RespSuccess
	} else {
		respBuf[1] = RespError
	}
	conn.Write(respBuf)
}

func (b *Broker) handleTCPCommitOffset(conn net.Conn, topic string, partition int32, data []byte) {
	var request struct {
		GroupID string `json:"group_id"`
		Offset  int64  `json:"offset"`
	}
	if err := json.Unmarshal(data, &request); err != nil {
		b.sendResponse(conn, RespError, "", 0)
		return
	}

	resp, err := http.Get(fmt.Sprintf("%s/consumer/commit_offset?group_id=%s&topic=%s&partition=%d&offset=%d",
		b.nameserverURL, request.GroupID, topic, partition, request.Offset))
	if err != nil {
		b.sendResponse(conn, RespError, "", 0)
		return
	}
	defer resp.Body.Close()

	respBuf := make([]byte, 2)
	respBuf[0] = Magic
	if resp.StatusCode == http.StatusOK {
		respBuf[1] = RespSuccess
	} else {
		respBuf[1] = RespError
	}
	conn.Write(respBuf)
}

func (b *Broker) handleTCPLeaveGroup(conn net.Conn, topic string, partition int32, data []byte) {
	var request struct {
		GroupID  string `json:"group_id"`
		ClientID string `json:"client_id"`
	}
	if err := json.Unmarshal(data, &request); err != nil {
		b.sendResponse(conn, RespError, "", 0)
		return
	}

	resp, err := http.Get(fmt.Sprintf("%s/consumer/leave?group_id=%s&client_id=%s",
		b.nameserverURL, request.GroupID, request.ClientID))
	if err != nil {
		b.sendResponse(conn, RespError, "", 0)
		return
	}
	defer resp.Body.Close()

	respBuf := make([]byte, 2)
	respBuf[0] = Magic
	if resp.StatusCode == http.StatusOK {
		respBuf[1] = RespSuccess
	} else {
		respBuf[1] = RespError
	}
	conn.Write(respBuf)
}
