package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type AuditRecord struct {
	Timestamp   int64  `json:"timestamp"`
	ClientIP    string `json:"client_ip"`
	Operation   string `json:"op"`
	Topic       string `json:"topic"`
	Partition   int    `json:"partition"`
	Offset      int64  `json:"offset"`
	GroupID     string `json:"group_id,omitempty"`
	MsgSize     int    `json:"msg_size"`
}

type AuditLogger struct {
	mu      sync.Mutex
	ch      chan AuditRecord
	done    chan struct{}
	logFile *os.File
}

func NewAuditLogger(logDir string) (*AuditLogger, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}

	logPath := filepath.Join(logDir, "audit.log")
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return nil, err
	}

	logger := &AuditLogger{
		ch:      make(chan AuditRecord, 1000),
		done:    make(chan struct{}),
		logFile: file,
	}

	go logger.run()

	return logger, nil
}

func (a *AuditLogger) run() {
	for {
		select {
		case record := <-a.ch:
			a.write(record)
		case <-a.done:
			return
		}
	}
}

func (a *AuditLogger) write(record AuditRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()

	record.Timestamp = time.Now().UnixNano()
	data, err := json.Marshal(record)
	if err != nil {
		fmt.Printf("[Audit] Failed to marshal record: %v\n", err)
		return
	}

	if _, err := a.logFile.WriteString(string(data) + "\n"); err != nil {
		fmt.Printf("[Audit] Failed to write record: %v\n", err)
	}
}

func (a *AuditLogger) Log(record AuditRecord) {
	select {
	case a.ch <- record:
	default:
		fmt.Printf("[Audit] Channel full, dropping record\n")
	}
}

func (a *AuditLogger) Close() {
	close(a.done)
	a.logFile.Close()
}

func (a *AuditLogger) LogProduce(clientIP, topic string, partition int, offset int64, msgSize int) {
	a.Log(AuditRecord{
		ClientIP:  clientIP,
		Operation: "PRODUCE",
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		MsgSize:   msgSize,
	})
}

func (a *AuditLogger) LogConsume(clientIP, topic string, partition int, offset int64, groupID string, msgSize int) {
	a.Log(AuditRecord{
		ClientIP:  clientIP,
		Operation: "CONSUME",
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		GroupID:   groupID,
		MsgSize:   msgSize,
	})
}