package nameserver

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

type TxStatus string

const (
	TxPending    TxStatus = "PENDING"
	TxCommitting TxStatus = "COMMITTING"
	TxCommitted  TxStatus = "COMMITTED"
	TxAborted    TxStatus = "ABORTED"
)

type TxMessage struct {
	Topic     string
	Partition int
	Key       string
	Value     []byte
}

type TxState struct {
	TxID        string
	Status      TxStatus
	ProducerID  string
	PartitionInfo []TxMessage
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type TransactionCoordinator struct {
	mu            sync.Mutex
	transactions  map[string]*TxState
	txLogFile     string
	dataDir       string
	timeout       time.Duration
	stopCh        chan struct{}
}

func NewTransactionCoordinator(dataDir string) (*TransactionCoordinator, error) {
	tc := &TransactionCoordinator{
		transactions: make(map[string]*TxState),
		dataDir:      dataDir,
		txLogFile:    fmt.Sprintf("%s/tx_log.dat", dataDir),
		timeout:      30 * time.Second,
		stopCh:       make(chan struct{}),
	}

	os.MkdirAll(dataDir, 0755)
	tc.loadTxLog()

	go tc.startTimeoutChecker()

	return tc, nil
}

func (tc *TransactionCoordinator) BeginTx(producerID string) (string, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	txID := fmt.Sprintf("%s-%d-%d", producerID, time.Now().UnixNano(), randInt())

	tx := &TxState{
		TxID:        txID,
		Status:      TxPending,
		ProducerID:  producerID,
		PartitionInfo: make([]TxMessage, 0),
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	tc.transactions[txID] = tx
	tc.persistTx(tx)

	return txID, nil
}

func (tc *TransactionCoordinator) AddMessage(txID string, topic string, partition int, key string, value []byte) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tx, exists := tc.transactions[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	if tx.Status != TxPending {
		return fmt.Errorf("cannot add message to transaction in state %s", tx.Status)
	}

	tx.PartitionInfo = append(tx.PartitionInfo, TxMessage{
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Value:     value,
	})
	tx.UpdatedAt = time.Now()

	tc.persistTx(tx)
	return nil
}

func (tc *TransactionCoordinator) CommitTx(txID string) error {
	tc.mu.Lock()
	tx, exists := tc.transactions[txID]
	if !exists {
		tc.mu.Unlock()
		return fmt.Errorf("transaction %s not found", txID)
	}

	if tx.Status != TxPending {
		tc.mu.Unlock()
		return fmt.Errorf("cannot commit transaction in state %s", tx.Status)
	}

	tx.Status = TxCommitting
	tx.UpdatedAt = time.Now()
	tc.persistTx(tx)
	tc.mu.Unlock()

	err := tc.doCommit(txID)

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err != nil {
		tx.Status = TxAborted
		tx.UpdatedAt = time.Now()
		tc.persistTx(tx)
		return err
	}

	tx.Status = TxCommitted
	tx.UpdatedAt = time.Now()
	tc.persistTx(tx)

	return nil
}

func (tc *TransactionCoordinator) RollbackTx(txID string) error {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tx, exists := tc.transactions[txID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txID)
	}

	tx.Status = TxAborted
	tx.UpdatedAt = time.Now()
	tc.persistTx(tx)

	return nil
}

func (tc *TransactionCoordinator) GetTxStatus(txID string) (TxStatus, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tx, exists := tc.transactions[txID]
	if !exists {
		return "", fmt.Errorf("transaction %s not found", txID)
	}

	return tx.Status, nil
}

func (tc *TransactionCoordinator) GetPendingTransactions(producerID string) []*TxState {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	result := make([]*TxState, 0)
	for _, tx := range tc.transactions {
		if tx.ProducerID == producerID && tx.Status == TxPending {
			result = append(result, tx)
		}
	}
	return result
}

func (tc *TransactionCoordinator) doCommit(txID string) error {
	tc.mu.Lock()
	_, exists := tc.transactions[txID]
	if !exists {
		tc.mu.Unlock()
		return fmt.Errorf("transaction %s not found", txID)
	}
	tc.mu.Unlock()

	return nil
}

func (tc *TransactionCoordinator) startTimeoutChecker() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-tc.stopCh:
			return
		case <-ticker.C:
			tc.mu.Lock()
			now := time.Now()
			for txID, tx := range tc.transactions {
				if tx.Status == TxPending && now.Sub(tx.CreatedAt) > tc.timeout {
					fmt.Printf("[TC] Transaction %s timed out, rolling back\n", txID)
					tx.Status = TxAborted
					tx.UpdatedAt = now
					tc.persistTx(tx)
				}
			}
			tc.mu.Unlock()
		}
	}
}

func (tc *TransactionCoordinator) persistTx(tx *TxState) error {
	// Rewrite the entire log to avoid unbounded growth
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	for _, tx := range tc.transactions {
		if err := enc.Encode(tx); err != nil {
			return err
		}
	}

	f, err := os.OpenFile(tc.txLogFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(buf.Bytes()); err != nil {
		return err
	}
	return f.Sync()
}

func (tc *TransactionCoordinator) loadTxLog() error {
	data, err := ioutil.ReadFile(tc.txLogFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if len(data) == 0 {
		return nil
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	for {
		var tx TxState
		if err := dec.Decode(&tx); err != nil {
			break
		}
		tc.transactions[tx.TxID] = &tx
	}

	return nil
}

func (tc *TransactionCoordinator) Close() {
	close(tc.stopCh)
}

func randInt() int {
	return int(time.Now().UnixNano() % 1000000)
}