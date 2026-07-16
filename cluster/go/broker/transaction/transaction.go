package transaction

import (
	"fmt"
	"sync"
	"time"
)

type TransactionID string

type TransactionState string

const (
	TransactionPending  TransactionState = "PENDING"
	TransactionPrepared TransactionState = "PREPARED"
	TransactionCommitted TransactionState = "COMMITTED"
	TransactionRolledBack TransactionState = "ROLLED_BACK"
	TransactionUnknown  TransactionState = "UNKNOWN"
)

type TransactionMessage struct {
	Topic     string
	Partition int
	Data      []byte
	Offset    int64
}

type Transaction struct {
	ID             TransactionID
	State          TransactionState
	Messages       []*TransactionMessage
	ProducerID     string
	StartTime      time.Time
	PrepareTime    time.Time
	CommitTime     time.Time
	RollbackTime   time.Time
}

type TransactionManager struct {
	mu              sync.RWMutex
	transactions    map[TransactionID]*Transaction
	pendingTxns     map[string][]*Transaction
	maxTransactions int
	expireAfter     time.Duration
}

func NewTransactionManager(maxTransactions int, expireAfter time.Duration) *TransactionManager {
	tm := &TransactionManager{
		transactions:    make(map[TransactionID]*Transaction),
		pendingTxns:     make(map[string][]*Transaction),
		maxTransactions: maxTransactions,
		expireAfter:     expireAfter,
	}

	go tm.cleanupExpired()

	return tm
}

func (tm *TransactionManager) cleanupExpired() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		tm.mu.Lock()
		now := time.Now()
		for _, txn := range tm.transactions {
			if now.Sub(txn.StartTime) > tm.expireAfter {
				if txn.State == TransactionPending || txn.State == TransactionPrepared {
					txn.State = TransactionRolledBack
					txn.RollbackTime = now
				}
			}
		}
		tm.mu.Unlock()
	}
}

func (tm *TransactionManager) BeginTransaction(producerID string) TransactionID {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if len(tm.transactions) >= tm.maxTransactions {
		var oldest TransactionID
		var oldestTime time.Time
		for id, txn := range tm.transactions {
			if oldestTime.IsZero() || txn.StartTime.Before(oldestTime) {
				oldest = id
				oldestTime = txn.StartTime
			}
		}
		delete(tm.transactions, oldest)
	}

	txnID := TransactionID(fmt.Sprintf("%s-%d", producerID, time.Now().UnixNano()))
	txn := &Transaction{
		ID:          txnID,
		State:       TransactionPending,
		Messages:    make([]*TransactionMessage, 0),
		ProducerID:  producerID,
		StartTime:   time.Now(),
	}

	tm.transactions[txnID] = txn

	if _, exists := tm.pendingTxns[producerID]; !exists {
		tm.pendingTxns[producerID] = make([]*Transaction, 0)
	}
	tm.pendingTxns[producerID] = append(tm.pendingTxns[producerID], txn)

	return txnID
}

func (tm *TransactionManager) AddMessage(txnID TransactionID, topic string, partition int, data []byte) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn, exists := tm.transactions[txnID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txnID)
	}

	if txn.State != TransactionPending {
		return fmt.Errorf("cannot add message to transaction in state %s", txn.State)
	}

	txn.Messages = append(txn.Messages, &TransactionMessage{
		Topic:     topic,
		Partition: partition,
		Data:      data,
	})

	return nil
}

func (tm *TransactionManager) Prepare(txnID TransactionID) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn, exists := tm.transactions[txnID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txnID)
	}

	if txn.State != TransactionPending {
		return fmt.Errorf("cannot prepare transaction in state %s", txn.State)
	}

	if len(txn.Messages) == 0 {
		return fmt.Errorf("cannot prepare empty transaction")
	}

	txn.State = TransactionPrepared
	txn.PrepareTime = time.Now()

	return nil
}

func (tm *TransactionManager) Commit(txnID TransactionID) ([]*TransactionMessage, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn, exists := tm.transactions[txnID]
	if !exists {
		return nil, fmt.Errorf("transaction %s not found", txnID)
	}

	if txn.State != TransactionPrepared {
		return nil, fmt.Errorf("cannot commit transaction in state %s", txn.State)
	}

	txn.State = TransactionCommitted
	txn.CommitTime = time.Now()

	return txn.Messages, nil
}

func (tm *TransactionManager) Rollback(txnID TransactionID) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn, exists := tm.transactions[txnID]
	if !exists {
		return fmt.Errorf("transaction %s not found", txnID)
	}

	if txn.State != TransactionPending && txn.State != TransactionPrepared {
		return fmt.Errorf("cannot rollback transaction in state %s", txn.State)
	}

	txn.State = TransactionRolledBack
	txn.RollbackTime = time.Now()

	return nil
}

func (tm *TransactionManager) GetTransaction(txnID TransactionID) (*Transaction, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	txn, ok := tm.transactions[txnID]
	return txn, ok
}

func (tm *TransactionManager) GetProducerTransactions(producerID string) []*Transaction {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	txns, ok := tm.pendingTxns[producerID]
	if !ok {
		return nil
	}

	result := make([]*Transaction, 0)
	for _, txn := range txns {
		if txn.State == TransactionPending || txn.State == TransactionPrepared {
			result = append(result, txn)
		}
	}

	return result
}

func (tm *TransactionManager) GetActiveTransactions() int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	count := 0
	for _, txn := range tm.transactions {
		if txn.State == TransactionPending || txn.State == TransactionPrepared {
			count++
		}
	}
	return count
}