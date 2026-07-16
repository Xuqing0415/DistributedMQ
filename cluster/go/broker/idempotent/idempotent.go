package idempotent

import (
	"fmt"
	"sync"
	"time"
)

type ProducerID string

type SequenceNumber int64

type MessageKey struct {
	ProducerID     ProducerID
	SequenceNumber SequenceNumber
}

type IdempotentManager struct {
	mu            sync.RWMutex
	producerState map[ProducerID]*ProducerState
	maxProducers  int
	expireAfter   time.Duration
}

type ProducerState struct {
	LastSequenceNumber SequenceNumber
	LastActiveTime     time.Time
	TopicPartition     string
}

func NewIdempotentManager(maxProducers int, expireAfter time.Duration) *IdempotentManager {
	im := &IdempotentManager{
		producerState: make(map[ProducerID]*ProducerState),
		maxProducers:  maxProducers,
		expireAfter:   expireAfter,
	}

	go im.cleanupExpired()

	return im
}

func (im *IdempotentManager) cleanupExpired() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		im.mu.Lock()
		now := time.Now()
		for pid, state := range im.producerState {
			if now.Sub(state.LastActiveTime) > im.expireAfter {
				delete(im.producerState, pid)
			}
		}
		im.mu.Unlock()
	}
}

func (im *IdempotentManager) CheckAndRecord(pid ProducerID, seq SequenceNumber, topicPartition string) (bool, error) {
	im.mu.Lock()
	defer im.mu.Unlock()

	state, exists := im.producerState[pid]
	if !exists {
		if len(im.producerState) >= im.maxProducers {
			var oldest ProducerID
			var oldestTime time.Time
			for id, s := range im.producerState {
				if oldestTime.IsZero() || s.LastActiveTime.Before(oldestTime) {
					oldest = id
					oldestTime = s.LastActiveTime
				}
			}
			delete(im.producerState, oldest)
		}

		im.producerState[pid] = &ProducerState{
			LastSequenceNumber: seq,
			LastActiveTime:     time.Now(),
			TopicPartition:     topicPartition,
		}
		return true, nil
	}

	if state.TopicPartition != topicPartition {
		return false, fmt.Errorf("producer %s is bound to different topic/partition: %s vs %s",
			pid, state.TopicPartition, topicPartition)
	}

	if seq < state.LastSequenceNumber {
		return false, fmt.Errorf("sequence number %d is less than last recorded %d", seq, state.LastSequenceNumber)
	}

	if seq == state.LastSequenceNumber {
		return false, nil
	}

	state.LastSequenceNumber = seq
	state.LastActiveTime = time.Now()
	return true, nil
}

func (im *IdempotentManager) GetProducerState(pid ProducerID) (*ProducerState, bool) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	state, ok := im.producerState[pid]
	return state, ok
}

func (im *IdempotentManager) RemoveProducer(pid ProducerID) {
	im.mu.Lock()
	defer im.mu.Unlock()

	delete(im.producerState, pid)
}

func (im *IdempotentManager) GetActiveProducers() int {
	im.mu.RLock()
	defer im.mu.RUnlock()

	return len(im.producerState)
}