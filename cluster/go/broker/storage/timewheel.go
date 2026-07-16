package storage

import (
	"fmt"
	"sync"
	"time"
)

type TimeWheel struct {
	mu           sync.Mutex
	tickMs       int64
	wheelSize    int
	currentIndex int
	buckets      []*bucket
	ticker       *time.Ticker
	stopCh       chan struct{}
	dispatchCh   chan *DelayedMessage
}

type bucket struct {
	mu       sync.Mutex
	messages []*DelayedMessage
}

type DelayedMessage struct {
	Topic     string
	Partition int
	Data      []byte
	DeliveryAt time.Time
}

func NewTimeWheel(tickMs int64, wheelSize int) *TimeWheel {
	tw := &TimeWheel{
		tickMs:      tickMs,
		wheelSize:   wheelSize,
		buckets:     make([]*bucket, wheelSize),
		stopCh:      make(chan struct{}),
		dispatchCh:  make(chan *DelayedMessage, 1000),
	}

	for i := 0; i < wheelSize; i++ {
		tw.buckets[i] = &bucket{messages: make([]*DelayedMessage, 0)}
	}

	go tw.run()

	return tw
}

func (tw *TimeWheel) run() {
	tw.ticker = time.NewTicker(time.Duration(tw.tickMs) * time.Millisecond)
	defer tw.ticker.Stop()

	for {
		select {
		case <-tw.stopCh:
			return
		case <-tw.ticker.C:
			tw.tick()
		}
	}
}

func (tw *TimeWheel) tick() {
	tw.mu.Lock()
	currentBucket := tw.buckets[tw.currentIndex]
	tw.currentIndex = (tw.currentIndex + 1) % tw.wheelSize
	tw.mu.Unlock()

	currentBucket.mu.Lock()
	messages := currentBucket.messages
	currentBucket.messages = currentBucket.messages[:0]
	currentBucket.mu.Unlock()

	for _, msg := range messages {
		if time.Now().After(msg.DeliveryAt) || time.Now().Equal(msg.DeliveryAt) {
			tw.dispatchMessage(msg)
		} else {
			tw.AddMessage(msg)
		}
	}
}

func (tw *TimeWheel) AddMessage(msg *DelayedMessage) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	delayMs := int64(msg.DeliveryAt.Sub(time.Now()).Milliseconds())
	if delayMs <= 0 {
		tw.dispatchMessage(msg)
		return
	}

	ticks := delayMs / tw.tickMs
	index := (tw.currentIndex + int(ticks)) % tw.wheelSize

	tw.buckets[index].mu.Lock()
	tw.buckets[index].messages = append(tw.buckets[index].messages, msg)
	tw.buckets[index].mu.Unlock()
}

func (tw *TimeWheel) dispatchMessage(msg *DelayedMessage) {
	select {
	case tw.dispatchCh <- msg:
	default:
		fmt.Printf("[TimeWheel] Dispatch channel full, dropping message: topic=%s\n", msg.Topic)
	}
}

func (tw *TimeWheel) GetDispatchChannel() <-chan *DelayedMessage {
	return tw.dispatchCh
}

func (tw *TimeWheel) Close() {
	close(tw.stopCh)
}

func (tw *TimeWheel) GetPendingCount() int {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	count := 0
	for _, bucket := range tw.buckets {
		bucket.mu.Lock()
		count += len(bucket.messages)
		bucket.mu.Unlock()
	}
	return count
}