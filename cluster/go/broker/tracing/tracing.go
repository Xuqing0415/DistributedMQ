package tracing

import (
	"fmt"
	"sync"
	"time"
)

type TraceID string

type TraceEvent struct {
	Timestamp time.Time
	EventType TraceEventType
	Service   string
	NodeID    string
	Details   map[string]interface{}
}

type TraceEventType string

const (
	TraceEventProduce      TraceEventType = "PRODUCE"
	TraceEventReceive      TraceEventType = "RECEIVE"
	TraceEventPersist      TraceEventType = "PERSIST"
	TraceEventReplicate    TraceEventType = "REPLICATE"
	TraceEventCommit       TraceEventType = "COMMIT"
	TraceEventConsume      TraceEventType = "CONSUME"
	TraceEventAck          TraceEventType = "ACK"
	TraceEventRetry        TraceEventType = "RETRY"
	TraceEventDLQ          TraceEventType = "DLQ"
	TraceEventDelayed      TraceEventType = "DELAYED"
	TraceEventDispatch     TraceEventType = "DISPATCH"
)

type TraceContext struct {
	TraceID TraceID
	Topic   string
	Partition int
	Offset  int64
	Events  []*TraceEvent
	Created time.Time
	Expires time.Time
}

type Tracer struct {
	mu          sync.RWMutex
	traces      map[TraceID]*TraceContext
	maxTraces   int
	expireAfter time.Duration
}

func NewTracer(maxTraces int, expireAfter time.Duration) *Tracer {
	t := &Tracer{
		traces:      make(map[TraceID]*TraceContext),
		maxTraces:   maxTraces,
		expireAfter: expireAfter,
	}

	go t.cleanupExpired()

	return t
}

func (t *Tracer) cleanupExpired() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		t.mu.Lock()
		now := time.Now()
		for traceID, ctx := range t.traces {
			if ctx.Expires.Before(now) {
				delete(t.traces, traceID)
			}
		}
		t.mu.Unlock()
	}
}

func (t *Tracer) StartTrace(topic string, partition int) *TraceContext {
	traceID := TraceID(fmt.Sprintf("%d-%d-%d", time.Now().UnixNano(), partition, len(t.traces)))

	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.traces) >= t.maxTraces {
		var oldest TraceID
		var oldestTime time.Time
		for id, ctx := range t.traces {
			if oldestTime.IsZero() || ctx.Created.Before(oldestTime) {
				oldest = id
				oldestTime = ctx.Created
			}
		}
		delete(t.traces, oldest)
	}

	ctx := &TraceContext{
		TraceID:    traceID,
		Topic:      topic,
		Partition:  partition,
		Events:     make([]*TraceEvent, 0),
		Created:    time.Now(),
		Expires:    time.Now().Add(t.expireAfter),
	}

	t.traces[traceID] = ctx

	return ctx
}

func (t *Tracer) GetTrace(traceID TraceID) (*TraceContext, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	ctx, ok := t.traces[traceID]
	return ctx, ok
}

func (t *Tracer) AddEvent(traceID TraceID, eventType TraceEventType, service, nodeID string, details map[string]interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ctx, ok := t.traces[traceID]
	if !ok {
		return
	}

	ctx.Events = append(ctx.Events, &TraceEvent{
		Timestamp: time.Now(),
		EventType: eventType,
		Service:   service,
		NodeID:    nodeID,
		Details:   details,
	})
}

func (t *Tracer) UpdateOffset(traceID TraceID, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ctx, ok := t.traces[traceID]
	if !ok {
		return
	}

	ctx.Offset = offset
}

func (t *Tracer) GetActiveTraces() []*TraceContext {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*TraceContext, 0, len(t.traces))
	for _, ctx := range t.traces {
		result = append(result, ctx)
	}
	return result
}

func (t *Tracer) GetTraceSummary(traceID TraceID) string {
	ctx, ok := t.GetTrace(traceID)
	if !ok {
		return fmt.Sprintf("Trace %s not found", traceID)
	}

	var summary string
	summary += fmt.Sprintf("TraceID: %s\n", ctx.TraceID)
	summary += fmt.Sprintf("Topic: %s, Partition: %d, Offset: %d\n", ctx.Topic, ctx.Partition, ctx.Offset)
	summary += fmt.Sprintf("Created: %v\n", ctx.Created)
	summary += "Events:\n"

	for i, event := range ctx.Events {
		summary += fmt.Sprintf("  [%d] %v %s/%s: %v\n", i+1, event.Timestamp.Format("15:04:05.000"), event.Service, event.NodeID, event.EventType)
		for k, v := range event.Details {
			summary += fmt.Sprintf("       %s: %v\n", k, v)
		}
	}

	return summary
}