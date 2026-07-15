package nameserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type TopicInfo struct {
	Topic      string
	Partitions map[int]*PartitionInfo
}

type PartitionInfo struct {
	PartitionID int
	Leader      string
	Replicas    []string
	ISR         []string
}

type BrokerInfo struct {
	BrokerID   int
	Address    string
	Hostname   string
	Port       int
	LastUpdate time.Time
}

type NameServer struct {
	mu       sync.Mutex
	topics   map[string]*TopicInfo
	brokers  map[int]*BrokerInfo
	nextID   int
	addr     string
	httpPort int
}

func NewNameServer(addr string, httpPort int) *NameServer {
	return &NameServer{
		topics:   make(map[string]*TopicInfo),
		brokers:  make(map[int]*BrokerInfo),
		nextID:   1,
		addr:     addr,
		httpPort: httpPort,
	}
}

func (n *NameServer) Start() error {
	http.HandleFunc("/register", n.handleRegister)
	http.HandleFunc("/topic/create", n.handleCreateTopic)
	http.HandleFunc("/topic/info", n.handleTopicInfo)
	http.HandleFunc("/broker/list", n.handleBrokerList)
	http.HandleFunc("/route", n.handleRoute)

	return http.ListenAndServe(fmt.Sprintf(":%d", n.httpPort), nil)
}

func (n *NameServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	addr := r.Form.Get("addr")
	hostname := r.Form.Get("hostname")
	port := r.Form.Get("port")

	n.mu.Lock()
	defer n.mu.Unlock()

	broker := &BrokerInfo{
		BrokerID:   n.nextID,
		Address:    addr,
		Hostname:   hostname,
		Port:       0,
		LastUpdate: time.Now(),
	}

	n.nextID++
	n.brokers[broker.BrokerID] = broker

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"broker_id": %d}`, broker.BrokerID)
}

func (n *NameServer) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	topic := r.Form.Get("topic")
	if topic == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "topic is required"}`)
		return
	}

	partitions := 1
	if partitionsStr := r.Form.Get("partitions"); partitionsStr != "" {
		var err error
		partitions, err = strconv.Atoi(partitionsStr)
		if err != nil || partitions < 1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, `{"error": "invalid partitions value"}`)
			return
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.topics[topic]; exists {
		w.WriteHeader(http.StatusConflict)
		fmt.Fprintf(w, `{"error": "topic already exists"}`)
		return
	}

	brokerIDs := n.getAvailableBrokers()
	if len(brokerIDs) == 0 {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"error": "no brokers available"}`)
		return
	}

	topicInfo := &TopicInfo{
		Topic:      topic,
		Partitions: make(map[int]*PartitionInfo),
	}

	for i := 0; i < partitions; i++ {
		leaderID := brokerIDs[i%len(brokerIDs)]
		replicas := make([]string, 0)
		isr := make([]string, 0)

		for j := 0; j < 3 && j < len(brokerIDs); j++ {
			idx := (i + j) % len(brokerIDs)
			addr := n.brokers[brokerIDs[idx]].Address
			replicas = append(replicas, addr)
			isr = append(isr, addr)
		}

		topicInfo.Partitions[i] = &PartitionInfo{
			PartitionID: i,
			Leader:      replicas[0],
			Replicas:    replicas,
			ISR:         isr,
		}
	}

	n.topics[topic] = topicInfo

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"topic":      topic,
		"partitions": partitions,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) handleTopicInfo(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	topic := r.Form.Get("topic")
	if topic == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "topic is required"}`)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	topicInfo, exists := n.topics[topic]
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"error": "topic not found"}`)
		return
	}

	partitions := make(map[int]map[string]interface{})
	for pid, info := range topicInfo.Partitions {
		partitions[pid] = map[string]interface{}{
			"leader":   info.Leader,
			"replicas": info.Replicas,
			"isr":      info.ISR,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"topic":      topic,
		"partitions": partitions,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) handleBrokerList(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()

	brokers := make([]map[string]interface{}, 0, len(n.brokers))
	for _, broker := range n.brokers {
		brokers = append(brokers, map[string]interface{}{
			"broker_id": broker.BrokerID,
			"address":   broker.Address,
			"hostname":  broker.Hostname,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"brokers": brokers,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) handleRoute(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	topic := r.Form.Get("topic")
	if topic == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "topic is required"}`)
		return
	}
	key := r.Form.Get("key")

	n.mu.Lock()
	defer n.mu.Unlock()

	topicInfo, exists := n.topics[topic]
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"error": "topic not found"}`)
		return
	}

	if len(topicInfo.Partitions) == 0 {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": "no partitions available"}`)
		return
	}

	partitionID := 0
	if key != "" {
		partitionID = int(hashString(key)) % len(topicInfo.Partitions)
		if partitionID < 0 {
			partitionID = -partitionID
		}
	}

	partitionInfo, exists := topicInfo.Partitions[partitionID]
	if !exists {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": "partition not found"}`)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"topic":      topic,
		"partition":  partitionID,
		"leader":     partitionInfo.Leader,
		"replicas":   partitionInfo.Replicas,
		"isr":        partitionInfo.ISR,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) getAvailableBrokers() []int {
	ids := make([]int, 0, len(n.brokers))
	for id := range n.brokers {
		ids = append(ids, id)
	}
	return ids
}

func (n *NameServer) GetTopicInfo(topic string) (*TopicInfo, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	info, exists := n.topics[topic]
	return info, exists
}

func (n *NameServer) GetBrokerInfo(brokerID int) (*BrokerInfo, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	info, exists := n.brokers[brokerID]
	return info, exists
}

func hashString(s string) int64 {
	var h int64 = 0
	for i := 0; i < len(s); i++ {
		h = 31*h + int64(s[i])
	}
	return h
}
