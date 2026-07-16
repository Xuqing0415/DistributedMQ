package nameserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
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
	ClientAddr string
	Hostname   string
	Port       int
	LastUpdate time.Time
}

type MemberMeta struct {
	ClientID    string
	Heartbeat   time.Time
	Assignments map[int]bool
}

type ConsumerGroup struct {
	GroupID     string
	Members     map[string]*MemberMeta
	Topic       string
	Partitions  []int
	Generation  int
}

type NameServer struct {
	mu              sync.Mutex
	topics          map[string]*TopicInfo
	brokers         map[int]*BrokerInfo
	consumerGroups  map[string]*ConsumerGroup
	nextID          int
	addr            string
	httpPort        int
	offsetStore     *OffsetStore
	txCoordinator   *TransactionCoordinator
	stopCh          chan struct{}
}

func NewNameServer(addr string, httpPort int) *NameServer {
	offsetStore, _ := NewOffsetStore("./data/nameserver")
	txCoordinator, _ := NewTransactionCoordinator("./data/nameserver")

	ns := &NameServer{
		topics:          make(map[string]*TopicInfo),
		brokers:         make(map[int]*BrokerInfo),
		consumerGroups:  make(map[string]*ConsumerGroup),
		nextID:          1,
		addr:            addr,
		httpPort:        httpPort,
		offsetStore:     offsetStore,
		txCoordinator:   txCoordinator,
		stopCh:          make(chan struct{}),
	}

	go ns.startHeartbeatChecker()

	return ns
}

func (n *NameServer) Start() error {
	http.HandleFunc("/register", n.handleRegister)
	http.HandleFunc("/topic/create", n.handleCreateTopic)
	http.HandleFunc("/topic/info", n.handleTopicInfo)
	http.HandleFunc("/broker/list", n.handleBrokerList)
	http.HandleFunc("/broker/peers", n.handleBrokerPeers)
	http.HandleFunc("/route", n.handleRoute)
	http.HandleFunc("/consumer/join", n.handleJoinGroup)
	http.HandleFunc("/consumer/heartbeat", n.handleHeartbeat)
	http.HandleFunc("/consumer/commit_offset", n.handleCommitOffset)
	http.HandleFunc("/consumer/leave", n.handleLeaveGroup)
	http.HandleFunc("/consumer/offset", n.handleGetOffset)
	http.HandleFunc("/tx/begin", n.handleBeginTx)
	http.HandleFunc("/tx/add", n.handleAddMessage)
	http.HandleFunc("/tx/commit", n.handleCommitTx)
	http.HandleFunc("/tx/rollback", n.handleRollbackTx)
	http.HandleFunc("/tx/status", n.handleTxStatus)

	return http.ListenAndServe(fmt.Sprintf(":%d", n.httpPort), nil)
}

func (n *NameServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	addr := r.Form.Get("addr")
	clientAddr := r.Form.Get("client_addr")
	hostname := r.Form.Get("hostname")

	if addr == "" || clientAddr == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "addr and client_addr are required"}`)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	for _, broker := range n.brokers {
		if broker.Address == addr {
			broker.ClientAddr = clientAddr
			broker.Hostname = hostname
			broker.LastUpdate = time.Now()
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"broker_id": %d}`, broker.BrokerID)
			return
		}
	}

	broker := &BrokerInfo{
		BrokerID:   n.nextID,
		Address:    addr,
		ClientAddr: clientAddr,
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
		replicas := make([]string, 0)
		isr := make([]string, 0)

		for j := 0; j < 3 && j < len(brokerIDs); j++ {
			idx := (i + j) % len(brokerIDs)
			addr := n.brokers[brokerIDs[idx]].ClientAddr
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

func (n *NameServer) handleBrokerPeers(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()

	peers := make([]string, 0, len(n.brokers))
	for _, broker := range n.brokers {
		peers = append(peers, broker.Address)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"peers": peers,
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

	if partitionStr := r.Form.Get("partition"); partitionStr != "" {
		var err error
		partitionID, err = strconv.Atoi(partitionStr)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, `{"error": "invalid partition"}`)
			return
		}
	} else if key := r.Form.Get("key"); key != "" {
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
	for id, broker := range n.brokers {
		if time.Since(broker.LastUpdate) < 30*time.Second {
			ids = append(ids, id)
		}
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

func (n *NameServer) handleJoinGroup(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	groupID := r.Form.Get("group_id")
	clientID := r.Form.Get("client_id")
	topic := r.Form.Get("topic")

	if groupID == "" || clientID == "" || topic == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "group_id, client_id, and topic are required"}`)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	group, exists := n.consumerGroups[groupID]
	if !exists {
		topicInfo, exists := n.topics[topic]
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, `{"error": "topic not found"}`)
			return
		}

		partitions := make([]int, 0, len(topicInfo.Partitions))
		for pid := range topicInfo.Partitions {
			partitions = append(partitions, pid)
		}
		sort.Ints(partitions)

		group = &ConsumerGroup{
			GroupID:    groupID,
			Members:    make(map[string]*MemberMeta),
			Topic:      topic,
			Partitions: partitions,
			Generation: 1,
		}
		n.consumerGroups[groupID] = group
	}

	if group.Topic != topic {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "group already subscribed to different topic"}`)
		return
	}

	member, exists := group.Members[clientID]
	if !exists {
		member = &MemberMeta{
			ClientID:    clientID,
			Heartbeat:   time.Now(),
			Assignments: make(map[int]bool),
		}
		group.Members[clientID] = member
	} else {
		member.Heartbeat = time.Now()
	}

	n.doRebalance(groupID)

	assignments := make([]int, 0)
	for pid := range member.Assignments {
		assignments = append(assignments, pid)
	}
	sort.Ints(assignments)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"group_id":     groupID,
		"generation":   group.Generation,
		"client_id":    clientID,
		"assignments":  assignments,
		"topic":        topic,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	groupID := r.Form.Get("group_id")
	clientID := r.Form.Get("client_id")

	if groupID == "" || clientID == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "group_id and client_id are required"}`)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	group, exists := n.consumerGroups[groupID]
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"error": "consumer group not found"}`)
		return
	}

	member, exists := group.Members[clientID]
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"error": "member not found in group"}`)
		return
	}

	member.Heartbeat = time.Now()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status":    "ok",
		"group_id":  groupID,
		"client_id": clientID,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) handleCommitOffset(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	groupID := r.Form.Get("group_id")
	topic := r.Form.Get("topic")
	partitionStr := r.Form.Get("partition")
	offsetStr := r.Form.Get("offset")

	if groupID == "" || topic == "" || partitionStr == "" || offsetStr == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "group_id, topic, partition, and offset are required"}`)
		return
	}

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "invalid partition"}`)
		return
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "invalid offset"}`)
		return
	}

	if n.offsetStore != nil {
		if err := n.offsetStore.PutOffset(groupID, topic, partition, offset); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"error": "failed to save offset: %v"}`, err)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status":    "ok",
		"group_id":  groupID,
		"topic":     topic,
		"partition": partition,
		"offset":    offset,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) handleLeaveGroup(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	groupID := r.Form.Get("group_id")
	clientID := r.Form.Get("client_id")

	if groupID == "" || clientID == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "group_id and client_id are required"}`)
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	group, exists := n.consumerGroups[groupID]
	if !exists {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"error": "consumer group not found"}`)
		return
	}

	delete(group.Members, clientID)

	if len(group.Members) == 0 {
		delete(n.consumerGroups, groupID)
	} else {
		n.doRebalance(groupID)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status":    "ok",
		"group_id":  groupID,
		"client_id": clientID,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) handleGetOffset(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	groupID := r.Form.Get("group_id")
	topic := r.Form.Get("topic")
	partitionStr := r.Form.Get("partition")

	if groupID == "" || topic == "" || partitionStr == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "group_id, topic, and partition are required"}`)
		return
	}

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "invalid partition"}`)
		return
	}

	offset := int64(0)
	if n.offsetStore != nil {
		var err error
		offset, err = n.offsetStore.GetOffset(groupID, topic, partition)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `{"error": "failed to get offset: %v"}`, err)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"group_id":  groupID,
		"topic":     topic,
		"partition": partition,
		"offset":    offset,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) doRebalance(groupID string) {
	group, exists := n.consumerGroups[groupID]
	if !exists {
		return
	}

	members := make([]string, 0, len(group.Members))
	for clientID := range group.Members {
		members = append(members, clientID)
	}
	sort.Strings(members)

	if len(members) == 0 {
		return
	}

	partitions := group.Partitions
	partitionsPerMember := len(partitions) / len(members)
	remainder := len(partitions) % len(members)

	for _, member := range group.Members {
		member.Assignments = make(map[int]bool)
	}

	partitionIndex := 0
	for i, memberID := range members {
		count := partitionsPerMember
		if i < remainder {
			count += 1
		}

		for j := 0; j < count && partitionIndex < len(partitions); j++ {
			group.Members[memberID].Assignments[partitions[partitionIndex]] = true
			partitionIndex++
		}
	}

	group.Generation++

	fmt.Printf("[Rebalance] Group=%s, Generation=%d, Members=%v\n", groupID, group.Generation, members)
	for _, member := range members {
		assignments := make([]int, 0)
		for pid := range group.Members[member].Assignments {
			assignments = append(assignments, pid)
		}
		sort.Ints(assignments)
		fmt.Printf("[Rebalance] Client=%s, Assignments=%v\n", member, assignments)
	}
}

func (n *NameServer) startHeartbeatChecker() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			deadMembers := make(map[string][]string)

			for groupID, group := range n.consumerGroups {
				for clientID, member := range group.Members {
					if time.Since(member.Heartbeat) > 30*time.Second {
						deadMembers[groupID] = append(deadMembers[groupID], clientID)
					}
				}
			}

			for groupID, clients := range deadMembers {
				fmt.Printf("[Heartbeat] Group=%s, Dead members=%v\n", groupID, clients)
				for _, clientID := range clients {
					delete(n.consumerGroups[groupID].Members, clientID)
				}

				if len(n.consumerGroups[groupID].Members) > 0 {
					n.doRebalance(groupID)
				} else {
					delete(n.consumerGroups, groupID)
				}
			}
			n.mu.Unlock()
		}
	}
}

func (n *NameServer) handleBeginTx(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	producerID := r.Form.Get("producer_id")
	if producerID == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "producer_id is required"}`)
		return
	}

	txID, err := n.txCoordinator.BeginTx(producerID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": "%v"}`, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"tx_id":       txID,
		"producer_id": producerID,
		"status":      "PENDING",
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) handleAddMessage(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	txID := r.Form.Get("tx_id")
	topic := r.Form.Get("topic")
	partitionStr := r.Form.Get("partition")
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	if txID == "" || topic == "" || partitionStr == "" || value == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "tx_id, topic, partition, and value are required"}`)
		return
	}

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "invalid partition"}`)
		return
	}

	if err := n.txCoordinator.AddMessage(txID, topic, partition, key, []byte(value)); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": "%v"}`, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status": "ok",
		"tx_id":  txID,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) handleCommitTx(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	txID := r.Form.Get("tx_id")
	if txID == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "tx_id is required"}`)
		return
	}

	if err := n.txCoordinator.CommitTx(txID); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": "%v"}`, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status": "COMMITTED",
		"tx_id":  txID,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) handleRollbackTx(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	txID := r.Form.Get("tx_id")
	if txID == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "tx_id is required"}`)
		return
	}

	if err := n.txCoordinator.RollbackTx(txID); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": "%v"}`, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"status": "ABORTED",
		"tx_id":  txID,
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) handleTxStatus(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "failed to parse form"}`)
		return
	}

	txID := r.Form.Get("tx_id")
	if txID == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, `{"error": "tx_id is required"}`)
		return
	}

	status, err := n.txCoordinator.GetTxStatus(txID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"error": "%v"}`, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	response := map[string]interface{}{
		"tx_id":  txID,
		"status": string(status),
	}
	json.NewEncoder(w).Encode(response)
}

func (n *NameServer) Stop() {
	close(n.stopCh)
	if n.offsetStore != nil {
		n.offsetStore.Close()
	}
	if n.txCoordinator != nil {
		n.txCoordinator.Close()
	}
}
