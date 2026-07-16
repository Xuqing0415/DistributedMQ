package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type ConfigState int

const (
	ConfigStable ConfigState = iota
	ConfigJoint
)

type RaftNode struct {
	mu             sync.RWMutex
	state          NodeState
	currentTerm    uint64
	votedFor       string
	log            []*LogEntry
	commitIndex    uint64
	lastApplied    uint64
	nextIndex      map[string]uint64
	matchIndex     map[string]uint64
	peers          map[string]*Peer
	selfAddr       string
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan<- *LogEntry
	stopCh         chan struct{}
	dataDir        string
	grpcServer     *grpc.Server
	clientConnPool map[string]*grpc.ClientConn
	pendingCommit  map[uint64]chan struct{}
	isr            map[string]bool
	isrTicker      *time.Ticker
	maxLag         uint64
	currentConfig  map[string]bool
	newConfig      map[string]bool
	configState    ConfigState
	persistedIndex uint64
}

type Peer struct {
	addr    string
	learner bool
}

func NewRaftNode(selfAddr string, peers []string, applyCh chan<- *LogEntry, dataDir string) *RaftNode {
	node := &RaftNode{
		state:          Follower,
		currentTerm:    0,
		votedFor:       "",
		log:            []*LogEntry{{Term: 0, Index: 0, Type: EntryType_DATA}},
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make(map[string]uint64),
		matchIndex:     make(map[string]uint64),
		peers:          make(map[string]*Peer),
		selfAddr:       selfAddr,
		applyCh:        applyCh,
		stopCh:         make(chan struct{}),
		dataDir:        dataDir,
		grpcServer:     nil,
		clientConnPool: make(map[string]*grpc.ClientConn),
		pendingCommit:  make(map[uint64]chan struct{}),
		isr:            make(map[string]bool),
		maxLag:         10000,
		currentConfig:  make(map[string]bool),
		newConfig:      nil,
		configState:    ConfigStable,
		persistedIndex: 0,
	}

	node.currentConfig[selfAddr] = true
	for _, addr := range peers {
		if addr != selfAddr {
			node.peers[addr] = &Peer{addr: addr}
			node.currentConfig[addr] = true
			node.isr[addr] = true
		}
	}

	node.loadState()
	node.resetElectionTimer()
	node.resetHeartbeatTimer()

	go node.startGRPCServer()

	return node
}

func (r *RaftNode) Start() {
	go r.run()
}

func (r *RaftNode) Stop() {
	close(r.stopCh)
	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
	if r.heartbeatTimer != nil {
		r.heartbeatTimer.Stop()
	}
	if r.grpcServer != nil {
		r.grpcServer.GracefulStop()
	}
	for _, conn := range r.clientConnPool {
		conn.Close()
	}
}

func (r *RaftNode) run() {
	for {
		select {
		case <-r.stopCh:
			return
		case <-r.electionTimer.C:
			r.mu.Lock()
			if r.state == Leader {
				r.resetElectionTimer()
				r.mu.Unlock()
				continue
			}
			r.startElection()
			r.mu.Unlock()
		case <-r.heartbeatTimer.C:
			r.mu.Lock()
			if r.state == Leader {
				r.sendHeartbeats()
			}
			r.mu.Unlock()
		}
	}
}

func (r *RaftNode) resetElectionTimer() {
	if r.electionTimer != nil {
		r.electionTimer.Stop()
	}
	duration := time.Duration(150+rand.Intn(150)) * time.Millisecond
	r.electionTimer = time.NewTimer(duration)
}

func (r *RaftNode) resetHeartbeatTimer() {
	if r.heartbeatTimer != nil {
		r.heartbeatTimer.Stop()
	}
	r.heartbeatTimer = time.NewTimer(50 * time.Millisecond)
}

func (r *RaftNode) getQuorumSize() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.configState == ConfigJoint {
		oldQuorum := len(r.currentConfig)/2 + 1
		newQuorum := len(r.newConfig)/2 + 1
		return oldQuorum + newQuorum
	}

	return len(r.currentConfig)/2 + 1
}

func (r *RaftNode) startElection() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.selfAddr
	r.resetElectionTimer()

	args := &RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  r.selfAddr,
		LastLogIndex: uint64(len(r.log) - 1),
		LastLogTerm:  r.log[len(r.log)-1].Term,
	}

	votes := 1
	oldConfigVotes := 1
	newConfigVotes := 0
	var wg sync.WaitGroup

	r.mu.RLock()
	configState := r.configState
	oldConfigCount := len(r.currentConfig)
	newConfigCount := len(r.newConfig)
	peersCopy := make([]*Peer, 0, len(r.peers))
	for _, peer := range r.peers {
		peersCopy = append(peersCopy, peer)
	}
	r.mu.RUnlock()

	if len(peersCopy) == 0 {
		r.becomeLeader()
		r.saveState()
		return
	}

	nonLearnerPeers := make([]*Peer, 0, len(peersCopy))
	for _, peer := range peersCopy {
		if !peer.learner {
			nonLearnerPeers = append(nonLearnerPeers, peer)
		}
	}

	for _, peer := range nonLearnerPeers {
		wg.Add(1)
		go func(p *Peer, term uint64) {
			defer wg.Done()
			reply := r.requestVote(p.addr, args)
			r.mu.Lock()

			if reply.Term > r.currentTerm {
				r.becomeFollower(reply.Term)
				r.mu.Unlock()
				return
			}

			if r.state == Candidate && r.currentTerm == term && reply.VoteGranted {
				votes++

				if configState == ConfigJoint {
					if r.currentConfig[p.addr] {
						oldConfigVotes++
					}
					if r.newConfig[p.addr] {
						newConfigVotes++
					}

					oldQuorum := oldConfigCount/2 + 1
					newQuorum := newConfigCount/2 + 1

					if oldConfigVotes >= oldQuorum && newConfigVotes >= newQuorum {
						r.becomeLeader()
					}
				} else {
					if votes > (len(nonLearnerPeers)+1)/2 {
						r.becomeLeader()
					}
				}
			}
			r.mu.Unlock()
		}(peer, r.currentTerm)
	}

	go func(term uint64) {
		wg.Wait()
		r.mu.Lock()
		if r.state == Candidate && r.currentTerm == term {
			r.startElection()
		}
		r.mu.Unlock()
	}(r.currentTerm)

	r.saveState()
}

func (r *RaftNode) becomeLeader() {
	r.state = Leader
	for peerAddr := range r.peers {
		r.nextIndex[peerAddr] = uint64(len(r.log))
		r.matchIndex[peerAddr] = 0
		r.isr[peerAddr] = true
	}
	r.sendHeartbeats()
	r.resetHeartbeatTimer()
	r.startISRTicker()
}

func (r *RaftNode) startISRTicker() {
	if r.isrTicker != nil {
		r.isrTicker.Stop()
	}
	r.isrTicker = time.NewTicker(500 * time.Millisecond)
	go func() {
		for {
			select {
			case <-r.isrTicker.C:
				r.checkISR()
			case <-r.stopCh:
				return
			}
		}
	}()
}

func (r *RaftNode) checkISR() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return
	}

	for peerAddr := range r.peers {
		replicaLag := r.commitIndex - r.matchIndex[peerAddr]
		wasInISR := r.isr[peerAddr]

		if replicaLag > r.maxLag {
			r.isr[peerAddr] = false
			if wasInISR {
				fmt.Printf("[ISR] Removed %s from ISR, lag=%d\n", peerAddr, replicaLag)
			}
		} else {
			r.isr[peerAddr] = true
			if !wasInISR {
				fmt.Printf("[ISR] Added %s back to ISR, lag=%d\n", peerAddr, replicaLag)
			}
		}
	}
}

func (r *RaftNode) getISRSize() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 1
	for _, inISR := range r.isr {
		if inISR {
			count++
		}
	}
	return count
}

func (r *RaftNode) becomeFollower(term uint64) {
	r.state = Follower
	r.currentTerm = term
	r.votedFor = ""
	r.resetElectionTimer()
	r.saveState()
}

func (r *RaftNode) sendHeartbeats() {
	for _, peer := range r.peers {
		go r.sendAppendEntries(peer.addr)
	}
	r.resetHeartbeatTimer()
}

func (r *RaftNode) sendAppendEntries(peerAddr string) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}

	nextIdx := r.nextIndex[peerAddr]
	if nextIdx == 0 {
		nextIdx = 1
		r.nextIndex[peerAddr] = 1
	}

	logLen := uint64(len(r.log))
	snapshotThreshold := uint64(1000)
	if nextIdx < logLen-snapshotThreshold {
		r.mu.Unlock()
		go r.sendSnapshot(peerAddr)
		return
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 && int(prevLogIndex) < len(r.log) {
		prevLogTerm = r.log[prevLogIndex].Term
	}

	entries := r.log[prevLogIndex+1:]

	args := &AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderId:     r.selfAddr,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	reply := r.appendEntries(peerAddr, args)

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.currentTerm {
		r.becomeFollower(reply.Term)
		return
	}

	if r.state != Leader {
		return
	}

	if reply.Success {
		r.nextIndex[peerAddr] = prevLogIndex + uint64(len(entries)) + 1
		r.matchIndex[peerAddr] = r.nextIndex[peerAddr] - 1

		r.updateCommitIndex()
	} else {
		if r.nextIndex[peerAddr] > 1 {
			r.nextIndex[peerAddr]--
			go r.sendAppendEntries(peerAddr)
		}
	}
}

func (r *RaftNode) updateCommitIndex() {
	for n := r.commitIndex + 1; n < uint64(len(r.log)); n++ {
		if r.log[n].Term != r.currentTerm {
			break
		}

		oldCount := 1
		newCount := 1

		if r.currentConfig[r.selfAddr] {
			oldCount = 1
		}
		if r.newConfig != nil && r.newConfig[r.selfAddr] {
			newCount = 1
		}

		for peerAddr, idx := range r.matchIndex {
			if idx >= n {
				if r.currentConfig[peerAddr] {
					oldCount++
				}
				if r.newConfig != nil && r.newConfig[peerAddr] {
					newCount++
				}
			}
		}

		oldQuorum := len(r.currentConfig)/2 + 1
		newQuorum := len(r.newConfig)/2 + 1

		if r.configState == ConfigJoint {
			if oldCount >= oldQuorum && newCount >= newQuorum {
				r.commitIndex = n

				if ch, ok := r.pendingCommit[n]; ok {
					close(ch)
					delete(r.pendingCommit, n)
				}
			} else {
				break
			}
		} else {
			if oldCount >= oldQuorum {
				r.commitIndex = n

				if ch, ok := r.pendingCommit[n]; ok {
					close(ch)
					delete(r.pendingCommit, n)
				}
			} else {
				break
			}
		}
	}

	go r.applyCommittedEntries()
}

func (r *RaftNode) applyCommittedEntries() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		entry := r.log[r.lastApplied]

		if entry.Type == EntryType_JOINT_CONSENSUS || entry.Type == EntryType_CONF_CHANGE {
			r.applyConfigChangeLocked(entry)
			continue
		}

		if r.applyCh != nil {
			r.mu.Unlock()
			select {
			case r.applyCh <- entry:
			case <-r.stopCh:
				return
			}
			r.mu.Lock()
		}
	}
}

func (r *RaftNode) isLogUpToDate(lastLogIndex, lastLogTerm uint64) bool {
	lastIndex := uint64(len(r.log) - 1)
	lastTerm := r.log[lastIndex].Term

	if lastTerm > lastLogTerm {
		return true
	}
	if lastTerm == lastLogTerm && lastIndex > lastLogIndex {
		return true
	}
	return false
}

func (r *RaftNode) SubmitCommand(topic string, partition int32, data []byte) (uint64, <-chan struct{}, error) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return 0, nil, fmt.Errorf("not leader")
	}

	entry := &LogEntry{
		Term:      r.currentTerm,
		Index:     uint64(len(r.log)),
		Type:      EntryType_DATA,
		Topic:     topic,
		Partition: partition,
		Data:      data,
	}
	entryIndex := entry.Index
	r.log = append(r.log, entry)
	r.saveState()

	if len(r.peers) == 0 {
		r.commitIndex = entryIndex
		r.applyCommittedEntries()
		r.mu.Unlock()
		ch := make(chan struct{})
		close(ch)
		return entryIndex, ch, nil
	}

	ch := make(chan struct{})
	r.pendingCommit[entryIndex] = ch

	r.mu.Unlock()

	r.sendHeartbeats()
	return entryIndex, ch, nil
}

func (r *RaftNode) GetState() (NodeState, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.currentTerm
}

func (r *RaftNode) AddNode(nodeAddr string) error {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return fmt.Errorf("not leader")
	}

	if r.currentConfig[nodeAddr] {
		r.mu.Unlock()
		return fmt.Errorf("node %s already in cluster", nodeAddr)
	}

	if r.configState == ConfigJoint {
		r.mu.Unlock()
		return fmt.Errorf("another configuration change is in progress")
	}
	r.mu.Unlock()

	return r.changeConfig(nodeAddr, true)
}

func (r *RaftNode) RemoveNode(nodeAddr string) error {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return fmt.Errorf("not leader")
	}

	if !r.currentConfig[nodeAddr] {
		r.mu.Unlock()
		return fmt.Errorf("node %s not in cluster", nodeAddr)
	}

	if nodeAddr == r.selfAddr && len(r.currentConfig) == 1 {
		r.mu.Unlock()
		return fmt.Errorf("cannot remove the only node")
	}

	if r.configState == ConfigJoint {
		r.mu.Unlock()
		return fmt.Errorf("another configuration change is in progress")
	}
	r.mu.Unlock()

	return r.changeConfig(nodeAddr, false)
}

func (r *RaftNode) changeConfig(nodeAddr string, add bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	newConfig := make(map[string]bool)
	for addr := range r.currentConfig {
		newConfig[addr] = true
	}
	if add {
		newConfig[nodeAddr] = true
	} else {
		delete(newConfig, nodeAddr)
	}

	r.newConfig = newConfig
	r.configState = ConfigJoint

	oldPeers := make([]string, 0, len(r.currentConfig))
	for addr := range r.currentConfig {
		oldPeers = append(oldPeers, addr)
	}

	newPeers := make([]string, 0, len(newConfig))
	for addr := range newConfig {
		newPeers = append(newPeers, addr)
	}

	entry := &LogEntry{
		Term:   r.currentTerm,
		Index:  uint64(len(r.log)),
		Type:   EntryType_JOINT_CONSENSUS,
		Config: &ConfigEntry{OldPeers: oldPeers, NewPeers: newPeers},
	}
	r.log = append(r.log, entry)
	r.saveState()

	fmt.Printf("[Raft] Joint consensus initiated: %s node %s, old=%v, new=%v\n",
		map[bool]string{true: "add", false: "remove"}[add], nodeAddr, oldPeers, newPeers)

	return nil
}

func (r *RaftNode) applyConfigChangeLocked(entry *LogEntry) {
	if entry.Type == EntryType_JOINT_CONSENSUS && entry.Config != nil {
		r.currentConfig = make(map[string]bool)
		for _, addr := range entry.Config.OldPeers {
			r.currentConfig[addr] = true
		}

		r.newConfig = make(map[string]bool)
		for _, addr := range entry.Config.NewPeers {
			r.newConfig[addr] = true
		}

		r.configState = ConfigJoint

		for _, addr := range entry.Config.NewPeers {
			if _, exists := r.peers[addr]; !exists && addr != r.selfAddr {
				r.peers[addr] = &Peer{addr: addr, learner: true}
				r.isr[addr] = true
				r.nextIndex[addr] = uint64(len(r.log))
				r.matchIndex[addr] = 0
			}
		}

		fmt.Printf("[Raft] Entered JOINT state: old=%v, new=%v\n", r.currentConfig, r.newConfig)

		if r.state == Leader {
			go r.appendStableConfig()
		}
	} else if entry.Type == EntryType_CONF_CHANGE && entry.Config != nil {
		r.currentConfig = make(map[string]bool)
		for _, addr := range entry.Config.NewPeers {
			r.currentConfig[addr] = true
		}

		r.newConfig = nil
		r.configState = ConfigStable

		for addr := range r.peers {
			if !r.currentConfig[addr] {
				delete(r.peers, addr)
				delete(r.isr, addr)
				delete(r.nextIndex, addr)
				delete(r.matchIndex, addr)
			}
		}

		fmt.Printf("[Raft] Entered STABLE state: config=%v\n", r.currentConfig)
	}
}

func (r *RaftNode) appendStableConfig() {
	r.mu.Lock()
	if r.state != Leader || r.configState != ConfigJoint {
		r.mu.Unlock()
		return
	}

	newPeers := make([]string, 0, len(r.newConfig))
	for addr := range r.newConfig {
		newPeers = append(newPeers, addr)
	}

	entry := &LogEntry{
		Term:   r.currentTerm,
		Index:  uint64(len(r.log)),
		Type:   EntryType_CONF_CHANGE,
		Config: &ConfigEntry{NewPeers: newPeers},
	}
	r.log = append(r.log, entry)
	r.saveState()
	r.mu.Unlock()

	fmt.Printf("[Raft] Appended STABLE_CONFIG entry: %v\n", newPeers)

	r.sendHeartbeats()
}

func (r *RaftNode) getConfigSize() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.currentConfig)
}

func (r *RaftNode) saveState() {
	if r.dataDir == "" {
		return
	}

	os.MkdirAll(r.dataDir, 0755)

	state := struct {
		CurrentTerm    uint64
		VotedFor       string
		CommitIndex    uint64
		LastApplied    uint64
		LogLength      int
		ConfigState    ConfigState
		CurrentConfig  map[string]bool
		NewConfig      map[string]bool
		PersistedIndex uint64
	}{
		CurrentTerm:    r.currentTerm,
		VotedFor:       r.votedFor,
		CommitIndex:    r.commitIndex,
		LastApplied:    r.lastApplied,
		LogLength:      len(r.log),
		ConfigState:    r.configState,
		CurrentConfig:  r.currentConfig,
		NewConfig:      r.newConfig,
		PersistedIndex: r.persistedIndex,
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(state); err != nil {
		return
	}

	ioutil.WriteFile(fmt.Sprintf("%s/raft_state.dat", r.dataDir), buf.Bytes(), 0644)

	r.appendLogEntries()
}

func (r *RaftNode) appendLogEntries() {
	if r.dataDir == "" {
		return
	}

	logFile := fmt.Sprintf("%s/raft_log.dat", r.dataDir)
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	startIdx := int(r.persistedIndex) + 1
	for i := startIdx; i < len(r.log); i++ {
		if err := enc.Encode(r.log[i]); err != nil {
			return
		}
	}

	f.Write(buf.Bytes())
	f.Sync()

	r.persistedIndex = uint64(len(r.log) - 1)
}

func (r *RaftNode) getLastPersistedLogIndex() int {
	return int(r.persistedIndex)
}

func (r *RaftNode) loadState() {
	if r.dataDir == "" {
		return
	}

	data, err := ioutil.ReadFile(fmt.Sprintf("%s/raft_state.dat", r.dataDir))
	if err != nil {
		return
	}

	var state struct {
		CurrentTerm    uint64
		VotedFor       string
		CommitIndex    uint64
		LastApplied    uint64
		LogLength      int
		ConfigState    ConfigState
		CurrentConfig  map[string]bool
		NewConfig      map[string]bool
		PersistedIndex uint64
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&state); err != nil {
		return
	}

	r.currentTerm = state.CurrentTerm
	r.votedFor = state.VotedFor
	r.commitIndex = state.CommitIndex
	r.lastApplied = state.LastApplied
	r.configState = state.ConfigState
	r.currentConfig = state.CurrentConfig
	r.newConfig = state.NewConfig
	r.persistedIndex = state.PersistedIndex

	r.loadLogEntries(state.LogLength)
}

func (r *RaftNode) loadLogEntries(expectedLength int) {
	if r.dataDir == "" {
		return
	}

	logFile := fmt.Sprintf("%s/raft_log.dat", r.dataDir)
	data, err := ioutil.ReadFile(logFile)
	if err != nil {
		return
	}

	if len(data) == 0 {
		return
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	r.log = r.log[:1]
	for {
		var entry LogEntry
		if err := dec.Decode(&entry); err != nil {
			break
		}
		r.log = append(r.log, &entry)
	}
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func (r *RaftNode) startGRPCServer() {
	lis, err := net.Listen("tcp", r.selfAddr)
	if err != nil {
		return
	}

	r.grpcServer = grpc.NewServer()
	RegisterRaftServiceServer(r.grpcServer, r)

	go func() {
		if err := r.grpcServer.Serve(lis); err != nil {
			return
		}
	}()
}

func (r *RaftNode) RequestVote(ctx context.Context, args *RequestVoteArgs) (*RequestVoteReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		return &RequestVoteReply{Term: r.currentTerm, VoteGranted: false}, nil
	}

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}

	if (r.votedFor == "" || r.votedFor == args.CandidateId) &&
		r.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		r.votedFor = args.CandidateId
		r.resetElectionTimer()
		r.saveState()
		return &RequestVoteReply{Term: r.currentTerm, VoteGranted: true}, nil
	}

	return &RequestVoteReply{Term: r.currentTerm, VoteGranted: false}, nil
}

func (r *RaftNode) AppendEntries(ctx context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	lastLogIndex := uint64(len(r.log) - 1)

	if args.Term < r.currentTerm {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false, LastLogIndex: lastLogIndex}, nil
	}

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}

	r.resetElectionTimer()

	if args.PrevLogIndex >= uint64(len(r.log)) {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false, ConflictIndex: uint64(len(r.log)), LastLogIndex: lastLogIndex}, nil
	}

	if args.PrevLogIndex > 0 && r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false, ConflictIndex: args.PrevLogIndex, LastLogIndex: lastLogIndex}, nil
	}

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + uint64(i)
		if index >= uint64(len(r.log)) {
			r.log = append(r.log, entry)
		} else if r.log[index].Term != entry.Term {
			r.log = r.log[:index]
			r.log = append(r.log, entry)
		}
	}

	if args.LeaderCommit > r.commitIndex {
		r.commitIndex = min(args.LeaderCommit, uint64(len(r.log))-1)
		r.applyCommittedEntries()
	}

	lastLogIndex = uint64(len(r.log) - 1)
	r.saveState()
	return &AppendEntriesReply{Term: r.currentTerm, Success: true, LastLogIndex: lastLogIndex}, nil
}

func (r *RaftNode) getClient(peerAddr string) (RaftServiceClient, error) {
	r.mu.Lock()
	conn, ok := r.clientConnPool[peerAddr]
	if !ok {
		r.mu.Unlock()
		var err error
		conn, err = grpc.Dial(peerAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithDefaultCallOptions(grpc.CallContentSubtype("json")))
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		r.clientConnPool[peerAddr] = conn
		r.mu.Unlock()
	} else {
		r.mu.Unlock()
	}
	return NewRaftServiceClient(conn), nil
}

func (r *RaftNode) requestVote(peerAddr string, args *RequestVoteArgs) *RequestVoteReply {
	client, err := r.getClient(peerAddr)
	if err != nil {
		return &RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	}

	reply, err := client.RequestVote(context.Background(), args)
	if err != nil {
		return &RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	}
	return reply
}

func (r *RaftNode) appendEntries(peerAddr string, args *AppendEntriesArgs) *AppendEntriesReply {
	client, err := r.getClient(peerAddr)
	if err != nil {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false}
	}

	reply, err := client.AppendEntries(context.Background(), args)
	if err != nil {
		return &AppendEntriesReply{Term: r.currentTerm, Success: false}
	}
	return reply
}

func (r *RaftNode) installSnapshot(peerAddr string, args *InstallSnapshotArgs) *InstallSnapshotReply {
	client, err := r.getClient(peerAddr)
	if err != nil {
		return &InstallSnapshotReply{Term: r.currentTerm, Success: false}
	}

	reply, err := client.InstallSnapshot(context.Background(), args)
	if err != nil {
		return &InstallSnapshotReply{Term: r.currentTerm, Success: false}
	}
	return reply
}

func (r *RaftNode) InstallSnapshot(ctx context.Context, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		return &InstallSnapshotReply{Term: r.currentTerm, Success: false}, nil
	}

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}

	r.resetElectionTimer()

	if args.LastIncludedIndex <= r.commitIndex {
		return &InstallSnapshotReply{Term: r.currentTerm, Success: true}, nil
	}

	r.log = r.log[:1]

	var buf bytes.Buffer
	buf.Write(args.Data)
	dec := gob.NewDecoder(&buf)

	for {
		var entry LogEntry
		if err := dec.Decode(&entry); err != nil {
			break
		}
		r.log = append(r.log, &entry)
	}

	r.lastApplied = args.LastIncludedIndex
	r.commitIndex = args.LastIncludedIndex
	r.persistedIndex = args.LastIncludedIndex

	r.saveState()

	fmt.Printf("[Raft] Snapshot installed, lastIndex=%d, lastTerm=%d\n",
		args.LastIncludedIndex, args.LastIncludedTerm)

	return &InstallSnapshotReply{Term: r.currentTerm, Success: true}, nil
}

func (r *RaftNode) createSnapshot() ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	for i := 0; i < len(r.log); i++ {
		if err := enc.Encode(*r.log[i]); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (r *RaftNode) sendSnapshot(peerAddr string) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}

	lastIndex := uint64(len(r.log) - 1)
	lastTerm := r.log[lastIndex].Term

	data, err := r.createSnapshot()
	if err != nil {
		r.mu.Unlock()
		return
	}

	args := &InstallSnapshotArgs{
		Term:              r.currentTerm,
		LeaderId:          r.selfAddr,
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Data:              data,
	}
	r.mu.Unlock()

	reply := r.installSnapshot(peerAddr, args)

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.currentTerm {
		r.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		r.nextIndex[peerAddr] = lastIndex + 1
		r.matchIndex[peerAddr] = lastIndex
		fmt.Printf("[Raft] Snapshot sent to %s, nextIndex=%d\n", peerAddr, r.nextIndex[peerAddr])
	}
}