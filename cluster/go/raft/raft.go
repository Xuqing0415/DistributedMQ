package raft

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

type RaftNode struct {
	mu             sync.Mutex
	state          NodeState
	currentTerm    uint64
	votedFor       string
	log            []LogEntry
	commitIndex    uint64
	lastApplied    uint64
	nextIndex      map[string]uint64
	matchIndex     map[string]uint64
	peers          map[string]*Peer
	selfAddr       string
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan<- []byte
	stopCh         chan struct{}
	dataDir        string
}

type Peer struct {
	addr string
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateId  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderId     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

func NewRaftNode(selfAddr string, peers []string, applyCh chan<- []byte, dataDir string) *RaftNode {
	node := &RaftNode{
		state:         Follower,
		currentTerm:   0,
		votedFor:      "",
		log:           make([]LogEntry, 1),
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make(map[string]uint64),
		matchIndex:    make(map[string]uint64),
		peers:         make(map[string]*Peer),
		selfAddr:      selfAddr,
		applyCh:       applyCh,
		stopCh:        make(chan struct{}),
		dataDir:       dataDir,
	}

	for _, addr := range peers {
		if addr != selfAddr {
			node.peers[addr] = &Peer{addr: addr}
		}
	}

	node.loadState()
	node.resetElectionTimer()

	go node.startHTTPServer()

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

func (r *RaftNode) startElection() {
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.selfAddr
	r.resetElectionTimer()

	args := RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  r.selfAddr,
		LastLogIndex: uint64(len(r.log) - 1),
		LastLogTerm:  r.log[len(r.log)-1].Term,
	}

	votes := 1
	var wg sync.WaitGroup

	for _, peer := range r.peers {
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
				if votes > (len(r.peers)+1)/2 {
					r.becomeLeader()
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
	}
	r.sendHeartbeats()
	r.resetHeartbeatTimer()
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

	prevLogIndex := r.nextIndex[peerAddr] - 1
	prevLogTerm := uint64(0)
	if prevLogIndex >= 0 && int(prevLogIndex) < len(r.log) {
		prevLogTerm = r.log[prevLogIndex].Term
	}

	entries := r.log[prevLogIndex+1:]
	args := AppendEntriesArgs{
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
		count := 1
		for _, idx := range r.matchIndex {
			if idx >= n {
				count++
			}
		}
		if count > (len(r.peers)+1)/2 && r.log[n].Term == r.currentTerm {
			r.commitIndex = n
			r.applyCommittedEntries()
		} else {
			break
		}
	}
}

func (r *RaftNode) applyCommittedEntries() {
	for r.lastApplied < r.commitIndex {
		r.lastApplied++
		if r.applyCh != nil {
			select {
			case r.applyCh <- r.log[r.lastApplied].Command:
			case <-r.stopCh:
				return
			}
		}
	}
}

func (r *RaftNode) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		return AppendEntriesReply{Term: r.currentTerm, Success: false}
	}

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}

	r.resetElectionTimer()

	if args.PrevLogIndex >= uint64(len(r.log)) {
		return AppendEntriesReply{Term: r.currentTerm, Success: false}
	}

	if args.PrevLogIndex > 0 && r.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return AppendEntriesReply{Term: r.currentTerm, Success: false}
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

	r.saveState()
	return AppendEntriesReply{Term: r.currentTerm, Success: true}
}

func (r *RaftNode) RequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		return RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	}

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}

	if (r.votedFor == "" || r.votedFor == args.CandidateId) &&
		r.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		r.votedFor = args.CandidateId
		r.resetElectionTimer()
		r.saveState()
		return RequestVoteReply{Term: r.currentTerm, VoteGranted: true}
	}

	return RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
}

func (r *RaftNode) isLogUpToDate(lastLogIndex, lastLogTerm uint64) bool {
	lastIndex := uint64(len(r.log) - 1)
	lastTerm := r.log[lastIndex].Term

	if lastTerm > lastLogTerm {
		return false
	}
	if lastTerm == lastLogTerm && lastIndex > lastLogIndex {
		return false
	}
	return true
}

func (r *RaftNode) SubmitCommand(command []byte) error {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return errors.New("not leader")
	}

	entry := LogEntry{
		Term:    r.currentTerm,
		Index:   uint64(len(r.log)),
		Command: command,
	}
	r.log = append(r.log, entry)
	r.saveState()
	r.mu.Unlock()

	r.sendHeartbeats()
	return nil
}

func (r *RaftNode) GetState() (NodeState, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state, r.currentTerm
}

func (r *RaftNode) saveState() {
	if r.dataDir == "" {
		return
	}

	os.MkdirAll(r.dataDir, 0755)

	state := struct {
		CurrentTerm uint64
		VotedFor    string
		Log         []LogEntry
	}{
		CurrentTerm: r.currentTerm,
		VotedFor:    r.votedFor,
		Log:         r.log,
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(state); err != nil {
		return
	}

	ioutil.WriteFile(fmt.Sprintf("%s/raft_state.dat", r.dataDir), buf.Bytes(), 0644)
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
		CurrentTerm uint64
		VotedFor    string
		Log         []LogEntry
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&state); err != nil {
		return
	}

	r.currentTerm = state.CurrentTerm
	r.votedFor = state.VotedFor
	if len(state.Log) > 0 {
		r.log = state.Log
	}
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func (r *RaftNode) startHTTPServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/requestVote", r.handleRequestVote)
	mux.HandleFunc("/appendEntries", r.handleAppendEntries)
	go http.ListenAndServe(r.selfAddr, mux)
}

func (r *RaftNode) handleRequestVote(w http.ResponseWriter, req *http.Request) {
	var args RequestVoteArgs
	if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer req.Body.Close()

	reply := r.RequestVote(args)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply)
}

func (r *RaftNode) handleAppendEntries(w http.ResponseWriter, req *http.Request) {
	var args AppendEntriesArgs
	if err := json.NewDecoder(req.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer req.Body.Close()

	reply := r.AppendEntries(args)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply)
}

func (r *RaftNode) requestVote(peerAddr string, args RequestVoteArgs) RequestVoteReply {
	var reply RequestVoteReply
	url := fmt.Sprintf("http://%s/requestVote", peerAddr)
	resp, err := makeHTTPRequest(url, args)
	if err != nil {
		return reply
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return reply
	}
	return reply
}

func (r *RaftNode) appendEntries(peerAddr string, args AppendEntriesArgs) AppendEntriesReply {
	var reply AppendEntriesReply
	url := fmt.Sprintf("http://%s/appendEntries", peerAddr)
	resp, err := makeHTTPRequest(url, args)
	if err != nil {
		return reply
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return reply
	}
	return reply
}

func makeHTTPRequest(url string, data interface{}) (*http.Response, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	return resp, nil
}
