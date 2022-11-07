package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Status int32

const (
	StatusCandidate Status = 0
	StatusFollower Status = 1
	StatusLeader Status = 2
)

func (status *Status) Load() Status {
	return Status(atomic.LoadInt32((*int32)(status)))
}

func (status *Status) Store(newVal Status) {
	atomic.StoreInt32((*int32)(status), int32(newVal))
}

func (status Status) String() string {
	switch status {
	case StatusCandidate:
		return "candidate"
	case StatusFollower:
		return "follower"
	case StatusLeader:
		return "leader"
	default:
		panic("impossible status")
	}
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (entry LogEntry) String() string {
	return fmt.Sprintf("{%d:%d %s}", entry.Term, entry.Index, "entry.Command")
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must 
	status           Status

	currentTerm      int
	votedFor         int

	mlog             []LogEntry

	// Leader's state
	commitIndex      int
	lastApplied      int

	// Control over followers
	nextIndex        []int
	matchIndex       []int

	chanHeartbeat    chan bool
	chanHasVoted     chan bool
	chanBecomeLeader chan bool
	chanApply        chan ApplyMsg

	// Temporary state
	totalVotes       int64
}

func (rf *Raft) log(format string, args ...interface{}) {
	val, exists := os.LookupEnv("DEBUG_RAFT")
	if exists && val == "1" {
		s := fmt.Sprintf(format, args...)
		fmt.Printf("[%d: term %d] %s\n", rf.me, rf.currentTerm, s)
	}
}

// Log operations

func (rf *Raft) baseEntry() *LogEntry {
	return &rf.mlog[0]
}

func (rf *Raft) entry(index int) LogEntry {
	baseIndex := rf.baseEntry().Index
	offset := index - baseIndex

	if offset < 0 || offset >= len(rf.mlog) {
		panic(fmt.Errorf("[%d] entry: no element of index %d (offset %d), last applied: %d, base index: %d", rf.me, index, offset, rf.lastApplied, rf.baseEntry().Index))
	}

	return rf.mlog[offset]
}

func (rf *Raft) logPrefix(index int) []LogEntry {
	baseIndex := rf.baseEntry().Index
	offset := index - baseIndex

	if offset < 0 || offset > len(rf.mlog) {
		panic(fmt.Errorf("logPrefix: wrong offset %d", offset))
	}

	return rf.mlog[:offset]
}

func (rf *Raft) logSuffix(index int) []LogEntry {
	baseIndex := rf.baseEntry().Index
	offset := index - baseIndex
	
	if offset < 0 || offset > len(rf.mlog) {
		panic(fmt.Errorf("logSuffix: wrong offset %d", offset))
	}

	return rf.mlog[offset:]
}

func (rf *Raft) getLastLogTerm() int {
	return rf.mlog[len(rf.mlog)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.mlog[len(rf.mlog)-1].Index
}

// Managing terms

func (rf *Raft) setTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
}

func (rf *Raft) nextTerm() {
	rf.currentTerm++
	rf.votedFor = -1
}

func (rf *Raft) evalCommitIndex() int {
	
	for i := rf.getLastLogIndex(); i > rf.commitIndex && rf.entry(i).Term == rf.currentTerm; i-- {
		replicated := 0
		
		for peer := range rf.peers {
			if peer == rf.me || rf.matchIndex[peer] >= i {
				replicated++
			}
		}

		if replicated > len(rf.peers) / 2 {
			return i
		}
	}
	
	return rf.commitIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	return rf.currentTerm, rf.status == StatusLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getRaftState())
}

func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.mlog)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	var currentTerm   int
	var votedFor      int
	var log           []LogEntry
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("Can't retrieve persisted data")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.mlog = log
		rf.commitIndex = rf.baseEntry().Index
		rf.lastApplied = rf.baseEntry().Index
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex, lastIndex := rf.baseEntry().Index, rf.getLastLogIndex()
	if baseIndex >= index || index > lastIndex {
		panic("bad snapshot")
	}

	rf.log("Trimming log up to index %d", index)

	term := rf.entry(index).Term
	rf.trimLog(index, term)

	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
}

func (rf *Raft) trimLog(snapshotIndex int, snapshotTerm int) {
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Index: snapshotIndex, Term: snapshotTerm})

	for i := len(rf.mlog) - 1; i >= 0; i-- {
		if rf.mlog[i].Index == snapshotIndex && rf.mlog[i].Term == snapshotTerm {
			newLog = append(newLog, rf.mlog[i+1:]...)
			break
		}
	}
	rf.mlog = newLog
}

type RequestVoteArgs struct {
	Term           int
	CandidateId    int
	LastLogIndex   int
	LastLogTerm    int
}

type RequestVoteReply struct {
	Term           int
	VoteGranted    bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.log("Vote requested: term %d is lower, for %d", args.Term, args.CandidateId)
		return
	}

	if args.Term > rf.currentTerm {
		rf.status = StatusFollower
		rf.setTerm(args.Term)
	}

	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()

	isUpToDate := false
	if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		isUpToDate = true
	}
	
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.chanHasVoted <- true
	}
	rf.log("Vote result: %v, for %d", reply.VoteGranted, args.CandidateId)
}

type AppendEntitiesArgs struct {
	Term           int
	LeaderId       int
	PrevLogIndex   int
	PrevLogTerm    int
	Entries        []LogEntry
	LeaderCommit   int
}

type AppendEntitiesReply struct {
	Term           int
	Success        bool
	// Used to optimize index decrement,
	// avoinding many decrements by one
	ActualIndex    int
}

func (rf *Raft) AppendEntries(args *AppendEntitiesArgs, reply *AppendEntitiesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.log("Appending: %v", *args)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ActualIndex = rf.getLastLogIndex() + 1
		return
	}
	
	if args.Term > rf.currentTerm {
		rf.status = StatusFollower
		rf.setTerm(args.Term)
	}

	rf.chanHeartbeat <- true

	reply.Term = rf.currentTerm

	// Not enough entries in log, ask for more
	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.ActualIndex = rf.getLastLogIndex() + 1
		return
	}
	
	finalEntry := rf.entry(args.PrevLogIndex)
	// Mismatching term of last known entry,
	// should rewrite log, hence asking for more
	if args.PrevLogIndex >= rf.baseEntry().Index && finalEntry.Term != args.PrevLogTerm {
		reply.Success = false
		// Scanning looking for first matching entry
		for i := args.PrevLogIndex; i >= rf.baseEntry().Index; i-- {
			if rf.entry(i).Term != finalEntry.Term {
				reply.ActualIndex = i + 1
				break
			}
		}
		
	} else if args.PrevLogIndex >= rf.baseEntry().Index {

		rf.mlog = append(rf.logPrefix(args.PrevLogIndex+1), args.Entries...)

		reply.Success = true

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			go rf.apply()
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.status = StatusFollower
		rf.setTerm(args.Term)
		rf.persist()
	}

	rf.chanHeartbeat <- true

	reply.Term = rf.currentTerm

	if args.LastIncludedIndex > rf.commitIndex {
		rf.trimLog(args.LastIncludedIndex, args.LastIncludedTerm)
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)
		
		// send snapshot to kv server
		msg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotTerm:  args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}
		rf.chanApply <- msg
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		rf.log("sendRequestVote: no response from %d", server)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntitiesArgs, reply *AppendEntitiesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		rf.log("sendAppendEntries: no response from %d", server)
	}
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		rf.log("sendInstallSnapshot: no response from %d", server)
	}
	return ok
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	isLeader := rf.status == StatusLeader
	if !isLeader {
		return -1, rf.currentTerm, isLeader
	}

	//rf.log("Command: %s", command)

	index := rf.getLastLogIndex() + 1
	rf.mlog = append(rf.mlog, LogEntry{Term: rf.currentTerm, Index: index, Command: command})
	rf.persist()

	return index, rf.currentTerm, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	rf.log("Killed")
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) proposeVote(peer int, request *RequestVoteArgs) {
	if rf.status.Load() != StatusCandidate {
		return
	}
	
	reply := RequestVoteReply{0, false}
	
	ok := rf.sendRequestVote(peer, request, &reply)
	
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		
		if rf.status.Load() != StatusCandidate || request.Term != rf.currentTerm {
			return
		}
		rf.log("Proposing vote to %d", peer)
		
		if rf.currentTerm < reply.Term {
			rf.status.Store(StatusFollower)
			rf.setTerm(reply.Term)
			return
		}
		
		if reply.VoteGranted {
			rf.log("Got vote from %d, total votes: %d/%d", peer, rf.totalVotes + 1, len(rf.peers))
			if atomic.AddInt64(&rf.totalVotes, 1) > int64(len(rf.peers) / 2) {
				rf.log("Has become a leader")
				rf.status = StatusLeader
				nextIndex := rf.getLastLogIndex() + 1
				for i := range rf.peers {
					rf.matchIndex[i] = 0
					rf.nextIndex[i] = nextIndex
				}
				rf.chanBecomeLeader <- true
			}
		}
	}
}

func (rf *Raft) startElections() {
	rf.mu.Lock()
	
	rf.nextTerm()
	rf.votedFor = rf.me

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()
	request := &RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, lastLogTerm}
	
	atomic.StoreInt64(&rf.totalVotes, 1)

	rf.mu.Unlock()
	
	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}
		
		go rf.proposeVote(peerId, request)
	}
}

func (rf *Raft) appendEntry(peer int, request *AppendEntitiesArgs) {
	reply := AppendEntitiesReply{}

	ok := rf.sendAppendEntries(peer, request, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		
		if rf.status.Load() != StatusLeader || request.Term != rf.currentTerm {
			return
		}

		if rf.currentTerm < reply.Term {
			rf.status = StatusFollower
			rf.setTerm(reply.Term)
			rf.persist()
			return
		}

		if !reply.Success {
			rf.log("Sending entry to %d failed", peer)
			rf.nextIndex[peer] = min(reply.ActualIndex, rf.getLastLogIndex())
			return
		}

		// Update peer's metadata and commit index on
		// successful update
		if len(request.Entries) > 0 {
			rf.matchIndex[peer] = request.Entries[len(request.Entries)-1].Index
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		}

		newCommitIndex := rf.evalCommitIndex()
		if rf.commitIndex != newCommitIndex {
			rf.commitIndex = newCommitIndex
			go rf.apply()
		}
	}
}

func (rf *Raft) sendSnapshot(peer int, request *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(peer, request, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		
		if rf.status.Load() != StatusLeader || reply.Term != rf.currentTerm {
			return
		}

		if rf.currentTerm < reply.Term {
			rf.status = StatusFollower
			rf.setTerm(reply.Term)
			rf.persist()
			return
		}

		// Update peer's metadata and commit index on
		// successful update
		rf.matchIndex[peer] = request.LastIncludedIndex
		rf.nextIndex[peer] = request.LastIncludedIndex + 1
		rf.commitIndex = rf.evalCommitIndex()
	}
}

func (rf *Raft) appendLog() {
	
	for peerId := range rf.peers {
		if peerId == rf.me {
			continue
		}

		if rf.status.Load() != StatusLeader {
			return
		}

		if rf.nextIndex[peerId] > rf.baseEntry().Index {
		
			request := &AppendEntitiesArgs{}

			rf.mu.Lock()

			request.LeaderId = rf.me
			request.Term = rf.currentTerm
			request.PrevLogIndex = rf.nextIndex[peerId] - 1
			request.PrevLogTerm = rf.entry(request.PrevLogIndex).Term
			request.Entries = rf.logSuffix(rf.nextIndex[peerId])
			request.LeaderCommit = rf.commitIndex

			rf.mu.Unlock()
			
			go rf.appendEntry(peerId, request)
		} else {

			request := &InstallSnapshotArgs{}

			rf.mu.Lock()

			request.LeaderId = rf.me
			request.Term = rf.currentTerm
			request.LastIncludedIndex = rf.baseEntry().Index
			request.LastIncludedTerm = rf.baseEntry().Term
			request.Data = rf.persister.ReadSnapshot()

			rf.mu.Unlock()

			go rf.sendSnapshot(peerId, request)
		}
	}
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	
	lastApplied := rf.lastApplied
	commitIndex := rf.commitIndex
	rf.lastApplied = rf.commitIndex
	
	rf.mu.Unlock()

	for i := lastApplied + 1; i <= commitIndex; i++ {
		//rf.log("index: %d, last applied: %d, commit index: %d", i, lastApplied, commitIndex)
		msg := ApplyMsg{}
		msg.CommandIndex = i
		msg.CommandValid = true
		msg.Command = rf.entry(i).Command
		rf.chanApply <- msg
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) loop() {
	for rf.killed() == false {
		rf.log("Status: %s, Committed: %d, Log: %s", rf.status, rf.commitIndex, rf.mlog)
		switch rf.status.Load() {
		case StatusCandidate:
			go rf.startElections()
			select {
			case <- rf.chanHeartbeat:
				rf.status.Store(StatusFollower)
			case <- rf.chanBecomeLeader:
			// Election timeout
			case <- time.After(time.Duration(rand.Intn(300) + 200) * time.Millisecond):
			}
		case StatusFollower:
			select {
			// Heartbeat messages:
			//   chanHeartbeat <- AppendLog
			//   chanHasVoted  <- RequestVote
			case <- rf.chanHasVoted:
			case <- rf.chanHeartbeat:
			// Election timeout
			case <- time.After(time.Duration(rand.Intn(300) + 200) * time.Millisecond):
				rf.status.Store(StatusCandidate)
			}
		case StatusLeader:
			go rf.appendLog()
			time.Sleep(20 * time.Millisecond)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.status = StatusFollower
	rf.setTerm(0)

	// Leader's state
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	rf.mlog = []LogEntry{{Term: 0}}

	rf.chanBecomeLeader = make(chan bool, 256)
	rf.chanHasVoted = make(chan bool, 256)
	rf.chanHeartbeat = make(chan bool, 256)
	rf.chanApply = applyCh
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//rf.readSnapshot(persister.ReadSnapshot())

	rf.log("(re)started with state: Status: %s, Committed: %d, Log: %s", rf.status, rf.commitIndex, rf.mlog)

	// start ticker goroutine to start elections
	go rf.loop()

	return rf
}
