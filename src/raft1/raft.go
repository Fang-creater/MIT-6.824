package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh chan raftapi.ApplyMsg

	//persist state
	currentTerm int
	votedFor    int        //-1 means haven't vote
	log         []LogEntry //0 is dummy entry{Term:0} real log begins at 1
	snapshot    []byte

	//volatile state
	state             int //0:follower 1：candidate 2:leader
	lastElectionReset time.Time
	commitIndex       int
	lastApplied       int

	//leader volatile
	nextIndex  []int
	matchIndex []int

	//peer
	triggers  []chan struct{}
	applyCond *sync.Cond
}

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

const (
	HeartbeatInterval  = 100 * time.Millisecond
	ElectionTimeoutMin = 300
	ElectionTimeoutMax = 600
)

func randElectionTimeout() time.Duration {
	ms := ElectionTimeoutMin + rand.Int63n(ElectionTimeoutMax-ElectionTimeoutMin)
	return time.Duration(ms) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("readPersist: decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func (rf *Raft) resetElectionTimer() {
	rf.lastElectionReset = time.Now()
}

func (rf *Raft) toFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.persist() // 3C
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	//select restriction for log
	lastIdx := len(rf.log) - 1
	lastTerm := rf.log[lastIdx].Term
	upToDate := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)
	if !upToDate {
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.persist() //3C
		reply.VoteGranted = true
		rf.lastElectionReset = time.Now()
	}
	//reply.Term = rf.currentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.lastElectionReset = time.Now()
	rf.persist() //3C
	term := rf.currentTerm
	rf.mu.Unlock()

	//lastIdx := len(rf.log) - 1
	/*
		args := &RequestVoteArgs{

			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: lastIdx,
			LastLogTerm:  rf.log[lastIdx].Term,
		}
	*/

	votes := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			//args := &RequestVoteArgs{Term: term, CandidateId: rf.me}(3A)
			rf.mu.Lock()
			if rf.currentTerm != term || rf.state != Candidate {
				rf.mu.Unlock()
				return
			}
			lastIdx := len(rf.log) - 1
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastIdx,
				LastLogTerm:  rf.log[lastIdx].Term,
			}
			rf.mu.Unlock()

			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(i, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term)
				return
			}

			if rf.currentTerm != term || rf.state != Candidate {
				return
			}

			if reply.VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					//rf.state = Leader
					//rf.lastElectionReset = time.Now()
					//go rf.broadcastHeartbeat(term)(3A)
					rf.becomeLeader(term)
				}
			}
		}(i)
	}
}

// initialize nextIndex and matchIndex
func (rf *Raft) becomeLeader(term int) {
	if rf.state == Leader || rf.currentTerm != term {
		return
	}
	rf.state = Leader
	rf.lastElectionReset = time.Now()

	n := len(rf.peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = len(rf.log) //make the first RPC's PrevLogIndex = 0
	}
	//rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1

	rf.signalAll()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	//fast retreat
	XTerm  int //conflict position of follower -1: F's log is too short
	XIndex int
	XLen   int //len(follower.log)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false

	//case 1 : term smaller,Fake Leader
	if args.Term < rf.currentTerm {
		return
	}
	//legal Leader,retreat to follower
	//rf.state = Follower
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
		//rf.persist() //3C
	} else {
		rf.state = Follower
	}
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm //term over unified assignment

	//case 2 : PrevLogIndex not exit or term don't match,fast retreat
	if args.PrevLogIndex >= len(rf.log) {
		reply.XTerm = -1
		reply.XLen = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > 0 && rf.log[i-1].Term == reply.XTerm {
			i--
		}
		reply.XIndex = i
		reply.XLen = len(rf.log)
		return
	}

	//case 3 : cut down confliction,then append
	for i, e := range args.Entries {
		pos := args.PrevLogIndex + 1 + i
		if pos >= len(rf.log) {
			rf.log = append(rf.log, e)
		} else if rf.log[pos].Term != e.Term {
			rf.log = append(rf.log[:pos], e)
		}
	}
	if len(args.Entries) > 0 {
		rf.persist() //3c
	}

	//case 4 : update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		last := len(rf.log) - 1
		if args.LeaderCommit < last {
			last = args.LeaderCommit
		}
		rf.commitIndex = last
		rf.applyCond.Signal()
	}

	//reply.Term = rf.currentTerm (Move to the top)
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//func (rf *Raft) signalReplicate(peer int) { }

func (rf *Raft) signalAll() {
	for i := range rf.triggers {
		if i != rf.me {
			select {
			case rf.triggers[i] <- struct{}{}:
			default:
			}
		}
	}
}

func (rf *Raft) replicator(peer int) {
	for {
		select {
		case <-rf.triggers[peer]:

		case <-time.After(HeartbeatInterval):

		}
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			continue
		}
		term := rf.currentTerm

		prevIndex := rf.nextIndex[peer] - 1
		if prevIndex < 0 {
			prevIndex = 0
		}
		if prevIndex >= len(rf.log) {
			rf.mu.Unlock()
			continue
		}
		//entries := make([]LogEntry, len(rf.log)-prevIndex-1)
		//copy(entries, rf.log[prevIndex+1:])
		var entries []LogEntry
		if prevIndex+1 < len(rf.log) {
			entries = make([]LogEntry, len(rf.log)-prevIndex-1)
			copy(entries, rf.log[prevIndex+1:])
		}
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  rf.log[prevIndex].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		if !ok || rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			continue
		}
		if reply.Term > rf.currentTerm {
			rf.toFollower(reply.Term)
			rf.mu.Unlock()
			continue
		}
		if reply.Success {
			newMatch := prevIndex + len(entries)
			newNext := newMatch + 1
			if newMatch > rf.matchIndex[peer] {
				rf.matchIndex[peer] = newMatch
			}
			if newNext > rf.nextIndex[peer] {
				rf.nextIndex[peer] = newNext
			}
			rf.advanceCommitIndex()
		} else {
			rf.backupNextIndex(peer, reply)
			select {
			case rf.triggers[peer] <- struct{}{}:
			default:
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) backupNextIndex(peer int, reply *AppendEntriesReply) {
	var next int
	if reply.XTerm == -1 {
		next = reply.XLen
	} else {
		//if leader has XTerm?
		i := len(rf.log) - 1
		for i > 0 && rf.log[i].Term != reply.XTerm {
			i--
		}
		if rf.log[i].Term == reply.XTerm {
			next = i + 1
		} else {
			next = reply.XIndex //retreat to F's XTerm
		}
	}
	if next >= rf.nextIndex[peer] {
		next = rf.nextIndex[peer] - 1
	}
	if next < 1 {
		next = 1
	}
	rf.nextIndex[peer] = next
}

// commit new commitIndex
func (rf *Raft) advanceCommitIndex() {
	if rf.state != Leader {
		return
	}
	n := len(rf.matchIndex)
	tmp := make([]int, n)
	copy(tmp, rf.matchIndex)
	sort.Ints(tmp)
	N := tmp[n/2] //majority

	if N > rf.commitIndex && N < len(rf.log) && rf.log[N].Term == rf.currentTerm {
		rf.commitIndex = N
		rf.applyCond.Signal()
	}
}

// send logs to applyCh in order
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		rf.lastApplied++
		idx := rf.lastApplied
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[idx].Command,
			CommandIndex: idx,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg //without lock
		rf.mu.Lock()
	}
}

/*
func (rf *Raft) broadcastHeartbeat(term int) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			if rf.state != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			args := &AppendEntriesArgs{Term: term, LeaderId: rf.me}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			if !rf.sendAppendEntries(i, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
			}
		}(i)
	}
}
*/

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	index := len(rf.log) // 0 is dummy
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.persist() //3c

	rf.signalAll()

	return index, term, true
}

func (rf *Raft) ticker() {
	for {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		//state, term := rf.state, rf.currentTerm(3A)
		state := rf.state
		rf.mu.Unlock()

		if state == Leader {
			//rf.broadcastHeartbeat(term)(3A)
			time.Sleep(HeartbeatInterval)
			continue
		} else {
			timeout := randElectionTimeout()
			time.Sleep(timeout)

			rf.mu.Lock()
			shouldStart := rf.state != Leader && time.Since(rf.lastElectionReset) >= timeout
			rf.mu.Unlock()

			if shouldStart {
				rf.startElection()
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.lastElectionReset = time.Now()

	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCond = sync.NewCond(&rf.mu)
	rf.triggers = make([]chan struct{}, len(peers))
	for i := range rf.triggers {
		rf.triggers[i] = make(chan struct{}, 1)
	}

	// initialize from state persisted before a crash
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.readPersist(persister.ReadRaftState())

	go rf.applier()
	for i := range rf.peers {
		if i != rf.me {
			go rf.replicator(i)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
