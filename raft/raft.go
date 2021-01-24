package raft

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	labrpc "6.824/labrpc"
)

// TODO: for part 2B
// 1. check for matches to update commit index when leader
// 2. Followers: check for updates to commit index and apply messages

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

const heartbeatDelay time.Duration = 1000 * time.Millisecond
const minElectionTimeout time.Duration = 2 * time.Second

// ApplyMsg -
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry is the entries of the log
type LogEntry struct {
	Command interface{}
	Term    int
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("Entry{ Command %v, Term: %d}", l.Command, l.Term)
}

// State is used to represent the current state of the node
type State uint8

const (
	// Follower represents follower state
	Follower State = iota
	// Candidate represents candidate state
	Candidate
	// Leader represents the leader state
	Leader
)

// Raft -
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyChan chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm     int
	votedFor        int
	log             []LogEntry
	currState       State
	electionTimeout time.Duration
	nextTimeout     time.Time

	commitIndex int
	lastApplied int
	applyCond   *sync.Cond

	// For leader only
	nextIndex       []int
	matchIndex      []int
	stopLeaderTasks int32
}

// GetState returns the currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.currState == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

	var term, votedFor int

	if err := d.Decode(&term); err != nil {
		log.Fatal("Decode term failed: ", err)
	}
	if err := d.Decode(&votedFor); err != nil {
		log.Fatal("Decode votedFor failed: ", err)
	}

	rf.currentTerm = term
	rf.votedFor = votedFor

	logEntries := make([]LogEntry, 0, 0)

	if err := d.Decode(logEntries); err != nil {
		log.Fatal("Decode log entries failed: ", err)
	}

	rf.log = logEntries
}

// AppendEntriesArgs is the arguments for the append entries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

func (a *AppendEntriesArgs) String() string {
	if len(a.Entries) == 0 {
		return fmt.Sprintf(
			"AppendEntries (Heartbeat) { Term: %d, LeaderID: %d, PrevLogIndex: %d, PrevLogTerm: %d, Entries: %v, LeaderCommit: %d }",
			a.Term, a.LeaderID, a.PrevLogIndex, a.PrevLogTerm, a.Entries, a.LeaderCommit)
	}
	return fmt.Sprintf(
		"AppendEntries { Term: %d, LeaderID: %d, PrevLogIndex: %d, PrevLogTerm: %d, Entries: %v, LeaderCommit: %d }",
		a.Term, a.LeaderID, a.PrevLogIndex, a.PrevLogTerm, a.Entries, a.LeaderCommit)
}

// AppendEntriesReply is the reply for the append entries RPC
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (a *AppendEntriesReply) String() string {
	return fmt.Sprintf("AppendEntriesReply { Term: %d, Success: %v }", a.Term, a.Success)
}

func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}

// AppendEntries is the append entries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	LPrintf(INFO, "Node %d: %v", rf.me, args)

	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if rf.lastLogIndex() < args.PrevLogIndex || rf.logTermAtIndex(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	rf.nextTimeout = time.Now().Add(rf.electionTimeout)

	for i, e := range args.Entries {
		if args.PrevLogIndex+1+i > rf.lastLogIndex() {
			break
		}
		if rf.logTermAtIndex(args.PrevLogIndex+1+i) != e.Term {
			rf.log = rf.log[:args.PrevLogIndex+i+1]
			break
		}
	}

	for i, e := range args.Entries {
		if args.PrevLogIndex+i+1 < len(rf.log) {
			rf.log[args.PrevLogIndex+i+1] = e
		} else {
			rf.log = append(rf.log, e)
		}
	}
	rf.mu.Unlock()

	rf.applyCond.L.Lock()
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.applyCond.Signal()
	}
	rf.applyCond.L.Unlock()

	reply.Success = true
}

// RequestVoteArgs - example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

func (r *RequestVoteArgs) String() string {
	return fmt.Sprintf(
		"RequestVote { Term: %d, CandidateID: %d, LastLogIndex: %d, LastLogTerm: %d }",
		r.Term, r.CandidateID, r.LastLogIndex, r.LastLogTerm)
}

// RequestVoteReply - example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	CurrTerm    int
	VoteGranted bool
}

func (r *RequestVoteReply) String() string {
	return fmt.Sprintf("RequestVoteReply { Term: %d, VoteGranted: %v }", r.CurrTerm, r.VoteGranted)
}

// RequestVote - example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.checkTerm(args.Term)

	termInvalid := args.Term < rf.currentTerm
	candidateLogAsRecent := args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())

	LPrintf(INFO, "Node %d: %v, LLT: %d, LLI: %d", rf.me, args, rf.lastLogTerm(), rf.lastLogIndex())
	reply.CurrTerm = rf.currentTerm

	alreadyVoted := rf.votedFor != -1 && rf.votedFor != args.CandidateID
	if termInvalid || !candidateLogAsRecent || alreadyVoted {
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateID
	reply.VoteGranted = true
}

func (rf *Raft) monitorElectionTimeout() {
	for !rf.killed() {
		rf.mu.Lock()

		timeout := rf.nextTimeout
		notLeader := rf.currState != Leader

		rf.mu.Unlock()

		if time.Now().After(timeout) && notLeader {
			go rf.startElection()

			rf.mu.Lock()
			rf.nextTimeout = rf.nextTimeout.Add(rf.electionTimeout)
			rf.mu.Unlock()
		}
		time.Sleep(rf.electionTimeout)
	}
}

func (rf *Raft) startElection() {
	LPrintf(INFO, "Node %d: Starting election", rf.me)

	rf.mu.Lock()

	rf.currState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	votes := 1
	received := 1
	majority := len(rf.peers) / 2

	responses := make(chan *RequestVoteReply, len(rf.peers))

	req := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		j := i
		go func() {
			var reply RequestVoteReply
			ok := rf.peers[j].Call("Raft.RequestVote", &req, &reply)
			if ok {
				LPrintf(INFO, "Node %d: from %d: %v", rf.me, j, reply.String())
				if rf.checkTermLocking(reply.CurrTerm) {
					return
				}
				responses <- &reply
			}
		}()
	}

	context, cancel := context.WithTimeout(context.Background(), rf.electionTimeout)

WaitForVotes:
	for {
		select {
		case <-context.Done():
			cancel()
			return
		case resp := <-responses:
			if resp.VoteGranted {
				votes++
			}
			received++
			if received >= len(rf.peers) || votes > majority {
				break WaitForVotes
			}
		}
	}
	cancel()

	if votes > majority {
		rf.becomeLeader()
	}
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()

	LPrintf(INFO, "-------- NODE %d BECAME LEADER. TERM IS %d --------", rf.me, rf.currentTerm)
	rf.currState = Leader
	rf.restartLeader()

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}

	rf.mu.Unlock()

	go rf.heartbeats()
}

func (rf *Raft) maybeUpdateLeaderCommitIndex() {
	rf.mu.Lock()

	currIndices := make([]int, len(rf.peers))
	copy(currIndices, rf.matchIndex)
	sort.Ints(currIndices)

	possIndex := currIndices[(len(rf.peers) / 2)]
	rf.applyCond.L.Lock()
	needsUpdate := possIndex > rf.commitIndex && rf.logTermAtIndex(possIndex) == rf.currentTerm

	rf.mu.Unlock()

	if needsUpdate {
		rf.commitIndex = possIndex
		rf.applyCond.Signal()
	}
	rf.applyCond.L.Unlock()
}

func (rf *Raft) applyMessages() {
	for {
		rf.applyCond.L.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
			if rf.killed() {
				rf.applyCond.L.Unlock()
				return
			}
		}
		rf.lastApplied++
		msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
		rf.applyCond.L.Unlock()
		rf.applyChan <- msg
	}
}

func (rf *Raft) heartbeats() {
	for !rf.killed() && !rf.leaderStopped() {
		rf.parallelAppendEntries(true)
		time.Sleep(heartbeatDelay)
	}
}

func (rf *Raft) makeAppendEntriesRequest(follower int, heartbeat bool) *AppendEntriesArgs {
	nextIndex := rf.nextIndex[follower]
	prevLogTerm := rf.logTermAtIndex(nextIndex - 1)

	req := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
	}

	if !heartbeat {
		for i := nextIndex; i < len(rf.log); i++ {
			req.Entries = append(req.Entries, LogEntry{Command: rf.log[i].Command, Term: rf.logTermAtIndex(i)})
		}
	}

	return req
}

func (rf *Raft) parallelAppendEntries(heartbeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		follower := i
		req := rf.makeAppendEntriesRequest(follower, heartbeat)

		go func() {
			var reply AppendEntriesReply
			ok := rf.peers[follower].Call("Raft.AppendEntries", req, &reply)
			if !ok {
				// Exit if request fails
				return
			}

			LPrintf(INFO, "Node %d: from %d: %v", rf.me, follower, reply.String())

			rf.mu.Lock()

			if rf.checkTerm(reply.Term) {
				// Exit if sender is outdated
				rf.mu.Unlock()
				return
			}

			if reply.Success {
				rf.nextIndex[follower] = req.PrevLogIndex + len(req.Entries) + 1
				rf.matchIndex[follower] = req.PrevLogIndex + len(req.Entries)
				// Exit on success
				rf.mu.Unlock()

				rf.maybeUpdateLeaderCommitIndex()
				return
			}

			rf.nextIndex[follower]--

			rf.mu.Unlock()

			if !heartbeat {
				for rf.callAppendEntries(follower, heartbeat) {
				}
			}

		}()
	}
}

func (rf *Raft) callAppendEntries(follower int, heartbeat bool) bool {
	rf.mu.Lock()

	req := rf.makeAppendEntriesRequest(follower, heartbeat)

	rf.mu.Unlock()

	var reply AppendEntriesReply

	ok := rf.peers[follower].Call("Raft.AppendEntries", req, &reply)
	if !ok {
		return false
	}
	LPrintf(INFO, "Node %d: from %d: %v", rf.me, follower, reply.String())

	rf.mu.Lock()
	// defer rf.mu.Unlock()

	if rf.checkTerm(reply.Term) {
		rf.mu.Unlock()
		return false
	}

	if reply.Success {
		rf.nextIndex[follower] = req.PrevLogIndex + len(req.Entries) + 1
		rf.matchIndex[follower] = req.PrevLogIndex + len(req.Entries)
		rf.mu.Unlock()

		rf.maybeUpdateLeaderCommitIndex()
		return false
	}
	rf.nextIndex[follower]--

	rf.mu.Unlock()
	return true
}

func (rf *Raft) updateFollower(follower int) {
	for rf.callAppendEntries(follower, false) {
	}
}

func (rf *Raft) checkTermLocking(newTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.checkTerm(newTerm)
}

func (rf *Raft) checkTerm(newTerm int) bool {
	outdated := rf.currentTerm < newTerm
	if outdated {
		LPrintf(DEBUG1, "Node %d: Outdated. CurrentTerm: %d, NewTerm: %d", rf.me, rf.currentTerm, newTerm)
		rf.currentTerm = newTerm
		rf.currState = Follower
		rf.votedFor = -1
		rf.stopLeader()
	}
	return outdated
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) logTermAtIndex(index int) int {
	if index < 0 {
		panic(fmt.Sprintf("Called 'logTermAtIndex' with index %d < 0\n", index))
	} else {
		return rf.log[index].Term
	}
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) stopLeader() {
	atomic.StoreInt32(&rf.stopLeaderTasks, 1)
}

func (rf *Raft) leaderStopped() bool {
	z := atomic.LoadInt32(&rf.stopLeaderTasks)
	return z == 1
}

func (rf *Raft) restartLeader() {
	atomic.StoreInt32(&rf.stopLeaderTasks, 0)
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
	return ok
}

// Start - the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's D if this
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()

	if rf.currState != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}

	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})

	index := rf.lastLogIndex()
	term := rf.currentTerm
	isLeader := true

	rf.mu.Unlock()

	rf.parallelAppendEntries(false)

	LPrintf(INFO, "Start: %v at %d", command, index)

	return index, term, isLeader
}

// Kill - the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.applyCond.L.Lock()
	rf.applyCond.Signal()
	rf.applyCond.L.Unlock()

	rf.stopLeader()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make - the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	timeout := minElectionTimeout + time.Duration((me+1)*300)*time.Millisecond
	LPrintf(DEBUG1, "Node %d, timeout: %v", me, timeout)

	rf := &Raft{
		peers:           peers,
		persister:       persister,
		me:              me,
		dead:            0,
		applyChan:       applyCh,
		currentTerm:     0,
		votedFor:        -1,
		log:             make([]LogEntry, 1, 1000),
		currState:       Follower,
		electionTimeout: timeout,
		nextTimeout:     time.Now().Add(timeout),
		commitIndex:     0,
		lastApplied:     0,
		applyCond:       sync.NewCond(&sync.Mutex{}),
		nextIndex:       make([]int, len(peers)),
		matchIndex:      make([]int, len(peers)),
		stopLeaderTasks: 1,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.monitorElectionTimeout()

	go rf.applyMessages()

	return rf
}
