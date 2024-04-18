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
	"bytes"
	"encoding/gob"
	"fmt"
	"sort"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// log entry
type Log struct {
	Command interface{}
	Term    int
	Index   int
}

// constants
const (
	// types
	Follower  int = 1
	Candidate int = 2
	Leader    int = 3

	// timeouts
	HeartbeatTout = 100 * time.Millisecond
	ElectionTout  = 200
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	curType        int // follower, candidate or leader
	currentTerm    int
	votedFor       int
	logs           []Log
	commitIndex    int
	lastApplied    int
	nextIndex      []int
	matchIndex     []int
	leaderId       int
	electionTimer  *time.Ticker
	heartbeatTimer *time.Ticker
	applyCh        chan ApplyMsg
	applyMsgCond   *sync.Cond
	isSnapshot     uint32
	isAppendLog    uint32
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := rf.curType == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	rf.persister.Save(rf.encodeRfState(), rf.persister.ReadSnapshot())
}

func (rf *Raft) encodeRfState() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	encodeAndCheck := func(v interface{}) {
		err := e.Encode(v)
		if err != nil {
			return
		}
	}

	encodeAndCheck(rf.dead)
	encodeAndCheck(rf.currentTerm)
	encodeAndCheck(rf.votedFor)
	encodeAndCheck(rf.logs)

	data := w.Bytes()
	return data
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	decodeAndCheck := func(v interface{}) error {
		return d.Decode(v)
	}

	var deadV int32
	var currentTermV int
	var votedForV int
	var logsV []Log

	err := decodeAndCheck(&deadV)
	err = decodeAndCheck(&currentTermV)
	err = decodeAndCheck(&votedForV)
	err = decodeAndCheck(&logsV)

	if err == nil {
		rf.dead, rf.currentTerm, rf.votedFor, rf.logs = deadV, currentTermV, votedForV, logsV
	} else {
		fmt.Println("ERROR readPersist")
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIncludedIndex := rf.firstLog().Index
	if lastIncludedIndex >= index {
		return
	}

	var tmp []Log
	rf.logs = append(tmp, rf.logs[index-lastIncludedIndex:]...)
	rf.logs[0].Command = nil

	rf.persister.Save(rf.encodeRfState(), snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	rf.changeState(Follower)

	firstIndex := rf.firstLog().Index
	if rf.commitIndex >= args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	if atomic.LoadUint32(&rf.isSnapshot) == 1 {
		reply.Term = -1
		rf.mu.Unlock()
		return
	}

	if rf.lastLog().Index <= args.LastIncludedIndex {
		rf.logs = make([]Log, 1)
	} else {
		var tmp []Log
		rf.logs = append(tmp, rf.logs[args.LastIncludedIndex-firstIndex:]...)
	}

	rf.logs[0].Term = args.LastIncludedTerm
	rf.logs[0].Index = args.LastIncludedIndex
	rf.logs[0].Command = nil
	rf.persister.Save(rf.encodeRfState(), args.Snapshot)

	rf.lastApplied, rf.commitIndex = args.LastIncludedIndex, args.LastIncludedIndex

	rf.isSnapshot = 1
	rf.mu.Unlock()

	for atomic.LoadUint32(&rf.isAppendLog) == 1 {
		time.Sleep(10 * time.Millisecond)
	}

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	atomic.StoreUint32(&rf.isSnapshot, 0)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		if rf.curType != Follower {
			rf.changeState(Follower)
		}
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		if rf.logMatch(args.LastLogIndex, args.LastLogTerm) {
			rf.changeState(Follower)
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) logMatch(lastLogIndex int, lastLogTerm int) bool {
	lastLog := rf.lastLog()
	if lastLogTerm > lastLog.Term || (lastLogTerm == lastLog.Term && lastLogIndex >= lastLog.Index) {
		return true
	}

	return false
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	MissmatchTerm int
	MismatchIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.changeState(Follower)
	rf.currentTerm = args.Term

	if args.PrevLogIndex < rf.firstLog().Index {
		reply.Success, reply.Term = false, 0
		return
	}

	if args.PrevLogIndex > rf.lastLog().Index || rf.logs[args.PrevLogIndex-rf.firstLog().Index].Term != args.PrevLogTerm {
		reply.Success, reply.Term = false, rf.currentTerm
		lastIndex := rf.lastLog().Index
		firstIndex := rf.firstLog().Index
		if args.PrevLogIndex > lastIndex {
			reply.MismatchIndex, reply.MissmatchTerm = lastIndex+1, -1
		} else {
			reply.MissmatchTerm = rf.logs[args.PrevLogIndex-firstIndex].Term

			index := args.PrevLogIndex - 1
			for index >= firstIndex && rf.logs[index-firstIndex].Term == reply.MissmatchTerm {
				index--
			}
			reply.MismatchIndex = index
		}
		return
	}

	firstIndex := rf.cropLogs(args)
	rf.logs = append(rf.logs, args.Entries[firstIndex:]...)
	newCommitIndex := min(args.LeaderCommit, rf.lastLog().Index)
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyMsgCond.Signal()
	}

	reply.Success, reply.Term = true, rf.currentTerm
}

func (rf *Raft) cropLogs(args *AppendEntriesArgs) int {
	firstIndex := rf.firstLog().Index

	for i, entry := range args.Entries {
		if entry.Index >= firstIndex+len(rf.logs) || rf.logs[entry.Index-firstIndex].Term != entry.Term {
			var tmp []Log
			rf.logs = append(tmp, rf.logs[:entry.Index-firstIndex]...)
			return i
		}
	}

	return len(args.Entries)
}

func (rf *Raft) appendLog(command interface{}) Log {
	log := Log{
		Term:    rf.currentTerm,
		Command: command,
		Index:   rf.lastLog().Index + 1,
	}

	rf.logs = append(rf.logs, log)
	rf.persist()
	return log
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.curType != Leader {
		return -1, -1, false
	}

	newLog := rf.appendLog(command)
	index = newLog.Index
	term = rf.currentTerm
	isLeader = rf.curType == Leader

	rf.sendEntries()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.curType = Follower

	rf.electionTimer = time.NewTicker(randElectionTime())
	rf.heartbeatTimer = time.NewTicker(HeartbeatTout)
	rf.heartbeatTimer.Stop()
	rf.logs = make([]Log, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyMsgCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	firstLog := rf.firstLog()
	rf.lastApplied = firstLog.Index
	rf.commitIndex = firstLog.Index
	lastLog := rf.lastLog()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLog.Index + 1
	}

	go rf.ticker()
	go rf.applyMsg()

	return rf
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.changeState(Candidate)
			rf.electionTimer.Reset(randElectionTime())
			rf.requestVotes()
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.curType == Leader {
				rf.sendEntries()
			}
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) applyMsg() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyMsgCond.Wait()
		}
		applyEntries := make([]Log, rf.commitIndex-rf.lastApplied)
		firstIndex := rf.firstLog().Index
		commitIndex := rf.commitIndex
		copy(applyEntries, rf.logs[rf.lastApplied-firstIndex+1:rf.commitIndex-firstIndex+1])

		if rf.isSnapshot == 1 {
			rf.isAppendLog = 0
			rf.mu.Unlock()
			continue
		}

		rf.isAppendLog = 1
		rf.mu.Unlock()

		for _, entry := range applyEntries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}

		atomic.StoreUint32(&rf.isAppendLog, 0)

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()

	}
}

func (rf *Raft) changeState(state int) {
	if state == Follower {
		rf.electionTimer.Reset(randElectionTime())
		rf.heartbeatTimer.Stop()
	} else if state == Leader {
		lastLog := rf.lastLog()
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		}
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(HeartbeatTout)
	}

	rf.curType = state
}

func (rf *Raft) requestVotes() {
	totalVotes := len(rf.peers)

	rf.currentTerm += 1
	rf.votedFor = rf.me
	numVotes := 1
	rf.persist()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastLog().Index,
		LastLogTerm:  rf.lastLog().Term,
	}

	for i := range rf.peers {
		if rf.me != i {
			go func(peer int) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(peer, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.curType == Candidate && rf.currentTerm == args.Term {
						if reply.VoteGranted {
							numVotes += 1

							if numVotes > totalVotes/2 {
								rf.changeState(Leader)
								rf.sendEntries()
							}
						}

						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.changeState(Follower)
							rf.persist()
						}
					}

				}
			}(i)
		}
	}

}

func (rf *Raft) sendEntries() {
	for i := range rf.peers {
		if rf.me != i {
			go func(peer int) {
				rf.mu.Lock()
				if rf.curType != Leader {
					rf.mu.Unlock()
					return
				}

				firstIndex := rf.firstLog().Index
				if rf.nextIndex[peer] <= firstIndex {
					firstLog := rf.firstLog()
					args := &InstallSnapshotArgs{
						Term:              rf.currentTerm,
						LeaderId:          rf.me,
						LastIncludedIndex: firstLog.Index,
						LastIncludedTerm:  firstLog.Term,
						Snapshot:          rf.persister.ReadSnapshot(),
					}
					rf.mu.Unlock()

					reply := &InstallSnapshotReply{}
					if rf.sendInstallSnapshot(peer, args, reply) {
						rf.mu.Lock()
						rf.onInstallSnapshotReply(peer, args, reply)
						rf.mu.Unlock()
					}
				} else {
					logs := rf.logs[rf.nextIndex[peer]-firstIndex:]
					logCopy := make([]Log, len(logs))
					copy(logCopy, logs)
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderID:     rf.me,
						PrevLogIndex: rf.nextIndex[peer] - 1,
						PrevLogTerm:  rf.logs[rf.nextIndex[peer]-1-firstIndex].Term,
						Entries:      logCopy,
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()

					reply := &AppendEntriesReply{}
					if rf.sendAppendEntries(peer, args, reply) {
						rf.mu.Lock()
						rf.onAppendEntriesReply(peer, args, reply)
						rf.mu.Unlock()
					}
				}

			}(i)
		}
	}
}

func (rf *Raft) onAppendEntriesReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.curType == Leader && rf.currentTerm == args.Term {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.changeState(Follower)
			rf.persist()
		} else {
			if reply.Success {
				rf.matchIndex[peer] = max(rf.matchIndex[peer], args.PrevLogIndex+len(args.Entries))
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				n := len(rf.matchIndex)
				sortMatchIndex := make([]int, n)
				copy(sortMatchIndex, rf.matchIndex)
				sortMatchIndex[rf.me] = rf.lastLog().Index
				sort.Ints(sortMatchIndex)

				newCommitIndex := sortMatchIndex[n/2]
				if newCommitIndex > rf.commitIndex && newCommitIndex <= rf.lastLog().Index && rf.logs[newCommitIndex-rf.firstLog().Index].Term == rf.currentTerm {
					rf.commitIndex = newCommitIndex
					rf.applyMsgCond.Signal()
				}
			} else if reply.Term == rf.currentTerm {
				firstIndex := rf.firstLog().Index
				rf.nextIndex[peer] = max(reply.MismatchIndex, rf.matchIndex[peer]+1)

				if reply.MissmatchTerm != -1 {
					boundary := max(firstIndex, reply.MismatchIndex)
					for i := args.PrevLogIndex; i >= boundary; i-- {
						if rf.logs[i-firstIndex].Term == reply.MissmatchTerm {
							rf.nextIndex[peer] = i + 1
							break
						}
					}
				}
			}
		}

	}
}

func (rf *Raft) onInstallSnapshotReply(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.curType == Leader && rf.currentTerm == args.Term {
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.changeState(Follower)
			rf.persist()
			return
		} else if rf.currentTerm == reply.Term {
			rf.matchIndex[peer] = max(rf.matchIndex[peer], args.LastIncludedIndex) // 这一行代码容易忘记
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		}
	}

}

func randElectionTime() time.Duration {
	ms := rand.Int63() % 200
	duration := time.Duration(ElectionTout+ms) * time.Millisecond
	return duration
}

func (rf *Raft) firstLog() Log {
	return rf.logs[0]
}

func (rf *Raft) lastLog() Log {
	return rf.logs[len(rf.logs)-1]
}
