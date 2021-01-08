package raft

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

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"mit.edu/filosfino/6.824/src/labgob"
	"mit.edu/filosfino/6.824/src/labrpc"
)

func randTime(base int) time.Duration {
	return time.Duration(base+rand.Intn(base)) * time.Millisecond
}

// import "bytes"
// import "mit.edu/filosfino/6.824/src/labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	Leader    = "Leader"
	Follower  = "Follower"
	Candidate = "Candidate"
)

type Log struct {
	Term    int
	Leader  int
	Command string
}

type Raft struct {
	// 控制 currentTerm, log, votedFor 一致性读写
	mu   sync.Mutex // Lock to protect shared access to this peer's state
	cond *sync.Cond

	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	dead           int32               // set by Kill()
	role           string
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	// persistent state for all
	currentTerm int
	votedFor    *int
	log         []Log

	// volatile state for all
	commitIndex int
	lastApplied int

	// volatile state for leader
	nextIndex          []int
	matchIndex         []int
	heartbeat_interval int
	election_interval  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&votedFor) != nil {
		panic("decode failed")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.log = log
		rf.votedFor = &votedFor
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.cond.L.Lock()
	defer rf.cond.L.Unlock()
	defer rf.electionTimer.Reset(randTime(rf.election_interval))

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if rf.votedFor == nil || rf.votedFor == &args.CandidateId || args.LastLogIndex >= len(rf.log) {
		rf.currentTerm = args.Term
		rf.votedFor = &args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []Log
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.electionTimer.Reset(randTime(rf.election_interval))

	// TODO: 实现
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.role != Follower {
			rf.role = Follower
		}
		reply.Term = args.Term
		reply.Success = true
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
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
func (rf *Raft) Start(
	command interface{},
) (int, int, bool) {
	term, isLeader := rf.GetState()
	index := rf.commitIndex + 1

	// Your code here (2B).
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

func (rf *Raft) KickOffElection() {
	rf.cond.L.Lock()
	// 改自身状态
	voteCount := 0
	rf.votedFor = &rf.me
	rf.role = Candidate
	rf.currentTerm++

	// 准备参数
	LastLogTerm := 0
	if len(rf.log)-1 >= 0 {
		LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	LastLogIndex := 0
	if len(rf.log) >= 1 {
		LastLogIndex = len(rf.log) - 1
	}
	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: LastLogIndex,
		LastLogTerm:  LastLogTerm,
	}
	rf.cond.L.Unlock()

	successThreshold := len(rf.peers)/2 + 1
	requestVoteFinishedChan := make(chan bool, len(rf.peers))
	for i, _ := range rf.peers {
		if i == rf.me {
			requestVoteFinishedChan <- true
			continue
		}
		go func(i int) {
			requestVoteReply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, requestVoteArgs, requestVoteReply)
			if !ok {
				requestVoteFinishedChan <- false
				return
			}
			if requestVoteReply.Term == requestVoteArgs.Term {
				requestVoteFinishedChan <- requestVoteReply.VoteGranted
				return
			} else {
				if requestVoteReply.Term > rf.currentTerm {
					rf.cond.L.Lock()
					rf.currentTerm = requestVoteReply.Term
					rf.votedFor = nil
					rf.cond.L.Unlock()
				}
				requestVoteFinishedChan <- false
				return
			}
		}(i)
	}

	for range rf.peers {
		vote := <-requestVoteFinishedChan
		if vote {
			voteCount++
			term, isLeader := rf.GetState()
			if requestVoteArgs.Term == term && !isLeader {
				if voteCount >= successThreshold {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// 成功
					rf.role = Leader

					go func() {
						heartbeatIndex := 0
						for !rf.killed() {
							term, isLeader := rf.GetState()
							if !isLeader {
								break
							}
							for i := range rf.peers {
								if i == rf.me {
									continue
								}
								go func(i int) {
									args := &AppendEntriesArgs{
										Term:     term,
										LeaderId: rf.me,
										// TODO: 剩余字段
									}
									reply := &AppendEntriesReply{}
									ok := rf.sendAppendEntries(i, args, reply)
									if !ok {
										return
									}
									rf.mu.Lock()
									if reply.Term > term {
										rf.role = Follower
										rf.currentTerm = reply.Term
										rf.votedFor = nil
									}
									rf.mu.Unlock()
								}(i)
							}
							time.Sleep(time.Duration(rf.heartbeat_interval) * time.Millisecond)
							heartbeatIndex++
						}
					}()
					return
				}
			}
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = Follower
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
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}

	rf.heartbeat_interval = 300
	rf.election_interval = 300

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.dead = 0
	rf.mu = sync.Mutex{}
	rf.cond = sync.NewCond(&rf.mu)
	rf.role = Follower

	// Volatile state for server
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Volatile state for leaders
	rf.matchIndex = []int{}
	rf.nextIndex = []int{}

	// None Volatile state
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.log = []Log{}
	rf.electionTimer = time.NewTimer(randTime(rf.election_interval))

	rf.readPersist(persister.ReadRaftState())

	// election timeout
	go func() {
		for !rf.killed() {
			<-rf.electionTimer.C
			if !rf.killed() {
				rf.KickOffElection()
			}
			rf.mu.Lock()
			rf.electionTimer.Reset(randTime(rf.election_interval))
			rf.mu.Unlock()
		}
	}()

	return rf
}
