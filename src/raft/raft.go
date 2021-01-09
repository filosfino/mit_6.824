package raft

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
	Command interface{}
}

type Raft struct {
	// 控制 currentTerm, log, votedFor 一致性读写
	mu sync.Mutex // Lock to protect shared access to this peer's state

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
	logs        []Log

	// volatile state for all
	commitIndex int
	lastApplied int

	// volatile state for leader
	nextIndex         []int
	matchIndex        []int
	heartbeatInterval int
	electionInterval  int

	applyCh chan ApplyMsg
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d@%d %s", rf.me, rf.currentTerm, rf.role)
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.logs)
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
		rf.logs = log
		rf.votedFor = &votedFor
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.electionTimer.Reset(randTime(rf.electionInterval))

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		// 无条件接受
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = &args.CandidateID

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		// 同 term
		if (rf.votedFor == nil || rf.votedFor == &args.CandidateID) && args.LastLogIndex >= len(rf.logs) {
			rf.currentTerm = args.Term
			rf.votedFor = &args.CandidateID
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []Log
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.electionTimer.Reset(randTime(rf.electionInterval))

	DPrintf("%d@%d <- %d@%d ℹ️ %+v", rf.me, rf.currentTerm, args.LeaderID, args.Term, args)

	reply.Term = rf.currentTerm
	reply.Success = true

	// 发送者不足以成为 leader -> deny
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// 收到后期 leader 信息, 自己回退为 follower
	if args.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = args.Term
		rf.votedFor = nil
	}

	if len(args.Entries) > 0 {
		// 处理信息
		if args.PrevLogIndex == -1 {
			// 回退到了起点
			reply.Success = true
			rf.logs = args.Entries
		} else if len(rf.logs)-1 < args.PrevLogIndex {
			// PrevLogIndex 尚不存在
			reply.Success = false
		} else {
			// PrevLogIndex 存在
			localLog := rf.logs[args.PrevLogIndex]
			termMatch := localLog.Term == args.PrevLogTerm
			if termMatch {
				// 追加后面的
				rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
				reply.Success = true
			} else {
				// 不匹配的话删除不匹配的
				rf.logs = rf.logs[:args.PrevLogIndex]
				reply.Success = false
			}
		}
		DPrintf("%d@%d ⭐ logs: %d, commitIndex: %d, lastApplied: %d", rf.me, rf.currentTerm, len(rf.logs), rf.commitIndex, rf.lastApplied)
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.logs)-1)
		DPrintf("%d@%d commitIndex: %d", rf.me, rf.currentTerm, rf.commitIndex)
		rf.ensureApplied()
	}
}

func (rf *Raft) ensureApplied() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{true, rf.logs[rf.lastApplied].Command, rf.lastApplied}
		DPrintf("%d@%d ✨ applied: %v", rf.me, rf.currentTerm, rf.logs[rf.lastApplied])
	}
}

func (rf *Raft) ensureLeaderCommitIndex() {
	i := rf.commitIndex

	for {
		i++
		matched := 1
		for peer, peerMatchIndex := range rf.matchIndex {
			if peer == rf.me {
				continue
			}
			if peerMatchIndex >= i {
				matched++
			}
		}
		majorityMatched := matched >= len(rf.peers)/2+1
		inCurrentTerm := len(rf.logs) > i && rf.logs[i].Term == rf.currentTerm
		if !(majorityMatched && inCurrentTerm) {
			break
		}
	}
	if i-1 != rf.commitIndex {
		DPrintf("%d@%d ⭐ ⭐ commitIndex: %d -> %d", rf.me, rf.currentTerm, rf.commitIndex, i-1)
		rf.commitIndex = i - 1
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Leader {
		rf.logs = append(rf.logs, Log{rf.currentTerm, rf.me, command})
		DPrintf("%d@%d 😊 new command: %v", rf.me, rf.currentTerm, command)
		DPrintf("%d@%d\nlogs: %v", rf.me, rf.currentTerm, rf.logs)
	}
	return len(rf.logs) - 1, rf.currentTerm, rf.role == Leader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) KickOffElection() {
	rf.mu.Lock()
	// 改自身状态
	voteCount := 0
	rf.votedFor = &rf.me
	rf.role = Candidate
	rf.currentTerm++
	DPrintf("%d@%d ✋", rf.me, rf.currentTerm)

	// 准备参数
	LastLogTerm := 0
	if len(rf.logs) >= 1 {
		LastLogTerm = rf.logs[len(rf.logs)-1].Term
	}
	LastLogIndex := 0
	if len(rf.logs) >= 1 {
		LastLogIndex = len(rf.logs) - 1
	}
	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: LastLogIndex,
		LastLogTerm:  LastLogTerm,
	}
	successThreshold := len(rf.peers)/2 + 1
	requestVoteFinishedChan := make(chan bool, len(rf.peers))
	rf.mu.Unlock()

	for i := range rf.peers {
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
					rf.mu.Lock()
					rf.currentTerm = requestVoteReply.Term
					rf.votedFor = nil
					rf.mu.Unlock()
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

					DPrintf("%d@%d ✅ wins election", rf.me, rf.currentTerm)
					rf.role = Leader
					for i := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.logs)
						rf.matchIndex[i] = -1
					}
					go rf.startHeartbeat()
					return
				}
			}
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = Follower
}

func (rf *Raft) sendAppendEntriesLogs(peer int, term int, sync bool) {
	if sync {
		DPrintf("%d@%d -> %d 🔄", rf.me, rf.currentTerm, peer)
	} else {
		DPrintf("%d@%d -> %d 💚", rf.me, rf.currentTerm, peer)
	}
	rf.mu.Lock()
	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := -1
	if len(rf.logs)-1 >= prevLogIndex && prevLogIndex >= 0 {
		prevLogTerm = rf.logs[prevLogIndex].Term
	}
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
		Entries:      make([]Log, 0),
	}
	if sync {
		args.Entries = rf.logs[rf.nextIndex[peer]:]
	}
	rf.mu.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, reply)
	DPrintf("%d@%d <- %d 💚 %v", rf.me, rf.currentTerm, peer, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if reply.Term > term {
		rf.role = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = nil
	}
	if sync && len(args.Entries) > 0 {
		if !reply.Success {
			rf.nextIndex[peer] = Max(rf.nextIndex[peer]-1, 0)
		} else {
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1

			rf.ensureLeaderCommitIndex()
			rf.ensureApplied()
		}
		DPrintf("%d@%d\nnextIndex: %v\nmatchIndex: %v", rf.me, rf.currentTerm, rf.nextIndex, rf.matchIndex)
	}
	rf.mu.Unlock()
}

func (rf *Raft) startHeartbeat() {
	heartbeatIndex := 0
	shouldSync := make([]bool, len(rf.peers))
	for {
		term, isLeader := rf.GetState()
		if rf.killed() || !isLeader {
			break
		}
		DPrintf("\n\n-------------------\n%d@%d 💚 %d\n%v", rf.me, rf.currentTerm, heartbeatIndex, rf.logs)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendAppendEntriesLogs(i, term, shouldSync[i])
			shouldSync[i] = true
		}
		rf.resetHeartbeatTimer()
		heartbeatIndex++
		DPrintf("%d@%d 💚 %d\n-------------------\n\n", rf.me, rf.currentTerm, heartbeatIndex)
		<-rf.heartbeatTimer.C
	}
}

func (rf *Raft) resetHeartbeatTimer() {
	interval := time.Duration(rf.heartbeatInterval) * time.Millisecond
	if rf.heartbeatTimer == nil {
		rf.heartbeatTimer = time.NewTimer(interval)
	} else {
		rf.heartbeatTimer.Reset(interval)
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
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg,
) *Raft {
	DPrintf("%d initing", me)
	rf := &Raft{}
	rf.applyCh = applyCh

	rf.heartbeatInterval = 150
	rf.electionInterval = 300

	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.dead = 0
	rf.mu = sync.Mutex{}
	rf.role = Follower

	// Volatile state for server
	rf.commitIndex = -1
	rf.lastApplied = -1

	// Volatile state for leaders
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = -1
	}

	// None Volatile state
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.logs = []Log{}
	rf.electionTimer = time.NewTimer(randTime(rf.electionInterval))

	rf.readPersist(persister.ReadRaftState())

	// election timeout
	go func() {
		for {
			<-rf.electionTimer.C
			_, isLeader := rf.GetState()
			if rf.killed() {
				break
			}
			if !isLeader {
				rf.KickOffElection()
			}
			rf.mu.Lock()
			rf.electionTimer.Reset(randTime(rf.electionInterval))
			rf.mu.Unlock()
		}
	}()

	DPrintf("%d inited", me)
	return rf
}
