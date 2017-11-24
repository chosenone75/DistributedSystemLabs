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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "bytes"
import "encoding/gob"
//import "fmt"
//
// state of raft server
//
const (
	LEADER  = iota
	FOLLOWER
	CANDIDATE
)
//
//some constant about timeout such as ELECTION TIMEOUT mentioned in the paper.
//
const (
    HEARTBEAT = time.Millisecond * 50
    ElectionMinTime = 150
    ElectionMaxTime = 300
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct{
   Term int // the term
   Command interface{} // command
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
        currentTerm int //last term server has been, 0 at first
	votedFor int //candidatedId that the server has voted for
	log      []LogEntry
	
	commitIndex int // index of the highest log entry known to be commited, 0 at first
	lastApplied int // index of the highest log entry applied to the state machine

	//Only useful for leader
	nextIndex []int // for each server,index of the next log entry to send to that server
        matchIndex []int // for each server, index of highest log entry known to be replicated on server.
        
        //how many servers vote for me
        voteCount int

	//state and applymsg
	state int
	applyCh chan ApplyMsg
	
	// timer for timeout
	timer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
}
//helper function
// get the last log index
func (rf *Raft) lastLogIndex() int {
    return len(rf.log)-1
}
//helper function
// get the last log entry's term
func (rf *Raft) lastLogTerm() int {
    return rf.log[rf.lastLogIndex()].Term
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	if data != nil{
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.votedFor)
		d.Decode(&rf.log)
	}
}

type AppendEntriesArgs struct{
     Term int //leader'term
     LeaderId int //so that the follower can redirect request
     PrevLogIndex int //
     PrevLogTerm int
     Entries []LogEntry // log entries to store (empty for heartbeat)
     LeaderCommit int // leader's last commitIndex
}

type AppendEntriesReply struct{
     Term int // current term for leader to update itself
     Success bool // true if follower has entry matching prevLogIndex and prevLogTerm
     NextIndex int // the last log entry index same as the args
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply){
     
     //fmt.Printf("AppendEntries into server %v\n",rf.me)	
     rf.mu.Lock()
     defer rf.persist()
     defer rf.resetTimer()
     defer rf.mu.Unlock()

     //reply
     reply.Success = true
     
     // for older leader to update itself
     if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm     
        reply.Success = false
	return
     }
     
     if args.Term > rf.currentTerm{
        rf.currentTerm = args.Term
	rf.state = FOLLOWER
	rf.votedFor = -1
     }

     reply.Term = rf.currentTerm
     
     // if prevLogIndex or prevLogTerm doesn't match
     if args.PrevLogIndex >= 0 && (rf.lastLogIndex() < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm ) {
	     
	     reply.NextIndex = rf.lastLogIndex()
             
	     if reply.NextIndex > args.PrevLogIndex {
	        reply.NextIndex = args.PrevLogIndex
	     }

              		  
	     reply.Success = false
             // find the log entry that first matches leader's
	     for reply.NextIndex >= 0 {
	         if rf.log[reply.NextIndex].Term == args.PrevLogTerm {
		         break
		 }
		 reply.NextIndex -= 1
	     }

	     return
     }

     // append new entry
     if len(args.Entries) != 0 {
             // replicate log entries
	     //fmt.Println("replicating")
	     // delete the following logs
	     rf.log = rf.log[:args.PrevLogIndex+1]
	     rf.log = append(rf.log, args.Entries...)
	     reply.Success = true

	     // commit logs
	     if rf.lastLogIndex() >= args.LeaderCommit {
	         rf.commitIndex = args.LeaderCommit
	     }else{
	         rf.commitIndex = rf.lastLogIndex()
	     }

	     go rf.commitLogs()
	     reply.NextIndex = rf.lastLogIndex()
	     return
     }else{
	  // heartbeat 
          if rf.lastLogIndex() >= args.LeaderCommit{
	     rf.commitIndex = args.LeaderCommit
	  }else{
	     rf.commitIndex = rf.lastLogIndex()
	  }

          go rf.commitLogs()
           
	  reply.NextIndex =  args.PrevLogIndex
	  reply.Success = true
          return
     }
     return
}


//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int // candidate's term
	CandidateId int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int //currentTerm defined in Raft so that the candidate can update itself
	VoteGranted bool // true for vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
        
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
        reply.VoteGranted = false

	if args.Term < rf.currentTerm {
	   reply.VoteGranted = false
	   return
	}

	if args.Term > rf.currentTerm {
	   rf.currentTerm = args.Term
	   rf.state = FOLLOWER
	   rf.votedFor = -1
	   rf.persist()
	}

	reply.Term = rf.currentTerm
        
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
	     // check if candidate's log is as least up-to-date as receiver's log
	     if len(rf.log) > 0 {
		     if args.LastLogTerm < rf.lastLogTerm() ||  (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex < rf.lastLogIndex()){
			 // the server is more up-to-date
		         reply.VoteGranted = false
			 return
		     }
	    } 
	    reply.VoteGranted = true
	    // update the server
	    rf.votedFor = args.CandidateId
	    rf.state = FOLLOWER
	    rf.persist()
	    rf.resetTimer()
	    return
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) handleVoteResult(reply RequestVoteReply){
     rf.mu.Lock()
     defer rf.mu.Unlock()
     
     if reply.Term > rf.currentTerm {
         rf.currentTerm = reply.Term
	 rf.state = FOLLOWER
	 rf.votedFor = -1
	 rf.resetTimer()
	 return
     }

     if rf.state == CANDIDATE && reply.VoteGranted {
         rf.voteCount += 1

	 if rf.voteCount >= len(rf.peers)/2 + 1{
	     rf.state = LEADER

	     // init the variables on the leader server

	     for i := 0;i<len(rf.peers);i++{
	         if i == rf.me {
		    continue
		 }
		 rf.nextIndex[i] = len(rf.log)
		 rf.matchIndex[i] = -1
	     }
	     rf.resetTimer()
	 }
     }
     return
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
       ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
       return ok
}

func (rf *Raft) handleAppendEntries(server int,reply AppendEntriesReply) {
     rf.mu.Lock()
     defer rf.mu.Unlock()
     
     if rf.state != LEADER {
        return
     }

     if reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.resetTimer()
	return
     }

     if reply.Success {
        // append entries to server successfully, update the nextindex and matchindex
	rf.nextIndex[server] = reply.NextIndex + 1
        rf.matchIndex[server] = reply.NextIndex
        
	reply_count := 1
        
	for i :=0; i < len(rf.peers); i++ {
	    if i == rf.me {
	       continue 
	    }
	    if rf.matchIndex[i] >= rf.matchIndex[server] {
	        reply_count += 1
	    }
	}

        if reply_count >= len(rf.peers)/2 + 1 && rf.commitIndex < rf.matchIndex[server]  && rf.log[rf.matchIndex[server]].Term == rf.currentTerm {
	      rf.commitIndex = rf.matchIndex[server]
	      go rf.commitLogs()
	}

     }else{
         rf.nextIndex[server] = reply.NextIndex	+ 1
	 rf.broadcastAppendEntries()
     }
}

func (rf *Raft) commitLogs(){
	for i:=rf.lastApplied+1; i <= rf.commitIndex; i++{
	    rf.applyCh <- ApplyMsg{Index:i+1, Command:rf.log[i].Command}
	}
        rf.lastApplied = rf.commitIndex
}

func (rf *Raft) broadcastAppendEntries(){
    for i:= 0;i < len(rf.peers);i++{
        if i == rf.me{
	   continue
	}

        args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[i] - 1

	if args.PrevLogIndex  >= 0 {
	   args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}

	if rf.nextIndex[i] < len(rf.log){
	   args.Entries = rf.log[rf.nextIndex[i]:] 
	}

        args.LeaderCommit  = rf.commitIndex

	go func(server int, args AppendEntriesArgs){
	    reply := AppendEntriesReply{}
	    ok := rf.sendAppendEntries(server,args,&reply)
	    if ok{
	       rf.handleAppendEntries(server,reply)
	    }
	}(i,args)
    }
}

func (rf *Raft) handleTime(){
     rf.mu.Lock()
     defer rf.persist()
     defer rf.mu.Unlock()

     if _,leader := rf.GetState(); leader{
	 //fmt.Printf("server %v\n",rf.me)
         rf.broadcastAppendEntries()    
     }else{

       rf.state = CANDIDATE
       rf.currentTerm += 1
       rf.votedFor = rf.me
       rf.voteCount = 1
       
       args := RequestVoteArgs{}
       args.Term = rf.currentTerm
       args.CandidateId = rf.me
       args.LastLogIndex = rf.lastLogIndex()
       
       // avoid -1 index
       if len(rf.log) > 0{
          args.LastLogTerm = rf.log[args.LastLogIndex].Term
       }

       for server :=0; server < len(rf.peers);server++{
           if server == rf.me{
	      continue
	   }

	   go func(server int, args RequestVoteArgs){
	         var reply RequestVoteReply
		 ok := rf.sendRequestVote(server,args,&reply)
		 if ok {
		   rf.handleVoteResult(reply)
		 }
	   }(server,args)
       }
     }
     rf.resetTimer()
}


//
// reset timer 
//
func (rf *Raft) resetTimer(){
    if rf.timer == nil{
        rf.timer = time.NewTimer(time.Millisecond * 1000)
	go func(){
	    for {
		<- rf.timer.C
		rf.handleTime()
	    }
	}()
    }
    new_timeout := HEARTBEAT
    if rf.state != LEADER{
       // set Election Timeout for other state
       new_timeout = time.Millisecond * time.Duration(ElectionMinTime + rand.Int63n(ElectionMaxTime - ElectionMinTime))
    }
    rf.timer.Reset(new_timeout)
}
//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	_, isLeader := rf.GetState()
        
	if !isLeader {
	   return index,term,isLeader
	}
	curLog := LogEntry{Command:command,Term:rf.currentTerm}
        rf.log = append(rf.log, curLog)
	
        index = rf.lastLogIndex() + 1
	term = rf.currentTerm
        rf.persist()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here.
	rf.state = FOLLOWER
        rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
        rf.log = make([]LogEntry,0)

	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
        rf.matchIndex = make([]int, len(rf.peers))

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.timer = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

        rf.resetTimer()
	return rf
}
