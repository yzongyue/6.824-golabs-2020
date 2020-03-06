package raft

import (
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("req_vote")
	defer rf.unlock("req_vote")
	defer func() {
		rf.log("get request vote, args:%+v, reply:%+v", args, reply)
	}()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	reply.Term = rf.term
	reply.VoteGranted = false

	if args.Term < rf.term {
		return
	} else if args.Term == rf.term {
		if rf.role == Leader {
			return
		}
		if rf.voteFor == args.CandidateId {
			reply.VoteGranted = true
			return
		}
		if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
			// 已投给其他 server
			return
		}
		// 还一种可能:没有投票
	}

	defer rf.persist()
	if args.Term > rf.term {
		rf.term = args.Term
		rf.voteFor = -1
		rf.changeRole(Follower)
	}

	if lastLogTerm > args.LastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		// 选取限制
		return
	}

	rf.term = args.Term
	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
	rf.changeRole(Follower)
	rf.resetElectionTimer()
	rf.log("vote for:%d", args.CandidateId)
	return
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())
}

func (rf *Raft) sendRequestVoteToPeer(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	// 当网络出错 call 迅速返回时，会产生大量 goroutine 及 rpc
	// 所以加了 sleep
	// TODO
	t := time.NewTimer(RPCTimeout)
	defer t.Stop()
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	for {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout)
		ch := make(chan bool, 1)
		r := RequestVoteReply{}

		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, &r)
			if ok == false {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()

		select {
		case <-t.C:
			return
		case <-rpcTimer.C:
			continue
		case ok := <-ch:
			if !ok {
				continue
			} else {
				reply.Term = r.Term
				reply.VoteGranted = r.VoteGranted
				return
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.lock("start_election")
	rf.electionTimer.Reset(randElectionTimeout())
	if rf.role == Leader {
		rf.unlock("start_election")
		return
	}
	rf.log("start election")
	rf.changeRole(Candidate)
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.persist()
	rf.unlock("start_election")

	grantedCount := 1
	chResCount := 1
	votesCh := make(chan bool, len(rf.peers))
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(ch chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVoteToPeer(index, &args, &reply)
			ch <- reply.VoteGranted
			if reply.Term > args.Term {
				rf.lock("start_ele_change_term")
				if rf.term < reply.Term {
					rf.term = reply.Term
					rf.changeRole(Follower)
					rf.resetElectionTimer()
					rf.persist()
				}
				rf.unlock("start_ele_change_term")
			}
		}(votesCh, index)
	}

	for {
		r := <-votesCh
		chResCount += 1
		if r == true {
			grantedCount += 1
		}
		if chResCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || chResCount-grantedCount > len(rf.peers)/2 {
			break
		}
	}

	if grantedCount <= len(rf.peers)/2 {
		rf.log("grantedCount <= len/2:count:%d", grantedCount)
		return
	}

	rf.lock("start_ele2")
	rf.log("before try change to leader,count:%d, args:%+v", grantedCount, args)
	if rf.term == args.Term && rf.role == Candidate {
		rf.changeRole(Leader)
		rf.persist()
	}
	if rf.role == Leader {
		rf.resetHeartBeatTimers()
	}
	rf.unlock("start_ele2")
}
