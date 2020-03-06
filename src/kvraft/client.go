package kvraft

import (
	"labrpc"
	"log"
	"time"
)
import "crypto/rand"
import "math/big"

const (
	ChangeLeaderInterval = time.Millisecond * 20
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	leaderId int
	DebugLog bool // print log
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) genMsgId() msgId {
	return msgId(nrand())
}

func (ck *Clerk) log(v ...interface{}) {
	if ck.DebugLog {
		log.Printf("client:%d leaderid: %d, log:%v", ck.clientId, ck.leaderId, v)
	}
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.log("in get: ", key)
	args := GetArgs{Key: key, MsgId: ck.genMsgId(), ClientId: ck.clientId}
	leaderId := ck.leaderId
	for {
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			ck.log("req server err not ok", leaderId)
		} else if reply.Err != OK {
			ck.log("req server err", leaderId, ok, reply.Err)
		}

		if !ok {
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			ck.log("get kv", key, reply.Value)
			ck.leaderId = leaderId
			return reply.Value
		case ErrNoKey:
			ck.log("get err no key", key)
			ck.leaderId = leaderId
			return ""
		case ErrTimeOut:
			continue
		default:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		MsgId:    ck.genMsgId(),
		ClientId: ck.clientId,
	}
	leaderId := ck.leaderId
	for {
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.log("req server err not ok", leaderId)
		} else if reply.Err != OK {
			ck.log("req server err", leaderId, ok, reply.Err)
		}
		if !ok {
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			ck.log("put append key ok:", key, value)
			return
		case ErrNoKey:
			log.Fatal("client putappend get err nokey")
		case ErrWrongLeader:
			time.Sleep(ChangeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		case ErrTimeOut:
			continue
		default:
			log.Fatal("client unknown err", reply.Err)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
