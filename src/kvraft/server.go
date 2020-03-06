package kvraft

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

const WaitCmdTimeOut = time.Millisecond * 500 // 好慢。。。
const MaxLockTime = time.Millisecond * 10     // debug

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	MsgId    msgId
	ReqId    int64
	ClientId int64
	Key      string
	Value    string
	Method   string
}

type NotifyMsg struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	stopCh  chan struct{}

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	msgNotify   map[int64]chan NotifyMsg
	lastApplies map[int64]msgId // last apply put/append msg
	data        map[string]string

	persister      *raft.Persister
	lastApplyIndex int
	lastApplyTerm  int

	DebugLog  bool      // print log
	lockStart time.Time // debug 用，找出长时间 lock
	lockEnd   time.Time
	lockName  string
}

func (kv *KVServer) lock(m string) {
	kv.mu.Lock()
	kv.lockStart = time.Now()
	kv.lockName = m
}

func (kv *KVServer) unlock(m string) {
	kv.lockEnd = time.Now()
	duration := kv.lockEnd.Sub(kv.lockStart)
	kv.lockName = ""
	kv.mu.Unlock()
	if duration > MaxLockTime {
		kv.log(fmt.Sprintf("lock too long:%s:%s\n", m, duration))
	}
}

func (kv *KVServer) log(m string) {
	if kv.DebugLog {
		log.Printf("server me: %d, log:%s", kv.me, m)
	}
}

func (kv *KVServer) dataGet(key string) (err Err, val string) {
	if v, ok := kv.data[key]; ok {
		err = OK
		val = v
		return
	} else {
		err = ErrNoKey
		return
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.log(fmt.Sprintf("in rpc get, args:%+v", args))
	defer func() {
		kv.log(fmt.Sprintf("in rpc get, args:%+v, reply:%+v", args, reply))
	}()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		MsgId:    args.MsgId,
		ReqId:    nrand(),
		Key:      args.Key,
		Method:   "Get",
		ClientId: args.ClientId,
	}
	res := kv.waitCmd(op)
	reply.Err = res.Err
	reply.Value = res.Value
	kv.log(fmt.Sprintf("get key:%s, err:%s, v:%s", op.Key, reply.Err, reply.Value))
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	defer func() {
		kv.log(fmt.Sprintf("in rpc putappend, args:%+v, reply:%+v", args, reply))
	}()
	op := Op{
		MsgId:    args.MsgId,
		ReqId:    nrand(),
		Key:      args.Key,
		Value:    args.Value,
		Method:   args.Op,
		ClientId: args.ClientId,
	}
	reply.Err = kv.waitCmd(op).Err
}

func (kv *KVServer) removeCh(id int64) {
	kv.lock("removeCh")
	delete(kv.msgNotify, id)
	kv.unlock("removeCh")
}

func (kv *KVServer) waitCmd(op Op) (res NotifyMsg) {
	kv.log("waitcmd func enter")

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	kv.lock("waitCmd")
	ch := make(chan NotifyMsg, 1)
	kv.msgNotify[op.ReqId] = ch
	kv.unlock("waitCmd")
	kv.log(fmt.Sprintf("start cmd: index:%d, term:%d, op:%+v", index, term, op))
	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()
	select {
	case res = <-ch:
		kv.removeCh(op.ReqId)
		return
	case <-t.C:
		kv.removeCh(op.ReqId)
		res.Err = ErrTimeOut
		return
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	close(kv.stopCh)
	// Your code here, if desired.
}

func (kv *KVServer) isRepeated(clientId int64, id msgId) bool {
	if val, ok := kv.lastApplies[clientId]; ok {
		return val == id
	}
	return false
}

func (kv *KVServer) waitApplyCh() {
	for {
		select {
		case <-kv.stopCh:
			kv.log("stop ch get")
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				kv.log(fmt.Sprintf("get install sn,idx: %d", msg.CommandIndex))
				kv.lock("waitApplyCh_sn")
				kv.readPersist(kv.persister.ReadSnapshot())
				kv.unlock("waitApplyCh_sn")
				continue
			}
			msgIdx := msg.CommandIndex
			op := msg.Command.(Op)
			kv.lock("waitApplyCh")

			isRepeated := kv.isRepeated(op.ClientId, op.MsgId)
			switch op.Method {
			case "Put":
				if !isRepeated {
					kv.data[op.Key] = op.Value
					kv.lastApplies[op.ClientId] = op.MsgId
				}
			case "Append":
				if !isRepeated {
					_, v := kv.dataGet(op.Key)
					kv.data[op.Key] = v + op.Value
					kv.lastApplies[op.ClientId] = op.MsgId
				}

			case "Get":
			default:
				panic(fmt.Sprintf("unknown method: %s", op.Method))
			}
			kv.log(fmt.Sprintf("apply op: msgIdx:%d, op: %+v, data:%v", msgIdx, op, kv.data[op.Key]))
			kv.saveSnapshot(msgIdx)
			if ch, ok := kv.msgNotify[op.ReqId]; ok {
				_, v := kv.dataGet(op.Key)
				ch <- NotifyMsg{
					Err:   OK,
					Value: v,
				}
			}
			kv.unlock("waitApplyCh")
		}
	}
}

func (kv *KVServer) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	// need snapshot
	data := kv.genSnapshotData()
	// 这里加 go 的话可能会使 state size 过大
	kv.rf.SavePersistAndShnapshot(logIndex, data)
}

func (kv *KVServer) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.data); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.lastApplies); err != nil {
		panic(err)
	}
	data := w.Bytes()
	return data
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData map[string]string
	var lastApplies map[int64]msgId

	if d.Decode(&kvData) != nil ||
		d.Decode(&lastApplies) != nil {
		log.Fatal("kv read persist err")
	} else {
		kv.data = kvData
		kv.lastApplies = lastApplies
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.lastApplies = make(map[int64]msgId)
	kv.stopCh = make(chan struct{})
	kv.readPersist(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// kv.rf.DebugLog = false
	kv.DebugLog = false

	// You may need initialization code here.

	kv.msgNotify = make(map[int64]chan NotifyMsg)

	go kv.waitApplyCh()

	// for debug
	//go func() {
	//	for !kv.killed() {
	//		time.Sleep(time.Second * 2)
	//		kv.log(fmt.Sprintf("who has lock:%s, time:%v", kv.lockName, time.Now().Sub(kv.lockStart)))
	//	}
	//
	//}()

	return kv
}
