package shardkv

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"sync"
	"sync/atomic"
	"time"
)

const (
	PullConfigInterval       = time.Millisecond * 100
	PullShardsInterval       = time.Millisecond * 200
	WaitCmdTimeOut           = time.Millisecond * 500 // 好慢。。
	ReqCleanShardDataTimeOut = time.Millisecond * 500
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	config        shardmaster.Config // the latest config
	oldConfig     shardmaster.Config
	notifyCh      map[int64]chan NotifyMsg             // notify cmd done
	lastMsgIdx    [shardmaster.NShards]map[int64]int64 // clientId -> msgId map
	ownShards     map[int]bool
	data          [shardmaster.NShards]map[string]string // kv per shard
	waitShardIds  map[int]bool
	historyShards map[int]map[int]MergeShardData // configNum -> shard -> data
	mck           *shardmaster.Clerk

	dead           int32 // for stop
	stopCh         chan struct{}
	persister      *raft.Persister
	lastApplyIndex int
	lastApplyTerm  int

	pullConfigTimer *time.Timer
	pullShardsTimer *time.Timer

	DebugLog  bool      // print log
	lockStart time.Time // debug 用，找出长时间 lock
	lockEnd   time.Time
	lockName  string
}

func (kv *ShardKV) lock(m string) {
	kv.mu.Lock()
	kv.lockStart = time.Now()
	kv.lockName = m
}

func (kv *ShardKV) unlock(m string) {
	kv.lockEnd = time.Now()
	duration := kv.lockEnd.Sub(kv.lockStart)
	kv.lockName = ""
	kv.mu.Unlock()
	if duration > time.Millisecond*2 {
		kv.log(fmt.Sprintf("lock too long:%s:%s\n", m, duration))
	}
}

func (kv *ShardKV) log(m string) {
	if kv.DebugLog {
		log.Printf("server me: %d, gid:%d, config:%+v, waitid:%+v, log:%s",
			kv.me, kv.gid, kv.config, kv.waitShardIds, m)
	}
}

func (kv *ShardKV) isRepeated(shardId int, clientId int64, id int64) bool {
	if val, ok := kv.lastMsgIdx[shardId][clientId]; ok {
		return val == id
	}
	return false
}

func (kv *ShardKV) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	// need snapshot
	data := kv.genSnapshotData()
	kv.rf.SavePersistAndShnapshot(logIndex, data)
}

func (kv *ShardKV) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(kv.data) != nil ||
		e.Encode(kv.lastMsgIdx) != nil ||
		e.Encode(kv.waitShardIds) != nil ||
		e.Encode(kv.historyShards) != nil ||
		e.Encode(kv.config) != nil ||
		e.Encode(kv.oldConfig) != nil ||
		e.Encode(kv.ownShards) != nil {
		panic("gen snapshot data encode err")
	}

	data := w.Bytes()
	return data
}

func (kv *ShardKV) readSnapShotData(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData [shardmaster.NShards]map[string]string
	var lastMsgIdx [shardmaster.NShards]map[int64]int64
	var waitShardIds map[int]bool
	var historyShards map[int]map[int]MergeShardData
	var config shardmaster.Config
	var oldConfig shardmaster.Config
	var ownShards map[int]bool

	if d.Decode(&kvData) != nil ||
		d.Decode(&lastMsgIdx) != nil ||
		d.Decode(&waitShardIds) != nil ||
		d.Decode(&historyShards) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&oldConfig) != nil ||
		d.Decode(&ownShards) != nil {
		log.Fatal("kv read persist err")
	} else {
		kv.data = kvData
		kv.lastMsgIdx = lastMsgIdx
		kv.waitShardIds = waitShardIds
		kv.historyShards = historyShards
		kv.config = config
		kv.oldConfig = oldConfig
		kv.ownShards = ownShards
	}
}

func (kv *ShardKV) configReady(configNum int, key string) Err {
	if configNum == 0 || configNum != kv.config.Num {
		kv.log("configReadyerr1")
		return ErrWrongGroup
	}
	shardId := key2shard(key)
	if _, ok := kv.ownShards[shardId]; !ok {
		kv.log("configReadyerr2")
		return ErrWrongGroup
	}
	if _, ok := kv.waitShardIds[shardId]; ok {
		kv.log("configReadyerr3")
		return ErrWrongGroup
	}
	return OK
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	close(kv.stopCh)
	kv.log("kill kv get")
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) pullConfig() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullConfigTimer.C:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.pullConfigTimer.Reset(PullConfigInterval)
				break
			}

			kv.lock("pullConfig")
			lastNum := kv.config.Num
			kv.log(fmt.Sprintf("pull config get last:%d", lastNum))
			kv.unlock("pullConfig")

			config := kv.mck.Query(lastNum + 1)
			if config.Num == lastNum+1 {
				// 找到新的 config
				kv.log(fmt.Sprintf("pull config found config: %+v, lastNum:%d", config, lastNum))
				kv.lock("pullConfig")
				if len(kv.waitShardIds) == 0 && kv.config.Num+1 == config.Num {
					kv.log(fmt.Sprintf("pull config start config: %+v, lastNum:%d", config, lastNum))
					kv.unlock("pullConfig")
					kv.rf.Start(config.Copy())
				} else {
					kv.unlock("pullConfig")
				}
			}
			kv.pullConfigTimer.Reset(PullConfigInterval)
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persister = persister

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan struct{})
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, kv.gid)
	// kv.rf.DebugLog = false
	kv.DebugLog = false

	// You may need initialization code here.
	kv.data = [shardmaster.NShards]map[string]string{}
	for i, _ := range kv.data {
		kv.data[i] = make(map[string]string)
	}
	kv.lastMsgIdx = [shardmaster.NShards]map[int64]int64{}
	for i, _ := range kv.lastMsgIdx {
		kv.lastMsgIdx[i] = make(map[int64]int64)
	}

	kv.waitShardIds = make(map[int]bool)
	kv.historyShards = make(map[int]map[int]MergeShardData)
	config := shardmaster.Config{
		Num:    0,
		Shards: [shardmaster.NShards]int{},
		Groups: map[int][]string{},
	}
	kv.config = config
	kv.oldConfig = config
	kv.readSnapShotData(kv.persister.ReadSnapshot())

	kv.notifyCh = make(map[int64]chan NotifyMsg)
	kv.pullConfigTimer = time.NewTimer(PullConfigInterval)
	kv.pullShardsTimer = time.NewTimer(PullShardsInterval)

	go kv.waitApplyCh()
	go kv.pullConfig()
	go kv.pullShards()

	// for debug
	//go func() {
	//	for !kv.killed() {
	//		time.Sleep(time.Second * 2)
	//		d := time.Now().Sub(kv.lockStart)
	//		if kv.lockName != "" && d > time.Millisecond * 5 {
	//			kv.log(fmt.Sprintf("kv who has lock:%s, time:%v", kv.lockName, d))
	//		}
	//		kv.log(fmt.Sprintf("kv applyCh len:%d", len(kv.applyCh)))
	//	}
	//
	//}()

	return kv
}
