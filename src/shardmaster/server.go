package shardmaster

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sort"
	"sync"
	"time"
)

const WaitCmdTimeOut = time.Millisecond * 500
const MaxLockTime = time.Millisecond * 10 // debug

type NotifyMsg struct {
	Err         Err
	WrongLeader bool
	Config      Config
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	stopCh  chan struct{}

	// Your data here.
	msgNotify   map[int64]chan NotifyMsg
	lastApplies map[int64]msgId // last apply put/append msg

	configs []Config // indexed by config num

	DebugLog  bool
	lockStart time.Time // debug 用，找出长时间 lock
	lockEnd   time.Time
	lockName  string
}

type Op struct {
	// Your data here.
	MsgId    msgId
	ReqId    int64
	Args     interface{}
	Method   string
	ClientId int64
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	res := sm.runCmd("Join", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	res := sm.runCmd("Leave", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	res := sm.runCmd("Move", args.MsgId, args.ClientId, *args)
	reply.Err, reply.WrongLeader = res.Err, res.WrongLeader
}

func (sm *ShardMaster) adjustConfig(config *Config) {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
	} else if len(config.Groups) == 1 {
		// set shards one gid
		for k, _ := range config.Groups {
			for i, _ := range config.Shards {
				config.Shards[i] = k
			}
		}
	} else if len(config.Groups) <= NShards {
		avg := NShards / len(config.Groups)
		// 每个 gid 分 avg 个 shard
		otherShardsCount := NShards - avg*len(config.Groups)
		needLoop := false
		lastGid := 0

	LOOP:
		sm.log(fmt.Sprintf("config: %+v", config))
		var keys []int
		for k := range config.Groups {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, gid := range keys {
			lastGid = gid
			count := 0
			// 先 count 已有的
			for _, val := range config.Shards {
				if val == gid {
					count += 1
				}
			}

			// 判断是否需要改变
			if count == avg {
				continue
			} else if count > avg && otherShardsCount == 0 {
				// 减少到 avg
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg {
							config.Shards[i] = 0
						} else {
							c += 1
						}
					}
				}

			} else if count > avg && otherShardsCount > 0 {
				// 减到 othersShardsCount 为 0
				// 若还 count > avg, set to 0
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg+otherShardsCount {
							config.Shards[i] = 0
						} else {
							if c == avg {
								otherShardsCount -= 1
							} else {
								c += 1
							}

						}
					}
				}

			} else {
				// count < avg, 此时有可能没有位置
				for i, val := range config.Shards {
					if count == avg {
						break
					}
					if val == 0 && count < avg {
						config.Shards[i] = gid
					}
				}

				if count < avg {
					sm.log(fmt.Sprintf("needLoop: %+v, %+v, %d ", config, otherShardsCount, gid))
					needLoop = true
				}

			}

		}

		if needLoop {
			needLoop = false
			goto LOOP
		}

		// 可能每一个 gid 都 >= avg，但此时有空的 shard
		if lastGid != 0 {
			for i, val := range config.Shards {
				if val == 0 {
					config.Shards[i] = lastGid
				}
			}
		}

	} else {
		// len(config.Groups) > NShards
		// 每个 gid 最多一个， 会有空余 gid
		gids := make(map[int]int)
		emptyShards := make([]int, 0, NShards)
		for i, gid := range config.Shards {
			if gid == 0 {
				emptyShards = append(emptyShards, i)
				continue
			}
			if _, ok := gids[gid]; ok {
				emptyShards = append(emptyShards, i)
				config.Shards[i] = 0
			} else {
				gids[gid] = 1
			}
		}
		n := 0
		if len(emptyShards) > 0 {
			var keys []int
			for k := range config.Groups {
				keys = append(keys, k)
			}
			sort.Ints(keys)
			for _, gid := range keys {
				if _, ok := gids[gid]; !ok {
					config.Shards[emptyShards[n]] = gid
					n += 1
				}
				if n >= len(emptyShards) {
					break
				}
			}
		}

	}

}

func (sm *ShardMaster) join(args JoinArgs) {
	config := sm.getConfigByIndex(-1)
	config.Num += 1

	for k, v := range args.Servers {
		config.Groups[k] = v
	}

	sm.adjustConfig(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) leave(args LeaveArgs) {
	config := sm.getConfigByIndex(-1)
	config.Num += 1

	for _, gid := range args.GIDs {
		delete(config.Groups, gid)
		for i, v := range config.Shards {
			if v == gid {
				config.Shards[i] = 0
			}
		}
	}
	sm.adjustConfig(&config)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) move(args MoveArgs) {
	config := sm.getConfigByIndex(-1)
	config.Num += 1
	config.Shards[args.Shard] = args.GID
	sm.configs = append(sm.configs, config)

}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	defer func() {
		sm.log(fmt.Sprintf("sm query: args:%+v, reply:%+v\n", args, reply))
	}()
	// Your code here.
	sm.lock("query")
	if args.Num > 0 && args.Num < len(sm.configs) {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = sm.getConfigByIndex(args.Num)
		sm.unlock("query")
		return
	}
	sm.unlock("query")

	res := sm.runCmd("Query", args.MsgId, args.ClientId, *args)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
	reply.Config = res.Config
}

func (sm *ShardMaster) getConfigByIndex(idx int) Config {
	if idx < 0 || idx >= len(sm.configs) {
		return sm.configs[len(sm.configs)-1].Copy()
	} else {
		return sm.configs[idx].Copy()
	}
}

func (sm *ShardMaster) lock(m string) {
	sm.mu.Lock()
	sm.lockStart = time.Now()
	sm.lockName = m
}

func (sm *ShardMaster) unlock(m string) {
	sm.lockEnd = time.Now()
	duration := sm.lockEnd.Sub(sm.lockStart)
	sm.lockName = ""
	sm.mu.Unlock()
	if duration > MaxLockTime {
		sm.log(fmt.Sprintf("lock too long:%s:%s\n", m, duration))
	}
}

func (sm *ShardMaster) log(m string) {
	if sm.DebugLog {
		log.Printf("shardmaster me: %d, configs:%+v, log:%s", sm.me, sm.configs, m)
	}
}

func (sm *ShardMaster) runCmd(method string, id msgId, clientId int64, args interface{}) (res NotifyMsg) {
	op := Op{
		MsgId:    id,
		ReqId:    nrand(),
		Args:     args,
		Method:   method,
		ClientId: clientId,
	}
	res = sm.waitCmd(op)
	return

}

func (sm *ShardMaster) waitCmd(op Op) (res NotifyMsg) {
	sm.log(fmt.Sprintf("%+v", op))
	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		res.WrongLeader = true
		return
	}
	sm.lock("waitCmd")
	ch := make(chan NotifyMsg, 1)
	sm.msgNotify[op.ReqId] = ch
	sm.unlock("waitCmd")
	sm.log(fmt.Sprintf("start cmd: index:%d, term:%d, op:%+v", index, term, op))
	t := time.NewTimer(WaitCmdTimeOut)
	defer t.Stop()
	select {
	case res = <-ch:
		sm.removeCh(op.ReqId)
		return
	case <-t.C:
		sm.removeCh(op.ReqId)
		res.WrongLeader = true // 少改点 client 代码
		res.Err = ErrTimeout
		return
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	close(sm.stopCh)
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) removeCh(id int64) {
	sm.lock("removech")
	delete(sm.msgNotify, id)
	sm.unlock("removech")
}

func (sm *ShardMaster) apply() {
	for {
		select {
		case <-sm.stopCh:
			return
		case msg := <-sm.applyCh:
			if !msg.CommandValid {
				continue
			}
			sm.log(fmt.Sprintf("get msg:%+v", msg))
			op := msg.Command.(Op)

			sm.lock("apply")
			isRepeated := sm.isRepeated(op.ClientId, op.MsgId)
			if !isRepeated {
				switch op.Method {
				case "Join":
					sm.join(op.Args.(JoinArgs))
				case "Leave":
					sm.leave(op.Args.(LeaveArgs))
				case "Move":
					sm.move(op.Args.(MoveArgs))
				case "Query":
				default:
					panic("unknown method")
				}
			}
			res := NotifyMsg{
				Err:         OK,
				WrongLeader: false,
			}
			if op.Method != "Query" {
				sm.lastApplies[op.ClientId] = op.MsgId
			} else {
				res.Config = sm.getConfigByIndex(op.Args.(QueryArgs).Num)
			}
			if ch, ok := sm.msgNotify[op.ReqId]; ok {
				sm.log(fmt.Sprintf("sm apply: op:%+v, res.config:%+v\n", op, res.Config))
				ch <- res
			}
			sm.unlock("apply2")
		}
	}

}

func (sm *ShardMaster) isRepeated(clientId int64, id msgId) bool {
	if val, ok := sm.lastApplies[clientId]; ok {
		return val == id
	}
	return false
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	labgob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.applyCh = make(chan raft.ApplyMsg, 100)
	sm.stopCh = make(chan struct{})
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.DebugLog = false
	// sm.rf.DebugLog = true

	sm.lastApplies = make(map[int64]msgId)
	sm.msgNotify = make(map[int64]chan NotifyMsg)

	// Your code here.
	go sm.apply()
	return sm
}
