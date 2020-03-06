package shardkv

import (
	"fmt"
	"shardmaster"
	"time"
)

func (kv *ShardKV) FetchShardData(args *FetchShardDataArgs, reply *FetchShardDataReply) {
	kv.lock("fetchShardData")
	defer kv.unlock("fetchShardData")
	defer kv.log(fmt.Sprintf("resp fetchsharddata:args:%+v, reply:%+v", args, reply))

	if args.ConfigNum >= kv.config.Num {
		return
	}

	if configData, ok := kv.historyShards[args.ConfigNum]; ok {
		if shardData, ok := configData[args.ShardNum]; ok {
			reply.Success = true
			reply.Data = make(map[string]string)
			reply.MsgIndexes = make(map[int64]int64)
			for k, v := range shardData.Data {
				reply.Data[k] = v
			}
			for k, v := range shardData.MsgIndexes {
				reply.MsgIndexes[k] = v
			}
		}
	}
	return
}

func (kv *ShardKV) historyDataExist(configNum int, shardId int) bool {
	if _, ok := kv.historyShards[configNum]; ok {
		if _, ok = kv.historyShards[configNum][shardId]; ok {
			return true
		}
	}
	return false
}

func (kv *ShardKV) CleanShardData(args *CleanShardDataArgs, reply *CleanShardDataReply) {
	kv.lock("cleanShardData")

	if args.ConfigNum >= kv.config.Num {
		// 此时没有数据
		kv.unlock("cleanShardData")
		return
	}
	kv.unlock("cleanShardData")

	_, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		return
	}
	// 简单处理下。。。
	for i := 0; i < 10; i++ {
		kv.lock("cleanShardData")
		exist := kv.historyDataExist(args.ConfigNum, args.ShardNum)
		kv.unlock("cleanShardData")
		if !exist {
			reply.Success = true
			return
		}
		time.Sleep(time.Millisecond * 20)
	}
	return
}

func (kv *ShardKV) reqCleanShardData(config shardmaster.Config, shardId int) {
	configNum := config.Num
	args := &CleanShardDataArgs{
		ConfigNum: configNum,
		ShardNum:  shardId,
	}

	t := time.NewTimer(ReqCleanShardDataTimeOut)
	defer t.Stop()

	for {
		for _, s := range config.Groups[config.Shards[shardId]] {
			reply := &CleanShardDataReply{}
			srv := kv.make_end(s)
			done := make(chan bool, 1)
			r := false

			go func(args *CleanShardDataArgs, reply *CleanShardDataReply) {
				done <- srv.Call("ShardKV.CleanShardData", args, reply)
			}(args, reply)

			t.Reset(ReqCleanShardDataTimeOut)

			select {
			case <-kv.stopCh:
				return
			case r = <-done:
			case <-t.C:

			}
			if r == true && reply.Success == true {
				return
			}
		}
		kv.lock("reqCleanShardData")
		if kv.config.Num != configNum+1 || len(kv.waitShardIds) == 0 {
			kv.unlock("reqCleanShardData")
			break
		}
		kv.unlock("reqCleanShardData")
	}
}

func (kv *ShardKV) pullShards() {
	for {
		select {
		case <-kv.stopCh:
			return
		case <-kv.pullShardsTimer.C:
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.lock("pullShards")
				for shardId, _ := range kv.waitShardIds {
					go kv.pullShard(shardId, kv.oldConfig)
				}
				kv.unlock("pullShards")
			}
			kv.pullShardsTimer.Reset(PullShardsInterval)
		}
	}
}

func (kv *ShardKV) pullShard(shardId int, config shardmaster.Config) {
	args := FetchShardDataArgs{
		ConfigNum: config.Num,
		ShardNum:  shardId,
	}

	for _, s := range config.Groups[config.Shards[shardId]] {
		srv := kv.make_end(s)
		reply := FetchShardDataReply{}
		if ok := srv.Call("ShardKV.FetchShardData", &args, &reply); ok {
			if reply.Success {
				kv.lock("pullShard")
				if _, ok = kv.waitShardIds[shardId]; ok && kv.config.Num == config.Num+1 {
					replyCopy := reply.Copy()
					mergeArgs := MergeShardData{
						ConfigNum:  args.ConfigNum,
						ShardNum:   args.ShardNum,
						Data:       replyCopy.Data,
						MsgIndexes: replyCopy.MsgIndexes,
					}
					kv.log(fmt.Sprintf("pullShard get data:%+v", mergeArgs))
					kv.unlock("pullShard")
					_, _, isLeader := kv.rf.Start(mergeArgs)
					if !isLeader {
						break
					}
				} else {
					kv.unlock("pullShard")
				}
			}
		}
	}
}
