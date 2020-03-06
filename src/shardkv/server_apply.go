package shardkv

import (
	"fmt"
	"raft"
	"shardmaster"
	"time"
)

func (kv *ShardKV) dataGet(key string) (err Err, val string) {
	if v, ok := kv.data[key2shard(key)][key]; ok {
		err = OK
		val = v
		return
	} else {
		err = ErrNoKey
		return err, ""
	}
}

func (kv *ShardKV) waitApplyCh() {
	defer func() {
		// for debug
		kv.log(fmt.Sprintf("kvkilled:%v", kv.killed()))
		time.Sleep(time.Millisecond * 1000)
		kv.log(fmt.Sprintf("kv applych killed, applych len:%d", len(kv.applyCh)))
	}()
	for {
		select {
		case <- kv.stopCh:
			return
		case msg := <-kv.applyCh:
			if !msg.CommandValid {
				kv.log(fmt.Sprintf("get install sn,idx: %d", msg.CommandIndex))
				kv.applySnapshot()
				continue
			}
			kv.log(fmt.Sprintf("get applymsg: idx:%d, msg:%+v", msg.CommandIndex, msg))
			if op, ok := msg.Command.(Op); ok {
				kv.applyOp(msg, op)
			} else if config, ok := msg.Command.(shardmaster.Config); ok {
				kv.applyConfig(msg, config)
			} else if mergeData, ok := msg.Command.(MergeShardData); ok {
				kv.applyMergeShardData(msg, mergeData)
			} else if cleanUp, ok := msg.Command.(CleanShardDataArgs); ok {
				kv.applyCleanUp(msg, cleanUp)
			} else {
				panic("applyerr")
			}
		}
	}
}

func (kv *ShardKV) applySnapshot() {
	kv.lock("waitApplyCh_sn")
	kv.readSnapShotData(kv.persister.ReadSnapshot())
	kv.unlock("waitApplyCh_sn")
}

func (kv *ShardKV) applyOp(msg raft.ApplyMsg, op Op) {
	msgIdx := msg.CommandIndex
	kv.lock("waitApplyCh")
	kv.log(fmt.Sprintf("in applyOp:%+v", op))

	shardId := key2shard(op.Key)
	isRepeated := kv.isRepeated(shardId, op.ClientId, op.MsgId)

	if kv.configReady(op.ConfigNum, op.Key) == OK {

		switch op.Op {
		case "Put":
			if !isRepeated {
				kv.data[shardId][op.Key] = op.Value
				kv.lastMsgIdx[shardId][op.ClientId] = op.MsgId
			}
		case "Append":
			if !isRepeated {
				_, v := kv.dataGet(op.Key)
				kv.data[shardId][op.Key] = v + op.Value
				kv.lastMsgIdx[shardId][op.ClientId] = op.MsgId
			}

		case "Get":
		default:
			panic(fmt.Sprintf("unknown method: %s", op.Op))
		}
		kv.log(fmt.Sprintf("apply op: msgIdx:%d, op: %+v, data:%v", msgIdx, op, kv.data[shardId][op.Key]))
		kv.saveSnapshot(msgIdx)
		if ch, ok := kv.notifyCh[op.ReqId]; ok {
			nm := NotifyMsg{Err:OK}
			if op.Op == "Get" {
				nm.Err, nm.Value = kv.dataGet(op.Key)
			}
			ch <- nm
		}
		kv.unlock("waitApplyCh")

	} else {
		// config not ready
		if ch, ok := kv.notifyCh[op.ReqId]; ok {
			ch <- NotifyMsg{Err:ErrWrongGroup}
		}
		kv.unlock("waitApplyCh")
		return
	}
}

func (kv *ShardKV) applyConfig(msg raft.ApplyMsg, config shardmaster.Config) {
	kv.lock("applyConfig")
	defer kv.unlock("applyConfig")
	kv.log(fmt.Sprintf("in applyConfig:%+v", config))

	if config.Num <= kv.config.Num {
		kv.saveSnapshot(msg.CommandIndex)
		return
	}
	if config.Num != kv.config.Num+1 {
		panic("applyConfig err")
	}

	// pull config 判断 waitid
	//if len(kv.waitShardIds) != 0 {
	//	kv.log(fmt.Sprintf("got new config:%d, but we have waitshardid now", config.Num))
	//	return
	//}

	oldConfig := kv.config.Copy()
	deleteShardIds := make([]int, 0, shardmaster.NShards)
	ownShardIds := make([]int, 0, shardmaster.NShards)
	newShardIds := make([]int, 0, shardmaster.NShards)

	for i := 0; i < shardmaster.NShards; i++ {
		if config.Shards[i] == kv.gid {
			ownShardIds = append(ownShardIds, i)
			if oldConfig.Shards[i] != kv.gid {
				newShardIds = append(newShardIds, i)
			}
		} else {
			if oldConfig.Shards[i] == kv.gid {
				deleteShardIds = append(deleteShardIds, i)
			}
		}
	}

	d := make(map[int]MergeShardData)
	for _, shardId := range deleteShardIds {
		mergeShardData := MergeShardData{
			ConfigNum:  oldConfig.Num,
			ShardNum:   shardId,
			Data:       kv.data[shardId],
			MsgIndexes: kv.lastMsgIdx[shardId],
		}
		d[shardId] = mergeShardData
		kv.data[shardId] = make(map[string]string)
		kv.lastMsgIdx[shardId] = make(map[int64]int64)
	}
	kv.historyShards[oldConfig.Num] = d

	kv.ownShards = make(map[int]bool)
	for _, shardId := range ownShardIds {
		kv.ownShards[shardId] = true
	}
	kv.waitShardIds = make(map[int]bool)
	if oldConfig.Num != 0 {
		for _, shardId := range newShardIds {
			kv.waitShardIds[shardId] = true
		}
	}

	kv.config = config.Copy()
	kv.oldConfig = oldConfig
	kv.saveSnapshot(msg.CommandIndex)
}

func (kv *ShardKV) applyMergeShardData(msg raft.ApplyMsg, data MergeShardData) {
	kv.lock("applyMergeShardData")
	defer kv.unlock("applyMergeShardData")
	defer kv.saveSnapshot(msg.CommandIndex)
	kv.log(fmt.Sprintf("in applyMerge:%+v, msgidx:%d", data, msg.CommandIndex))

	if kv.config.Num != data.ConfigNum+1 {
		return
	}
	if _, ok := kv.waitShardIds[data.ShardNum]; !ok {
		return
	}
	kv.data[data.ShardNum] = make(map[string]string)
	kv.lastMsgIdx[data.ShardNum] = make(map[int64]int64)
	for k, v := range data.Data {
		kv.data[data.ShardNum][k] = v
	}
	for k, v := range data.MsgIndexes {
		kv.lastMsgIdx[data.ShardNum][k] = v
	}
	delete(kv.waitShardIds, data.ShardNum)
	go kv.reqCleanShardData(kv.oldConfig, data.ShardNum)
}

func (kv *ShardKV) applyCleanUp(msg raft.ApplyMsg, data CleanShardDataArgs) {
	kv.lock("ApplyCleanUp")
	kv.log(fmt.Sprintf("applyCleanup:msg:%+v, data:%+v", msg, data))
	if kv.historyDataExist(data.ConfigNum, data.ShardNum) {
		delete(kv.historyShards[data.ConfigNum], data.ShardNum)
	}
	kv.saveSnapshot(msg.CommandIndex)
	kv.unlock("ApplyCleanUp")
}
