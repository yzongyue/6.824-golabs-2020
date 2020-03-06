package shardmaster

import "labgob"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

const (
	OK             = "OK"
	ErrWrongLeader = "wrongLeader"
	ErrTimeout     = "timeout"
)

func init() {
	labgob.Register(Config{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c *Config) Copy() Config {
	//var ss [NShards]int
	//for i, v := range c.Shards {
	//	ss[i] = v
	//}
	config := Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: make(map[int][]string),
	}
	for gid, s := range c.Groups {
		config.Groups[gid] = append([]string{}, s...)
	}
	return config
}

type Err string
type msgId int64

type CommonArgs struct {
	MsgId    msgId
	ClientId int64
}

type JoinArgs struct {
	CommonArgs
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	CommonArgs
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	CommonArgs
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	CommonArgs
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
