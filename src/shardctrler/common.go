package shardctrler

import (
	"math"
	"sort"
)

//
// Shard controler: assigns shards to replication groups.
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

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK      = "OK"
	TIMEOUT = "Timeout"
)

type OpType string

const (
	Join  = "Join"
	Move  = "Move"
	Leave = "Leave"
	Query = "Query"
)

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	RequestId int64
	ClientId  int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	RequestId int64
	ClientId  int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	RequestId int64
	ClientId  int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int // desired config number
	RequestId int64
	ClientId  int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type CommonReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type ReplyContext struct {
	LastRequestId int64
	Reply         CommonReply
}

func mapCopy(oldMap map[int][]string) map[int][]string {
	newMap := make(map[int][]string, len(oldMap))

	for k, v := range oldMap {
		newMap[k] = append([]string{}, v...)
	}

	return newMap
}

func addMap(m1, m2 map[int][]string) map[int][]string {
	result := make(map[int][]string, len(m1)+len(m2))
	for k, v := range m1 {
		result[k] = append([]string{}, v...)
	}

	for k, v := range m2 {
		result[k] = append(result[k], v...)
	}

	return result
}

func rebalanceShards(shards [NShards]int, groups map[int][]string) [NShards]int {
	if len(groups) == 0 {
		return [NShards]int{}
	}

	shard2group := shardToGroup(shards)
	for gid, _ := range groups {
		if _, ok := shard2group[gid]; !ok {
			shard2group[gid] = make([]int, 0)
		}
	}

	for {
		minGroup, maxGroup := minShardGroup(shard2group), maxShardGroup(shard2group)

		if maxGroup != 0 && len(shard2group[maxGroup])-len(shard2group[minGroup]) <= 1 {
			return shards
		}

		changeSid := shard2group[maxGroup][0]
		shards[changeSid] = minGroup
		shard2group[maxGroup] = shard2group[maxGroup][1:]
		shard2group[minGroup] = append(shard2group[minGroup], changeSid)
	}
}

func shardToGroup(shards [NShards]int) map[int][]int {
	shardToGroup := make(map[int][]int)

	for sid, gid := range shards {
		if _, ok := shardToGroup[gid]; !ok {
			shardToGroup[gid] = make([]int, 0)
		}
		shardToGroup[gid] = append(shardToGroup[gid], sid)
	}

	return shardToGroup
}

func NewConfig() Config {
	return Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
}

func minShardGroup(shard2group map[int][]int) int {
	var sortedGids []int

	for gid := range shard2group {
		if gid != 0 {
			sortedGids = append(sortedGids, gid)
		}
	}
	sort.Ints(sortedGids)

	minNum, minGid := math.MaxInt, -1

	for _, gid := range sortedGids {
		numShards := len(shard2group[gid])
		if numShards < minNum {
			minNum, minGid = numShards, gid
		}
	}

	return minGid
}

func maxShardGroup(shard2group map[int][]int) int {
	var sortedGids []int

	for gid := range shard2group {
		sortedGids = append(sortedGids, gid)
	}
	sort.Ints(sortedGids)

	maxNum, maxGid := math.MinInt, -1

	for _, gid := range sortedGids {
		if gid == 0 && len(shard2group[0]) > 0 {
			return 0
		}

		numShards := len(shard2group[gid])
		if numShards > maxNum {
			maxNum, maxGid = numShards, gid
		}
	}

	return maxGid
}
