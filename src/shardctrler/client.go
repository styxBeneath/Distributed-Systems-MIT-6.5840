package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ClientId  int64
	RequestId int64
	LeaderId  int
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
	// Your code here.
	ck.LeaderId = 0
	ck.ClientId = nrand()
	ck.RequestId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num:       num,
		ClientId:  ck.ClientId,
		RequestId: ck.RequestId,
	}

	for {
		var reply QueryReply
		server := ck.servers[ck.LeaderId]

		if !server.Call("ShardCtrler.Query", args, &reply) || reply.Err == TIMEOUT || reply.WrongLeader {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			continue
		}

		ck.RequestId++
		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers:   servers,
		ClientId:  ck.ClientId,
		RequestId: ck.RequestId,
	}

	for {
		var reply JoinReply
		server := ck.servers[ck.LeaderId]

		if !server.Call("ShardCtrler.Join", args, &reply) || reply.Err == TIMEOUT || reply.WrongLeader {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			continue
		}

		ck.RequestId++
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs:      gids,
		ClientId:  ck.ClientId,
		RequestId: ck.RequestId,
	}

	for {
		var reply LeaveReply
		server := ck.servers[ck.LeaderId]

		if !server.Call("ShardCtrler.Leave", args, &reply) || reply.Err == TIMEOUT || reply.WrongLeader {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			continue
		}

		ck.RequestId++
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:     shard,
		GID:       gid,
		ClientId:  ck.ClientId,
		RequestId: ck.RequestId,
	}

	for {
		var reply MoveReply
		server := ck.servers[ck.LeaderId]

		if !server.Call("ShardCtrler.Move", args, &reply) || reply.Err == TIMEOUT || reply.WrongLeader {
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			continue
		}

		ck.RequestId++
		return
	}
}
