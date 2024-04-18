package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	RequestId int64
	OpType    string
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// 3A
	stateMachine   *MemoryKV
	notifyChanMap  map[int]chan *CommonReply
	lastRequestMap map[int64]ReplyContext
	// 3B
	persist     *raft.Persister
	lastApplied int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    "Get",
		Key:       args.Key,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	notifyChan := make(chan *CommonReply, 1)

	kv.mu.Lock()
	kv.notifyChanMap[index] = notifyChan
	kv.mu.Unlock()

	select {
	case ret := <-notifyChan:
		reply.Value, reply.Err = ret.Value, ret.Error
		if !kv.checkLeader(term) {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = TimeOut
	}

	kv.mu.Lock()
	delete(kv.notifyChanMap, index)
	kv.mu.Unlock()
}

func (kv *KVServer) checkLeader(expectedTerm int) bool {
	currentTerm, isLeader := kv.rf.GetState()
	return isLeader && currentTerm == expectedTerm
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	if kv.isOldRequest(args.ClientId, args.RequestId) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	op := Op{
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	notifyChan := make(chan *CommonReply, 1)

	kv.mu.Lock()
	kv.notifyChanMap[index] = notifyChan
	kv.mu.Unlock()

	select {
	case ret := <-notifyChan:
		reply.Err = ret.Error
		if !kv.checkLeader(term) {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(ExecuteTimeout):
		reply.Err = TimeOut
	}

	kv.mu.Lock()
	delete(kv.notifyChanMap, index)
	kv.mu.Unlock()
}

func (kv *KVServer) getSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.stateMachine)
	if err != nil {
		return nil
	}
	err = e.Encode(kv.lastRequestMap)
	if err != nil {
		return nil
	}
	kvstate := w.Bytes()
	return kvstate
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var stateMachine MemoryKV
	var lastRequestMap map[int64]ReplyContext

	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastRequestMap) != nil {
		panic("decode persist state fail")
	}

	kv.stateMachine = &stateMachine
	kv.lastRequestMap = lastRequestMap
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.stateMachine = NewMemoryKV()
	kv.notifyChanMap = make(map[int]chan *CommonReply)
	kv.lastRequestMap = make(map[int64]ReplyContext)
	kv.persist = persister

	kv.restoreSnapshot(persister.ReadSnapshot())

	go kv.applier()
	return kv
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case applyMsg := <-kv.applyCh:
			kv.mu.Lock()
			if applyMsg.CommandValid {
				if applyMsg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				reply := kv.apply(applyMsg.Command)
				currentTerm, isLeader := kv.rf.GetState()
				if isLeader && applyMsg.CommandTerm == currentTerm {
					kv.notify(applyMsg.CommandIndex, reply)
				}

				if kv.maxraftstate != -1 && kv.persist.RaftStateSize() > kv.maxraftstate {
					kv.rf.Snapshot(applyMsg.CommandIndex, kv.getSnapshot())
				}

				kv.lastApplied = applyMsg.CommandIndex
			} else if applyMsg.SnapshotValid {
				if applyMsg.SnapshotIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				kv.restoreSnapshot(applyMsg.Snapshot)
				kv.lastApplied = applyMsg.SnapshotIndex

			} else {
				panic("1111")
			}

			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) notify(index int, reply *CommonReply) {
	if notifyCh, ok := kv.notifyChanMap[index]; ok {
		notifyCh <- reply
	}
}

func (kv *KVServer) apply(cmd interface{}) *CommonReply {
	reply := &CommonReply{}
	op := cmd.(Op)
	if op.OpType != "Get" && kv.isOldRequest(op.ClientId, op.RequestId) {
		reply.Error = OK
	} else {
		reply = kv.applyLogToSM(&op)
		if op.OpType != "Get" {
			kv.updateLastRequest(&op, reply)
		}
	}

	reply.Error = OK
	return reply
}

func (kv *KVServer) applyLogToSM(op *Op) *CommonReply {
	var reply = &CommonReply{}

	switch op.OpType {
	case "Get":
		reply.Value = kv.stateMachine.get(op.Key)
	case "Put":
		kv.stateMachine.put(op.Key, op.Value)
	case "Append":
		kv.stateMachine.appendVal(op.Key, op.Value)
	}

	reply.Error = OK

	return reply
}

func (kv *KVServer) isOldRequest(clientId, requestId int64) bool {
	cxt, ok := kv.lastRequestMap[clientId]
	return ok && requestId <= cxt.LastRequestId
}

func (kv *KVServer) updateLastRequest(op *Op, reply *CommonReply) {

	ctx := ReplyContext{
		LastRequestId: op.RequestId,
		Reply:         *reply,
	}

	lastCtx, ok := kv.lastRequestMap[op.ClientId]
	if (ok && lastCtx.LastRequestId < op.RequestId) || !ok {
		kv.lastRequestMap[op.ClientId] = ctx
	}
}
