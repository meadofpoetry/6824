package kvraft

import (
	//"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Index int

type OpKind int

const (
	OpGet    OpKind = 0
	OpPut    OpKind = 1
	OpAppend OpKind = 2
)

type Op struct {
	Operation OpKind
	Key       string
	Value     string

	CId       ClientId
	RId       RequestId
}

type OpResult struct {
	Value     string
	Exists    bool

	CId       ClientId
	RId       RequestId
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// State
	state   map[string]string
	// Response subscriptions and deduplication
	// RequestId are sequential, thus
	// we can deduplicate fulfilled requests
	subs    map[Index]chan OpResult
	reqIds  map[ClientId]RequestId
}

func (kv *KVServer) apply(msg raft.ApplyMsg) {
	if !msg.CommandValid {
		return
	}

	index := msg.CommandIndex
	op := msg.Command.(Op)
	result := OpResult{CId: op.CId, RId: op.RId}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	if op.Operation == OpGet {
		v, exists := kv.state[op.Key]
		result.Value = v
		result.Exists = exists
	} else { // Put|Append
		lastId, exists := kv.reqIds[op.CId]
		if !exists || op.RId > lastId {
			kv.reqIds[op.CId] = op.RId
			if op.Operation == OpPut {
				kv.state[op.Key] = op.Value
			} else {
				kv.state[op.Key] = kv.state[op.Key] + op.Value
			}
		}
	}

	resultCh, stillWaiting := kv.subs[Index(index)]
	if !stillWaiting {
		return
	}
	resultCh <- result
}

func (kv *KVServer) append(op Op) (bool, *OpResult) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false, nil
	}

	kv.mu.Lock()
	resultCh, exists := kv.subs[Index(index)]
	if !exists {
		resultCh = make(chan OpResult)
		kv.subs[Index(index)] = resultCh
	}
	kv.mu.Unlock()
	
	defer func() {
		kv.mu.Lock()
		delete(kv.subs, Index(index))
		kv.mu.Unlock()
	}()
	
	select {
	case result := <- resultCh:
		if result.CId != op.CId || result.RId != op.RId {
			return false, nil
		}
		return true, &result
		
	case <- time.After(500 * time.Millisecond):
		return false, nil
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op {
		Operation: OpGet,
		Key: args.Key,
		CId: args.CId,
		RId: args.RId,
	}

	ok, result := kv.append(op)

	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	if !result.Exists {
		reply.Err = ErrNoKey
		return
	}

	reply.Err = OK
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	var operation OpKind

	if args.Op == "Put" {
		operation = OpPut
	} else {
		operation = OpAppend
	}
	
	op := Op {
		Operation: operation,
		Key: args.Key,
		Value: args.Value,	
		CId: args.CId,
		RId: args.RId,
	}

	ok, _ := kv.append(op)

	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
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
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) loop() {
	waitTime := 100 * time.Millisecond
	timer := time.NewTimer(waitTime)
	
	for atomic.LoadInt32(&kv.dead) != 1 {
		select {
		case msg := <-kv.applyCh:
			//fmt.Println("Message received: ", msg)
			kv.apply(msg)
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(waitTime)
			
		case <- timer.C:
			timer.Reset(waitTime)
		}
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.state = make(map[string]string)
	kv.subs = make(map[Index]chan OpResult)
	kv.reqIds = make(map[ClientId]RequestId)

	go kv.loop()

	return kv
}
