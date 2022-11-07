package kvraft

import (
	"crypto/rand"
	//"fmt"
	"math/big"
	"sync/atomic"

	"6.824/labrpc"
)


type Clerk struct {
	servers     []*labrpc.ClientEnd

	leader      int64

	clientId    ClientId
	nextRequest RequestId
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) nextRequestId() RequestId {
	return RequestId(atomic.AddUint64((*uint64)(&ck.nextRequest), 1))
}

func (ck *Clerk) shiftLeader() {
	nextLeader := (ck.leader + 1) % int64(len(ck.servers))
	atomic.StoreInt64(&ck.leader, nextLeader)
}

func (ck *Clerk) call(call string, args interface{}, reply Statuser) {
	for {
		leader := atomic.LoadInt64(&ck.leader)
		ok := ck.servers[leader].Call(call, args, reply)
		if ok && reply.Status() != ErrWrongLeader {
			break
		}
		reply.Clean()
		ck.shiftLeader()
	}
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.leader = 0

	ck.clientId = ClientId(nrand())
	ck.nextRequest = 1
	
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key, CId: ck.clientId, RId: ck.nextRequestId()}
	reply := GetReply{}
	
	ck.call("KVServer.Get", &args, &reply)

	//fmt.Printf("[%d] %s Get: %s -> %s\n", args.CId, reply.Err, args.Key, reply.Value)
	
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{Key: key, Value: value, Op: op, CId: ck.clientId, RId: ck.nextRequestId()}
	reply := PutAppendReply{}
	
	ck.call("KVServer.PutAppend", &args, &reply)

	//fmt.Printf("[%d] %s %s: %s -> %s\n", args.CId, reply.Err, args.Op, args.Key, args.Value)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
