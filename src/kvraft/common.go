package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)


type ClientId  uint64
type RequestId uint64

type Err string

type Statuser interface {
	Status() Err
	Clean()
}

type Requester interface {
	Id() (ClientId, RequestId)
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CId   ClientId
	RId   RequestId
}

type PutAppendReply struct {
	Err Err
}

func (r *PutAppendArgs) Id() (ClientId, RequestId) {
	return r.CId, r.RId
}

func (r *PutAppendReply) Status() Err {
	return r.Err
}

func (r *PutAppendReply) Clean() {
	r.Err = ""
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CId ClientId
	RId RequestId
}

type GetReply struct {
	Err   Err
	Value string
}

func (r *GetArgs) Id() (ClientId, RequestId) {
	return r.CId, r.RId
}

func (r *GetReply) Status() Err {
	return r.Err
}

func (r *GetReply) Clean() {
	r.Err = ""
}
