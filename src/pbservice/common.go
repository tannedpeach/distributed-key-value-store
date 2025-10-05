package pbservice

import "hash/fnv"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key      string
	Value    string
	DoHash   bool
	Op       string
	ServerId int64 // For PutHash
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type AppendArgs struct {
	Key      string
	Value    string
	Op       string
	ServerId int64
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type AppendReply struct {
	Err Err
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key      string
	ServerId int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type DatabaseToBackupArgs struct {
	DB    map[string]string
	ReqDB map[int64]Det
}

type DatabaseToBackupReply struct {
	Err Err
}

// Your RPC definitions here.

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
