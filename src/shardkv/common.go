package shardkv

  import	"hash/fnv"
	import "shardmaster"


//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
  ClientId int64
	SeqNum   int64

}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
  ClientId int64
	SeqNum   int64
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferShardArgs struct {
	ConfigNum int
	Shard     int
}

type TransferShardReply struct {
	Err       Err
	ConfigNum int
	Shard     int
	Data      map[string]string
	Executed  map[int64]int64            // clientId -> highest seqNum
	Results   map[int64]map[int64]string // clientId -> seqNum -> result
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func Key2Shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}
