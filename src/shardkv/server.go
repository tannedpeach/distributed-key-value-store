package shardkv


	import "encoding/gob"
	import "fmt"
	import "log"
	import"math/rand"
	import "net"
	import "net/rpc"
	import "os"
	import "paxos"
	import "shardmaster"
	import "strconv"
	import "sync"
	import "syscall"
	import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Operation string // "Get", "Put", "PutHash", "Reconfigure"
	Key       string
	Value     string
	ClientId  int64
	SeqNum    int64

	// For Reconfiguration
	Config        shardmaster.Config
	ShardData     map[int]map[string]string          // shard -> key -> value
	ShardExecuted map[int]map[int64]int64            // shard -> clientId -> highest seqNum
	ShardResults  map[int]map[int64]map[int64]string // shard -> clientId -> seqNum -> result

}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

  // Your definitions here.
  config   shardmaster.Config
	db       map[string]string
	seq      int                        // next sequence number to try
	executed map[int64]int64            // clientId -> highest executed seqNum
	results  map[int64]map[int64]string // clientId -> seqNum -> result
}

func (kv *ShardKV) waitForAgreement(seq int) Op {
	to := 10 * time.Millisecond
	for !kv.dead {
		decided, val := kv.px.Status(seq)
		if decided {
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	return Op{}
}

func (kv *ShardKV) ownsShard(shard int) bool {
	return kv.config.Shards[shard] == kv.gid
}

func (kv *ShardKV) applyOp(op Op) string {
	if op.Operation == "Reconfigure" {
		kv.reconfigure(op)
		return ""
	}

	// check if op has already been executed
	// check for duplicate
	lastSeq, exists := kv.executed[op.ClientId]
	if exists && lastSeq >= op.SeqNum {
		// Already executed this operation
		var result string
		if seqResults, ok := kv.results[op.ClientId]; ok {
			result = seqResults[op.SeqNum]
		}
		return result
	}

	shard := Key2Shard(op.Key)

	// Only execute operations for shards we own
	if !kv.ownsShard(shard) {
		// Don't execute or store client state for shards we don't own
		return "ErrWrongGroup"
	}

	var result string
	if op.Operation == "Get" {
		val, exists := kv.db[op.Key]
		if exists {
			result = val
		} else {
			result = ""
		}
	} else if op.Operation == "Put" {
		kv.db[op.Key] = op.Value
		result = ""
	} else if op.Operation == "PutHash" {
		oldVal := kv.db[op.Key]
		result = oldVal
		newVal := strconv.Itoa(int(hash(oldVal + op.Value)))
		kv.db[op.Key] = newVal
	}

	// Store client state only for shards we own
	kv.executed[op.ClientId] = op.SeqNum
	if kv.results[op.ClientId] == nil {
		kv.results[op.ClientId] = make(map[int64]string)
	}
	kv.results[op.ClientId][op.SeqNum] = result

	return result
}

func (kv *ShardKV) processLog(upToSeq int) {
	// process all decided entries from px.Min() to upToSeq
	// this allows us to skip holes in the log
	for i := kv.px.Min(); i <= upToSeq; i++ {
		decided, val := kv.px.Status(i)
		if decided {
			op := val.(Op)
			kv.applyOp(op)
		}
	}
}

// process any pending log entries
func (kv *ShardKV) catchUp() {
	// Process any decided entries that may have accumulated
	maxSeq := kv.px.Max()
	for i := kv.px.Min(); i <= maxSeq; i++ {
		decided, val := kv.px.Status(i)
		if decided {
			op := val.(Op)
			kv.applyOp(op)
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// catch up with any pending log entries (including reconfigurations)
	kv.catchUp()

	shard := Key2Shard(args.Key)
	if !kv.ownsShard(shard) {
		reply.Err = ErrWrongGroup
		return nil
	}

	op := Op{
		Operation: "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}

	for !kv.dead {
		seq := kv.seq
		kv.seq++

		kv.px.Start(seq, op)
		decidedOp := kv.waitForAgreement(seq)
		kv.processLog(seq)
		kv.px.Done(seq)

		// check if configuration changed and we no longer own the shard
		if !kv.ownsShard(shard) {
			reply.Err = ErrWrongGroup
			return nil
		}

		if decidedOp.ClientId == op.ClientId && decidedOp.SeqNum == op.SeqNum {
			var result string
			if seqResults, ok := kv.results[op.ClientId]; ok {
				result = seqResults[op.SeqNum]
			}
			reply.Err = OK
			reply.Value = result
			return nil
		}
	}
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	
	kv.catchUp()

	shard := Key2Shard(args.Key)
	if !kv.ownsShard(shard) {
		reply.Err = ErrWrongGroup
		return nil
	}

	opType := "Put"
	if args.DoHash {
		opType = "PutHash"
	}

	op := Op{
		Operation: opType,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		SeqNum:    args.SeqNum,
	}

	for !kv.dead {
		seq := kv.seq
		kv.seq++

		kv.px.Start(seq, op)
		decidedOp := kv.waitForAgreement(seq)
		kv.processLog(seq)
		kv.px.Done(seq)

	
		if !kv.ownsShard(shard) {
			reply.Err = ErrWrongGroup
			return nil
		}

		if decidedOp.ClientId == op.ClientId && decidedOp.SeqNum == op.SeqNum {
			var result string
			if seqResults, ok := kv.results[op.ClientId]; ok {
				result = seqResults[op.SeqNum]
			}
			reply.Err = OK
			reply.PreviousValue = result
			return nil
		}
	}
	return nil
}

func (kv *ShardKV) reconfigure(op Op) {
	if op.Config.Num <= kv.config.Num {
		return 
	}

	
	if op.Config.Num != kv.config.Num+1 {
		return
	}

	DPrintf("Server %v (GID %v): Reconfiguring from %v to %v", kv.me, kv.gid, kv.config.Num, op.Config.Num)

	oldConfig := kv.config
	newConfig := op.Config

	// merge shard data from other groups
	if op.ShardData != nil {
		for shard, data := range op.ShardData {
			if newConfig.Shards[shard] == kv.gid {
				DPrintf("Server %v (GID %v): Receiving shard %v with %v keys", kv.me, kv.gid, shard, len(data))
				for k, v := range data {
					kv.db[k] = v
				}
			}
		}
	}

	// merge client state for incoming shards
	if op.ShardExecuted != nil && op.ShardResults != nil {
		for shard := range op.ShardData {
			if newConfig.Shards[shard] == kv.gid {
				// Merge executed map
				if executed, ok := op.ShardExecuted[shard]; ok {
					for clientId, seqNum := range executed {
						if existingSeq, okExist := kv.executed[clientId]; !okExist || seqNum > existingSeq {
							kv.executed[clientId] = seqNum
						}
					}
				}
				// merge results map
				if results, ok := op.ShardResults[shard]; ok {
					for clientId, seqResults := range results {
						if kv.results[clientId] == nil {
							kv.results[clientId] = make(map[int64]string)
						}
						for seq, result := range seqResults {
							// Only copy if we don't have this result yet
							if _, exists := kv.results[clientId][seq]; !exists {
								kv.results[clientId][seq] = result
							}
						}
					}
				}
			}
		}
	}

	// remove data for shards we no longer own
	for k := range kv.db {
		shard := Key2Shard(k)
		if oldConfig.Shards[shard] == kv.gid && newConfig.Shards[shard] != kv.gid {
		}
	}

	kv.config = newConfig
}

func (kv *ShardKV) TransferShard(args *TransferShardArgs, reply *TransferShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	
	kv.catchUp()

	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard
	reply.Data = make(map[string]string)
	reply.Executed = make(map[int64]int64)
	reply.Results = make(map[int64]map[int64]string)

	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrWrongGroup
		return nil
	}


	for k, v := range kv.db {
		if Key2Shard(k) == args.Shard {
			reply.Data[k] = v
		}
	}

	// send complete client state for all clients
	for clientId, seqNum := range kv.executed {
		reply.Executed[clientId] = seqNum
		if results, ok := kv.results[clientId]; ok {
			reply.Results[clientId] = make(map[int64]string)
			for seq, result := range results {
				reply.Results[clientId][seq] = result
			}
		}
	}

	reply.Err = OK
	return nil
}

func (kv *ShardKV) tick() {
	kv.mu.Lock()
	if kv.dead {
		kv.mu.Unlock()
		return
	}

	kv.catchUp()

	currentConfigNum := kv.config.Num
	currentConfig := kv.config
	kv.mu.Unlock()

	newConfig := kv.sm.Query(-1)

	if newConfig.Num > currentConfigNum {
		nextConfigNum := currentConfigNum + 1
		configToApply := kv.sm.Query(nextConfigNum)

		DPrintf("Server %v (GID %v): Latest config is %v (currently at %v). Queried for config %v, got config %v with shards %v",
			kv.me, kv.gid, newConfig.Num, currentConfigNum, nextConfigNum, configToApply.Num, configToApply.Shards)

		if configToApply.Num == nextConfigNum {
			DPrintf("Server %v (GID %v): Processing config %v. Current shards: %v", kv.me, kv.gid, nextConfigNum, currentConfig.Shards)
			shardsToFetch := make(map[int]int64) // shard -> source gid
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if configToApply.Shards[shard] == kv.gid && currentConfig.Shards[shard] != kv.gid {
					oldGid := currentConfig.Shards[shard]
					DPrintf("Server %v (GID %v): Shard %v moving from GID %v to us in config %v", kv.me, kv.gid, shard, oldGid, nextConfigNum)
					if oldGid != 0 {
						shardsToFetch[shard] = oldGid
					}
				}
			}
			DPrintf("Server %v (GID %v): Need to fetch %v shards for config %v", kv.me, kv.gid, len(shardsToFetch), nextConfigNum)

			shardData := make(map[int]map[string]string)
			shardExecuted := make(map[int]map[int64]int64)
			shardResults := make(map[int]map[int64]map[int64]string)

			allShardsReceived := true
			for shard, gid := range shardsToFetch {
				servers, ok := currentConfig.Groups[gid]
				if !ok {
					allShardsReceived = false
					continue
				}

				args := &TransferShardArgs{
					ConfigNum: currentConfigNum,
					Shard:     shard,
				}

				shardReceived := false
		
				for _, srv := range servers {
					var reply TransferShardReply
					ok := call(srv, "ShardKV.TransferShard", args, &reply)
					if ok && reply.Err == OK {
						shardData[shard] = reply.Data
						shardExecuted[shard] = reply.Executed
						shardResults[shard] = reply.Results
						shardReceived = true
						break
					}
				}

				if !shardReceived {
					allShardsReceived = false
				}
			}

		
			if allShardsReceived {
				op := Op{
					Operation:     "Reconfigure",
					Config:        configToApply,
					ShardData:     shardData,
					ShardExecuted: shardExecuted,
					ShardResults:  shardResults,
				}

				kv.mu.Lock()
				seq := kv.seq
				kv.seq++
				kv.mu.Unlock()

				kv.px.Start(seq, op)
			}
		}
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})
	gob.Register(shardmaster.Config{})
	gob.Register(TransferShardArgs{})
	gob.Register(TransferShardReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	  // Your initialization code here.
  // Don't call Join().
	kv.config = shardmaster.Config{Num: 0}
	kv.db = make(map[string]string)
	kv.seq = 0
	kv.executed = make(map[int64]int64)
	kv.results = make(map[int64]map[int64]string)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
