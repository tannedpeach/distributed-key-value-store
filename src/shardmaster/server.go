package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "sort"

type ShardMaster struct {
	mu sync.Mutex
	l net.Listener
	me int
	dead bool // for testing
	unreliable bool // for testing
	px *paxos.Paxos
  
	configs []Config // indexed by config num
	applied int      // highest Paxos seq
}

type Op struct {
	// Your data here.
	Operation string   // "Join", "Leave", "Move"
	GID       int64    // for Join/Leave
	Servers   []string // for Join
	Shard     int      // for Move
	Num       int      // for Query
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{
		Operation: "Join",
		GID:       args.GID,
		Servers:   args.Servers,
	}

	sm.processOp(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Operation: "Leave", GID: args.GID}

	sm.processOp(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Operation: "Move", Shard: args.Shard, GID: args.GID,}

	sm.processOp(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Operation: "Query", Num: args.Num}
	sm.processOp(op)
	if args.Num == -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[sm.applied]
	} else {
		reply.Config = sm.configs[args.Num]
	}

	return nil
}

func (sm *ShardMaster) processOp(op Op) {
	for {
		seq := sm.applied + 1
		sm.px.Start(seq, op)

		decidedOp := sm.waitForAgreement(seq)
		sm.applyOp(decidedOp)
		sm.px.Done(seq)
		sm.applied = seq

		if sm.sameOp(decidedOp, op) {
			return
		}
	}
}

func (sm *ShardMaster) waitForAgreement(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, val := sm.px.Status(seq)
		if decided {
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) sameOp(op1, op2 Op) bool {
	if op1.Operation != op2.Operation {
		return false
	}
	if op1.GID != op2.GID {
		return false
	}
	if op1.Shard != op2.Shard {
		return false
	}
	if len(op1.Servers) != len(op2.Servers) {
		return false
	}
	for i := range op1.Servers {
		if op1.Servers[i] != op2.Servers[i] {
			return false
		}
	}
	return true
}

func (sm *ShardMaster) applyOp(op Op) {
	lastConfig := sm.configs[len(sm.configs)-1]

	newConfig := Config{Num:lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: make(map[int64][]string)}

	// copy groups map
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	if op.Operation == "Join" {
		newConfig.Groups[op.GID] = op.Servers
		sm.rebalance(&newConfig)
	} else if op.Operation == "Leave" {
		delete(newConfig.Groups, op.GID)
		// Reassign shards from leaving group
		for i := range newConfig.Shards {
			if newConfig.Shards[i] == op.GID {
				newConfig.Shards[i] = 0
			}
		}
		sm.rebalance(&newConfig)
	} else if op.Operation == "Move" {
		newConfig.Shards[op.Shard] = op.GID
	}

	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) rebalance(config *Config) {
	numGroups := len(config.Groups)
	if numGroups == 0 {
		for i := range config.Shards {
			config.Shards[i] = 0
		}
		return
	}

	// count shards per group
	shardCount := map[int64]int{}
	for _, gid := range config.Shards {
		if gid != 0 {
			shardCount[gid]++
		}
	}

	target := NShards / numGroups

	// sort groups by GID
	gids := make([]int64, 0, len(config.Groups))
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Slice(gids, func(i, j int) bool { return gids[i] < gids[j] })

	// move shards from overloaded groups to underloaded groups
	for _, gid := range gids {
		for shardCount[gid] < target {
			// find shard from overloaded group or unassigned
			for i := range config.Shards {
				owner := config.Shards[i]
				if owner == 0 || shardCount[owner] > target ||
					(shardCount[owner] == target+1 && owner < gid) {
					config.Shards[i] = gid
					shardCount[owner]--
					shardCount[gid]++
					break
				}
			}
		}
	}

	// assign remaining unassigned shards
	for i := range config.Shards {
		if config.Shards[i] == 0 {
			// find group with fewest shards
			minGid := gids[0]
			minCount := shardCount[minGid]
			for _, gid := range gids {
				if shardCount[gid] < minCount {
					minGid = gid
					minCount = shardCount[gid]
				}
			}
			config.Shards[i] = minGid
			shardCount[minGid]++
		}
	}
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me
  
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
  
	rpcs := rpc.NewServer()
	rpcs.Register(sm)
  
	sm.px = paxos.Make(servers, me, rpcs)
  
	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me]);
	if e != nil {
	  log.Fatal("listen error: ", e);
	}
	sm.l = l
  
	// please do not change any of the following code,
	// or do anything to subvert it.
  
	go func() {
	  for sm.dead == false {
		conn, err := sm.l.Accept()
		if err == nil && sm.dead == false {
		  if sm.unreliable && (rand.Int63() % 1000) < 100 {
			// discard the request.
			conn.Close()
		  } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
		if err != nil && sm.dead == false {
		  fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
		  sm.Kill()
		}
	  }
	}()
  
	return sm
}
