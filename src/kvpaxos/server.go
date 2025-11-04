package kvpaxos


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
import "strconv"
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
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string // "Get", "Put", "PutHash"
	Key       string
	Value     string
	ClientId  int64
	SeqNum    int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	db       map[string]string  // key-value database
	seq      int  // next sequence number to try
	executed map[int64]int64  // clientId -> highest executed seqNum 
	results  map[int64]string  // clientId -> result of last operation
}

// wait for paxos instances to reach agreement on a sequence number
func (kv *KVPaxos) waitForAgreement(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, val := kv.px.Status(seq)
		if decided {
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

// apply an operation to the database 
func (kv *KVPaxos) applyOp(op Op) string {
	// check if op has already been executed
	lastSeq, exists := kv.executed[op.ClientId]
  if exists && lastSeq >= op.SeqNum {
		result, ok := kv.results[op.ClientId]
    if ok {
			return result
		}
		return ""
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

	kv.executed[op.ClientId] = op.SeqNum
	kv.results[op.ClientId] = result

	return result
}

// process log entries up to and including seq
func (kv *KVPaxos) processLog(upToSeq int) {
	for i := kv.px.Min(); i <= upToSeq; i++ {
		decided, val := kv.px.Status(i)
		if decided {
			op := val.(Op)
			kv.applyOp(op)
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{Operation: "Get", Key: args.Key, ClientId: args.ClientId, SeqNum: args.SeqNum}

	// add to log
	for {
		seq := kv.seq
		kv.seq++

		kv.px.Start(seq, op)
		decidedOp := kv.waitForAgreement(seq)
		kv.processLog(seq)
		kv.px.Done(seq)

		// if our operation was chosen
		if decidedOp.ClientId == op.ClientId && decidedOp.SeqNum == op.SeqNum {
			result := kv.applyOp(op)
			reply.Err = OK
			reply.Value = result
			return nil
		}
		// otherwise try again
	}
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	opType := "Put"
	if args.DoHash {
		opType = "PutHash"
	}

	op := Op{Operation: opType, Key: args.Key,Value:  args.Value, ClientId: args.ClientId, SeqNum: args.SeqNum,}

	for {
		seq := kv.seq
		kv.seq++

		kv.px.Start(seq, op)
		decidedOp := kv.waitForAgreement(seq)
		kv.processLog(seq)
		kv.px.Done(seq)

		if decidedOp.ClientId == op.ClientId && decidedOp.SeqNum == op.SeqNum {
			result := kv.applyOp(op)
			reply.Err = OK
			reply.PreviousValue = result
			return nil
		}
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.seq = 0
	kv.executed = make(map[int64]int64)
	kv.results = make(map[int64]string)

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}