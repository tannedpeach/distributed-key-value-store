package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"src/viewservice"
	"strconv"
	"sync"
	"syscall"
	"time"
)

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type Det struct {
	Key           string
	Value         string
	Oper          string
	PreviousValue string
}
type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}

	table    map[string]string //key-value pair database
	mu       sync.Mutex
	currView viewservice.View //current view
	reqTable map[int64]Det    //requests client sends
	isSync   bool             //flag to check if primary and backup are updated with eachother

	// Your declarations here.
}

func (pb *PBServer) UpdateBackupPut(args *AppendArgs, reply *AppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.currView.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	//check if we've arleady seen this
	prev, ok := pb.reqTable[args.ServerId]
	if ok && prev.Key == args.Key && prev.Value == args.InputValue && prev.Oper == args.Op {
		reply.Err = OK
		return nil
	}
	//update table
	if args.Op == "Put" {
		pb.table[args.Key] = args.Value
	} else if args.Op == "Append" {
		p := pb.table[args.Key]
		pb.table[args.Key] = p + args.Value
	}
	pb.reqTable[args.ServerId] = Det{args.Key, args.InputValue, args.Op, args.PreviousValue}
	reply.Err = OK

	return nil

}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.currView.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	// get previous value for PutHash
	var prevValue string
	if args.DoHash {
		prevValue = pb.table[args.Key]
	}

	// check if this request has already been seen
	prev, ok := pb.reqTable[args.ServerId]
	if ok && prev.Key == args.Key && prev.Value == args.Value && prev.Oper == "Put" {
		reply.Err = OK
		if args.DoHash {
			reply.PreviousValue = prev.PreviousValue
		}
		return nil
	}

	// update table
	var valueToStore string
	if args.DoHash {
		// For PutHash, store hash(old_value + new_value)
		h := hash(prevValue + args.Value)
		valueToStore = strconv.Itoa(int(h))
		pb.table[args.Key] = valueToStore
	} else {
		valueToStore = args.Value
		pb.table[args.Key] = valueToStore
	}

	pb.reqTable[args.ServerId] = Det{args.Key, args.Value, "Put", prevValue}
	reply.Err = OK
	reply.PreviousValue = prevValue

	// update backup server
	if pb.currView.Backup != "" {
		backupArgs := &AppendArgs{
			Key:           args.Key,
			Value:         valueToStore,
			InputValue:    args.Value,
			Op:            "Put",
			ServerId:      args.ServerId,
			PreviousValue: prevValue,
		}
		var backupReply AppendReply
		ok := call(pb.currView.Backup, "PBServer.UpdateBackupPut", backupArgs, &backupReply)
		if !ok || backupReply.Err != OK {
			pb.isSync = true
		}
	}

	return nil
}
func (pb *PBServer) GetToBackup(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//  Should only perform update on the backup server
	if pb.currView.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	//  Check if we have seen this request before, and if so, return previously calculated value
	prev, ok := pb.reqTable[args.ServerId]
	if ok && prev.Key == args.Key && prev.Oper == "Get" {
		reply.Value = prev.Value
		reply.Err = OK
		return nil
	}

	//  Update backup's database with Get operation
	val, ok := pb.table[args.Key]
	if ok {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	//  Update requests map
	pb.reqTable[args.ServerId] = Det{args.Key, reply.Value, "Get", ""}

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.currView.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	prev, ok := pb.reqTable[args.ServerId]
	if ok && prev.Key == args.Key && prev.Oper == "Get" {
		reply.Value = prev.Value
		reply.Err = OK
		return nil
	}

	val, ok := pb.table[args.Key]
	if ok {
		reply.Value = val
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	//  Update requests map
	pb.reqTable[args.ServerId] = Det{args.Key, reply.Value, "Get", ""}

	//  Propagate update to the backup server
	if pb.currView.Backup != "" {
		//  Save the value we're returning to the client
		valueToReturn := reply.Value
		//  Send an RPC request, use a separate reply for the backup
		var backupReply GetReply
		ok := call(pb.currView.Backup, "PBServer.GetToBackup", args, &backupReply)
		//  If something went wrong (e.g. server crashed and update didn't go through)
		if !ok || backupReply.Err == ErrWrongServer || backupReply.Value != valueToReturn {
			pb.isSync = true
		}
		//  Restore the reply value for the client
		reply.Value = valueToReturn
	}

	return nil
}

func (pb *PBServer) DatabaseToBackup(args *DatabaseToBackupArgs, reply *DatabaseToBackupReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//  Ping viewservice for current view
	nView, err := pb.vs.Ping(pb.currView.Viewnum)
	if err != nil {
		reply.Err = ErrWrongServer
		return nil
	}

	//  Should only perform update on the backup server
	if nView.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	//  Update backup's database
	pb.table = args.DB
	pb.reqTable = args.ReqDB

	reply.Err = OK
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	nView, _ := pb.vs.Ping(pb.currView.Viewnum)

	if nView.Primary == pb.me && pb.currView.Backup != nView.Backup && nView.Backup != "" {
		pb.isSync = true
	}
	if pb.isSync == true {
		pb.isSync = false
		args := DatabaseToBackupArgs{pb.table, pb.reqTable}
		var reply DatabaseToBackupReply

		ok := call(nView.Backup, "PBServer.DatabaseToBackup", args, &reply)
		if !ok || reply.Err != OK {
			pb.isSync = true
		}
	}
	pb.currView = nView
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.currView = viewservice.View{Primary: "", Backup: "", Viewnum: 0}
	pb.table = make(map[string]string)
	pb.reqTable = make(map[int64]Det)
	pb.isSync = false
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
