package pbservice

import (
	"log"
	"viewservice"
)
import "net/rpc"
import "fmt"

// You'll probably need to uncomment these:
import "time"
import "crypto/rand"
import "math/big"

var gid = 0

type Clerk struct {
	vs            *viewservice.Clerk
	id            int
	primaryServer string
	// Your declarations here
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(vshost, me)
	ck.id = gid
	gid++
	ck.vs = viewservice.MakeClerk(vshost, me)

	return ck
}
func (ck *Clerk) PrimaryServer() bool {
	v, ok := ck.vs.Get()
	if !ok {
		log.Println("Client ", ck.id, " couldn't connect to viewservice")
	} else {
		ck.primaryServer = v.Primary
	}
	return ok
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
func (ck *Clerk) Get(key string) string {
	ck.PrimaryServer()
	args := &GetArgs{}
	args.Key = key
	var reply GetReply
	ok := false
	for true {
		log.Print("Client ", ck.id, "-> Primary "+
			viewservice.ConvertToString(ck.primaryServer)+": Get ", key)
		ok = call(ck.primaryServer, "PBServer.Get", args, &reply)
		if ok && reply.Err != ErrWrongServer {
			break
		}
		log.Print("Client ", ck.id, " req. didn't go through, ok = ", ok)
		time.Sleep(10 * viewservice.PingInterval)
		ck.PrimaryServer()
	}
	if reply.Err == ErrNoKey {
		reply.Value = ""
	}
	log.Print("Client ", ck.id, ": received ", reply.Value, " for key ", key)

	return reply.Value
}

// tell the primary to update key's value.
// must keep trying until it succeeds.
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {

	ck.PrimaryServer()
	args := &PutArgs{}
	args.Key = key
	args.Value = value
	var reply PutReply
	ok := false
	for true {
		log.Print("Client ", ck.id, "-> Primary "+viewservice.ConvertToString(ck.primaryServer)+": Put ", key, " ", value)
		ok = call(ck.primaryServer, "PBServer.Put", args, &reply)
		if ok && reply.Err == OK {
			break
		}
		log.Print("Server couldn't complete Put : try again")
		time.Sleep(3 * viewservice.PingInterval)
		ck.PrimaryServer()
	}
	log.Print("Client ", ck.id, ": Put ", key, " ", value, " completed")

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
