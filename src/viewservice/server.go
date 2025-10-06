package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	pingMap    map[string]time.Time
	currView   View
	primaryAck bool
	idle       string
}

func ServerDead(server string, vs *ViewServer) bool {
	switch server {
	case "primary":
		timeElapsed := time.Since(vs.pingMap[vs.currView.Primary])
		return timeElapsed > DeadPings*PingInterval
	case "backup":
		timeElapsed := time.Since(vs.pingMap[vs.currView.Backup])
		return timeElapsed > DeadPings*PingInterval
	case "idle":
		timeElapsed := time.Since(vs.pingMap[vs.idle])
		return timeElapsed > DeadPings*PingInterval
	}
	return false
}
func (vs *ViewServer) ChangeView(primary string, backup string) {
	vs.currView = View{vs.currView.Viewnum + 1, primary, backup}
	vs.primaryAck = false
	vs.idle = ""
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	client := args.Me
	viewNum := args.Viewnum

	vs.pingMap[client] = time.Now()

	switch client {
	case vs.currView.Primary:
		if viewNum == 0 && vs.primaryAck == true {
			vs.ChangeView(vs.currView.Backup, vs.idle)
		} else if vs.currView.Viewnum == viewNum {
			vs.primaryAck = true
		}
	case vs.currView.Backup:
		if viewNum == 0 && vs.primaryAck == true {
			vs.ChangeView(vs.currView.Primary, vs.idle)
		}
	default:
		//log.Printf("client %s, viewNum %d\n", client, vs.view.Viewnum)
		if vs.currView.Viewnum == 0 {
			vs.ChangeView(client, "")
		} else {
			vs.idle = client
		}
		//log.Printf("client %s, viewNum %d\n", client, vs.view.Viewnum)
	}
	//else {
	//	// an idle server comes in. put into the waitlist
	//	vs.assignRole(args.Me)
	//}

	//if !vs.initDone {
	//	vs.currView.Primary = args.Me
	//	vs.initDone = true
	//	vs.UpdateView(vs.currView)
	//} else if vs.currView.Backup == "" && vs.primaryAck && args.Me != vs.currView.Primary {
	//	vs.currView.Backup = args.Me
	//	vs.UpdateView(vs.currView)
	//} else if args.Me == vs.currView.Primary && vs.currView.Viewnum == args.Viewnum {
	//	vs.primaryAck = true
	//} else if args.Viewnum == 0 && args.Me == vs.currView.Primary && vs.primaryAck {
	//	vs.currView.Primary = vs.currView.Backup
	//	vs.currView.Backup = ""
	//	vs.UpdateView(vs.currView)
	//} else if args.Viewnum == 0 && args.Me == vs.currView.Backup && vs.primaryAck {
	//	vs.currView.Backup = ""
	//	vs.UpdateView(vs.currView)
	//}
	reply.View = vs.currView
	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.currView
	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if ServerDead("primary", vs) {
		if vs.primaryAck == true {
			vs.ChangeView(vs.currView.Backup, vs.idle)
		}
	}

	if ServerDead("backup", vs) {
		if vs.primaryAck && vs.idle != "" {
			vs.ChangeView(vs.currView.Primary, vs.idle)
		}
	}
	if ServerDead("idle", vs) {
		vs.idle = ""
	} else {
		if vs.primaryAck == true && vs.currView.Backup == "" && vs.idle != "" {
			vs.ChangeView(vs.currView.Primary, vs.idle)
		}
	}

	//
	//if vs.currView.Primary != "" && ServerDead("primary", vs) && vs.primaryAck {
	//	vs.currView.Primary = vs.currView.Backup
	//	vs.currView.Backup = ""
	//	vs.primaryAck = false
	//	vs.currView.Viewnum++
	//}
	//if vs.currView.Backup != "" && ServerDead("backup", vs) && vs.primaryAck {
	//	vs.currView.Backup = ""
	//	vs.primaryAck = false
	//	vs.currView.Viewnum++
	//}
}

// tell the server to shut itself down.
// for testing.
// please don't change this function.
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.pingMap = make(map[string]time.Time)
	vs.currView = View{Primary: "", Backup: "", Viewnum: 0}
	vs.primaryAck = false
	vs.idle = ""

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
