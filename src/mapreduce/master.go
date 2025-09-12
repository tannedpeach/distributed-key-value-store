package mapreduce

import (
	"container/list"
)
import "fmt"

type WorkerInfo struct {
	address string
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	mapJobDone := make(chan int)
	for i := 0; i < mr.nMap; i++ {
		mapWorker := DoJobArgs{mr.file, Map, i, mr.nReduce}
		go func() {
			address := <-mr.registerChannel
			mapResult := call(address, "Worker.DoJob", mapWorker, nil)
			mapJobDone <- 1
			mr.registerChannel <- address
			fmt.Println("mapres", mapResult)
		}()
	}
	for i := 0; i < mr.nMap; i++ {
		<-mapJobDone
	}

	reduceJobDone := make(chan int)
	for i := 0; i < mr.nReduce; i++ {
		reduceWorker := DoJobArgs{mr.file, Reduce, i, mr.nMap}
		go func() {
			address := <-mr.registerChannel
			result := call(address, "Worker.DoJob", reduceWorker, nil)
			reduceJobDone <- 1
			mr.registerChannel <- address
			fmt.Println("reduceres", result)
		}()
	}
	for i := 0; i < mr.nReduce; i++ {
		<-reduceJobDone
	}

	return mr.KillWorkers()
}
