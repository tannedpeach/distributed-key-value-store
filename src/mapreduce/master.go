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
	reduceJobDone := make(chan int)
	mapJobs := make(chan int, mr.nMap)
	reduceJobs := make(chan int, mr.nReduce)
	for i := 0; i < mr.nMap; i++ {
		mapJobs <- i
	}
	for i := 0; i < mr.nReduce; i++ {
		reduceJobs <- i
	}

	go func() {
		for j := range mapJobs {
			mapWorker := DoJobArgs{mr.file, Map, j, mr.nReduce}
			go func() {
				address := <-mr.registerChannel
				mapResult := call(address, "Worker.DoJob", mapWorker, nil)
				if mapResult {
					mapJobDone <- 1
					mr.registerChannel <- address
				} else {
					mapJobs <- j
				}
			}()
		}
	}()

	for i := 0; i < mr.nMap; i++ {
		<-mapJobDone
	}

	go func() {
		for k := range reduceJobs {
			reduceWorker := DoJobArgs{mr.file, Reduce, k, mr.nMap}
			go func() {
				address := <-mr.registerChannel
				result := call(address, "Worker.DoJob", reduceWorker, nil)
				if result {
					reduceJobDone <- 1
					mr.registerChannel <- address
				} else {
					reduceJobs <- k
				}
			}()
		}
	}()

	for i := 0; i < mr.nReduce; i++ {
		<-reduceJobDone
	}

	return mr.KillWorkers()
}
