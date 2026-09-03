package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mu      sync.Mutex
	nReduce int
	nMap    int
	files   []string

	mapTasks    []TaskState
	reduceTasks []TaskState

	phase int //map:0 reduce:1 done:2
}

type TaskState struct {
	Id     int
	File   string
	Status int
	//0:work is created no worker do
	//1:work is in progress
	//2:work is done
	StartAt time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//map status
	if c.phase == 0 {
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == 0 {
				c.mapTasks[i].Status = 1
				c.mapTasks[i].StartAt = time.Now()
				reply.Task = Task{
					Type:    MapTask,
					TaskId:  c.mapTasks[i].Id,
					NReduce: c.nReduce,
					NMap:    c.nMap,
					File:    c.mapTasks[i].File,
				}
				return nil
			}
		}

		allDone := true
		for i := range c.mapTasks {
			if c.mapTasks[i].Status != 2 {
				allDone = false
				break
			}
		}

		if allDone { //switch to reduce status
			c.phase = 1
			for i := 0; i < c.nReduce; i++ {
				c.reduceTasks = append(c.reduceTasks, TaskState{
					Id:     i,
					Status: 0,
				})
			}
			//allocate reduce task
			return c.RequestTask(args, reply)
		}

		//let worker wait
		reply.Task.Type = WaitTask
		return nil
	}
	//reduce phase
	if c.phase == 1 {
		
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:    len(files),
		nReduce: nReduce,
		files:   files,
		phase:   0,
	}

	// Your code here.
	for i, f := range files {
		c.mapTasks = append(c.mapTasks, TaskState{
			Id:     i,
			File:   f,
			Status: 0,
		})
	}

	c.server(sockname)
	return &c
}
