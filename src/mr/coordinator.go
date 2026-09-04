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
	Version int
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
				reply.Version = c.mapTasks[i].Version
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
			c.reduceTasks[0].Status = 1
			c.reduceTasks[0].StartAt = time.Now()
			reply.Task = Task{
				Type:    ReduceTask,
				TaskId:  0,
				NReduce: c.nReduce,
				NMap:    c.nMap,
			}
			reply.Version = c.reduceTasks[0].Version
			return nil
		}

		//let worker wait
		reply.Task.Type = WaitTask
		return nil
	}
	//reduce phase
	if c.phase == 1 {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == 0 {
				c.reduceTasks[i].Status = 1
				c.reduceTasks[i].StartAt = time.Now()
				reply.Task = Task{
					Type:    ReduceTask,
					TaskId:  c.reduceTasks[i].Id,
					NReduce: c.nReduce,
					NMap:    c.nMap,
				}
				reply.Version = c.reduceTasks[i].Version
				return nil
			}
		}
		allDone := true
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status != 2 {
				allDone = false
				break
			}
		}
		if allDone {
			c.phase = 2
			reply.Task.Type = ExitTask
			return nil
		}

		reply.Task.Type = WaitTask
		return nil
	}
	reply.Task.Type = ExitTask
	return nil
}

func (c *Coordinator) ReportDone(args *ReportDoneArgs, reply *ReportDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		if c.mapTasks[args.TaskId].Version == args.Version && c.mapTasks[args.TaskId].Status == 1 {
			c.mapTasks[args.TaskId].Status = 2 // done
		}
	} else {
		if c.reduceTasks[args.TaskId].Version == args.Version && c.reduceTasks[args.TaskId].Status == 1 {
			c.reduceTasks[args.TaskId].Status = 2
		}
	}
	return nil
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
	c.mu.Lock()
	defer c.mu.Unlock()

	// Your code here.

	return c.phase == 2
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

	go c.timeoutWatcher()
	c.server(sockname)
	return &c
}

func (c *Coordinator) timeoutWatcher() {
	for {
		time.Sleep(1 * time.Second)
		c.mu.Lock()
		now := time.Now()

		if c.phase == 0 {
			for i := range c.mapTasks {
				if c.mapTasks[i].Status == 1 &&
					now.Sub(c.mapTasks[i].StartAt) > 10*time.Second {
					c.mapTasks[i].Status = 0 // return 0 status
					c.mapTasks[i].Version++
				}
			}
		} else if c.phase == 1 {
			for i := range c.reduceTasks {
				if c.reduceTasks[i].Status == 1 &&
					now.Sub(c.reduceTasks[i].StartAt) > 10*time.Second {
					c.reduceTasks[i].Status = 0
					c.reduceTasks[i].Version++
				}
			}
		}
		c.mu.Unlock()
	}
}
