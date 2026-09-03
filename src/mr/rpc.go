package mr

// RPC definitions.
//
// remember to capitalize all names.

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type Task struct {
	Type    TaskType
	TaskId  int
	NReduce int
	NMap    int
	File    string
}

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	Task Task
}
