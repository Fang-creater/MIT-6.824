package mr

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			return
		}

		task := reply.Task
		taskVersion := reply.Version

		switch task.Type {
		case MapTask:
			doMap(task, mapf)
			call("Coordinator.ReportDone", &ReportDoneArgs{
				TaskType: MapTask,
				TaskId:   task.TaskId,
				Version:  taskVersion,
			}, &ReportDoneReply{})
		case ReduceTask:
			doReduce(task, reducef)
			call("Coordinator.ReportDone", &ReportDoneArgs{
				TaskType: ReduceTask,
				TaskId:   task.TaskId,
				Version:  taskVersion,
			}, &ReportDoneReply{})
		case WaitTask:
			time.Sleep(500 * time.Millisecond)
		case ExitTask:
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

func doMap(task Task, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(task.File)
	if err != nil {
		log.Fatalf("doMap: can't read %v", task.File)
	}
	kvs := mapf(task.File, string(content))

	encoders := make([]*json.Encoder, task.NReduce)
	files := make([]*os.File, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		tmp, err := os.CreateTemp(".", "mr-map-tmp-*")
		if err != nil {
			log.Fatal("doMap: can't create temp file")
		}
		files[i] = tmp
		encoders[i] = json.NewEncoder(tmp)
	}

	for _, kv := range kvs {
		bucket := ihash(kv.Key) % task.NReduce
		encoders[bucket].Encode(&kv)
	}

	for i, f := range files {
		f.Close()
		oname := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		os.Rename(f.Name(), oname)
	}
}

func doReduce(task Task, reducef func(string, []string) string) {
	var kvs []KeyValue

	for i := 0; i < task.NMap; i++ {
		fname := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		f, err := os.Open(fname)
		if err != nil {
			log.Printf("doReduce: can't open %s: %v", fname, err)
			continue
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		f.Close()
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	tmp, err := os.CreateTemp(".", "mr-out-tmp-*")
	if err != nil {
		log.Fatal("doReduce: can't create temp file")
	}

	i := 0
	for i < len(kvs) {
		j := i
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i] = kvs[k].Value
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(tmp, "%v %v\n", kvs[i].Key, output)
		i = j
	}

	tmp.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tmp.Name(), oname)
}
