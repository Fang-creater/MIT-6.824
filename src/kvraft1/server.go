package kvraft

import (
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KvEntry struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	mu sync.Mutex
	kv map[string]KvEntry
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case *rpc.GetArgs:
		return kv.DoGet(args)
	case rpc.GetArgs:
		return kv.DoGet(&args)
	case *rpc.PutArgs:
		return kv.DoPut(args)
	case rpc.PutArgs:
		return kv.DoPut(&args)
	}
	return nil
}

func (kv *KVServer) DoGet(args *rpc.GetArgs) rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	e, ok := kv.kv[args.Key]
	if !ok {
		return rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}
	}
	return rpc.GetReply{Value: e.value, Version: e.version, Err: rpc.OK}
}

func (kv *KVServer) DoPut(args *rpc.PutArgs) rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	e, ok := kv.kv[args.Key]
	if !ok {
		if args.Version == 0 {
			kv.kv[args.Key] = KvEntry{value: args.Value, version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		return rpc.PutReply{Err: rpc.ErrNoKey}
	}

	if args.Version != e.version {
		return rpc.PutReply{Err: rpc.ErrVersion}
	}

	kv.kv[args.Key] = KvEntry{value: args.Value, version: e.version + 1}
	return rpc.PutReply{Err: rpc.OK}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, req := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	res, ok := req.(rpc.GetReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = res
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, req := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	res, ok := req.(rpc.PutReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = res
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}
	kv.kv = make(map[string]KvEntry)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
