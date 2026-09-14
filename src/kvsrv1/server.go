package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KvEntry struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kv map[string]KvEntry // key -> (value, version)
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kv = make(map[string]KvEntry)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	e, ok := kv.kv[args.Key]
	if !ok {
		reply.Value = ""
		reply.Version = 0
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = e.value
	reply.Version = e.version
	reply.Err = rpc.OK
	DPrintf("Get(%q) -> (%q, %d)", args.Key, e.value, e.version)
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	e, ok := kv.kv[args.Key]
	if !ok {
		if args.Version == 0 {
			kv.kv[args.Key] = KvEntry{value: args.Value, version: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		DPrintf("Put(%q, %q, %d) on missing key -> %v", args.Key, args.Value, args.Version, reply.Err)
		return
	}

	if args.Version != e.version {
		reply.Err = rpc.ErrVersion
		DPrintf("Put(%q, %q, %d) version mismatch (have %d) -> ErrVersion", args.Key, args.Value, args.Version, e.version)
		return
	}

	kv.kv[args.Key] = KvEntry{value: args.Value, version: e.version + 1}
	reply.Err = rpc.OK
	DPrintf("Put(%q, %q, %d) -> version %d", args.Key, args.Value, args.Version, e.version+1)
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
