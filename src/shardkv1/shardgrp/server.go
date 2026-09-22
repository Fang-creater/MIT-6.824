package shardgrp

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

const (
	ShardNotOwned = iota // this group doesn't serve the shard
	ShardServing         // this group serves the shard
	ShardFrozen          // frozen: serving Gets, rejecting Puts
)

type kvEntry struct {
	Value   string
	Version rpc.Tversion
}

type shardRec struct {
	Data  map[string]kvEntry
	State int
	Num   shardcfg.Tnum // largest config Num seen for this shard
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	mu     sync.Mutex
	shards [shardcfg.NShards]shardRec
}

// initShards initializes shard records and gives the first group initial ownership.
func (kv *KVServer) initShards() {
	for s := range kv.shards {
		kv.shards[s].Data = make(map[string]kvEntry)
		kv.shards[s].Num = 0
		if kv.gid == shardcfg.Gid1 {
			// The first group starts out owning every shard.
			kv.shards[s].State = ShardServing
		} else {
			kv.shards[s].State = ShardNotOwned
		}
	}
}

// DoOp applies a Raft-committed request to the corresponding shard operation.
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch r := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(r)
	case *rpc.GetArgs:
		if r != nil {
			return kv.doGet(*r)
		}
	case rpc.PutArgs:
		return kv.doPut(r)
	case *rpc.PutArgs:
		if r != nil {
			return kv.doPut(*r)
		}
	case shardrpc.FreezeShardArgs:
		return kv.doFreeze(r)
	case *shardrpc.FreezeShardArgs:
		if r != nil {
			return kv.doFreeze(*r)
		}
	case shardrpc.InstallShardArgs:
		return kv.doInstall(r)
	case *shardrpc.InstallShardArgs:
		if r != nil {
			return kv.doInstall(*r)
		}
	case shardrpc.DeleteShardArgs:
		return kv.doDelete(r)
	case *shardrpc.DeleteShardArgs:
		if r != nil {
			return kv.doDelete(*r)
		}
	}
	return nil
}

// doGet reads a key after verifying that this group owns its shard.
func (kv *KVServer) doGet(a rpc.GetArgs) any {
	rep := rpc.GetReply{}
	s := shardcfg.Key2Shard(a.Key)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.shards[s].State == ShardNotOwned {
		rep.Err = rpc.ErrWrongGroup
		return rep
	}
	e, ok := kv.shards[s].Data[a.Key]
	if !ok {
		rep.Err = rpc.ErrNoKey
		return rep
	}
	rep.Value = e.Value
	rep.Version = e.Version
	rep.Err = rpc.OK
	return rep
}

// doPut conditionally updates a key when this group serves its shard.
func (kv *KVServer) doPut(a rpc.PutArgs) any {
	rep := rpc.PutReply{}
	s := shardcfg.Key2Shard(a.Key)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// A frozen (moving) or un-owned shard rejects writes.
	if kv.shards[s].State != ShardServing {
		rep.Err = rpc.ErrWrongGroup
		return rep
	}

	e, ok := kv.shards[s].Data[a.Key]
	if !ok {
		if a.Version != 0 {
			rep.Err = rpc.ErrNoKey
			return rep
		}
		kv.shards[s].Data[a.Key] = kvEntry{Value: a.Value, Version: 1}
		rep.Err = rpc.OK
		return rep
	}
	if a.Version != e.Version {
		rep.Err = rpc.ErrVersion
		return rep
	}
	kv.shards[s].Data[a.Key] = kvEntry{Value: a.Value, Version: e.Version + 1}
	rep.Err = rpc.OK
	return rep
}

// doFreeze stops writes to a shard and returns its encoded state for migration.
func (kv *KVServer) doFreeze(a shardrpc.FreezeShardArgs) any {
	rep := shardrpc.FreezeShardReply{Num: a.Num}
	s := a.Shard

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if a.Num < kv.shards[s].Num {
		// Stale request
		rep.Err = rpc.OK
		return rep
	}
	if a.Num > kv.shards[s].Num {
		kv.shards[s].Num = a.Num
	}

	switch kv.shards[s].State {
	case ShardServing:
		kv.shards[s].State = ShardFrozen
		rep.State = encodeShard(kv.shards[s].Data)
	case ShardFrozen:
		// duplicate freeze: hand back the same data
		rep.State = encodeShard(kv.shards[s].Data)
	default:
		// shard not exist
		rep.State = nil
	}
	rep.Err = rpc.OK
	return rep
}

// doInstall installs a migrated shard unless this server already has newer data.
func (kv *KVServer) doInstall(a shardrpc.InstallShardArgs) any {
	rep := shardrpc.InstallShardReply{}
	s := a.Shard

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if a.Num <= kv.shards[s].Num {
		// Already installed or newer
		rep.Err = rpc.OK
		return rep
	}
	kv.shards[s].Num = a.Num
	kv.shards[s].Data = decodeShard(a.State)
	kv.shards[s].State = ShardServing
	rep.Err = rpc.OK
	return rep
}

// doDelete drops a migrated shard unless the request is stale.
func (kv *KVServer) doDelete(a shardrpc.DeleteShardArgs) any {
	rep := shardrpc.DeleteShardReply{}
	s := a.Shard

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if a.Num < kv.shards[s].Num {
		// stale op, shard has been moved
		rep.Err = rpc.OK
		return rep
	}
	if a.Num > kv.shards[s].Num {
		kv.shards[s].Num = a.Num
	}
	kv.shards[s].Data = make(map[string]kvEntry)
	kv.shards[s].State = ShardNotOwned
	rep.Err = rpc.OK
	return rep
}

// Snapshot serializes all shard records for Raft snapshotting.
func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	return w.Bytes()
}

// Restore replaces shard records with those decoded from a Raft snapshot.
func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if data == nil || len(data) == 0 {
		return
	}
	var shards [shardcfg.NShards]shardRec
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&shards) != nil {
		return
	}
	kv.shards = shards
	for s := range kv.shards {
		if kv.shards[s].Data == nil {
			kv.shards[s].Data = make(map[string]kvEntry)
		}
	}
}

// Get submits a read through Raft so ownership is checked in log order.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	// Submit through Raft.
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err // ErrWrongLeader: clerk should try another server
		return
	}
	if r, ok := res.(rpc.GetReply); ok {
		*reply = r
		return
	}
	if r, ok := res.(*rpc.GetReply); ok && r != nil {
		*reply = *r
		return
	}
	reply.Err = rpc.ErrWrongGroup
}

// Put submits a conditional write through Raft and returns its applied result.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	// See Get: the ownership check must execute in log order, not before
	// submitting, so a restarted group can replay a pending InstallShard.
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if r, ok := res.(rpc.PutReply); ok {
		*reply = r
		return
	}
	if r, ok := res.(*rpc.PutReply); ok && r != nil {
		*reply = *r
		return
	}
	reply.Err = rpc.ErrWrongGroup
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if r, ok := res.(shardrpc.FreezeShardReply); ok {
		*reply = r
		return
	}
	/*
		if r, ok := res.(*shardrpc.FreezeShardReply); ok && r != nil {
			*reply = *r
			return
		}
	*/
	reply.Err = rpc.ErrWrongGroup
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if r, ok := res.(shardrpc.InstallShardReply); ok {
		*reply = r
		return
	}
	if r, ok := res.(*shardrpc.InstallShardReply); ok && r != nil {
		*reply = *r
		return
	}
	reply.Err = rpc.ErrWrongGroup
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if r, ok := res.(shardrpc.DeleteShardReply); ok {
		*reply = r
		return
	}
	if r, ok := res.(*shardrpc.DeleteShardReply); ok && r != nil {
		*reply = *r
		return
	}
	reply.Err = rpc.ErrWrongGroup
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(map[string]kvEntry{})

	kv := &KVServer{gid: gid, me: me}
	kv.initShards()
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}

// encodeShard serializes one shard's key/value records for transfer.
func encodeShard(m map[string]kvEntry) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(m) != nil {
		return nil
	}
	return w.Bytes()
}

// decodeShard deserializes transferred shard records into a usable map.
func decodeShard(b []byte) map[string]kvEntry {
	m := make(map[string]kvEntry)
	if len(b) == 0 {
		return m
	}
	r := bytes.NewBuffer(b)
	d := labgob.NewDecoder(r)
	if d.Decode(&m) != nil {
		return make(map[string]kvEntry)
	}
	if m == nil {
		return make(map[string]kvEntry)
	}
	return m
}
