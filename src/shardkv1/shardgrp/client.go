package shardgrp

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
	mu sync.Mutex // guards leader
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
}

func (ck *Clerk) setLeader(i int) {
	ck.mu.Lock()
	ck.leader = i
	ck.mu.Unlock()
}

func (ck *Clerk) len() int {
	return len(ck.servers)
}

func (ck *Clerk) srv(i int) string {
	return ck.servers[i%len(ck.servers)]
}

// GetOnce tries every replica once.  Unlike Get, it never waits for a
// permanently unavailable group.  The sharded client uses this while a
// configuration change may have removed the group altogether.
func (ck *Clerk) GetOnce(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	start := ck.Leader()
	for i := 0; i < ck.len(); i++ {
		idx := (start + i) % ck.len()
		reply := rpc.GetReply{}
		if !ck.Call(ck.srv(idx), "KVServer.Get", &args, &reply) {
			continue
		}
		if reply.Err == rpc.ErrWrongLeader {
			continue
		}
		ck.setLeader(idx)
		return reply.Value, reply.Version, reply.Err
	}
	return "", 0, rpc.ErrWrongLeader
}

// PutOnce tries every replica once.  A caller that gets ErrWrongLeader must
// re-check the shard configuration and retry the same write.
func (ck *Clerk) PutOnce(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	start := ck.Leader()
	for i := 0; i < ck.len(); i++ {
		idx := (start + i) % ck.len()
		reply := rpc.PutReply{}
		if !ck.Call(ck.srv(idx), "KVServer.Put", &args, &reply) {
			continue
		}
		if reply.Err == rpc.ErrWrongLeader {
			continue
		}
		ck.setLeader(idx)
		return reply.Err
	}
	return rpc.ErrWrongLeader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := rpc.GetArgs{Key: key}
	for {
		start := ck.Leader()
		for i := 0; i < ck.len(); i++ {
			idx := (start + i) % ck.len()
			reply := rpc.GetReply{}
			ok := ck.Call(ck.srv(idx), "KVServer.Get", &args, &reply)
			if !ok {
				continue // try the next server
			}
			if reply.Err == rpc.ErrWrongLeader {
				continue
			}
			ck.setLeader(idx)
			return reply.Value, reply.Version, reply.Err
		}
		time.Sleep(100 * time.Millisecond) // wait leader election
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{Key: key, Value: value, Version: version}

	// when re-sent, not sure if previous sent is committed
	sent := false

	for {
		start := ck.Leader()
		for i := 0; i < ck.len(); i++ {
			idx := (start + i) % ck.len()
			reply := rpc.PutReply{}
			ok := ck.Call(ck.srv(idx), "KVServer.Put", &args, &reply)
			if !ok {
				sent = true // the request may have been executed
				continue
			}
			if reply.Err == rpc.ErrWrongLeader {
				// An earlier attempt may have been committed, not sure
				sent = true
				continue
			}
			ck.setLeader(idx)
			if reply.Err == rpc.ErrVersion && sent {
				return rpc.ErrMaybe
			}
			return reply.Err
		}
		sent = true
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	for {
		start := ck.Leader()
		for i := 0; i < ck.len(); i++ {
			idx := (start + i) % ck.len()
			reply := shardrpc.FreezeShardReply{}
			ok := ck.Call(ck.srv(idx), "KVServer.FreezeShard", &args, &reply)
			if !ok {
				continue
			}
			if reply.Err == rpc.ErrWrongLeader {
				continue
			}
			ck.setLeader(idx)
			return reply.State, reply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	for {
		start := ck.Leader()
		for i := 0; i < ck.len(); i++ {
			idx := (start + i) % ck.len()
			reply := shardrpc.InstallShardReply{}
			ok := ck.Call(ck.srv(idx), "KVServer.InstallShard", &args, &reply)
			if !ok {
				continue
			}
			if reply.Err == rpc.ErrWrongLeader {
				continue
			}
			ck.setLeader(idx)
			return reply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	for {
		start := ck.Leader()
		for i := 0; i < ck.len(); i++ {
			idx := (start + i) % ck.len()
			reply := shardrpc.DeleteShardReply{}
			ok := ck.Call(ck.srv(idx), "KVServer.DeleteShard", &args, &reply)
			if !ok {
				continue
			}
			if reply.Err == rpc.ErrWrongLeader {
				continue
			}
			ck.setLeader(idx)
			return reply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
}
