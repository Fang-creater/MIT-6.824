package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"sync"
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
	mu sync.Mutex
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

// GetClerk returns the cached clerk for gid, if one has been created.
func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// clerkFor returns the cached group clerk or creates one from servers.
func (ck *Clerk) clerkFor(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if c, ok := ck.rcks[gid]; ok && c != nil {
		return c
	}
	c := shardgrp.MakeClerk(ck.clnt, servers)
	ck.rcks[gid] = c
	return c
}

// groupFor resolves key's current shard group and returns its clerk.
func (ck *Clerk) groupFor(key string) (tester.Tgid, *shardgrp.Clerk, bool) {
	cfg := ck.sck.Query()
	if cfg == nil {
		return 0, nil, false
	}
	sh := shardcfg.Key2Shard(key)
	gid, srvs, ok := cfg.GidServers(sh)
	if !ok || gid == 0 || len(srvs) == 0 {
		return 0, nil, false
	}
	return gid, ck.clerkFor(gid, srvs), true
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	for {
		_, gck, ok := ck.groupFor(key)
		if !ok {
			// no configuration
			time.Sleep(100 * time.Millisecond)
			continue
		}
		v, ver, err := gck.GetOnce(key)
		// traverse each copy once, no waiting  indefinitely
		if err == rpc.ErrWrongGroup || err == rpc.ErrWrongLeader {
			// the configuration moved on: re-read it and retry
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return v, ver, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	sent := false

	for {
		_, gck, ok := ck.groupFor(key)
		if !ok {
			// no configuration
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err := gck.PutOnce(key, value, version)
		// traverse each copy once, no waiting indefinitely
		if err == rpc.ErrWrongGroup || err == rpc.ErrWrongLeader {
			// shard moved: re-read and retry
			sent = true
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err == rpc.ErrVersion && sent {
			// Put may have been executed
			return rpc.ErrMaybe
		}
		return err
	}
}
