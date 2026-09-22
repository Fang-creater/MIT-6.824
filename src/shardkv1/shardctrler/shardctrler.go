package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

const configKey = "ctrl-config"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	v := cfg.String()
	for {
		if sck.Put(configKey, v, 0) == rpc.OK {
			return
		} //When update Get() -> Put()
		if _, ver, err := sck.Get(configKey); err == rpc.OK {
			if sck.Put(configKey, v, ver) == rpc.OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	old := sck.Query()
	if old == nil {
		// No configuration.
		return
	}
	if new.Num <= old.Num {
		// Stale request.
		return
	}

	// A group that is leaving appears only in old.Groups;
	// A group that is joining appears only in new.Groups.
	// Look in both.
	clerks := make(map[tester.Tgid]*shardgrp.Clerk)
	clerkFor := func(gid tester.Tgid) *shardgrp.Clerk {
		if gid == 0 {
			return nil
		}
		if ck, ok := clerks[gid]; ok {
			return ck
		}
		srvs := old.Groups[gid]
		if len(srvs) == 0 {
			srvs = new.Groups[gid]
		}
		if len(srvs) == 0 {
			return nil
		}
		ck := shardgrp.MakeClerk(sck.clnt, srvs)
		clerks[gid] = ck
		return ck
	}

	num := new.Num

	for s := shardcfg.Tshid(0); s < shardcfg.NShards; s++ {
		from := old.Shards[s]
		to := new.Shards[s]
		if from == to {
			continue
		}
		src := clerkFor(from)
		dst := clerkFor(to)

		// 1. freeze source's shard.
		var state []byte
		if src != nil {
			sck.retry(func() bool {
				st, err := src.FreezeShard(s, num)
				if err == rpc.OK {
					state = st
					return true
				}
				return false
			})
		}

		// 2. install destination's shard.
		if dst != nil {
			sck.retry(func() bool {
				return dst.InstallShard(s, state, num) == rpc.OK
			})
		}

		// 3. delete the frozen shard.
		if src != nil {
			sck.retry(func() bool {
				return src.DeleteShard(s, num) == rpc.OK
			})
		}
	}

	// 4. clients now can find shards.
	sck.postConfig(new)
}

// postConfig publishes cfg with optimistic version checking until it succeeds.
func (sck *ShardCtrler) postConfig(cfg *shardcfg.ShardConfig) {
	v := cfg.String()
	for {
		_, ver, err := sck.Get(configKey)
		if err == rpc.ErrNoKey {
			if sck.Put(configKey, v, 0) == rpc.OK {
				return
			}
		} else if err == rpc.OK {
			if sck.Put(configKey, v, ver) == rpc.OK {
				return
			}
			// ErrVersion: someone else updated (re-read and retry).
			// Use new controller's version
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	v, _, err := sck.Get(configKey)
	if err != rpc.OK {
		// ErrNoKey: the controller has no configuration yet.
		return nil
	}
	return shardcfg.FromString(v)
}

// retry invokes f until it succeeds or the retry limit is reached.
func (sck *ShardCtrler) retry(f func() bool) {
	for i := 0; i < 100; i++ {
		if f() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	return
}
