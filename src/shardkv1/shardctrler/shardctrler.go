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

const (
	configKey        = "ctrl-config"
	pendingConfigKey = "ctrl-pending-config"
)

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
	current := sck.configAt(configKey)
	pending := sck.configAt(pendingConfigKey)
	if pending == nil || current == nil || pending.Num <= current.Num {
		return
	}
	sck.completeChange(current, pending)
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
	old := sck.Query()
	if old == nil {
		// No configuration.
		return
	}
	if new.Num <= old.Num {
		// Stale request.
		return
	}
	if !sck.claimPendingConfig(old, new) {
		time.Sleep(100 * time.Millisecond)
		return
	}
	sck.completeChange(old, new)
}

// completeChange moves shards from old to new, then makes new visible to clients.
func (sck *ShardCtrler) completeChange(old, new *shardcfg.ShardConfig) {
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
			if !sck.retry(func() bool {
				st, err := src.FreezeShard(s, num)
				if err == rpc.OK {
					state = st
					return true
				}
				return false
			}) {
				return
			}
		}

		// 2. install destination's shard.
		if dst != nil {
			if !sck.retry(func() bool {
				return dst.InstallShard(s, state, num) == rpc.OK
			}) {
				return
			}
		}

		// 3. delete the frozen shard.
		if src != nil {
			if !sck.retry(func() bool {
				return src.DeleteShard(s, num) == rpc.OK
			}) {
				return
			}
		}
	}

	// Clients can find the new owners only after all shard moves finish.
	sck.postConfig(new)
}

// claimPendingConfig records cfg if no controller has already claimed a newer
// configuration than current. It returns false when another controller won.
func (sck *ShardCtrler) claimPendingConfig(current, cfg *shardcfg.ShardConfig) bool {
	v := cfg.String()
	for {
		pending, ver, err := sck.configAtVersion(pendingConfigKey)
		if err == rpc.ErrNoKey {
			if sck.Put(pendingConfigKey, v, 0) == rpc.OK {
				return true
			}
		} else if err == rpc.OK {
			if pending.Num == cfg.Num && pending.String() == v {
				return true
			}
			if pending.Num > current.Num {
				return false
			}
			if sck.Put(pendingConfigKey, v, ver) == rpc.OK {
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// postConfig publishes cfg only when it is newer than the visible configuration.
func (sck *ShardCtrler) postConfig(cfg *shardcfg.ShardConfig) {
	v := cfg.String()
	for {
		current, ver, err := sck.Get(configKey)
		if err == rpc.ErrNoKey {
			if sck.Put(configKey, v, 0) == rpc.OK {
				return
			}
		} else if err == rpc.OK {
			if shardcfg.FromString(current).Num >= cfg.Num {
				return
			}
			if sck.Put(configKey, v, ver) == rpc.OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// configAt reads and decodes the configuration stored at key.
func (sck *ShardCtrler) configAt(key string) *shardcfg.ShardConfig {
	cfg, _, err := sck.configAtVersion(key)
	if err != rpc.OK {
		return nil
	}
	return cfg
}

// configAtVersion reads and decodes a configuration together with its KV version.
func (sck *ShardCtrler) configAtVersion(key string) (*shardcfg.ShardConfig, rpc.Tversion, rpc.Err) {
	v, ver, err := sck.Get(key)
	if err != rpc.OK {
		return nil, ver, err
	}
	return shardcfg.FromString(v), ver, rpc.OK
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	return sck.configAt(configKey)
}

// retry invokes f until it succeeds and reports whether it did so in time.
func (sck *ShardCtrler) retry(f func() bool) bool {
	for i := 0; i < 100; i++ {
		if f() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
