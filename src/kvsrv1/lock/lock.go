package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

const pollInterval = 10 * time.Millisecond

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name    string // value : lock's state
	id      string // unique id of each lock
	version rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.name = lockname
	for {
		lk.id = kvtest.RandValue(8)
		if lk.id != "" {
			break
		}
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		v, version, err := lk.ck.Get(lk.name)
		switch err {
		case rpc.ErrNoKey:
			// The key doesn't exist yet.
			// Create a new lock.
			v, version = "", 0
		case rpc.OK:
		default:
			// Unexpected; try again.
			continue
		}

		if v != "" && v != lk.id {
			// Someone else holds the lock.
			// Wait and look again.
			time.Sleep(pollInterval)
			continue
		}

		// The lock looks free (or we already hold it). Try to claim it
		// with a conditional put: it only succeeds if the version is
		// still the one we just read, so if two clients race, exactly
		// one of them gets OK.
		putErr := lk.ck.Put(lk.name, lk.id, version)
		switch putErr {
		case rpc.OK:
			// hold the lock
			lk.version = version + 1
			return
		case rpc.ErrMaybe:
			// Put not sure
			if cur, curVer, e := lk.ck.Get(lk.name); e == rpc.OK {
				if cur == lk.id { // hold the lock
					lk.version = curVer
					return
				}
			}
			continue
		case rpc.ErrVersion:
			// Another client slipped in
			continue
		default:
			// ErrNoKey,retry
			continue
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	v := lk.version
	for {
		putErr := lk.ck.Put(lk.name, "", v)
		switch putErr {
		case rpc.OK:
			return
		case rpc.ErrMaybe:
			// Hold not sure
			cur, curVer, e := lk.ck.Get(lk.name)
			if e != rpc.OK {
				// Lost the key
				return
			}
			if cur == "" {
				return // released
			}
			if cur != lk.id {
				// Someone else acquired it
				return
			}
			v = curVer // try again
			continue
		case rpc.ErrVersion:
			cur, curVer, e := lk.ck.Get(lk.name)
			if e != rpc.OK || cur != lk.id {
				return
			}
			v = curVer
			continue
		default:
			// ErrNoKey
			return
		}
	}
}
