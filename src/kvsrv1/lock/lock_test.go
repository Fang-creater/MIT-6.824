package lock

import (
	//	"log"
	"fmt"
	"strconv"
	"testing"
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

const (
	NACQUIRE = 10
	NCLNT    = 10
	NSEC     = 2
)

func TestReliableBasic(t *testing.T) {
	ts := kvsrv.MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin(fmt.Sprintf("Test: a single Acquire and Release"))

	name1 := kvtest.RandValue(12)
	ck := ts.MakeClerk()
	lk1 := MakeLock(ck, name1)
	lk1.Acquire()
	lk1.Release()
}

// check that locks with different names can
// be held at the same time.
func TestReliableNested(t *testing.T) {
	ts := kvsrv.MakeTestKV(t, true)
	defer ts.Cleanup()

	ts.Begin(fmt.Sprintf("Test: one client, two locks"))

	name1 := kvtest.RandValue(12)
	name2 := kvtest.RandValue(12)
	ck := ts.MakeClerk()
	lk1 := MakeLock(ck, name1)
	lk2 := MakeLock(ck, name2)

	lk1.Acquire()
	lk2.Acquire()
	lk2.Release()
	lk1.Release()

	lk2.Acquire()
	lk1.Acquire()
	lk1.Release()
	lk1.Acquire()
	lk2.Release()
	lk1.Release()
}

func oneClient(t *testing.T, me int, ck kvtest.IKVClerk,
	done chan struct{}, name1 string, name2 string) kvtest.ClntRes {
	lk1 := MakeLock(ck, name1)
	lk2 := MakeLock(ck, name2)

	lk1.Acquire()
	lk2.Acquire()
	lk1.Release()
	lk2.Release()

	ck.Put("l0", "", 0)
	for i := 1; true; i++ {
		select {
		case <-done:
			return kvtest.ClntRes{i, 0}
		default:
			lk1.Acquire()

			// log.Printf("%d: acquired lock", me)

			b := strconv.Itoa(me)
			val, ver, err := ck.Get("l0")
			if err == rpc.OK {
				if val != "" {
					t.Fatalf("%d: two clients acquired lock %v", me, val)
				}
			} else {
				t.Fatalf("%d: get failed %v", me, err)
			}

			err = ck.Put("l0", string(b), ver)
			if !(err == rpc.OK || err == rpc.ErrMaybe) {
				t.Fatalf("%d: put failed %v", me, err)
			}

			time.Sleep(10 * time.Millisecond)

			err = ck.Put("l0", "", ver+1)
			if !(err == rpc.OK || err == rpc.ErrMaybe) {
				t.Fatalf("%d: put failed %v", me, err)
			}

			// log.Printf("%d: release lock", me)

			lk1.Release()
		}
	}
	return kvtest.ClntRes{}
}

// Run test clients
func runClients(t *testing.T, nclnt int, reliable bool) {
	ts := kvsrv.MakeTestKV(t, reliable)
	defer ts.Cleanup()

	ts.Begin(fmt.Sprintf("Test: %d lock clients", nclnt))

	name1 := kvtest.RandValue(12)
	name2 := kvtest.RandValue(12)

	ts.SpawnClientsAndWait(nclnt, NSEC*time.Second, func(me int, myck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
		return oneClient(t, me, myck, done, name1, name2)
	})
}

func TestOneClientReliable(t *testing.T) {
	runClients(t, 1, true)
}

func TestManyClientsReliable(t *testing.T) {
	runClients(t, NCLNT, true)
}

func TestOneClientUnreliable(t *testing.T) {
	runClients(t, 1, false)
}

func TestManyClientsUnreliable(t *testing.T) {
	runClients(t, NCLNT, false)
}
