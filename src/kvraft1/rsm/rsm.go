package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

const submitTimeout = 500 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int // which rsm (peer)
	Id  int64
	Req any // the operation
}

type opResult struct {
	val any
	err rpc.Err
}

type waiter struct {
	id   int64
	term int
	ch   chan opResult // buffered
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	dead        bool            // set when applyCh is closed
	pending     map[int]*waiter // raft log index -> the Submit() waiting on it
	lastApplied int             // highest index we have handed to DoOp
	nextId      int64           // source of unique op ids
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg, 1000),
		sm:           sm,
		pending:      make(map[int]*waiter),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
		go rsm.reader()
	}
	return rsm
}

// reader reads committed operations from Raft's applyCh, hands each one
// to the state machine, and wakes up the Submit() call that is waiting
// for it (if this peer is the one that submitted it).
//
// This single goroutine is the only caller of DoOp(), so operations are
// applied in log order on every replica -- which is exactly why
// replicas stay identical.
func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.SnapshotValid {
			// Raft installed a snapshot: replace the state machine
			// state and forget anything we had applied before it.
			rsm.mu.Lock()
			rsm.sm.Restore(msg.Snapshot)
			rsm.lastApplied = msg.SnapshotIndex
			rsm.mu.Unlock()
			continue
		}
		if !msg.CommandValid {
			continue
		}

		op, ok := commandToOp(msg.Command)
		if !ok {
			continue
		}

		if msg.CommandIndex <= rsm.lastApplied {
			// Already applied (can happen after a snapshot restore).
			continue
		}

		// Call DoOp without holding rsm.mu: DoOp is the service's code
		// and must never block while we hold our lock.
		result := rsm.sm.DoOp(op.Req)

		rsm.mu.Lock()
		rsm.lastApplied = msg.CommandIndex
		if w, ok := rsm.pending[msg.CommandIndex]; ok {
			delete(rsm.pending, msg.CommandIndex)
			var res opResult
			if w.id == op.Id {
				// Our operation committed: hand back its result.
				res = opResult{val: result, err: rpc.OK}
			} else {
				// A different operation landed on our index, so our
				// operation was lost when leadership changed.
				res = opResult{err: rpc.ErrWrongLeader}
			}
			select {
			case w.ch <- res:
			default:
			}
		}
		rsm.mu.Unlock()

		// Part C: if rsm.maxraftstate != -1 and the raft state has
		// grown past it, call rsm.rf.Snapshot(index, rsm.sm.Snapshot()).
	}

	// applyCh was closed: we are being killed.  Wake everybody up so
	// that no Submit() goroutine is left blocked forever.
	rsm.mu.Lock()
	rsm.dead = true
	for idx, w := range rsm.pending {
		select {
		case w.ch <- opResult{err: rpc.ErrWrongLeader}:
		default:
		}
		delete(rsm.pending, idx)
	}
	rsm.mu.Unlock()
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// commandToOp decodes the command Raft handed us back into an Op.
func commandToOp(cmd any) (Op, bool) {
	switch v := cmd.(type) {
	case Op:
		return v, true
	case *Op:
		return *v, true
	}
	return Op{}, false
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	// return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	rsm.mu.Lock()
	if rsm.dead || rsm.rf == nil {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	rsm.nextId++
	op := Op{Me: rsm.me, Id: rsm.nextId, Req: req}

	// Start() appends the op to the log.  Registering the waiter in the
	// same critical section guarantees the reader cannot deliver the
	// result before we are listening for it.
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	w := &waiter{id: op.Id, term: term, ch: make(chan opResult, 1)}
	rsm.pending[index] = w
	rsm.mu.Unlock()

	for {
		select {
		case res := <-w.ch:
			return res.err, res.val

		case <-time.After(submitTimeout):
			// Safety net: if we lost leadership (or the raft term
			// moved on), our op will never commit -- tell the client
			// to look for a new leader.  If we are still the leader
			// in the same term, keep waiting.
			rsm.mu.Lock()
			if rsm.dead {
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			curTerm, isLeader := rsm.rf.GetState()
			lost := !isLeader || curTerm != term
			if lost && rsm.pending[index] == w {
				delete(rsm.pending, index)
			}
			rsm.mu.Unlock()
			if lost {
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
