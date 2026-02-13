package raft

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"

)

const (
	SnapShotInterval = 10
)

// The interface from a server (each one runs inside its own process)
// to the tester (which runs inside a separate process).
type Itester interface {
	CheckLogs(int, raftapi.ApplyMsg) (string, bool)
	IngestLog(int, map[int]any)
	ApplyErr(int, string)
}

type rfsrv struct {
	ts          Itester
	me          int
	lastApplied int
	persister   *tester.Persister

	mu   sync.Mutex
	raft raftapi.Raft
	log  map[int]any // for snapshots
}

func NewRfsrv(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	// tc is a client to talk to the tester
	ts := newTesterProxy(tc)
	s := newRfsrv(ts, ends, grp, srv, persister, tester.MaxRaftState > 0)
	return []any{s.raft, s}
}

// Each Raft server uses a raft library to Start a command and read
// committed commands from the library's apply channel.  The server
// can be run in two configurations: without and without snapshots.
func newRfsrv(ts Itester, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister, snapshot bool) *rfsrv {

	// grab a copy of the initial snapshot, to avoid
	// a possible race with raft.Make() and the
	// threads it starts, which might call persist().
	sn := persister.ReadSnapshot()

	s := &rfsrv{
		ts:        ts,
		me:        srv,
		log:       map[int]any{},
		persister: persister,
	}
	applyCh := make(chan raftapi.ApplyMsg)
	if !tester.UseRaftStateMachine {
		s.raft = Make(ends, srv, persister, applyCh)
	}
	if snapshot {
		if sn != nil && len(sn) > 0 {
			// mimic KV server and process snapshot now.
			// ideally Raft should send it up on applyCh...
			err := s.ingestSnap(sn, -1)
			if err != "" {
				ts.ApplyErr(srv, err)
				log.Fatalf("ingestSnap err %v", err)
			}
			ts.IngestLog(s.me, s.log)
		}
		go s.applierSnap(applyCh)
	} else {
		go s.applier(applyCh)
	}
	return s
}

func (rs *rfsrv) Start(command interface{}) (int, int, bool) {
	rf := rs.getraft()
	if rf == nil {
		return 0, 0, false
	}
	return rf.Start(command)
}

func (rs *rfsrv) GetState() (int, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.raft.GetState()
}

func (rs *rfsrv) getraft() raftapi.Raft {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.raft
}

// The Raft server sends each command into an ChecLogs RPC to the
// tester so that the tester knows what the server has received and
// can check against what it expected.
func (rs *rfsrv) applier(applyCh chan raftapi.ApplyMsg) {
	for m := range applyCh {
		if m.CommandValid == false {
			// ignore other types of ApplyMsg
		} else {
			err_msg, prevok := rs.ts.CheckLogs(rs.me, m)
			if m.CommandIndex > 1 && prevok == false {
				err_msg = fmt.Sprintf("server %v apply out of order %v", rs.me, m.CommandIndex)
			}
			if err_msg != "" {
				rs.ts.ApplyErr(rs.me, err_msg)
				// keep reading after error so that Raft doesn't block
				// holding locks...
			}
		}
	}
}

// Periodically snapshot raft state. When receiving an snapshot on the
// apply channel communicate in a IngestLog RPC the snapshot to
// tester.
func (rs *rfsrv) applierSnap(applyCh chan raftapi.ApplyMsg) {
	if rs.raft == nil {
		return // ???
	}

	for m := range applyCh {
		err_msg := ""
		if m.SnapshotValid {
			err_msg = rs.ingestSnap(m.Snapshot, m.SnapshotIndex)
			rs.ts.IngestLog(rs.me, rs.log)
		} else if m.CommandValid {
			if m.CommandIndex != rs.lastApplied+1 {
				err_msg = fmt.Sprintf("server %v apply out of order, expected index %v, got %v", rs.me, rs.lastApplied+1, m.CommandIndex)
			}

			if err_msg == "" {
				var prevok bool
				err_msg, prevok = rs.ts.CheckLogs(rs.me, m)
				if err_msg != "ErrRPC" && m.CommandIndex > 1 && prevok == false {
					err_msg = fmt.Sprintf("server %v apply out of order %v", rs.me, m.CommandIndex)
				}
			}

			rs.log[m.CommandIndex] = m.Command // for shapshots
			rs.lastApplied = m.CommandIndex

			if (m.CommandIndex+1)%SnapShotInterval == 0 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(m.CommandIndex)
				var xlog []any
				for j := 0; j <= m.CommandIndex; j++ {
					xlog = append(xlog, rs.log[j])
				}
				e.Encode(xlog)
				start := tester.GetAnnotatorTimestamp()
				rf := rs.getraft()
				rf.Snapshot(m.CommandIndex, w.Bytes())
				desp := fmt.Sprintf("snapshot created by %v", rs.me)
				details := fmt.Sprintf(
					"snapshot created by server %v after applying the command at index %v",
					rs.me,
					m.CommandIndex)
				tester.PostAnnotatorInfoInterval(start, desp, details)
			}
		} else {
			// Ignore other types of ApplyMsg.
		}
		if err_msg != "" {
			rs.ts.ApplyErr(rs.me, err_msg)
			// keep reading after error so that Raft doesn't block
			// holding locks...
		}
	}
}

// returns "" or error string
func (rs *rfsrv) ingestSnap(snapshot []byte, index int) string {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if snapshot == nil {
		return "nil snapshot"
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	var xlog []any
	if d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&xlog) != nil {
		return "failed to decode snapshot"
	}
	if index != -1 && index != lastIncludedIndex {
		err := fmt.Sprintf("server %v snapshot doesn't match m.SnapshotIndex", rs.me)
		return err
	}
	rs.log = map[int]any{}
	for j := 0; j < len(xlog); j++ {
		rs.log[j] = xlog[j]
	}
	rs.lastApplied = lastIncludedIndex
	return ""
}

type GetStateArgs struct{}

type GetStateReply struct {
	Term   int
	Leader bool
}

func (rs *rfsrv) GetStateRPC(args *GetStateArgs, rep *GetStateReply) {
	rep.Term, rep.Leader = rs.GetState()
}

type StartArgs struct {
	Command any
}

type StartReply struct {
	Index  int
	Term   int
	Leader bool
}

func (rs *rfsrv) StartRPC(args *StartArgs, rep *StartReply) {
	rep.Index, rep.Term, rep.Leader = rs.Start(args.Command)
}
