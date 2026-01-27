package rsm

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/tester1"
)

const (
	NSRV = 3
	NSEC = 10

	Gid = tester.GRP0
)

type Test struct {
	*tester.Config
	mu           sync.Mutex
	t            *testing.T
	g            *tester.ServerGrp
	maxraftstate int
	srvs         []*rsmServer
	leader       int
}

type IRSMServer interface {
	Submit(req any) (rpc.Err, any)
	GetCounter() int
}

type rsmServer struct {
	mu  sync.Mutex
	rsm IRSMServer
}

func newRSMServer(rsm IRSMServer) *rsmServer {
	return &rsmServer{
		rsm: rsm,
	}
}

func (rs *rsmServer) getRSM() IRSMServer {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.rsm
}

func (rs *rsmServer) Kill() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.rsm = nil
}

func makeTest(t *testing.T, maxraftstate int) *Test {
	ts := &Test{
		t:            t,
		maxraftstate: maxraftstate,
		srvs:         make([]*rsmServer, NSRV),
	}
	args := []string{fmt.Sprintf("--max-raft-state=%d", maxraftstate)}
	ts.Config = tester.MakeConfig(t, NSRV, true, "rsm1d", args)
	ts.g = ts.Group(tester.GRP0)
	for i := 0; i < NSRV; i++ {
		ts.mksrv(i, ts.g.DaemonClnt(i))
	}
	return ts
}

func (ts *Test) cleanup() {
	ts.End()
	ts.Config.Cleanup()
	ts.CheckTimeout()
}

func (ts *Test) mksrv(srv int, dc *tester.DaemonClnt) {
	ts.mu.Lock()
	ts.srvs[srv] = newRSMServer(newRSMproxy(dc))
	ts.mu.Unlock()
}

func (ts *Test) kill(srvs []int) {
	ts.g.Kill(srvs)
	tester.AnnotateShutdown(srvs)
}

func (ts *Test) restart(srvs []int) {
	ts.g.StartSrvs(srvs)
	ts.Group(tester.GRP0).ConnectAll()
	tester.AnnotateRestart(srvs)
	for _, srv := range srvs {
		ts.mksrv(srv, ts.g.DaemonClnt(srv))
	}
}

func inPartition(s int, p []int) bool {
	if p == nil {
		return true
	}
	for _, i := range p {
		if s == i {
			return true
		}
	}
	return false
}

func (ts *Test) onePartition(p []int, req any) any {
	// try all the servers, maybe one is the leader but give up after NSEC
	t0 := time.Now()
	for time.Since(t0).Seconds() < NSEC {
		index := ts.getLeader()
		for range ts.srvs {
			if index >= NSRV {
				log.Fatalf("index %d", index)
			}
			if index != -1 && ts.g.IsConnected(index) {
				s := ts.srvs[index]
				rsm := s.getRSM()
				if rsm != nil && inPartition(index, p) {
					err, rep := rsm.Submit(req)
					if err == rpc.OK {
						ts.Config.OpInc()
						ts.mu.Lock()
						ts.leader = index
						ts.mu.Unlock()
						//log.Printf("leader = %d", ts.leader)
						return rep
					}
				}
			}
			index = (index + 1) % len(ts.srvs)
		}
		time.Sleep(50 * time.Millisecond)
		//log.Printf("try again: no leader")
	}
	return nil
}

func (ts *Test) oneInc() IncRep {
	rep := ts.onePartition(nil, Inc{})
	if rep == nil {
		return IncRep{}
	}
	return rep.(IncRep)
}

func (ts *Test) oneNull() NullRep {
	rep := ts.onePartition(nil, Null{})
	if rep == nil {
		return NullRep{}
	}
	return rep.(NullRep)
}

func (ts *Test) checkCounter(v int, nsrv int) {
	to := 10 * time.Millisecond
	n := 0
	for iters := 0; iters < 30; iters++ {
		n = ts.countValue(v)
		if n >= nsrv {
			text := fmt.Sprintf("all %v servers have counter value %v", nsrv, v)
			tester.AnnotateCheckerSuccess(text, text)
			return
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
	}
	err := fmt.Sprintf("checkCounter: only %d srvs have %v instead of %d", n, v, nsrv)
	tester.AnnotateCheckerFailure(err, err)
	ts.Fatalf(err)
}

func (ts *Test) countValue(v int) int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	i := 0
	for _, s := range ts.srvs {
		c := s.rsm.GetCounter()
		if c == v {
			i += 1
		}
	}
	return i
}

func (ts *Test) disconnectLeader() int {
	//log.Printf("disconnect %d", ts.leader)
	ts.g.DisconnectAll(ts.leader)
	return ts.leader
}

func (ts *Test) connect(i int) {
	//log.Printf("connect %d", i)
	ts.g.ConnectOne(i)
}

func (ts *Test) getLeader() int {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.leader
}
