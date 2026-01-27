package rsm

import (
	"bytes"
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type Inc struct {
}

type IncRep struct {
	N int
}

type Null struct {
}

type NullRep struct {
}

type Dec struct {
}

func NewRSMSrv(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	s := newRSMSrv(ends, srv, persister, tester.MaxRaftState)
	return []any{s.rsm.rf, s}
}

type rsmSrv struct {
	me int

	mu      sync.Mutex
	rsm     *RSM
	counter int
}

func newRSMSrv(ends []*labrpc.ClientEnd, srv int, persister *tester.Persister, maxraftstate int) *rsmSrv {
	labgob.Register(Op{})
	labgob.Register(Inc{})
	labgob.Register(IncRep{})
	labgob.Register(Null{})
	labgob.Register(NullRep{})
	labgob.Register(Dec{})
	rs := &rsmSrv{me: srv}
	rs.rsm = MakeRSM(ends, srv, persister, maxraftstate, rs)
	return rs
}

func (rs *rsmSrv) GetCounter() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.counter
}

func (rs *rsmSrv) DoOp(req any) any {
	//log.Printf("%d: DoOp: %T(%v)", rs.me, req, req)
	switch req.(type) {
	case Inc:
		rs.mu.Lock()
		rs.counter += 1
		rs.mu.Unlock()
		return IncRep{rs.counter}
	case Null:
		return NullRep{}
	default:
		// wrong type! expecting an Inc.
		log.Fatalf("DoOp should execute only Inc and not %T", req)
	}
	return nil
}

func (rs *rsmSrv) Snapshot() []byte {
	//log.Printf("%d: snapshot", rs.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rs.counter)
	return w.Bytes()
}

func (rs *rsmSrv) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rs.counter) != nil {
		log.Fatalf("%v couldn't decode counter", rs.me)
	}
	//log.Printf("%d: restore %d", rs.me, rs.counter)
}

func (rs *rsmSrv) Submit(req any) (rpc.Err, any) {
	err, rep := rs.rsm.Submit(req)
	//log.Printf("Submit %d %v %v %T", rs.me, err, rep, rep)
	return err, rep
}

type SubmitArgs struct {
	Req any
}

type SubmitReply struct {
	Err rpc.Err
	Rep any
}

func (rs *rsmSrv) SubmitRPC(args *SubmitArgs, rep *SubmitReply) {
	rep.Err, rep.Rep = rs.Submit(args.Req)
}

type GetCounterArgs struct{}

type GetCounterReply struct {
	Count int
}

func (rs *rsmSrv) GetCounterRPC(args *GetCounterArgs, rep *GetCounterReply) {
	rep.Count = rs.GetCounter()
}
