package rsm

import (
	"log"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/tester1"
)

// For RPCs from tester to RSM server
type RSMproxy struct {
	dc *tester.DaemonClnt
}

func newRSMproxy(dc *tester.DaemonClnt) *RSMproxy {
	labgob.Register(Inc{})
	labgob.Register(IncRep{})
	labgob.Register(Dec{})
	labgob.Register(Null{})
	labgob.Register(NullRep{})
	return &RSMproxy{dc: dc}
}

func (rsp *RSMproxy) Submit(req any) (rpc.Err, any) {
	args := &SubmitArgs{Req: req}
	var rep SubmitReply
	if ok := rsp.dc.Call("rsmSrv.SubmitRPC", args, &rep); !ok {
		//log.Printf("%v: rsp.Submit err %v %T", rsp.rpcc.Server(), rep, rep)
	}
	return rep.Err, rep.Rep
}

func (rsp *RSMproxy) GetCounter() int {
	args := &GetCounterArgs{}
	var rep GetCounterReply
	if ok := rsp.dc.Call("rsmSrv.GetCounterRPC", args, &rep); !ok {
		log.Printf("rsp.GetCounter err %v %T", rep, rep)
	}
	return rep.Count
}
