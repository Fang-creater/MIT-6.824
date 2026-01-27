package raft

import (
	"log"

	"6.5840/raftapi"
	"6.5840/tester1"
)

// For Raft RPCs from tester to Raft server
type Rfproxy struct {
	dc *tester.DaemonClnt
}

func newRfproxy(dc *tester.DaemonClnt) *Rfproxy {
	return &Rfproxy{dc: dc}
}

func (rfp *Rfproxy) GetState() (int, bool) {
	args := &GetStateArgs{}
	var rep GetStateReply
	//log.Printf("rfp.GetState %v", rep)
	if ok := rfp.dc.Call("rfsrv.GetStateRPC", args, &rep); !ok {
		log.Printf("rfp.GetState failed")
	}
	return rep.Term, rep.Leader
}

func (rfp *Rfproxy) Start(command interface{}) (int, int, bool) {
	args := &StartArgs{
		Command: command,
	}
	var rep StartReply
	if ok := rfp.dc.Call("rfsrv.StartRPC", args, &rep); !ok {
		//log.Printf("rfp.Start %v failed", args)
	}
	//log.Printf("rfp.Start reply i %d t %d leader  %t", rep.Index, rep.Term, rep.Leader)
	return rep.Index, rep.Term, rep.Leader
}

// For RPCs from server to tester
type TesterProxy struct {
	*tester.TesterClnt
}

func newTesterProxy(tc *tester.TesterClnt) *TesterProxy {
	return &TesterProxy{tc}
}

func (tp *TesterProxy) CheckLogs(index int, m raftapi.ApplyMsg) (string, bool) {
	args := &CheckLogsArgs{
		Index: index,
		Msg:   m,
	}
	var rep CheckLogsReply
	ok := tp.Call("Test.CheckLogsRPC", args, &rep)
	if !ok {
		return "ErrRPC", false
	}
	return rep.Err, rep.Prevok
}

func (tp *TesterProxy) IngestLog(index int, m map[int]any) {
	args := &IngestLogArgs{
		Index: index,
		Log:   m,
	}
	var rep IngestLogReply
	ok := tp.Call("Test.IngestLogRPC", args, &rep)
	if !ok {
		//log.Printf("IngestLog: IngestLogRPC %d ok %t", index, ok)
	}
}

func (tp *TesterProxy) ApplyErr(index int, err string) {
	args := &ApplyErrArgs{
		Index: index,
		Err:   err,
	}
	var rep ApplyErrReply
	ok := tp.Call("Test.ApplyErrRPC", args, &rep)
	if !ok {
		//log.Printf("ApplyErr: ApplyErrRPC %d %q ok %t", index, err, ok)
	}
}
