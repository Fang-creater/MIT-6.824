package tester

import (
	"flag"
	//"log"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/tester1/sockrpc"
)

//
// Run a server as a daemon, inside its own process.  The tester sends
// RPCs to the daemon over a UNIX socket.
//

type DaemonSrv struct {
	endName   string
	gid       Tgid
	sid       int
	rpcc      *sockrpc.RPCClnt
	rpcs      *sockrpc.RPCSrv
	rpcsCtl   *sockrpc.RPCSrv
	ch        chan *Persister
	persister *Persister
	id        atomic.Int64
}

func sockctl(sock string) string {
	return sock + "-ctl"
}

// Start server and return the services to register with labrpc
type FstartServer func(tc *TesterClnt, ends []*labrpc.ClientEnd, grp Tgid, srv int, persister *Persister) []any

// A server process's main() calls InitDaemon() to initialize
func InitDaemon(args []string, mks FstartServer) error {
	runtime.GOMAXPROCS(4)

	//log.Printf("InitDaemon %v", args)

	// for safety, force quit after 10 minutes.
	go func() {
		time.Sleep(10 * 60 * time.Second)
		mep, _ := os.FindProcess(os.Getpid())
		mep.Kill()
	}()

	flag.Parse()

	// skip optional arguments
	s := 0
	for i, args := range os.Args[1:] {
		if args[0] == '-' {
			s = i + 1
		} else {
			break
		}
	}
	gid, err := strconv.Atoi(args[s])
	if err != nil {
		return err
	}

	sid, err := strconv.Atoi(args[s+1])
	if err != nil {
		return err
	}

	nsrv, err := strconv.Atoi(args[s+2])
	if err != nil {
		return err
	}

	testerEndName := args[s+3]
	endNames := args[s+4 : s+4+nsrv]

	// Set an network within the server daemon so that end.Call()'s in
	// by the server will work.  But, configures the ends to call
	// ds.forward, which forwards calls to the tester.
	ends := make([]*labrpc.ClientEnd, len(endNames))
	net := labrpc.MakeNetwork()
	ds := &DaemonSrv{
		endName: endNames[sid],
		ch:      make(chan *Persister),
	}
	for i, e := range endNames {
		ends[i] = net.MakeEnd(e)
		ends[i].SetCall(ds.forward)
	}

	// for RPCs to tester (e.g., Forward)
	ds.rpcc = sockrpc.NewRPCClnt(endNames[sid], testerEndName)
	// set the global for user-level annotation (`rpcc` is defined in `annotator.go`)
	rpcc = ds.rpcc
	// for ctl RPCS to this daemon (e.g., Start)
	ds.rpcsCtl = sockrpc.NewRPCSrv(sockctl(ds.endName))
	ds.rpcsCtl.AddService(ds)

	// wait until we receive Init RPC with snapshot
	ds.persister = <-ds.ch
	ds.rpcs = sockrpc.NewRPCSrv(ds.endName)
	svcs := mks(newTesterClnt(ds.rpcc), ends, Tgid(gid), sid, ds.persister)
	for _, svc := range svcs {
		ds.rpcs.AddService(svc)
	}

	// signal initialized
	ds.ch <- nil

	// Wait for the force quit above
	<-ds.ch
	return nil
}

// forwards RPC to tester, which will insert them into the lab network
func (ds *DaemonSrv) forward(end, method string, b []byte) ([]byte, bool) {
	//log.Printf("%d/%d: forward to %v %v len %d", ds.gid, ds.sid, end, method, len(b))
	args := &ForwardArgs{Method: method, Args: b, End: end, Id: ds.id.Add(1)}
	var reply ForwardReply
	ok := ds.rpcc.RPCMarshall("TesterRPC.Forward", args, &reply)
	if !ok {
		return nil, ok
	}
	return reply.Rep, reply.Ok
}

type InitArgs struct {
	Raftstate []byte
	Snapshot  []byte
}

type InitReply struct{}

func (ds *DaemonSrv) Init(args InitArgs, reply *InitReply) {
	p := MakePersister()
	p.Save(args.Raftstate, args.Snapshot)
	ds.ch <- p
}

func (ds *DaemonSrv) InitWait(args InitArgs, reply *InitReply) {
	<-ds.ch
}

type CheckpointPersisterArgs struct{}

type CheckpointPersisterReply struct {
	Raftstate []byte
	Snapshot  []byte
}

// Checkpoint persister state and return it to tester
func (ds *DaemonSrv) CheckpointPersister(args CheckpointPersisterArgs, reply *CheckpointPersisterReply) {
	// No more forwarding to tester
	ds.rpcc.Close()
	// Stop server from sending requests and replies (e.g.,
	// AppendEntriesReplies)
	ds.rpcs.Close()
	// Copy persister now; server may still update it later but those
	// updates will be lost when the server is killed.
	p := ds.persister.Checkpoint()
	reply.Raftstate = p.ReadRaftState()
	reply.Snapshot = p.ReadSnapshot()
}

type StateSizeArgs struct{}

type StateSizeReply struct {
	RaftSize int
	SnapSize int
}

func (ds *DaemonSrv) StateSize(args StateSizeArgs, reply *StateSizeReply) {
	reply.RaftSize = ds.persister.RaftStateSize()
	reply.SnapSize = ds.persister.SnapshotSize()
}

type MemSizeArgs struct{}

type MemSizeReply struct {
	MemSize uint64
}

func (ds *DaemonSrv) MemSize(args MemSizeArgs, reply *MemSizeReply) {
	runtime.GC()
	time.Sleep(1 * time.Second)
	runtime.GC()
	var st runtime.MemStats
	runtime.ReadMemStats(&st)
	reply.MemSize = st.HeapAlloc
}
