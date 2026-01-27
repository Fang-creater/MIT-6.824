package tester

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"6.5840/labrpc"
	"6.5840/tester1/demux"
	"6.5840/tester1/sockrpc"
)

//
// Tester side to interact with server daemons
//

var MaxRaftState int
var UseRaftStateMachine bool // to plug in another raft besided raft1

func init() {
	flag.BoolVar(&UseRaftStateMachine, "raft-state-machine", false, "use raft state machine")
	flag.IntVar(&MaxRaftState, "max-raft-state", -1, "max raft state")
}

type DaemonClnt struct {
	mu       sync.Mutex
	srv      int
	server   string
	endName  string
	cmd      *exec.Cmd
	rpcc     *sockrpc.RPCClnt
	rpccClnt *sockrpc.RPCClnt
	dmxclnt  *demux.DemuxClnt
	clnt     *Clnt
}

// path to daemon's binary
func path(prog, cwd string) string {
	i := strings.LastIndex(cwd, "src")
	return filepath.Join(cwd[:i], "src/main", prog)
}

func options() []string {
	opt := []string{}
	if UseRaftStateMachine {
		opt = append(opt, "--raft-state-machine")
	}
	return opt
}

// The tester calls RunDaemon to start a server as a process
func runDaemon(net *labrpc.Network, prog string, pargs []string, endName string, gid Tgid, srv int, endNames []string) (*DaemonClnt, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	args := []string{}
	args = append(args, pargs...)
	args = append(args, options()...)
	args = append(args,
		strconv.Itoa(int(gid)),
		strconv.Itoa(srv),
		strconv.Itoa(len(endNames)),
		endName)
	args = append(args, endNames...)
	cmd := exec.Command(path(prog, cwd), args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	clnt := makeClntTo(net, nil)
	dc := &DaemonClnt{
		srv:     srv,
		server:  ServerName(gid, srv),
		endName: endNames[srv],
		cmd:     cmd,
		clnt:    clnt,
	}
	dc.rpccClnt = sockrpc.NewRPCClnt("tester", sockctl(dc.endName))
	return dc, nil
}

func (dc *DaemonClnt) Call(method string, args any, rep any) bool {
	return dc.rpcc.RPCMarshall(method, args, rep)
}

func (dc *DaemonClnt) init(persister *Persister) {
	args := &InitArgs{
		Raftstate: persister.ReadRaftState(),
		Snapshot:  persister.ReadSnapshot(),
	}
	var reply InitReply
	if ok := dc.rpccClnt.RPCMarshall("DaemonSrv.Init", args, &reply); !ok {
		log.Printf("init: no return")
	}
	dc.rpcc = sockrpc.NewRPCClnt("tester", dc.endName)
}

func (dc *DaemonClnt) waitInit() {
	args := &InitArgs{}
	var reply InitReply
	if ok := dc.rpccClnt.RPCMarshall("DaemonSrv.InitWait", args, &reply); !ok {
		log.Printf("waitinit: no return")
	}
}

// forward to daemon
func (dc *DaemonClnt) forward(method string, args []byte) ([]byte, bool) {
	//log.Printf("forward to srv %q m %v l %d", dc.endName, method, len(args))
	return dc.rpcc.RPC(method, args)
}

func (dc *DaemonClnt) checkpointPersister() *Persister {
	args := &CheckpointPersisterArgs{}
	var reply CheckpointPersisterReply
	if ok := dc.rpccClnt.RPCMarshall("DaemonSrv.CheckpointPersister", args, &reply); !ok {
		log.Fatalf("checkpointPersister: no return")
		return nil
	}
	p := MakePersister()
	p.Save(reply.Raftstate, reply.Snapshot)
	return p
}

func (dc *DaemonClnt) stateSize() (int, int) {
	args := &StateSizeArgs{}
	var reply StateSizeReply
	if ok := dc.rpccClnt.RPCMarshall("DaemonSrv.StateSize", args, &reply); !ok {
		log.Fatalf("StateSize: no return")
	}
	//log.Printf("stateSize %d %d", reply.RaftSize, reply.SnapSize)
	return reply.RaftSize, reply.SnapSize
}

func (dc *DaemonClnt) memSize() uint64 {
	args := &MemSizeArgs{}
	var reply MemSizeReply
	if ok := dc.rpccClnt.RPCMarshall("DaemonSrv.MemSize", args, &reply); !ok {
		log.Fatalf("MemSize: no return")
	}
	// log.Printf("memSize %d", reply.MemSize)
	return reply.MemSize
}

func (dc *DaemonClnt) kill() {
	if err := dc.cmd.Process.Kill(); err != nil {
		log.Fatalf("Kill err %v", err)
	}
	if err := dc.cmd.Wait(); err != nil {
		//log.Printf("Wait err %v", err)
	}
	os.Remove(sockrpc.SockName(sockctl(dc.endName)))
	os.Remove(sockrpc.SockName(dc.endName))
}
