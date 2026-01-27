package tester

import (
	"log"
	"strconv"
	"sync"

	"6.5840/labrpc"
)

type Tgid int

// Each server has a name: i'th server of group gid. If there is only a single
// server, it its gid = 0 and its i is 0.
func ServerName(gid Tgid, i int) string {
	return "server-" + strconv.Itoa(int(gid)) + "-" + strconv.Itoa(i)
}

// The tester may have many groups of servers (e.g., one per Raft group).
// Groups are named 0, 1, and so on.
type Groups struct {
	mu      sync.Mutex
	net     *labrpc.Network
	prog    string
	args    []string
	endName string
	grps    map[Tgid]*ServerGrp
}

func newGroups(net *labrpc.Network, prog string, args []string, endName string) *Groups {
	return &Groups{net: net, prog: prog, args: args, endName: endName, grps: make(map[Tgid]*ServerGrp)}
}

func (gs *Groups) MakeGroup(prog string, args []string, gid Tgid, nsrv int) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	gs.grps[gid] = makeSrvGrp(gs.net, prog, args, gs.endName, gid, nsrv)
}

func (gs *Groups) lookupGroup(gid Tgid) *ServerGrp {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	return gs.grps[gid]
}

func (gs *Groups) delete(gid Tgid) {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	delete(gs.grps, gid)
}

func (gs *Groups) cleanup() {
	gs.mu.Lock()
	defer gs.mu.Unlock()

	for _, sg := range gs.grps {
		sg.Shutdown()
	}
}

type ServerGrp struct {
	net         *labrpc.Network
	prog        string
	args        []string
	srvs        []*Server
	servernames []string
	endName     string
	gid         Tgid
	connected   []bool // whether each server is on the net
	mu          sync.Mutex
}

func makeSrvGrp(net *labrpc.Network, prog string, args []string, endName string, gid Tgid, n int) *ServerGrp {
	sg := &ServerGrp{
		net:       net,
		prog:      prog,
		args:      args,
		endName:   endName,
		srvs:      make([]*Server, n),
		gid:       gid,
		connected: make([]bool, n),
	}
	for i, _ := range sg.srvs {
		sg.srvs[i] = makeServer(net, gid, n)
	}
	sg.servernames = make([]string, n)
	for i := 0; i < n; i++ {
		sg.servernames[i] = ServerName(gid, i)
	}
	return sg
}

func (sg *ServerGrp) N() int {
	return len(sg.srvs)
}

func (sg *ServerGrp) SrvNames() []string {
	return sg.servernames
}

func (sg *ServerGrp) SrvName(i int) string {
	return sg.servernames[i]
}

func (sg *ServerGrp) SrvNamesTo(to []int) []string {
	ns := make([]string, 0, len(to))
	for _, i := range to {
		ns = append(ns, sg.servernames[i])
	}
	return ns
}

func (sg *ServerGrp) all() []int {
	all := make([]int, len(sg.srvs))
	for i, _ := range sg.srvs {
		all[i] = i
	}
	return all
}

func (sg *ServerGrp) ConnectAll() {
	for i, _ := range sg.srvs {
		sg.ConnectOne(i)
	}
}

func (sg *ServerGrp) ConnectOne(i int) {
	sg.connect(i, sg.all())
}

func (sg *ServerGrp) Kill(srvs []int) {
	for _, srv := range srvs {
		sg.ShutdownServer(srv)
	}
}

// attach server i to servers listed in to caller must hold cfg.mu.
func (sg *ServerGrp) connect(i int, to []int) {
	//log.Printf("connect peer %d to %v\n", i, to)

	sg.connected[i] = true

	// connect outgoing end points
	sg.srvs[i].connect(sg, to)

	// connect incoming end points to me
	for j := 0; j < len(to); j++ {
		if sg.IsConnected(to[j]) {
			//log.Printf("connect %d (%v) to %d", to[j], sg.srvs[to[j]].endNames[i], i)
			endname := sg.srvs[to[j]].endNames[i]
			sg.net.Enable(endname, true)
		}
	}
}

// detach server from the servers listed in from
// caller must hold cfg.mu
func (sg *ServerGrp) disconnect(i int, from []int) {
	//log.Printf("%p: disconnect peer %d from %v", sg, i, from)

	sg.mu.Lock()
	sg.connected[i] = false
	sg.mu.Unlock()

	// outgoing ends
	sg.srvs[i].disconnect(from)

	// incoming ends
	for j := 0; j < len(from); j++ {
		s := sg.srvs[from[j]]
		if s.endNames != nil {
			endname := s.endNames[i]
			// log.Printf("%p: disconnect: %v", sg, endname)
			sg.net.Enable(endname, false)
		}
	}
}

func (sg *ServerGrp) DisconnectAll(i int) {
	sg.disconnect(i, sg.all())
}

func (sg *ServerGrp) IsConnected(i int) bool {
	defer sg.mu.Unlock()
	sg.mu.Lock()
	return sg.connected[i]
}

func (sg *ServerGrp) GetConnected() []bool {
	return sg.connected
}

func (sg *ServerGrp) MemSize() uint64 {
	memsize := uint64(0)
	for _, s := range sg.srvs {
		n := s.memSize()
		if n > memsize {
			memsize = n
		}
	}
	return memsize
}

// Maximum raft state size across all servers
func (sg *ServerGrp) RaftSize() int {
	logsize := 0
	for _, s := range sg.srvs {
		n := s.raftSize()
		if n > logsize {
			logsize = n
		}
	}
	return logsize
}

// Maximum snapshot size across all servers
func (sg *ServerGrp) SnapshotSize() int {
	snapshotsize := 0
	for _, s := range sg.srvs {
		n := s.snapshotSize()
		if n > snapshotsize {
			snapshotsize = n
		}
	}
	return snapshotsize
}

// If restart servers, first call shutdownserver
func (sg *ServerGrp) StartServer(i int) error {
	srv := sg.srvs[i].startServer(sg.gid)
	sg.srvs[i] = srv

	//log.Printf("StartServer %d %v %q", i, srv.endNames, sg.prog)

	labsrv := labrpc.MakeServer()

	// start a process to run server
	dc, err := runDaemon(sg.net, sg.prog, sg.args, sg.endName, sg.gid, i, srv.endNames)
	if err != nil {
		log.Printf("runDaemon err %v", err)
		return err
	}
	srv.dc = dc

	// send the save persistor state to server
	dc.init(srv.saved)
	dc.waitInit()

	// now add server to network
	labsrv.SetDispatch(dc.forward)
	sg.net.AddServer(ServerName(sg.gid, i), labsrv)
	return nil
}

func (sg *ServerGrp) DaemonClnt(i int) *DaemonClnt {
	return sg.srvs[i].dc
}

// create a full set of KV servers.
func (sg *ServerGrp) StartServers() {
	sg.StartSrvs(sg.all())
	sg.ConnectAll()
}

// Shutdown a server by isolating it
func (sg *ServerGrp) ShutdownServer(i int) {
	//log.Printf("ShutdownServer %v", ServerName(sg.gid, i))
	sg.disconnect(i, sg.all())

	// disable client connections to the server.
	sg.net.DeleteServer(ServerName(sg.gid, i))

	sg.srvs[i].shutdownServer()
}

func (sg *ServerGrp) Servers() []int {
	return sg.all()
}

func (sg *ServerGrp) Shutdown() {
	sg.Kill(sg.all())
}

func (sg *ServerGrp) StartSrvs(srvs []int) {
	for _, srv := range srvs {
		sg.StartServer(srv)
	}
}

// Partition servers into 2 groups and put current leader in minority
func (sg *ServerGrp) MakePartition(l int) ([]int, []int) {
	n := len(sg.srvs)
	p1 := make([]int, n/2+1)
	p2 := make([]int, n/2)
	j := 0
	for i := 0; i < n; i++ {
		if i != l {
			if j < len(p1) {
				p1[j] = i
			} else {
				p2[j-len(p1)] = i
			}
			j++
		}
	}
	p2[len(p2)-1] = l
	return p1, p2
}

func (sg *ServerGrp) Partition(p1 []int, p2 []int) {
	//log.Printf("partition servers into: %v %v\n", p1, p2)
	for i := 0; i < len(p1); i++ {
		sg.disconnect(p1[i], p2)
		sg.connect(p1[i], p1)
	}
	for i := 0; i < len(p2); i++ {
		sg.disconnect(p2[i], p1)
		sg.connect(p2[i], p2)
	}
}

func (sg *ServerGrp) RpcCount(server int) int {
	return sg.net.GetCount(ServerName(sg.gid, server))
}
