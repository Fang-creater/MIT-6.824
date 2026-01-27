package tester

import (
	//"log"
	"sync"

	"6.5840/labrpc"
)

type Server struct {
	mu       sync.Mutex
	net      *labrpc.Network
	saved    *Persister
	endNames []string
	clntEnds []*labrpc.ClientEnd
	dc       *DaemonClnt
}

func makeServer(net *labrpc.Network, gid Tgid, nsrv int) *Server {
	srv := &Server{net: net}
	srv.endNames = make([]string, nsrv)
	srv.clntEnds = make([]*labrpc.ClientEnd, nsrv)
	for j := 0; j < nsrv; j++ {
		// a fresh set of ends for this server to other servers
		srv.endNames[j] = Randstring(20)
		srv.clntEnds[j] = net.MakeEnd(srv.endNames[j])
		net.Connect(srv.endNames[j], ServerName(gid, j))
	}
	return srv
}

// If restart servers, first call ShutdownServer
func (s *Server) startServer(gid Tgid) *Server {
	srv := makeServer(s.net, gid, len(s.endNames))
	if s.saved == nil {
		srv.saved = MakePersister()
	} else {
		srv.saved = s.saved
	}
	return srv
}

// connect s to servers listed in to
func (s *Server) connect(sg *ServerGrp, to []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for j := 0; j < len(to); j++ {
		if sg.IsConnected(to[j]) {
			//log.Printf("connect to %d (%v)", to[j], s.endNames[to[j]])
			endname := s.endNames[to[j]]
			s.net.Enable(endname, true)
		}
	}
}

func (s *Server) disconnect(from []int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.endNames == nil {
		return
	}
	for j := 0; j < len(from); j++ {
		endname := s.endNames[from[j]]
		s.net.Enable(endname, false)
	}
}

func (s *Server) shutdownServer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dc != nil {
		p := s.dc.checkpointPersister()
		s.saved = p
		s.dc.kill()
		s.dc = nil
	}
}

func (s *Server) snapshotSize() int {
	if s.dc != nil {
		_, s := s.dc.stateSize()
		return s
	}
	return s.saved.SnapshotSize()
}

func (s *Server) raftSize() int {
	if s.dc != nil {
		s, _ := s.dc.stateSize()
		return s
	}
	return s.saved.RaftStateSize()
}

func (s *Server) memSize() uint64 {
	return s.dc.memSize()
}
