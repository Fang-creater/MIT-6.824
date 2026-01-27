package tester

import (
	//"log"

	"6.5840/tester1/sockrpc"
)

type TesterRPC struct {
	*sockrpc.RPCSrv
	cfg *Config
}

// Make a RPC server for the tester to receive RPCs from daemons
func newTesterRPCSrv(cfg *Config) *TesterRPC {
	trpc := &TesterRPC{
		cfg: cfg,
	}
	//log.Printf("newTesterRPCSrv %v", cfg.endName)
	trpc.RPCSrv = sockrpc.NewRPCSrv(cfg.endName)
	trpc.RPCSrv.AddService(trpc)
	return trpc
}

func (trpc *TesterRPC) cleanup() {
	//log.Printf("TesterRPCsrv: close %v", trpc.cfg.endName)
	trpc.Close()
}

type ForwardArgs struct {
	Method string
	End    string // client end for server
	Args   []byte
	Id     int64
}

type ForwardReply struct {
	Rep []byte
	Ok  bool
}

// Forward RPC to a deamon through the lab net
func (trpc *TesterRPC) Forward(args *ForwardArgs, reply *ForwardReply) {
	//log.Printf("%v: Forward args %v to end %q %d", trpc.Name(), args.Method, args.End, args.Id)
	end := trpc.cfg.net.LookupEnd(args.End)
	reply.Rep, reply.Ok = end.Forward(args.Method, args.Args)
}
