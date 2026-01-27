package tester

import (
	//"log"

	"6.5840/tester1/sockrpc"
)

type TesterClnt struct {
	*sockrpc.RPCClnt
}

func newTesterClnt(rpcc *sockrpc.RPCClnt) *TesterClnt {
	return &TesterClnt{rpcc}
}

func (tc *TesterClnt) Call(method string, args any, rep any) bool {
	return tc.RPCMarshall(method, args, rep)
}
