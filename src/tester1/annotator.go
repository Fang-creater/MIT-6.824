/// A simple RPC server to collect annotations made by separate Raft server processes. This file
/// does not cover all the annotation interface, just the ones required for now.

package tester

import (
	"6.5840/tester1/sockrpc"
)

/// Global variable initialized in `daemonsrv.go`.
var rpcc *sockrpc.RPCClnt

/// Annotator interface. The naming of Annotate is not consistent with others, but is shorter which
/// is good from the user perspective.

func Annotate(tag, desp, details string) {
	args := &PostAnnotatorPointArgs {
		Tag: tag,
		Desp: desp,
		Details: details,
	}
	var reply PostAnnotatorPointReply
	ok := rpcc.RPCMarshall("TesterRPC.PostAnnotatorPoint", args, &reply)
	if !ok {
		// fmt.Printf("failure to call TesterRPC.PostAnnotatorPoint with args = %v", args)
	}
}

func GetAnnotatorTimestamp() int64 {
	args := &GetAnnotatorTimestampArgs { }
	var reply GetAnnotatorTimestampReply
	ok := rpcc.RPCMarshall("TesterRPC.GetAnnotatorTimestamp", args, &reply)
	if !ok {
		// fmt.Printf("failure to call TesterRPC.GetAnnotatorTimestamp with args = %v", args)
		return 0
	}
	return reply.Timestamp
}

func PostAnnotatorInfoInterval(start int64, desp, details string) {
	args := &PostAnnotatorInfoIntervalArgs {
		Start: start,
		Desp: desp,
		Details: details,
	}
	var reply PostAnnotatorInfoIntervalReply
	ok := rpcc.RPCMarshall("TesterRPC.PostAnnotatorInfoInterval", args, &reply)
	if !ok {
		// fmt.Printf("failure to call TesterRPC.PostAnnotatorInfoInterval with args = %v", args)
	}
}

/// PostAnnotatorPoint RPC definitions.

type PostAnnotatorPointArgs struct {
	Tag     string
	Desp    string
	Details string
}

type PostAnnotatorPointReply struct { }

func (trpc *TesterRPC) PostAnnotatorPoint(
	args *PostAnnotatorPointArgs,
	reply *PostAnnotatorPointReply,
) {
	AnnotatePoint(args.Tag, args.Desp, args.Details)
}

/// GetAnnotatorTimestamp RPC definitions.

type GetAnnotatorTimestampArgs struct { }

type GetAnnotatorTimestampReply struct {
	Timestamp int64
}

func (trpc *TesterRPC) GetAnnotatorTimestamp(
	args *GetAnnotatorTimestampArgs,
	reply *GetAnnotatorTimestampReply,
) {
	reply.Timestamp = GetAnnotateTimestamp()
}

/// PostAnnotatorInfoInterval RPC definitions.

type PostAnnotatorInfoIntervalArgs struct {
	Start   int64
	Desp    string
	Details string
}

type PostAnnotatorInfoIntervalReply struct { }

func (trpc *TesterRPC) PostAnnotatorInfoInterval(
	args *PostAnnotatorInfoIntervalArgs,
	reply *PostAnnotatorInfoIntervalReply,
) {
	AnnotateInfoInterval(args.Start, args.Desp, args.Details)
}
