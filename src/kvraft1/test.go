package kvraft

import (
	"fmt"
	"sync"
	"testing"

	"6.5840/kvtest1"
	"6.5840/tester1"
)

const (
	Gid = tester.GRP0
)

type Test struct {
	t *testing.T
	*kvtest.Test
	part         string
	nclients     int
	nservers     int
	crash        bool
	partitions   bool
	maxraftstate int
	randomkeys   bool

	mu sync.Mutex
}

func MakeTest(t *testing.T, part string, nclients, nservers int, reliable bool, crash bool, partitions bool, maxraftstate int, randomkeys bool) *Test {
	ts := &Test{
		t:            t,
		part:         part,
		nclients:     nclients,
		nservers:     nservers,
		crash:        crash,
		partitions:   partitions,
		maxraftstate: maxraftstate,
		randomkeys:   randomkeys,
	}
	args := []string{fmt.Sprintf("--max-raft-state=%d", maxraftstate)}
	cfg := tester.MakeConfig(t, nservers, reliable, "kvraft1d", args)
	ts.Test = kvtest.MakeTest(t, cfg, randomkeys, ts)
	ts.Begin(ts.makeTitle())
	return ts
}

func (ts *Test) killall() {
	ts.Group(Gid).Shutdown()
	tester.AnnotateShutdownAll()
}

func (ts *Test) restartall() {
	ts.Group(Gid).StartServers()
	tester.AnnotateRestartAll()
}

func (ts *Test) MakeClerk() kvtest.IKVClerk {
	clnt := ts.Config.MakeClient()
	ck := MakeClerk(clnt, ts.Group(Gid).SrvNames())
	return &kvtest.TestClerk{ck, clnt, ts.Config}
}

func (ts *Test) DeleteClerk(ck kvtest.IKVClerk) {
	tck := ck.(*kvtest.TestClerk)
	ts.DeleteClient(tck.Clnt)
}

func (ts *Test) MakeClerkTo(to []int) kvtest.IKVClerk {
	ns := ts.Config.Group(Gid).SrvNamesTo(to)
	clnt := ts.Config.MakeClientTo(ns)
	ck := MakeClerk(clnt, ts.Group(Gid).SrvNames())
	return &kvtest.TestClerk{ck, clnt, ts.Config}
}

func (ts *Test) cleanup() {
	ts.Test.Cleanup()
}

func (ts *Test) makeTitle() string {
	title := "Test: "
	if ts.crash {
		// peers re-start, and thus persistence must work.
		title = title + "restarts, "
	}
	if ts.partitions {
		// the network may partition
		title = title + "partitions, "
	}
	if ts.maxraftstate != -1 {
		title = title + "snapshots, "
	}
	if ts.randomkeys {
		title = title + "random keys, "
	}
	if ts.nclients > 1 {
		title = title + "many clients"
	} else {
		title = title + "one client"
	}
	title = title + " (" + ts.part + ")" // 4A, 4B, 4C
	return title
}
