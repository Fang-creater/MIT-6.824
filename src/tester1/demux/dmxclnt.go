// The demux package multiplexes calls over a net.Conn and matches
// responses with requests using the call's tag.
package demux

import (
	"fmt"
	"log"
	"sync"

	"6.5840/labrpc"
)

type Ttag uint32

type DemuxClnt struct {
	callmap *callMap
	trans   TransportI
	mu      sync.Mutex
	nextTag Ttag
	clntEnd string
	srvEnd  string
}

type InitMsg struct {
	Clnt string
}

type reply struct {
	rep []byte
	ok  bool
	err error
}

func NewDemuxClnt(clnt, srv string, trans TransportI) (*DemuxClnt, error) {
	dmx := &DemuxClnt{
		callmap: newCallMap(),
		trans:   trans,
		clntEnd: clnt,
		srvEnd:  srv,
	}
	b := labrpc.Marshall(&InitMsg{Clnt: clnt})
	if err := trans.WriteCall(dmx.tag(), b, true); err != nil {
		return nil, err
	}
	go dmx.reader()
	return dmx, nil
}

func (dmx *DemuxClnt) tag() Ttag {
	dmx.mu.Lock()
	defer dmx.mu.Unlock()

	t := dmx.nextTag
	dmx.nextTag++
	return t
}

func (dmx *DemuxClnt) reply(tag Ttag, b []byte, oks bool, err error) {
	if ch, ok := dmx.callmap.remove(tag); ok {
		ch <- reply{b, oks, err}
	} else {
		log.Fatalf("%v: reply %v no matching req %v", dmx.srvEnd, b, tag)
	}
}

func (dmx *DemuxClnt) reader() {
	for {
		t, b, ok, err := dmx.trans.ReadCall()
		if err != nil {
			//log.Printf("%v: dmxclnt.reader rf err %v", dmx.srvEnd, err)
			dmx.callmap.close()
			break
		}
		dmx.reply(t, b, ok, nil)
	}
	outstanding := dmx.callmap.outstanding()
	for _, t := range outstanding {
		//log.Printf("%v: dmxclnt.reader reply fail %v", dmx.srvEnd, t)
		dmx.reply(t, nil, false, fmt.Errorf("reader reply fail"))
	}
}

func (dmx *DemuxClnt) SendReceive(b []byte) ([]byte, bool, error) {
	t := dmx.tag()
	ch := make(chan reply)
	if err := dmx.callmap.put(t, ch); err != nil {
		//log.Printf("SendReceive: enqueue req %v err %v", b, err)
		return nil, false, err
	}
	dmx.mu.Lock()
	err := dmx.trans.WriteCall(t, b, true)
	dmx.mu.Unlock()
	if err != nil {
		//log.Printf("WriteCall req %v error %v", t, err)
	}
	// Listen to the reply channel regardless of error status, so the reader
	// thread doesn't block indefinitely trying to deliver the "TErrUnreachable"
	// reply.
	rep := <-ch
	return rep.rep, rep.ok, rep.err
}

func (dmx *DemuxClnt) Close() error {
	if dmx.callmap.isClosed() {
		return nil
	}
	if err := dmx.trans.Close(); err != nil {
		log.Printf("%v: Close trans err %v", dmx.srvEnd, err)
	}
	return dmx.callmap.close()
}

func (dmx *DemuxClnt) IsClosed() bool {
	return dmx.callmap.isClosed()
}
