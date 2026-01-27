package demux

import (
	"io"
	"log"
	"sync"

	"6.5840/labrpc"
)

type ServerI interface {
	ServeRequest(clntEnd string, req []byte) ([]byte, bool)
}

type DemuxSrv struct {
	mu      sync.Mutex
	srv     ServerI
	closed  bool
	trans   TransportI
	srvEnd  string
	clntEnd string
}

func NewDemuxSrv(srvEnd string, srv ServerI, trans TransportI) *DemuxSrv {
	dmx := &DemuxSrv{srvEnd: srvEnd, srv: srv, trans: trans}
	var msg InitMsg
	_, b, _, err := trans.ReadCall()
	if err != nil {
		log.Fatalf("NewDemuxSrv: ReadCall err %v", err)
	}
	labrpc.Unmarshall(b, &msg)
	dmx.clntEnd = msg.Clnt
	go dmx.reader()
	return dmx
}

func (dmx *DemuxSrv) ClntEnd() string {
	return dmx.clntEnd
}

func (dmx *DemuxSrv) setClosed() bool {
	dmx.mu.Lock()
	defer dmx.mu.Unlock()
	c := dmx.closed
	dmx.closed = true
	return c
}

func (dmx *DemuxSrv) reader() {
	for {
		t, req, _, err := dmx.trans.ReadCall()
		if err != nil {
			if err == io.EOF {
				dmx.setClosed()
			} else {
				log.Printf("%v: dmxsrv.reader: clnt %v ReadCall err %v", dmx.srvEnd, dmx.clntEnd, err)
			}
			break
		}
		go func(t Ttag, req []byte) {
			rep, ok := dmx.srv.ServeRequest(dmx.clntEnd, req)
			var err error
			dmx.mu.Lock()
			err = dmx.trans.WriteCall(t, rep, ok)
			dmx.mu.Unlock()
			if err != nil {
				//log.Printf("%v: dmxssrv.reader: clnt %v WriteCall reply %v error %v", dmx.srvEnd, dmx.clntEnd, t, err)
			}
		}(t, req)
	}
}

func (dmx *DemuxSrv) Close() error {
	c := dmx.setClosed()
	if c {
		return nil
	}
	log.Printf("%v: Close clnt %v", dmx.srvEnd, dmx.clntEnd)
	if err := dmx.trans.Close(); err != nil {
		log.Printf("%v: Close trans clnt %v err %v", dmx.srvEnd, dmx.clntEnd, err)
	}

	return nil
}
