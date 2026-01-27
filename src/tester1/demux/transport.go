package demux

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	//"log"
	"net"

	"6.5840/labrpc"
)

const (
	MSGLEN = 65536
)

type TransportI interface {
	ReadCall() (Ttag, []byte, bool, error)
	WriteCall(Ttag, []byte, bool) error
	Close() error
}

type transport struct {
	conn net.Conn
	rdr  io.Reader
	wrt  *bufio.Writer
}

type msg struct {
	Tag  Ttag
	Data []byte
	Ok   bool
}

func NewTransport(c net.Conn) TransportI {
	return &transport{
		conn: c,
		rdr:  bufio.NewReaderSize(c, MSGLEN),
		wrt:  bufio.NewWriterSize(c, MSGLEN),
	}
}

func (t *transport) Close() error {
	return t.conn.Close()
}

func (t *transport) ReadCall() (Ttag, []byte, bool, error) {
	var nbyte uint32
	if err := binary.Read(t.rdr, binary.LittleEndian, &nbyte); err != nil {
		return 0, nil, false, err
	}
	b := make([]byte, nbyte)
	n, err := io.ReadFull(t.rdr, b)
	if err != nil {
		return 0, nil, false, err
	}
	if n != int(nbyte) {
		return 0, nil, false, fmt.Errorf("short read")
	}
	m := msg{}
	labrpc.Unmarshall(b, &m)
	return m.Tag, m.Data, m.Ok, nil
}

func (t *transport) WriteCall(tag Ttag, d []byte, ok bool) error {
	m := &msg{Tag: tag, Data: d, Ok: ok}
	b := labrpc.Marshall(m)
	if err := binary.Write(t.wrt, binary.LittleEndian, uint32(len(b))); err != nil {
		return err
	}
	if _, err := t.wrt.Write(b); err != nil {
		return err
	}
	if err := t.wrt.Flush(); err != nil {
		return err
	}
	return nil
}
