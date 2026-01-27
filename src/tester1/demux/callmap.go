package demux

import (
	"fmt"
	"sync"
)

// Map of outstanding calls indexed by tag
type callMap struct {
	sync.Mutex
	closed bool
	calls  map[Ttag]chan reply
}

func newCallMap() *callMap {
	return &callMap{calls: make(map[Ttag]chan reply)}
}

func (cm *callMap) close() error {
	cm.Lock()
	defer cm.Unlock()

	cm.closed = true
	return nil
}

func (cm *callMap) isClosed() bool {
	cm.Lock()
	defer cm.Unlock()

	return cm.closed
}

func (cm *callMap) put(tag Ttag, ch chan reply) error {
	cm.Lock()
	defer cm.Unlock()

	if cm.closed {
		return fmt.Errorf("conn closed")
	}
	cm.calls[tag] = ch
	return nil
}

func (cm *callMap) remove(tag Ttag) (chan reply, bool) {
	cm.Lock()
	defer cm.Unlock()

	if ch, ok := cm.calls[tag]; ok {
		delete(cm.calls, tag)
		return ch, true
	}
	return nil, false
}

func (cm *callMap) outstanding() []Ttag {
	cm.Lock()
	defer cm.Unlock()

	ts := make([]Ttag, 0, len(cm.calls))
	for t, _ := range cm.calls {
		ts = append(ts, t)
	}
	return ts
}
