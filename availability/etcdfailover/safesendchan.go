package etcdfailover

import (
	"sync"
	"fmt"
)

type SafeSendChan struct {
	ch     chan ElectionState
	mu     sync.Mutex
	closed bool
}

func NewSafeSendChan() *SafeSendChan {
	return &SafeSendChan{
		ch:	make(chan ElectionState, 1),
	}
}

func (sc *SafeSendChan) Send(state ElectionState) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if sc.closed {
		return fmt.Errorf("channel is already closed, cannot send %s", state)
	}
	sc.ch <- state
	return nil
}

func (sc *SafeSendChan) Close() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if !sc.closed {
		close(sc.ch)
		sc.closed = true
	}
}

func (sc *SafeSendChan) RecvChan() <-chan ElectionState {
	return sc.ch
}
