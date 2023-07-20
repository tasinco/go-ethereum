package rtprunerutils

import (
	"sync"
	"testing"
	"time"
)

func TestSignal(t *testing.T) {
	wakeCh := make(chan struct{})
	wokenCh := make(chan struct{})

	signalCh := make(chan struct{})
	signal := NewSignal(signalCh)

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-wakeCh
		signal.Signal()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range signalCh {
			wokenCh <- struct{}{}
			return
		}
	}()

	time.Sleep(5 * time.Second)

	wakeCh <- struct{}{}

	select {
	case <-wokenCh:
	case <-time.NewTimer(10 * time.Second).C:
		t.Fail()
	}
}
