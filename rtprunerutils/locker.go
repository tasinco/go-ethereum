package rtprunerutils

import (
	"log"
	"sync"
	"sync/atomic"
)

// Locker holds an exclusive lock until another caller wants access.
// The parent wanting to hold a lock will invoke ExclusiveLock and defer ExclusiveUnlock
// The parent can do work, but will need to check if Waiting() != 0, this indicates a
// child is wanting to gain access, and the parent should relinquish control to the child.
type Locker struct {
	locked     int32
	lock       sync.RWMutex
	signalLock sync.RWMutex
	signal     Signal
	tag        string
	cnt        uint64
}

func (l *Locker) RegisterSignal(signal Signal) {
	l.signalLock.Lock()
	defer l.signalLock.Unlock()
	l.signal = signal
}

func (l *Locker) RLock() {
	atomic.AddInt32(&l.locked, 1)
	l.lock.RLock()
}

func (l *Locker) RUnlock() {
	l.initSignal()
	l.lock.RUnlock()
	atomic.AddInt32(&l.locked, -1)

	l.signalLock.RLock()
	defer l.signalLock.RUnlock()
	l.signal.Signal()
}

func (l *Locker) Lock() {
	atomic.AddInt32(&l.locked, 1)
	l.lock.Lock()
}

func (l *Locker) Unlock() {
	l.initSignal()
	l.lock.Unlock()
	atomic.AddInt32(&l.locked, -1)

	l.signalLock.RLock()
	defer l.signalLock.RUnlock()
	l.signal.Signal()
}

func (l *Locker) ExclusiveLock() {
	cntx := atomic.AddUint64(&l.cnt, 1)
	waitingv := atomic.LoadInt32(&l.locked)
	if waitingv != 0 {
		log.Println("exclusive lock", l.tag, cntx, waitingv)
	}
	l.lock.Lock()
	if waitingv != 0 {
		log.Println("exclusive locked", l.tag, cntx, waitingv)
	}
}

func (l *Locker) ExclusiveUnlock() {
	l.lock.Unlock()
}

func (l *Locker) Waiting() int32 {
	return atomic.LoadInt32(&l.locked)
}

func (l *Locker) initSignal() {
	l.signalLock.Lock()
	defer l.signalLock.Unlock()
	if l.signal == nil {
		l.signal = NewNoSignal()
	}
}
