package rtprunerutils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLockerNotExclusive(t *testing.T) {
	lockerTest(t, false)
}

func TestLockerExclusive(t *testing.T) {
	lockerTest(t, true)
}

func lockerTest(t *testing.T, exclusive bool) {
	locker := Locker{}

	lockCh := make(chan struct{})
	lockChTrigger := make(chan struct{})

	go func() {
		for range lockCh {
			if exclusive {
				locker.Lock()
				lockChTrigger <- struct{}{}
				locker.Unlock()
			} else {
				locker.RLock()
				lockChTrigger <- struct{}{}
				locker.RUnlock()
			}
			return
		}
	}()

	locker.ExclusiveLock()
	assert.Equal(t, int32(0), locker.Waiting())

	// let the child place a lock
	lockCh <- struct{}{}

	// wait for the lock which will increase Waiting count
	for locker.Waiting() != 1 {
		time.Sleep(1 * time.Millisecond)
	}

	// someone is waiting, let go of the exclusive lock
	locker.ExclusiveUnlock()

	// the child will control the lock..
	<-lockChTrigger

	// when the child releases, Waiting count goes to 0
	for locker.Waiting() != 0 {
		time.Sleep(1 * time.Millisecond)
	}
}
