package rtpruner

import (
	"sync"
	"sync/atomic"
)

type trieStatusContainer struct {
	stateBloomLock sync.Mutex
	stateBloom     *stateBloom
	key            []byte
	count          uint64
	size           uint64
	rowsProcessed  uint64
}

func (p *trieStatusContainer) inc(sz uint64) {
	atomic.AddUint64(&p.count, 1)
	atomic.AddUint64(&p.size, sz)
}

func newTrieStatus() *trieStatusContainer {
	return &trieStatusContainer{}
}
