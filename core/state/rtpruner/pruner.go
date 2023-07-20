package rtpruner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rtprunerutils"
	"github.com/ethereum/go-ethereum/trie"
)

const (
	tickTime = 1 * time.Second
	// pruneAfter restart time after a full prune
	pruneAfter = 5 * time.Second
	// pruneDelta prune after no activity time
	pruneDelta = 1 * time.Second
)

var (
	emptyCode              = crypto.Keccak256Hash(nil)
	errNoIntercept         = errors.New("no intercept")
	errMissingGenesisHash  = errors.New("missing genesis hash")
	errMissingGenesisBlock = errors.New("missing genesis block")
)

var (
	_ Pruner = noPruner{}
	_ Pruner = (*pruner)(nil)
)

type Pruner interface {
	Stop()
	ConsumeTrieAtRoot(root common.Hash)
}

type pruner struct {
	stop              int32
	triedb            *trie.Database
	diskdb            ethdb.Database
	snapTree          *snapshot.Tree
	wg                sync.WaitGroup
	stopCh            chan struct{}
	signalCh          chan struct{}
	interceptDatabase *rtprunerutils.InterceptDatabase
	trieBuilder       *trieBuilder
	triePrunerLock    sync.Mutex
	triePruner        *triePruner
	prunerLock        rtprunerutils.Locker
	prunerTickTime    *time.Ticker
}

func New(triedb *trie.Database, diskdb ethdb.Database, snapTree *snapshot.Tree) (Pruner, error) {
	pruner, err := newPruner(triedb, diskdb, snapTree, true)
	if err != nil {
		return nil, err
	}

	pruner.Start()
	return pruner, nil
}

func newPruner(triedb *trie.Database, diskdb ethdb.Database, snapTree *snapshot.Tree, genesis bool) (*pruner, error) {
	interceptDb := rtprunerutils.GetInterceptDatabase(diskdb)

	if interceptDb == nil {
		return nil, errNoIntercept
	}

	signalCh := make(chan struct{})
	signal := rtprunerutils.NewSignal(signalCh)

	trieBuilder, err := newTrieBuilder(triedb, diskdb, snapTree, genesis, interceptDb)
	if err != nil {
		return nil, err
	}

	npruner := &pruner{
		signalCh:          signalCh,
		triedb:            triedb,
		diskdb:            diskdb,
		snapTree:          snapTree,
		stopCh:            make(chan struct{}),
		interceptDatabase: interceptDb,
		trieBuilder:       trieBuilder,
		prunerTickTime:    time.NewTicker(tickTime),
	}

	triedb.Locker().RegisterSignal(signal)
	snapTree.Locker().RegisterSignal(signal)

	return npruner, nil
}

// Start starts the blockPruneThread and triePruneThread
// in a goroutine
func (p *pruner) Start() {
	p.wg.Add(1)
	go p.triePruneThread()
}

// reset stops the interceptDatabase capture and resets
// the trie builder
func (p *pruner) reset() {
	p.interceptDatabase.CaptureStop()
	p.trieBuilder.reset()
}

// Stop stops the trie build and pruning processes.
// Blocking call, waits until all goroutines have been stopped.
func (p *pruner) Stop() {
	p.trieBuilder.stop()
	p.triePrunerLock.Lock()
	if p.triePruner != nil {
		p.triePruner.stop()
	}
	p.triePrunerLock.Unlock()

	noSignal := rtprunerutils.NewNoSignal()
	p.triedb.Locker().RegisterSignal(noSignal)
	p.snapTree.Locker().RegisterSignal(noSignal)

	close(p.stopCh)
	p.prunerTickTime.Stop()
	atomic.StoreInt32(&p.stop, 1)
	p.wg.Wait()
}

// isProcessStop returns whether the pruner has been stopped
func (p *pruner) isProcessStop() bool {
	return atomic.LoadInt32(&p.stop) != 0
}

// triePruneThread sets up triggers for start/stop and kicks
// off trie pruning. This function is expected to be triggered
// in a goroutine as it uses p.wg to notify when done.
func (p *pruner) triePruneThread() {
	defer func() {
		p.wg.Done()
	}()

	lastSignal := time.Now()
	for {
		select {
		case <-p.stopCh:
			return
		case <-p.signalCh:
			lastSignal = time.Now()
		case <-p.prunerTickTime.C:
			// force the tick to reset [tickTime] to clear a longer tick set when pruning was finished
			// if buildTrieAndPrune finishes without stopping, the pruneTickTime is set to [pruneAfter]
			p.prunerTickTime.Reset(tickTime)
			if !p.snapTree.Generating() && time.Since(lastSignal) > pruneDelta {
				p.buildTrieAndPrune()
			}
		}
	}
}

// buildTrieAndPrune initialises trieBuilder, builds trie
// and starts pruning. Resets the pruner and returns in
// case of an error
func (p *pruner) buildTrieAndPrune() {
	if err := p.trieBuilder.init(); err != nil {
		p.reset()
		if err != errNoPrunableStateRoot {
			// some other error
			log.Error("error initialising trie builder", err)
		}
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := p.trieBuilder.buildTrie(ctx, cancel); err != nil {
		cancel()
		p.reset()
		log.Error("error building trie", "err", err)
		return
	}

	// trie is scraped and additional updates are captured in the interceptor
	// it's ok to start pruning the db.
	if err := p.pruneTrie(); err != nil {
		cancel()
		p.reset()
		log.Error("prune error", "err", err)
	}
}

// pruneTrie prunes trie using triePruner
// resets the pruner state when it is not interrupted
// and trie pruning is complete
func (p *pruner) pruneTrie() error {
	p.triePrunerLock.Lock()
	defer p.triePrunerLock.Unlock()

	// lazy init because we rely on [p.trieBuilder.trieStatus]
	// and we get call here when [trieBuilder] is done building the trie
	p.triePruner = newTriePruner(p.snapTree, p.triedb, p.interceptDatabase, p.prunerLock)

	stopped, err := p.triePruner.prune(p.trieBuilder.trieStatus)
	if err != nil {
		return err
	}

	// we pruned (and not stopped) reset for next prune.
	if !stopped {
		p.prunerTickTime.Reset(pruneAfter)
		p.reset()
	}

	return nil
}

// ConsumeTrieAtRoot inserts the trie nodes starting at [root]
// from the dirties cache in the triedb into the bloom
func (p *pruner) ConsumeTrieAtRoot(root common.Hash) {
	/*
		the only time the intercept gets called is when the
		trie is Committed.

		This code does a memory Commit of the trie node at that root
		into the bloom at the time the block is inserted.

		And the reason we need to do this, is because at some point
		the block's root might become un-referenced.
		This would cause a delete in the dirties.
		We will miss that trie node because it was never committed
		into the disk and captured by the interceptor.

		The pruner will end up deleting the trie node from the disk,
		it won't know that trie node shouldn't be removed.
	*/
	if !p.trieBuilder.initialised() {
		return
	}

	p.trieBuilder.stateBloom.Put(root[:], root[:])
	p.triedb.IterateNodeAtRoot(root, func(hash common.Hash) {
		p.trieBuilder.stateBloom.Put(hash[:], hash[:])
	})
}
