package rtpruner

import (
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	utils "github.com/ethereum/go-ethereum/rtprunerutils"
	"github.com/ethereum/go-ethereum/trie"
)

type triePruner struct {
	stopCh            chan struct{}
	snapTree          *snapshot.Tree
	trieDB            *trie.Database
	interceptDatabase *utils.InterceptDatabase
	prunerLock        utils.Locker
}

func newTriePruner(snapTree *snapshot.Tree, trieDB *trie.Database, interceptDatabase *utils.InterceptDatabase, prunerLock utils.Locker) *triePruner {
	return &triePruner{snapTree: snapTree, trieDB: trieDB, interceptDatabase: interceptDatabase, prunerLock: prunerLock, stopCh: make(chan struct{})}
}

func (p *triePruner) stop() {
	select {
	case <-p.stopCh:
		// channel is closed, this is executed
	default:
		// channel is still open, the previous case is not executed
		close(p.stopCh)
	}
}

func (p *triePruner) isRequestStop() bool {
	select {
	case <-p.stopCh:
		return true
	default:
	}
	return p.snapTree.Locker().Waiting() > 0 ||
		p.trieDB.Locker().Waiting() > 0 ||
		p.interceptDatabase.Locker().Waiting() > 0 ||
		p.prunerLock.Waiting() > 0
}

func (p *triePruner) prune(trieStatus *trieStatusContainer) (bool, error) {
	p.prunerLock.ExclusiveLock()
	defer p.prunerLock.ExclusiveUnlock()

	trieStatus.stateBloomLock.Lock()
	defer trieStatus.stateBloomLock.Unlock()

	p.snapTree.Locker().ExclusiveLock()
	defer p.snapTree.Locker().ExclusiveUnlock()

	p.trieDB.Locker().ExclusiveLock()
	defer p.trieDB.Locker().ExclusiveUnlock()

	p.interceptDatabase.Locker().ExclusiveLock()
	defer p.interceptDatabase.Locker().ExclusiveUnlock()

	// something bad happened in the intercept, we need to give up for now...
	if p.interceptDatabase.Err() != nil {
		return true, p.interceptDatabase.Err()
	}

	lastcnt := atomic.LoadUint64(&trieStatus.count)

	stopped, err := p.pruneThreadWorker(trieStatus)
	if err != nil {
		return true, err
	}

	if lastcnt != atomic.LoadUint64(&trieStatus.count) {
		log.Info("pruned state",
			"stopped", stopped,
			"nodes", atomic.LoadUint64(&trieStatus.count),
			"size", common.StorageSize(atomic.LoadUint64(&trieStatus.size)),
			"processed", atomic.LoadUint64(&trieStatus.rowsProcessed),
		)
	}

	return stopped, nil
}

func (p *triePruner) pruneThreadWorker(trieStatus *trieStatusContainer) (bool, error) {
	batch := p.interceptDatabase.NewBatchNoLock()
	iter := p.interceptDatabase.NewIteratorNoLock(nil, trieStatus.key)
	defer iter.Release()

	var (
		rowsProcessed uint64 = 0
		dataProcessed uint64 = 0

		stopped = false
		lastkey []byte
	)

	for iter.Next() {
		rowsProcessed++
		if p.isRequestStop() && rowsProcessed > 2 {
			stopped = true
			break
		}

		key := iter.Key()
		lastkey = common.CopyBytes(key)

		// All state entries don't belong to specific state and genesis are deleted here
		// - trie node
		// - legacy contract code
		// - new-scheme contract code
		isCode, codeKey := rawdb.IsCodeKey(key)
		if len(key) == common.HashLength || isCode {
			checkKey := key
			if isCode {
				checkKey = codeKey
			}

			isDirty, err := p.trieDB.NodeIsDirty(common.BytesToHash(checkKey))
			if err != nil {
				return true, err
			}
			if !(trieStatus.stateBloom.Contain(checkKey) || isDirty) {
				dataProcessed += uint64(len(key))
				dataProcessed += uint64(len(iter.Value()))

				err := batch.Delete(key)
				if err != nil {
					return true, err
				}

				if batch.ValueSize() >= ethdb.IdealBatchSize {
					if err = batch.WriteNoLock(); err != nil {
						return true, err
					}
					batch.ResetNoLock()
					batch = p.interceptDatabase.NewBatchNoLock()

					atomic.AddUint64(&trieStatus.rowsProcessed, rowsProcessed)
					trieStatus.inc(dataProcessed)
					rowsProcessed = 0
					dataProcessed = 0

					iter.Release()
					iter = p.interceptDatabase.NewIteratorNoLock(nil, key)
					trieStatus.key = key
				}
			}
		}
		if batch.ValueSize() == 0 {
			trieStatus.key = key
		}
	}

	if iter.Error() != nil {
		return true, iter.Error()
	}

	if batch.ValueSize() > 0 {
		if err := batch.WriteNoLock(); err != nil {
			return true, err
		}
		atomic.AddUint64(&trieStatus.rowsProcessed, rowsProcessed)
		trieStatus.inc(dataProcessed)
		trieStatus.key = lastkey
	}

	return stopped, nil
}
