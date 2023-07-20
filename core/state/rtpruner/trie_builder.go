package rtpruner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rtprunerutils"
	"github.com/ethereum/go-ethereum/trie"
)

const (
	// bloomSize in MBs
	bloomSize uint64 = 64
)

var (
	errContractCodeRead    = errors.New("failed to read contract code")
	errStoping             = fmt.Errorf("stoping")
	errNoPrunableStateRoot = fmt.Errorf("no prunable root")
)

type trieBuilder struct {
	stopCh            chan struct{}
	inited            bool
	trieStatus        *trieStatusContainer
	triedb            *trie.Database
	diskdb            ethdb.Database
	snapTree          *snapshot.Tree
	genesisMemoryDb   *memorydb.Database
	stateBloom        *stateBloom
	stateRoot         common.Hash
	trieComplete      bool
	interceptDatabase *rtprunerutils.InterceptDatabase
}

func newTrieBuilder(triedb *trie.Database, diskdb ethdb.Database, snapTree *snapshot.Tree, includeGenesis bool, interceptDatabase *rtprunerutils.InterceptDatabase) (*trieBuilder, error) {
	genesisMemoryDb := memorydb.New()
	if includeGenesis {
		if err := extractGenesis(diskdb, 0, genesisMemoryDb); err != nil {
			return nil, err
		}
	}

	return &trieBuilder{
		stopCh:            make(chan struct{}),
		triedb:            triedb,
		diskdb:            diskdb,
		snapTree:          snapTree,
		genesisMemoryDb:   genesisMemoryDb,
		interceptDatabase: interceptDatabase,
	}, nil
}

func (p *trieBuilder) isStopped() bool {
	select {
	case <-p.stopCh:
		return true
	default:
		return false
	}
}

func (p *trieBuilder) stop() {
	select {
	case <-p.stopCh:
	default:
		close(p.stopCh)
	}
}

// Reset resets the internal state to allow Init and BuildTrie calls
// for rebuilding the trie into the bloom
func (p *trieBuilder) reset() {
	p.inited = false
	p.trieComplete = false
}

func (p *trieBuilder) initialised() bool {
	return p.inited
}

func (p *trieBuilder) isBloomCreated() bool {
	p.trieStatus.stateBloomLock.Lock()
	defer p.trieStatus.stateBloomLock.Unlock()
	return p.trieStatus.stateBloom != nil
}

// Init initialises the [trieBuilder]
// No effect if initialised already or if this builder has finished
// building the trie
// Call reset() to clear state and then Init again to re-initialise
func (p *trieBuilder) init() error {
	// no init if we've already initialised or trie build is complete
	// this will be nil when reset is called or on first init
	if p.inited || p.trieComplete {
		return nil
	}

	p.snapTree.Locker().ExclusiveLock()
	defer p.snapTree.Locker().ExclusiveUnlock()

	p.stateRoot = p.snapTree.DiskRootNoLock()
	//p.stateRoot = p.snapTree.DiskLayer()
	// no root was prune-able, skip
	if p.stateRoot == (common.Hash{}) {
		return errNoPrunableStateRoot
	}

	p.triedb.Locker().ExclusiveLock()
	defer p.triedb.Locker().ExclusiveUnlock()

	p.trieStatus = newTrieStatus()

	// make sure this root is on the disk.
	if err := p.triedb.CommitNoLock(p.stateRoot, false); err != nil {
		log.Error("prune error commit", "err", err)
		return err
	}

	var err error
	p.stateBloom, err = newStateBloomWithSize(bloomSize)
	if err != nil {
		log.Error("prune error bloom", "err", err)
		return err
	}

	// tell the intercept db to start capturing updates
	p.interceptDatabase.CaptureStart(p.stateBloom)

	p.inited = true
	return nil
}

// BuildTrie builds trie from given [root] into the [stateBloom]
// If [trieBuilder] has been previously stopped and is incomplete,
// calling this function will resume it
// If this builder has previously finished building trie, calling
// this function will do nothing
// Call reset() to clear state and then Init and BuildTrie to build trie again
func (p *trieBuilder) buildTrie(ctx context.Context, cancel context.CancelFunc) error {
	// if we've already built the trie, do nothing
	if p.trieComplete {
		return nil
	}

	if err := p.iterateTrieAtRoot(ctx, p.triedb, p.diskdb, p.stateRoot, p.stateBloom); err != nil {
		return err
	}

	// put in the genesis trie to keep it from pruning..
	genesisMemoryIter := p.genesisMemoryDb.NewIterator(nil, nil)
	defer genesisMemoryIter.Release()
	for genesisMemoryIter.Next() {
		if err := p.stateBloom.Put(genesisMemoryIter.Key(), genesisMemoryIter.Value()); err != nil {
			return err
		}
	}

	// set the state bloom indicating the trie iteration of this root is complete.
	p.trieStatus.stateBloomLock.Lock()
	p.trieStatus.stateBloom = p.stateBloom
	p.trieStatus.stateBloomLock.Unlock()

	p.trieComplete = true

	return nil
}

func (p *trieBuilder) iterateTrieAtRoot(ctx context.Context, triedb *trie.Database, src ethdb.Database, root common.Hash, dst ethdb.KeyValueWriter) error {
	var ipos uint64 = 0
	var slots uint64 = 0
	var codes uint64 = 0

	dstf := func(owner common.Hash, path []byte, hash common.Hash, blob []byte) {
		dst.Put(hash[:], blob)
	}

	accTrie, err := trie.NewStateTrie(trie.StateTrieID(root), triedb)
	if err != nil {
		return err
	}
	t := trie.NewStackTrie(dstf)

	logTick := time.Now()

	itx, err := accTrie.NodeIterator(nil)
	if err != nil {
		return err
	}

	accIt := trie.NewIterator(itx)

	errg, _ := errgroup.WithContext(ctx)
	errg.SetLimit(500)

	locIsStopped := func() bool {
		if p.isStopped() {
			return true
		}
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	}

	for accIt.Next() {
		if locIsStopped() {
			return errStoping
		}

		ipos++

		if time.Since(logTick) > 10*time.Second {
			log.Info("iterate trie", "accounts", ipos, "slots", slots, "codes", codes)
			logTick = time.Now()
		}

		// Retrieve the current account and flatten it into the internal format
		accountHash := common.BytesToHash(accIt.Key)
		acctVal := append([]byte{}, accIt.Value...)

		errg.Go(func() error {
			var accb types.StateAccount
			if err = rlp.DecodeBytes(acctVal, &accb); err != nil {
				return err
			}

			if !bytes.Equal(accb.CodeHash, emptyCode[:]) {
				codes++
				codeHash := common.BytesToHash(accb.CodeHash)
				code := rawdb.ReadCode(src, codeHash)
				if len(code) == 0 {
					return errContractCodeRead
				}
				rawdb.WriteCode(dst, codeHash, code)
			}

			if accb.Root != types.EmptyRootHash {
				storeTrie, err := trie.NewStateTrie(trie.StateTrieID(accb.Root), triedb)
				if err != nil {
					return err
				}

				t2 := trie.NewStackTrie(dstf)

				ix, err := storeTrie.NodeIterator(nil)
				if err != nil {
					return err
				}

				storeIt := trie.NewIterator(ix)
				for storeIt.Next() {
					if locIsStopped() {
						return errStoping
					}

					slots++
					storehash := common.BytesToHash(storeIt.Key)
					if err = t2.Update(storehash[:], storeIt.Value); err != nil {
						return err
					}
				}
				if _, err = t2.Commit(); err != nil {
					return err
				}
			}

			return nil
		})

		if err = t.Update(accountHash[:], acctVal); err != nil {
			return err
		}
	}

	if err := errg.Wait(); err != nil {
		return err
	}

	if locIsStopped() {
		return errStoping
	}

	if _, err = t.Commit(); err != nil {
		return err
	}

	log.Info("iterated trie", "accounts", ipos, "slots", slots, "codes", codes)

	return nil
}

func extractGenesis(db ethdb.Database, idx uint64, stateBloom ethdb.KeyValueWriter) error {
	genesisHash := rawdb.ReadCanonicalHash(db, idx)
	if genesisHash == (common.Hash{}) {
		return errMissingGenesisHash
	}

	genesis := rawdb.ReadBlock(db, genesisHash, idx)
	if genesis == nil {
		return errMissingGenesisBlock
	}

	t, err := trie.NewStateTrie(trie.StateTrieID(genesis.Root()), trie.NewDatabase(db))
	if err != nil {
		return err
	}

	accIter, err := t.NodeIterator(nil)
	if err != nil {
		return err
	}
	for accIter.Next(true) {
		hash := accIter.Hash()

		// Embedded nodes don't have hash.
		if hash != (common.Hash{}) {
			if err = stateBloom.Put(hash.Bytes(), []byte("")); err != nil {
				return err
			}
		}
		// If it's a leaf node, yes we are touching an account,
		// dig into the storage trie further.
		if accIter.Leaf() {
			var acc types.StateAccount
			lb := accIter.LeafBlob()
			if err = rlp.DecodeBytes(lb, &acc); err != nil {
				return err
			}
			if acc.Root != types.EmptyRootHash {
				storageTrie, err := trie.NewStateTrie(trie.StateTrieID(acc.Root), trie.NewDatabase(db))
				if err != nil {
					return err
				}
				storageIter, err := storageTrie.NodeIterator(nil)
				if err != nil {
					return err
				}
				for storageIter.Next(true) {
					hash := storageIter.Hash()
					if hash != (common.Hash{}) {
						if err = stateBloom.Put(hash.Bytes(), []byte("")); err != nil {
							return err
						}
					}
				}
				if storageIter.Error() != nil {
					return storageIter.Error()
				}
			}
			if !bytes.Equal(acc.CodeHash, emptyCode[:]) {
				if err = stateBloom.Put(acc.CodeHash, []byte("")); err != nil {
					return err
				}
			}
		}
	}
	return accIter.Error()
}
