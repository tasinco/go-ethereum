package rtpruner

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/rtprunerutils"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/stretchr/testify/assert"
)

func Test_TrieBuilder(t *testing.T) {
	cmdb := memorydb.New()
	db := rawdb.NewDatabase(cmdb)

	db = rtprunerutils.NewInterceptDatabase(db)

	dbst := state.NewDatabaseWithConfig(db, &trie.Config{})

	statedb1, _ := state.New(common.Hash{}, dbst, nil)

	rh := common.BytesToAddress([]byte("addr1"))
	statekey := common.BytesToHash([]byte("statek1"))
	stateval := common.BytesToHash([]byte("stakev1"))

	ch1 := []byte("codehash1")
	ch2 := []byte("codehash2")

	// first account
	statedb1.CreateAccount(rh)
	statedb1.SetBalance(rh, big.NewInt(3))
	statedb1.SetNonce(rh, 8)
	statedb1.SetState(rh, statekey, stateval)

	statedb1.SetCode(rh, ch1)

	root1, _ := statedb1.Commit(true)
	err := statedb1.Database().TrieDB().Commit(root1, false)
	assert.NoError(t, err)

	snapConfig := snapshot.Config{
		CacheSize:  256,
		Recovery:   true,
		NoBuild:    false,
		AsyncBuild: false,
	}

	// take snapshot
	trieDB := dbst.TrieDB()
	snaps, err := snapshot.New(snapConfig, db, trieDB, root1)
	if err != nil {
		t.Fatal(err)
	}

	//bh2 := common.BytesToHash([]byte("bh2"))

	// second account
	statedb2, _ := state.New(root1, dbst, snaps)
	statedb2.SetBalance(rh, big.NewInt(4))
	statedb2.SetCode(rh, ch2)

	// take snapshot
	root2, _ := statedb2.Commit(true)
	err = statedb2.Database().TrieDB().Commit(root2, false)
	assert.NoError(t, err)

	//_ = snaps.Flatten(bh2)

	builder, err := newTrieBuilder(trieDB, db, snaps, false, db.(*rtprunerutils.InterceptDatabase))
	assert.NoError(t, err)

	err = builder.init()
	assert.NoError(t, err)
	assert.NotNil(t, builder.trieStatus)

	ctx, cancel := context.WithCancel(context.Background())
	err = builder.buildTrie(ctx, cancel)
	cancel()
	assert.NoError(t, err)

	pruneLocker := rtprunerutils.Locker{}
	pruner := newTriePruner(snaps, trieDB, db.(*rtprunerutils.InterceptDatabase), pruneLocker)
	stopped, err := pruner.prune(builder.trieStatus)
	assert.NoError(t, err)

	assert.Equal(t, false, stopped) // pruner was not stopped during this run
	assert.Equal(t, uint64(1), builder.trieStatus.count)
}
