package rtpruner

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/core/types"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rtprunerutils"
	"github.com/ethereum/go-ethereum/trie"
)

func TestIntercept(t *testing.T) {
	cmdb := memorydb.New()
	db := rawdb.NewDatabase(cmdb)

	db = rtprunerutils.NewInterceptDatabase(db)

	dbst := state.NewDatabaseWithConfig(db, &trie.Config{})

	interceptDb := db.(*rtprunerutils.InterceptDatabase)
	if interceptDb == nil {
		t.Fatal("intercept not created")
	}

	statedb1, _ := state.New(common.Hash{}, dbst, nil)

	rh := common.BytesToAddress([]byte("addr1"))
	statekey := common.BytesToHash([]byte("statek1"))
	stateval := common.BytesToHash([]byte("stakev1"))

	ch1 := []byte("codehash1")
	ch2 := []byte("codehash2")

	statedb1.CreateAccount(rh)
	statedb1.SetBalance(rh, big.NewInt(3))
	statedb1.SetNonce(rh, 8)
	statedb1.SetState(rh, statekey, stateval)

	statedb1.SetCode(rh, ch1)

	cmdbIntercept := memorydb.New()
	interceptDb.CaptureStart(cmdbIntercept)

	root1, _ := statedb1.Commit(true)
	statedb1.Database().TrieDB().Commit(root1, false)

	// only 1 acct root
	items, accts, codes := toMap(cmdbIntercept)
	assertItems(t, items, []string{"24cb66d1c90ed17cbb6789e62de94e270f6f42b15e49b61495f0c02fc07a5ccd", "162681b4c56c782f9b2c9807d41351e5d11bcc59adee890c35c5485c34b1e024"})
	assertAccts(t, accts, nil)
	assertItems(t, codes, []string{"75f2c2ffbb6dbb3514950d24e6592e4d4fad3f7762f95f61783d20485962b561"})

	cmdbIntercept = memorydb.New()
	interceptDb.CaptureStart(cmdbIntercept)

	statedb2, _ := state.New(root1, dbst, nil)
	statedb2.SetBalance(rh, big.NewInt(4))
	statedb2.SetCode(rh, ch2)

	root2, _ := statedb2.Commit(true)
	statedb2.Database().TrieDB().Commit(root2, false)

	items, accts, codes = toMap(cmdbIntercept)
	assertItems(t, items, []string{"bd83bbd17efa1f9b68d9c3f6422479056a94e943fd6fb87dc23b815e04ecf0e7"})
	assertAccts(t, accts, nil)
	assertItems(t, codes, []string{"035ed7b39b34bd8b12143b7da122b1895d667fb5e5c0f71a7afa7b5d57229085"})
}

func TestPrune(t *testing.T) {
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

	statedb1.CreateAccount(rh)
	statedb1.SetBalance(rh, big.NewInt(3))
	statedb1.SetNonce(rh, 8)
	statedb1.SetState(rh, statekey, stateval)

	statedb1.SetCode(rh, ch1)

	root1, _ := statedb1.Commit(true)
	statedb1.Database().TrieDB().Commit(root1, false)

	// only 1 acct root
	items, accts, codes := toMap(cmdb)
	assertItems(t, items, []string{"24cb66d1c90ed17cbb6789e62de94e270f6f42b15e49b61495f0c02fc07a5ccd"})
	assertAccts(t, accts, []uint64{3})
	assertCodes(t, codes, [][]byte{ch1})

	snapConfig := snapshot.Config{
		CacheSize:  256,
		Recovery:   true,
		NoBuild:    false,
		AsyncBuild: false,
	}

	snaps, err := snapshot.New(snapConfig, db, dbst.TrieDB(), root1)
	if err != nil {
		t.Fatal(err)
	}
	pruner, _ := newPruner(dbst.TrieDB(), db, snaps, false)

	performPrune(t, pruner)

	statedb2, _ := state.New(root1, dbst, snaps)
	statedb2.SetBalance(rh, big.NewInt(4))
	statedb2.SetCode(rh, ch2)

	root2, _ := statedb2.Commit(true)
	statedb2.Database().TrieDB().Commit(root2, false)

	// two account roots
	items, accts, codes = toMap(cmdb)
	assertItems(t, items, []string{"24cb66d1c90ed17cbb6789e62de94e270f6f42b15e49b61495f0c02fc07a5ccd"})
	assertAccts(t, accts, []uint64{3, 4})
	assertCodes(t, codes, [][]byte{ch1, ch2})

	_ = snaps.Cap(root2, 0)

	performPrune(t, pruner)

	// pruned old acct root
	items, accts, codes = toMap(cmdb)

	assertItems(t, items, []string{"24cb66d1c90ed17cbb6789e62de94e270f6f42b15e49b61495f0c02fc07a5ccd"})
	assertAccts(t, accts, []uint64{4})
	assertCodes(t, codes, [][]byte{ch2})
}

func performPrune(t *testing.T, pruner *pruner) {
	err := pruner.trieBuilder.init()
	assert.NoError(t, err)

	// kick off the trie creation.
	pruner.buildTrieAndPrune()

	// wait for the bloom to be created
	for {
		if pruner.trieBuilder.isBloomCreated() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// perform the db prune (after the bloom is creaed)
	pruner.buildTrieAndPrune()
}

func assertItems(t *testing.T, items map[common.Hash][]byte, required []string) {
	if len(items) != len(required) {
		t.Fatal("items count invalid")
	}
	for _, need := range required {
		needbyte, err := hex.DecodeString(need)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := items[common.BytesToHash(needbyte)]; !ok {
			t.Fatalf("missing %s", need)
		}
	}
}

func assertAccts(t *testing.T, accts map[common.Hash]types.StateAccount, required []uint64) {
	assert.Equal(t, len(required), len(accts))

	for _, need := range required {
		needbalance := fmt.Sprintf("%d", need)
		found := false
		for _, v := range accts {
			if v.Balance.String() == needbalance {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing %s", needbalance)
		}
	}
}

func assertCodes(t *testing.T, codes map[common.Hash][]byte, required [][]byte) {
	if len(codes) != len(required) {
		t.Fatal("items count invalid")
	}
	for _, need := range required {
		found := false
		for _, v := range codes {
			if bytes.Equal(v, need) {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing %s", string(need))
		}
	}
}

func shortToAccount(raw []byte) (types.StateAccount, error) {
	elems, _, _ := rlp.SplitList(raw)
	_, rest, _ := rlp.SplitString(elems)
	val, _, _ := rlp.SplitString(rest)
	sa, err := types.FullAccount(val)
	if err != nil {
		return types.StateAccount{}, err
	}
	return *sa, err
}

func toMap(cmdb *memorydb.Database) (map[common.Hash][]byte, map[common.Hash]types.StateAccount, map[common.Hash][]byte) {
	res := make(map[common.Hash][]byte)
	ares := make(map[common.Hash]types.StateAccount)
	cres := make(map[common.Hash][]byte)

	itr := cmdb.NewIterator(nil, nil)
	for itr.Next() {
		k := itr.Key()
		isCode, codeKey := rawdb.IsCodeKey(k)
		if len(k) == common.HashLength || isCode {
			if isCode {
				cres[common.BytesToHash(codeKey)] = common.CopyBytes(itr.Value())
			} else {
				a, err := shortToAccount(itr.Value())
				if err == nil {
					ares[common.BytesToHash(k)] = a
				} else {
					res[common.BytesToHash(k)] = common.CopyBytes(itr.Value())
				}
			}
		}
	}
	return res, ares, cres
}
