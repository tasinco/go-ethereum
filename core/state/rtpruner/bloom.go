package rtpruner

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	bloomfilter "github.com/holiman/bloomfilter/v2"
)

const (
	bloomHasherSize = 8
)

var _ ethdb.KeyValueWriter = (*stateBloom)(nil)

type stateBloom struct {
	bloom *bloomfilter.Filter
}

// newStateBloomWithSize size is in MB
func newStateBloomWithSize(size uint64) (*stateBloom, error) {
	// m = size in bits.
	bloom, err := bloomfilter.New(size*1024*1024*8, 4)
	if err != nil {
		return nil, err
	}
	return &stateBloom{bloom: bloom}, nil
}

// Put implements the KeyValueWriter interface. But here only the key is needed.
func (bloom *stateBloom) Put(key []byte, _ []byte) error {
	// If the key length is not 32bytes, ensure it's contract code
	// entry with new scheme.
	if len(key) != common.HashLength {
		isCode, codeKey := rawdb.IsCodeKey(key)
		if !isCode {
			return nil
		}
		bloom.bloom.Add(stateBloomHasher(codeKey))
		return nil
	}
	bloom.bloom.Add(stateBloomHasher(key))
	return nil
}

// Delete removes the key from the key-value data store.
func (bloom *stateBloom) Delete(_ []byte) error {
	return nil
}

// Contain is the wrapper of the underlying contains function which
// reports whether the key is contained.
// - If it says yes, the key may be contained
// - If it says no, the key is definitely not contained.
func (bloom *stateBloom) Contain(key []byte) bool {
	return bloom.bloom.Contains(stateBloomHasher(key))
}

type stateBloomHasher []byte

func (f stateBloomHasher) Write(_ []byte) (n int, err error) { panic("not implemented") }
func (f stateBloomHasher) Sum(_ []byte) []byte               { panic("not implemented") }
func (f stateBloomHasher) Reset()                            { panic("not implemented") }
func (f stateBloomHasher) BlockSize() int                    { panic("not implemented") }
func (f stateBloomHasher) Size() int                         { return bloomHasherSize }
func (f stateBloomHasher) Sum64() uint64                     { return binary.BigEndian.Uint64(f) }
