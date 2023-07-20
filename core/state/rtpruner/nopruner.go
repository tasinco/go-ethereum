package rtpruner

import "github.com/ethereum/go-ethereum/common"

func NewNoPruner() Pruner { return &noPruner{} }

type noPruner struct{}

func (n noPruner) Stop()                              {}
func (n noPruner) ConsumeTrieAtRoot(root common.Hash) {}
