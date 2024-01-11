package stores

import (
	"go.sia.tech/core/chain"
	"go.sia.tech/core/types"
	"gorm.io/gorm"
)

var _ chain.Subscriber = (*chainSubscriber)(nil)

type (
	chainSubscriber struct {
		db  *gorm.DB
		tip types.ChainIndex
	}
)

func (cs *chainSubscriber) ProcessChainApplyUpdate(cau *chain.ApplyUpdate, mayCommit bool) error {
	panic("implement me")
}

func (cs *chainSubscriber) ProcessChainRevertUpdate(cru *chain.RevertUpdate) error {
	panic("implement me")
}

func (cs *chainSubscriber) processConsensusChangeHostDB(cau *chain.ApplyUpdate) {
	panic("implement me")
}

func (cs *chainSubscriber) processConsensusChangeContracts(cau *chain.ApplyUpdate) {
	panic("implement me")
}

func (cs *chainSubscriber) processConsensusChangeWallet(cau *chain.ApplyUpdate) {
	panic("implement me")
}
