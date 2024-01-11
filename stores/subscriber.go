package stores

import (
	"math"
	"sync"

	"go.sia.tech/core/chain"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// TODO: add support for v2 transactions and elements

var _ chain.Subscriber = (*chainSubscriber)(nil)

type (
	chainSubscriber struct {
		db            *gorm.DB
		tip           types.ChainIndex
		logger        *zap.SugaredLogger
		walletAddress types.Address

		// buffered state
		mu             sync.Mutex
		contractState  map[types.Hash256]contractState
		knownContracts map[types.FileContractID]struct{}
		outputs        []outputChange
		transactions   []txnChange
		proofs         map[types.Hash256]uint64
		revisions      map[types.Hash256]revisionUpdate
	}
)

func (cs *chainSubscriber) isKnownContract(id types.FileContractID) bool {
	_, ok := cs.knownContracts[id]
	return ok
}

func (cs *chainSubscriber) ProcessChainApplyUpdate(cau *chain.ApplyUpdate, mayCommit bool) error {
	panic("implement me")
}

func (cs *chainSubscriber) ProcessChainRevertUpdate(cru *chain.RevertUpdate) error {
	panic("implement me")
}

func (cs *chainSubscriber) commit() error {
	panic("implement me")
}

func (cs *chainSubscriber) processChainApplyUpdateHostDB(cau *chain.ApplyUpdate) {
	panic("implement me")
}

func (cs *chainSubscriber) processChainApplyUpdateContracts(cau *chain.ApplyUpdate) {
	// handle v1 contracts
	cau.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved, valid bool) {
		// TODO: consider valid?

		// ignore irrelevant contracts
		if !cs.isKnownContract(types.FileContractID(fce.ID)) {
			return
		}

		// 'pending' -> 'active'
		if cs.contractState[fce.ID] < contractStateActive {
			cs.contractState[fce.ID] = contractStateActive // 'pending' -> 'active'
			cs.logger.Infow("contract state changed: pending -> active",
				"fcid", fce.ID,
				"reason", "contract confirmed")
		}

		// renewed: 'active' -> 'complete'
		if rev != nil {
			cs.revisions[fce.ID] = revisionUpdate{
				height: cau.State.Index.Height,
				number: rev.FileContract.RevisionNumber,
				size:   rev.FileContract.Filesize,
			}
			if rev.FileContract.RevisionNumber == math.MaxUint64 && rev.FileContract.Filesize == 0 {
				cs.contractState[rev.ID] = contractStateComplete // renewed: 'active' -> 'complete'
				cs.logger.Infow("contract state changed: active -> complete",
					"fcid", rev.ID,
					"reason", "final revision confirmed")
			}
		}

		// storage proof: 'active' -> 'complete'
		if resolved {
			cs.proofs[rev.ID] = cau.State.Index.Height
			cs.contractState[rev.ID] = contractStateComplete // storage proof: 'active' -> 'complete'
			cs.logger.Infow("contract state changed: active -> complete",
				"fcid", rev.ID,
				"reason", "storage proof confirmed")
		}
	})

	// TODO: handle v2 contracts
}

func (cs *chainSubscriber) processChainRevertUpdateContracts(cau *chain.RevertUpdate) {
	cau.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved, valid bool) {
		// TODO: consider valid?

		// ignore irrelevant contracts
		if !cs.isKnownContract(types.FileContractID(fce.ID)) {
			return
		}

		// 'active' -> 'pending'
		if rev == nil {
			cs.contractState[fce.ID] = contractStatePending
		}

		// reverted renewal: 'complete' -> 'active'
		if rev != nil {
			// TODO: figure out if we can actually revert the revision here
			if rev.FileContract.RevisionNumber == math.MaxUint64 && rev.FileContract.Filesize == 0 {
				cs.contractState[rev.ID] = contractStateActive
				cs.logger.Infow("contract state changed: complete -> active",
					"fcid", rev.ID,
					"reason", "final revision reverted")
			}
		}

		// reverted storage proof: 'complete' -> 'active'
		if resolved {
			cs.contractState[rev.ID] = contractStateActive // revert from 'complete' to 'active'
			cs.logger.Infow("contract state changed: complete -> active",
				"fcid", rev.ID,
				"reason", "storage proof reverted")
		}
	})
}

func (cs *chainSubscriber) processChainApplyUpdateWallet(cau *chain.ApplyUpdate) {
	cau.ForEachSiacoinElement(func(sce types.SiacoinElement, spent bool) {
		// TODO: consider spent?

		// ignore irrelevant outputs
		if sce.SiacoinOutput.Address != cs.walletAddress {
			return
		}

		cs.outputs = append(cs.outputs, outputChange{
			addition: true,
			oid:      hash256(sce.ID),
			sco: dbSiacoinElement{
				Address:        hash256(sce.SiacoinOutput.Address),
				Value:          currency(sce.SiacoinOutput.Value),
				OutputID:       hash256(sce.ID),
				MaturityHeight: sce.MaturityHeight,
			},
		})

		// TODO: create fake transaction for matured siacoin output

		// TODO: create transactions
	})
	panic("implement me")
}

func (cs *chainSubscriber) processChainRevertUpdateWallet(cau *chain.ApplyUpdate) {
	cau.ForEachSiacoinElement(func(sce types.SiacoinElement, spent bool) {
		// TODO: consider spent?

		// ignore irrelevant outputs
		if sce.SiacoinOutput.Address != cs.walletAddress {
			return
		}

		cs.outputs = append(cs.outputs, outputChange{
			addition: false,
			oid:      hash256(sce.ID),
		})

		// TODO: remove fake transaction for no longer matured siacoin output

		// TODO: remove transactions
	})
	panic("implement me")
}
