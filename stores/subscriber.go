package stores

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// TODO: add support for v2 transactions and elements

var _ chain.Subscriber = (*chainSubscriber)(nil)

type (
	chainSubscriber struct {
		db             *gorm.DB
		tip            types.ChainIndex
		logger         *zap.SugaredLogger
		retryIntervals []time.Duration
		walletAddress  types.Address

		// buffered state
		mu              sync.Mutex
		closed          bool
		lastSave        time.Time
		knownContracts  map[types.FileContractID]struct{}
		persistInterval time.Duration
		persistTimer    *time.Timer

		announcements []announcement
		contractState map[types.Hash256]contractState
		hosts         map[types.PublicKey]struct{}
		mayCommit     bool
		outputs       []outputChange
		proofs        map[types.Hash256]uint64
		revisions     map[types.Hash256]revisionUpdate
		transactions  []txnChange
	}
)

func (cs *chainSubscriber) Close() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.closed = true
	cs.persistTimer.Stop()
	select {
	case <-cs.persistTimer.C:
	default:
	}

	return nil
}

func (cs *chainSubscriber) ProcessChainApplyUpdate(cau *chain.ApplyUpdate, mayCommit bool) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// check for shutdown, ideally this never happens since the subscriber is
	// unsubscribed first and then closed
	if cs.closed {
		return errors.New("shutting down")
	}

	cs.processChainApplyUpdateHostDB(cau)
	cs.processChainApplyUpdateContracts(cau)
	cs.processChainApplyUpdateWallet(cau)

	cs.tip = cau.State.Index
	cs.mayCommit = mayCommit

	return cs.tryCommit()
}

func (cs *chainSubscriber) ProcessChainRevertUpdate(cru *chain.RevertUpdate) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// check for shutdown, ideally this never happens since the subscriber is
	// unsubscribed first and then closed
	if cs.closed {
		return errors.New("shutting down")
	}

	cs.processChainRevertUpdateHostDB(cru)
	cs.processChainRevertUpdateContracts(cru)
	cs.processChainRevertUpdateWallet(cru)

	cs.tip = cru.State.Index
	cs.mayCommit = true

	return cs.tryCommit()
}

func (cs *chainSubscriber) isKnownContract(id types.FileContractID) bool {
	_, ok := cs.knownContracts[id]
	return ok
}

func (cs *chainSubscriber) commit() error {
	// Fetch allowlist
	var allowlist []dbAllowlistEntry
	if err := cs.db.
		Model(&dbAllowlistEntry{}).
		Find(&allowlist).
		Error; err != nil {
		cs.logger.Error(fmt.Sprintf("failed to fetch allowlist, err: %v", err))
	}

	// Fetch blocklist
	var blocklist []dbBlocklistEntry
	if err := cs.db.
		Model(&dbBlocklistEntry{}).
		Find(&blocklist).
		Error; err != nil {
		cs.logger.Error(fmt.Sprintf("failed to fetch blocklist, err: %v", err))
	}

	err := cs.retryTransaction(func(tx *gorm.DB) (err error) {
		if len(cs.announcements) > 0 {
			if err = insertAnnouncements(tx, cs.announcements); err != nil {
				return fmt.Errorf("%w; failed to insert %d announcements", err, len(cs.announcements))
			}
		}
		if len(cs.hosts) > 0 && (len(allowlist)+len(blocklist)) > 0 {
			for host := range cs.hosts {
				if err := updateBlocklist(tx, host, allowlist, blocklist); err != nil {
					cs.logger.Error(fmt.Sprintf("failed to update blocklist, err: %v", err))
				}
			}
		}
		for fcid, rev := range cs.revisions {
			if err := applyRevisionUpdate(tx, types.FileContractID(fcid), rev); err != nil {
				return fmt.Errorf("%w; failed to update revision number and height", err)
			}
		}
		for fcid, proofHeight := range cs.proofs {
			if err := updateProofHeight(tx, types.FileContractID(fcid), proofHeight); err != nil {
				return fmt.Errorf("%w; failed to update proof height", err)
			}
		}
		for _, oc := range cs.outputs {
			if oc.addition {
				err = applyUnappliedOutputAdditions(tx, oc.sco)
			} else {
				err = applyUnappliedOutputRemovals(tx, oc.oid)
			}
			if err != nil {
				return fmt.Errorf("%w; failed to apply unapplied output change", err)
			}
		}
		for _, tc := range cs.transactions {
			if tc.addition {
				err = applyUnappliedTxnAdditions(tx, tc.txn)
			} else {
				err = applyUnappliedTxnRemovals(tx, tc.txnID)
			}
			if err != nil {
				return fmt.Errorf("%w; failed to apply unapplied txn change", err)
			}
		}
		for fcid, cs := range cs.contractState {
			if err := updateContractState(tx, types.FileContractID(fcid), cs); err != nil {
				return fmt.Errorf("%w; failed to update chain state", err)
			}
		}
		if err := markFailedContracts(tx, cs.tip.Height); err != nil {
			return err
		}
		return updateChainIndex(tx, cs.tip)
	})
	if err != nil {
		return fmt.Errorf("%w; failed to apply updates", err)
	}

	cs.announcements = nil
	cs.contractState = make(map[types.Hash256]contractState)
	cs.hosts = make(map[types.PublicKey]struct{})
	cs.mayCommit = false
	cs.outputs = nil
	cs.proofs = make(map[types.Hash256]uint64)
	cs.revisions = make(map[types.Hash256]revisionUpdate)
	cs.transactions = nil
	cs.lastSave = time.Now()
	return nil
}

// shouldCommit returns whether the subscriber should commit its buffered state.
func (cs *chainSubscriber) shouldCommit() bool {
	mayCommit := cs.mayCommit
	persistIntervalPassed := time.Since(cs.lastSave) > cs.persistInterval
	hasAnnouncements := len(cs.announcements) > 0
	hasRevisions := len(cs.revisions) > 0
	hasProofs := len(cs.proofs) > 0
	hasOutputChanges := len(cs.outputs) > 0
	hasTxnChanges := len(cs.transactions) > 0
	hasContractState := len(cs.contractState) > 0
	return mayCommit || persistIntervalPassed || hasAnnouncements || hasRevisions ||
		hasProofs || hasOutputChanges || hasTxnChanges || hasContractState
}

func (cs *chainSubscriber) tryCommit() error {
	// commit if we can/should
	if !cs.shouldCommit() {
		return nil
	} else if err := cs.commit(); err != nil {
		cs.logger.Errorw("failed to commit chain update", zap.Error(err))
	}

	// force a persist if no block has been received for some time
	cs.persistTimer = time.AfterFunc(10*time.Second, func() {
		cs.mu.Lock()
		defer cs.mu.Unlock()
		if cs.closed {
			return
		} else if err := cs.commit(); err != nil {
			cs.logger.Errorw("failed to commit delayed chain update", zap.Error(err))
		}
	})
	return nil
}

func (cs *chainSubscriber) processChainApplyUpdateHostDB(cau *chain.ApplyUpdate) {
	panic("implement me")
}

func (cs *chainSubscriber) processChainRevertUpdateHostDB(cru *chain.RevertUpdate) {
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

func (cs *chainSubscriber) processChainRevertUpdateContracts(cru *chain.RevertUpdate) {
	cru.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved, valid bool) {
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

func (cs *chainSubscriber) processChainRevertUpdateWallet(cru *chain.RevertUpdate) {
	cru.ForEachSiacoinElement(func(sce types.SiacoinElement, spent bool) {
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

func (cs *chainSubscriber) retryTransaction(fc func(tx *gorm.DB) error, opts ...*sql.TxOptions) error {
	return retryTransaction(cs.db, cs.logger, fc, cs.retryIntervals, opts...)
}
