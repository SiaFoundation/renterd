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

var _ chain.Subscriber = (*chainSubscriber)(nil)

type (
	chainSubscriber struct {
		announcementMaxAge time.Duration
		db                 *gorm.DB
		tip                types.ChainIndex
		logger             *zap.SugaredLogger
		retryIntervals     []time.Duration
		walletAddress      types.Address

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

func NewChainSubscriber(db *gorm.DB, logger *zap.SugaredLogger, intvls []time.Duration, persistInterval time.Duration, addr types.Address, ancmtMaxAge time.Duration) *chainSubscriber {
	return &chainSubscriber{
		announcementMaxAge: ancmtMaxAge,
		db:                 db,
		logger:             logger,
		retryIntervals:     intvls,
		walletAddress:      addr,
		lastSave:           time.Now(),
		persistInterval:    persistInterval,

		contractState: make(map[types.Hash256]contractState),
		hosts:         make(map[types.PublicKey]struct{}),
		proofs:        make(map[types.Hash256]uint64),
		revisions:     make(map[types.Hash256]revisionUpdate),
	}
}

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
	// TODO: handle wallet here

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
	// TODO: handle wallet here

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
	b := cau.Block
	if time.Since(b.Timestamp) > cs.announcementMaxAge {
		return // ignore old announcements
	}
	chain.ForEachAnnouncement(b, func(ha chain.Announcement) {
		if ha.NetAddress == "" {
			return // ignore
		}
		cs.announcements = append(cs.announcements, announcement{
			Announcement: ha,
			blockHeight:  cau.State.Index.Height,
			blockID:      b.ID(),
			timestamp:    b.Timestamp,
		})
		cs.hosts[types.PublicKey(ha.PublicKey)] = struct{}{}
	})
}

func (cs *chainSubscriber) processChainRevertUpdateHostDB(cru *chain.RevertUpdate) {
	// nothing to do, we are not unannouncing hosts
}

func (cs *chainSubscriber) processChainApplyUpdateContracts(cau *chain.ApplyUpdate) {
	type revision struct {
		revisionNumber uint64
		fileSize       uint64
	}

	// generic helper for processing v1 and v2 contracts
	processContract := func(fcid types.Hash256, rev *revision, resolved, valid bool) {
		// ignore irrelevant contracts
		if !cs.isKnownContract(types.FileContractID(fcid)) {
			return
		}

		// 'pending' -> 'active'
		if cs.contractState[fcid] < contractStateActive {
			cs.contractState[fcid] = contractStateActive // 'pending' -> 'active'
			cs.logger.Infow("contract state changed: pending -> active",
				"fcid", fcid,
				"reason", "contract confirmed")
		}

		// renewed: 'active' -> 'complete'
		if rev != nil {
			cs.revisions[fcid] = revisionUpdate{
				height: cau.State.Index.Height,
				number: rev.revisionNumber,
				size:   rev.fileSize,
			}
			if rev.revisionNumber == types.MaxRevisionNumber && rev.fileSize == 0 {
				cs.contractState[fcid] = contractStateComplete // renewed: 'active' -> 'complete'
				cs.logger.Infow("contract state changed: active -> complete",
					"fcid", fcid,
					"reason", "final revision confirmed")
			}
		}

		// storage proof: 'active' -> 'complete/failed'
		if resolved {
			cs.proofs[fcid] = cau.State.Index.Height
			if valid {
				cs.contractState[fcid] = contractStateComplete
				cs.logger.Infow("contract state changed: active -> complete",
					"fcid", fcid,
					"reason", "storage proof valid")
			} else {
				cs.contractState[fcid] = contractStateFailed
				cs.logger.Infow("contract state changed: active -> failed",
					"fcid", fcid,
					"reason", "storage proof missed")
			}
		}
	}

	// v1 contracts
	cau.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved, valid bool) {
		var r *revision
		if rev != nil {
			r = &revision{
				revisionNumber: rev.FileContract.RevisionNumber,
				fileSize:       rev.FileContract.Filesize,
			}
		}
		processContract(fce.ID, r, resolved, valid)
	})

	// v2 contracts
	cau.ForEachV2FileContractElement(func(fce types.V2FileContractElement, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
		var r *revision
		if rev != nil {
			r = &revision{
				revisionNumber: rev.V2FileContract.RevisionNumber,
				fileSize:       rev.V2FileContract.Filesize,
			}
		}
		resolved := res != nil
		valid := false
		if res != nil {
			switch res.(type) {
			case *types.V2FileContractFinalization:
				valid = true
			case *types.V2FileContractRenewal:
				valid = true
			case *types.V2StorageProof:
				valid = true
			case *types.V2FileContractExpiration:
				valid = fce.V2FileContract.Filesize == 0
			}
		}
		processContract(fce.ID, r, resolved, valid)
	})
}

func (cs *chainSubscriber) processChainRevertUpdateContracts(cru *chain.RevertUpdate) {
	type revision struct {
		revisionNumber uint64
		fileSize       uint64
	}

	// generic helper for processing v1 and v2 contracts
	processContract := func(fcid types.Hash256, prevRev revision, rev *revision, resolved, valid bool) {
		// ignore irrelevant contracts
		if !cs.isKnownContract(types.FileContractID(fcid)) {
			return
		}

		// 'active' -> 'pending'
		if rev == nil {
			cs.contractState[fcid] = contractStatePending
		}

		// reverted renewal: 'complete' -> 'active'
		if rev != nil {
			cs.revisions[fcid] = revisionUpdate{
				height: cru.State.Index.Height,
				number: prevRev.revisionNumber,
				size:   prevRev.fileSize,
			}
			if rev.revisionNumber == math.MaxUint64 && rev.fileSize == 0 {
				cs.contractState[fcid] = contractStateActive
				cs.logger.Infow("contract state changed: complete -> active",
					"fcid", fcid,
					"reason", "final revision reverted")
			}
		}

		// reverted storage proof: 'complete/failed' -> 'active'
		if resolved {
			cs.contractState[fcid] = contractStateActive // revert from 'complete' to 'active'
			if valid {
				cs.logger.Infow("contract state changed: complete -> active",
					"fcid", fcid,
					"reason", "storage proof reverted")
			} else {
				cs.logger.Infow("contract state changed: failed -> active",
					"fcid", fcid,
					"reason", "storage proof reverted")
			}
		}
	}

	// v1 contracts
	cru.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved, valid bool) {
		var r *revision
		if rev != nil {
			r = &revision{
				revisionNumber: rev.FileContract.RevisionNumber,
				fileSize:       rev.FileContract.Filesize,
			}
		}
		prevRev := revision{
			revisionNumber: fce.FileContract.RevisionNumber,
			fileSize:       fce.FileContract.Filesize,
		}
		processContract(fce.ID, prevRev, r, resolved, valid)
	})

	// v2 contracts
	cru.ForEachV2FileContractElement(func(fce types.V2FileContractElement, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
		var r *revision
		if rev != nil {
			r = &revision{
				revisionNumber: rev.V2FileContract.RevisionNumber,
				fileSize:       rev.V2FileContract.Filesize,
			}
		}
		resolved := res != nil
		valid := false
		if res != nil {
			switch res.(type) {
			case *types.V2FileContractFinalization:
				valid = true
			case *types.V2FileContractRenewal:
				valid = true
			case *types.V2StorageProof:
				valid = true
			case *types.V2FileContractExpiration:
				valid = fce.V2FileContract.Filesize == 0
			}
		}
		prevRev := revision{
			revisionNumber: fce.V2FileContract.RevisionNumber,
			fileSize:       fce.V2FileContract.Filesize,
		}
		processContract(fce.ID, prevRev, r, resolved, valid)
	})
}

func (cs *chainSubscriber) retryTransaction(fc func(tx *gorm.DB) error, opts ...*sql.TxOptions) error {
	return retryTransaction(cs.db, cs.logger, fc, cs.retryIntervals, opts...)
}
