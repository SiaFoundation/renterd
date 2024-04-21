package stores

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var (
	_ chain.Subscriber = (*chainSubscriber)(nil)
	_ wallet.ApplyTx   = (*chainSubscriber)(nil)
	_ wallet.RevertTx  = (*chainSubscriber)(nil)
)

type (
	chainSubscriber struct {
		announcementMaxAge time.Duration
		db                 *gorm.DB
		logger             *zap.SugaredLogger
		persistInterval    time.Duration
		retryIntervals     []time.Duration
		walletAddress      types.Address

		// buffered state
		mu             sync.Mutex
		closed         bool
		lastSave       time.Time
		tip            types.ChainIndex
		knownContracts map[types.FileContractID]struct{}
		persistTimer   *time.Timer

		announcements []announcement
		events        []eventChange

		contractState map[types.Hash256]contractState
		mayCommit     bool
		outputs       map[types.Hash256]outputChange
		proofs        map[types.Hash256]uint64
		revisions     map[types.Hash256]revisionUpdate
	}
)

func newChainSubscriber(sqlStore *SQLStore, logger *zap.SugaredLogger, intvls []time.Duration, persistInterval time.Duration, walletAddress types.Address, ancmtMaxAge time.Duration) (*chainSubscriber, error) {
	// load known contracts
	var activeFCIDs []fileContractID
	if err := sqlStore.db.Model(&dbContract{}).
		Select("fcid").
		Find(&activeFCIDs).Error; err != nil {
		return nil, err
	}
	var archivedFCIDs []fileContractID
	if err := sqlStore.db.Model(&dbArchivedContract{}).
		Select("fcid").
		Find(&archivedFCIDs).Error; err != nil {
		return nil, err
	}
	knownContracts := make(map[types.FileContractID]struct{})
	for _, fcid := range append(activeFCIDs, archivedFCIDs...) {
		knownContracts[types.FileContractID(fcid)] = struct{}{}
	}

	return &chainSubscriber{
		announcementMaxAge: ancmtMaxAge,
		db:                 sqlStore.db,
		logger:             logger,
		retryIntervals:     intvls,

		walletAddress:   walletAddress,
		lastSave:        time.Now(),
		persistInterval: persistInterval,

		contractState:  make(map[types.Hash256]contractState),
		outputs:        make(map[types.Hash256]outputChange),
		proofs:         make(map[types.Hash256]uint64),
		revisions:      make(map[types.Hash256]revisionUpdate),
		knownContracts: knownContracts,
	}, nil
}

func (cs *chainSubscriber) Close() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.closed = true
	if cs.persistTimer != nil {
		cs.persistTimer.Stop()
		select {
		case <-cs.persistTimer.C:
		default:
		}
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
	if err := cs.processChainApplyUpdateWallet(cau); err != nil {
		return err
	}

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
	if err := cs.processChainRevertUpdateWallet(cru); err != nil {
		return err
	}

	cs.tip = cru.State.Index
	cs.mayCommit = true

	return cs.tryCommit()
}

func (cs *chainSubscriber) Tip() types.ChainIndex {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.tip
}

func (cs *chainSubscriber) addKnownContract(id types.FileContractID) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.knownContracts[id] = struct{}{}
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
			if len(allowlist)+len(blocklist) > 0 {
				updated := make(map[types.PublicKey]struct{})
				for _, ann := range cs.announcements {
					if _, seen := updated[ann.hk]; !seen {
						updated[ann.hk] = struct{}{}
						if err := updateBlocklist(tx, ann.hk, allowlist, blocklist); err != nil {
							cs.logger.Error(fmt.Sprintf("failed to update blocklist, err: %v", err))
						}
					}
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
				err = applyUnappliedOutputAdditions(tx, oc.se)
			} else {
				err = applyUnappliedOutputRemovals(tx, oc.se.OutputID)
			}
			if err != nil {
				return fmt.Errorf("%w; failed to apply unapplied output change", err)
			}
		}
		for _, tc := range cs.events {
			if tc.addition {
				err = applyUnappliedEventAdditions(tx, tc.event)
			} else {
				err = applyUnappliedEventRemovals(tx, tc.event.EventID)
			}
			if err != nil {
				return fmt.Errorf("%w; failed to apply unapplied event change", err)
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
	cs.mayCommit = false
	cs.outputs = make(map[types.Hash256]outputChange)
	cs.proofs = make(map[types.Hash256]uint64)
	cs.revisions = make(map[types.Hash256]revisionUpdate)
	cs.events = nil
	cs.lastSave = time.Now()
	return nil
}

// shouldCommit returns whether the subscriber should commit its buffered state.
func (cs *chainSubscriber) shouldCommit() bool {
	return cs.mayCommit && (time.Since(cs.lastSave) > cs.persistInterval ||
		len(cs.announcements) > 0 ||
		len(cs.revisions) > 0 ||
		len(cs.proofs) > 0 ||
		len(cs.outputs) > 0 ||
		len(cs.events) > 0 ||
		len(cs.contractState) > 0)
}

func (cs *chainSubscriber) tryCommit() error {
	// commit if we can/should
	if !cs.shouldCommit() {
		return nil
	} else if err := cs.commit(); err != nil {
		cs.logger.Errorw("failed to commit chain update", zap.Error(err))
		return err
	}

	// force a persist if no block has been received for some time
	if cs.persistTimer != nil {
		cs.persistTimer.Stop()
		select {
		case <-cs.persistTimer.C:
		default:
		}
	}
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
	chain.ForEachHostAnnouncement(b, func(hk types.PublicKey, ha chain.HostAnnouncement) {
		if ha.NetAddress == "" {
			return // ignore
		}
		cs.announcements = append(cs.announcements, announcement{
			blockHeight:      cau.State.Index.Height,
			blockID:          b.ID(),
			hk:               hk,
			timestamp:        b.Timestamp,
			HostAnnouncement: ha,
		})
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
	processContract := func(fcid types.Hash256, rev revision, resolved, valid bool) {
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
		if rev.revisionNumber == types.MaxRevisionNumber && rev.fileSize == 0 {
			cs.contractState[fcid] = contractStateComplete // renewed: 'active' -> 'complete'
			cs.logger.Infow("contract state changed: active -> complete",
				"fcid", fcid,
				"reason", "final revision confirmed")
		}
		cs.revisions[fcid] = revisionUpdate{
			height: cau.State.Index.Height,
			number: rev.revisionNumber,
			size:   rev.fileSize,
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
		var r revision
		if rev != nil {
			r.revisionNumber = rev.FileContract.RevisionNumber
			r.fileSize = rev.FileContract.Filesize
		} else {
			r.revisionNumber = fce.FileContract.RevisionNumber
			r.fileSize = fce.FileContract.Filesize
		}
		processContract(fce.ID, r, resolved, valid)
	})

	// v2 contracts
	cau.ForEachV2FileContractElement(func(fce types.V2FileContractElement, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
		var r revision
		if rev != nil {
			r.revisionNumber = rev.V2FileContract.RevisionNumber
			r.fileSize = rev.V2FileContract.Filesize
		} else {
			r.revisionNumber = fce.V2FileContract.RevisionNumber
			r.fileSize = fce.V2FileContract.Filesize
		}

		var valid bool
		var resolved bool
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

			resolved = true
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

func (cs *chainSubscriber) processChainApplyUpdateWallet(cau *chain.ApplyUpdate) error {
	return wallet.ApplyChainUpdates(cs, cs.walletAddress, []*chain.ApplyUpdate{cau})
}

func (cs *chainSubscriber) processChainRevertUpdateWallet(cru *chain.RevertUpdate) error {
	return wallet.RevertChainUpdate(cs, cs.walletAddress, cru)
}

func (cs *chainSubscriber) retryTransaction(fc func(tx *gorm.DB) error) error {
	return retryTransaction(context.Background(), cs.db, cs.logger, cs.retryIntervals, fc, func(err error) bool {
		return err == nil ||
			utils.IsErr(err, gorm.ErrRecordNotFound) ||
			utils.IsErr(err, context.Canceled) ||
			utils.IsErr(err, errNoSuchTable) ||
			utils.IsErr(err, errDuplicateEntry)
	})
}

// AddEvents is called with all relevant events added in the update.
func (cs *chainSubscriber) AddEvents(events []wallet.Event) error {
	for _, event := range events {
		cs.events = append(cs.events, eventChange{
			addition: true,
			event: dbWalletEvent{
				EventID:        hash256(event.ID),
				Inflow:         currency(event.Inflow),
				Outflow:        currency(event.Outflow),
				Transaction:    event.Transaction,
				MaturityHeight: event.MaturityHeight,
				Source:         string(event.Source),
				Timestamp:      event.Timestamp.Unix(),
				DBChainIndex: dbChainIndex{
					Height:  event.Index.Height,
					BlockID: hash256(event.Index.ID),
				},
			},
		})
	}
	return nil
}

// AddSiacoinElements is called with all new siacoin elements in the
// update. Ephemeral siacoin elements are not included.
func (cs *chainSubscriber) AddSiacoinElements(elements []wallet.SiacoinElement) error {
	for _, el := range elements {
		if _, ok := cs.outputs[el.ID]; ok {
			return fmt.Errorf("output %q already exists", el.ID)
		}
		cs.outputs[el.ID] = outputChange{
			addition: true,
			se: dbWalletOutput{
				OutputID:       hash256(el.ID),
				LeafIndex:      el.StateElement.LeafIndex,
				MerkleProof:    merkleProof{proof: el.StateElement.MerkleProof},
				Value:          currency(el.SiacoinOutput.Value),
				Address:        hash256(el.SiacoinOutput.Address),
				MaturityHeight: el.MaturityHeight,
				DBChainIndex: dbChainIndex{
					Height:  el.Index.Height,
					BlockID: hash256(el.Index.ID),
				},
			},
		}
	}

	return nil
}

// RemoveSiacoinElements is called with all siacoin elements that were
// spent in the update.
func (cs *chainSubscriber) RemoveSiacoinElements(ids []types.SiacoinOutputID) error {
	for _, id := range ids {
		if _, ok := cs.outputs[types.Hash256(id)]; ok {
			return fmt.Errorf("output %q not found", id)
		}

		cs.outputs[types.Hash256(id)] = outputChange{
			addition: false,
			se: dbWalletOutput{
				OutputID: hash256(id),
			},
		}
	}
	return nil
}

// WalletStateElements returns all state elements in the database. It is used
// to update the proofs of all state elements affected by the update.
func (cs *chainSubscriber) WalletStateElements() (elements []types.StateElement, _ error) {
	for id, el := range cs.outputs {
		elements = append(elements, types.StateElement{
			ID:          id,
			LeafIndex:   el.se.LeafIndex,
			MerkleProof: el.se.MerkleProof.proof,
		})
	}
	return
}

// UpdateStateElements updates the proofs of all state elements affected by the
// update.
func (cs *chainSubscriber) UpdateStateElements(elements []types.StateElement) error {
	for _, se := range elements {
		curr := cs.outputs[se.ID]
		curr.se.MerkleProof = merkleProof{proof: se.MerkleProof}
		curr.se.LeafIndex = se.LeafIndex
		cs.outputs[se.ID] = curr
	}
	return nil
}

// RevertIndex is called with the chain index that is being reverted. Any events
// and siacoin elements that were created by the index should be removed.
func (cs *chainSubscriber) RevertIndex(index types.ChainIndex) error {
	// remove any events that were added in the reverted block
	filtered := cs.events[:0]
	for i := range cs.events {
		if cs.events[i].event.DBChainIndex.convert() != index {
			filtered = append(filtered, cs.events[i])
		}
	}
	cs.events = filtered

	// remove any siacoin elements that were added in the reverted block
	for id, el := range cs.outputs {
		if el.se.DBChainIndex.convert() == index {
			delete(cs.outputs, id)
		}
	}

	return nil
}
