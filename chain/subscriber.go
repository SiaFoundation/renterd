package chain

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
)

type (
	ChainManager interface {
		Tip() types.ChainIndex
		OnReorg(fn func(types.ChainIndex)) (cancel func())
		UpdatesSince(index types.ChainIndex, max int) (rus []chain.RevertUpdate, aus []chain.ApplyUpdate, err error)
	}

	ChainStore interface {
		ApplyChainUpdate(ctx context.Context, cu Update) error
		ChainIndex() (types.ChainIndex, error)
	}

	ContractStore interface {
		AddSubscriber(context.Context, ContractStoreSubscriber) (map[types.FileContractID]api.ContractState, func(), error)
	}

	ContractStoreSubscriber interface {
		NewContractID(fcid types.FileContractID)
	}

	Subscriber struct {
		cm     ChainManager
		cs     ChainStore
		logger *zap.SugaredLogger

		announcementMaxAge time.Duration
		persistInterval    time.Duration
		walletAddress      types.Address

		mu      sync.Mutex
		closed  bool
		syncing bool

		// known contracts state
		knownContracts map[types.FileContractID]api.ContractState
		unsubscribeFn  func()

		// chain state
		lastSave     time.Time
		persistTimer *time.Timer
		nextUpdate   *Update
	}
)

func NewSubscriber(cm ChainManager, cs ChainStore, contracts ContractStore, walletAddress types.Address, announcementMaxAge, persistInterval time.Duration, logger *zap.SugaredLogger) (_ *Subscriber, err error) {
	// sanity check announcement max age
	if announcementMaxAge == 0 {
		return nil, errors.New("announcementMaxAge must be non-zero")
	}

	// create chain subscriber
	subscriber := &Subscriber{
		cm:     cm,
		cs:     cs,
		logger: logger,

		announcementMaxAge: announcementMaxAge,
		persistInterval:    persistInterval,
		walletAddress:      walletAddress,

		// locked
		lastSave: time.Now(),
	}

	// subscribe to contract id updates
	subscriber.knownContracts, subscriber.unsubscribeFn, err = contracts.AddSubscriber(context.Background(), subscriber)
	if err != nil {
		return nil, err
	}

	return subscriber, nil
}

func (cs *Subscriber) Close() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.closed = true
	cs.unsubscribeFn()
	if cs.persistTimer != nil {
		cs.persistTimer.Stop()
		select {
		case <-cs.persistTimer.C:
		default:
		}
	}
	return nil
}

func (cs *Subscriber) NewContractID(id types.FileContractID) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.knownContracts[id] = api.ContractStatePending
}

func (cs *Subscriber) ProcessChainApplyUpdate(cau chain.ApplyUpdate) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// check for shutdown, ideally this never happens since the subscriber is
	// unsubscribed first and then closed
	if cs.closed {
		return errors.New("shutting down")
	}

	// set the tip
	if cs.nextUpdate == nil {
		cs.nextUpdate = NewChainUpdate(cau.State.Index)
	} else {
		cs.nextUpdate.Index = cau.State.Index
	}

	// process chain updates
	cs.processChainApplyUpdateHostDB(cau)
	cs.processChainApplyUpdateContracts(cau)
	if err := cs.processChainApplyUpdateWallet(cau); err != nil {
		return err
	}

	return cs.tryCommit()
}

func (cs *Subscriber) ProcessChainRevertUpdate(cru chain.RevertUpdate) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// check for shutdown, ideally this never happens since the subscriber is
	// unsubscribed first and then closed
	if cs.closed {
		return errors.New("shutting down")
	}

	// set the tip
	if cs.nextUpdate == nil {
		cs.nextUpdate = NewChainUpdate(cru.State.Index)
	} else {
		cs.nextUpdate.Index = cru.State.Index
	}

	// process chain updates
	cs.processChainRevertUpdateHostDB(cru)
	cs.processChainRevertUpdateContracts(cru)
	if err := cs.processChainRevertUpdateWallet(cru); err != nil {
		return err
	}

	return cs.tryCommit()
}

func (cs *Subscriber) Subscribe() (func(), error) {
	index, err := cs.cs.ChainIndex()
	if err != nil {
		return nil, err
	}
	if err := cs.sync(index); err != nil {
		return nil, fmt.Errorf("failed to subscribe to chain manager: %w", err)
	}

	reorgChan := make(chan types.ChainIndex, 1)
	go func() {
		for range reorgChan {
			lastTip, err := cs.cs.ChainIndex()
			if err != nil {
				cs.logger.Error("failed to get last committed index", zap.Error(err))
				continue
			}
			if err := cs.sync(lastTip); err != nil {
				cs.logger.Error("failed to sync store", zap.Error(err))
			}
		}
	}()

	return cs.cm.OnReorg(func(index types.ChainIndex) {
		select {
		case reorgChan <- index:
		default:
		}
	}), nil
}

func (cs *Subscriber) TriggerSync() {
	go func() {
		index, err := cs.cs.ChainIndex()
		if err != nil {
			cs.logger.Errorw("failed to get last committed index", zap.Error(err))
		}

		if err := cs.sync(index); err != nil {
			cs.logger.Errorw("failed to sync chain", zap.Error(err))
		}
	}()
}

func (cs *Subscriber) commit() error {
	if err := cs.cs.ApplyChainUpdate(context.Background(), *cs.nextUpdate); err != nil {
		return fmt.Errorf("failed to apply chain update: %w", err)
	}
	cs.lastSave = time.Now()
	cs.nextUpdate = nil
	return nil
}

func (cs *Subscriber) tryCommit() error {
	// commit if we can/should
	if !cs.nextUpdate.HasUpdates() && time.Since(cs.lastSave) < cs.persistInterval {
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
		if cs.closed || cs.syncing || cs.nextUpdate == nil || !cs.nextUpdate.HasUpdates() {
			return
		} else if err := cs.commit(); err != nil {
			cs.logger.Errorw("failed to commit delayed chain update", zap.Error(err))
		}
	})
	return nil
}

func (cs *Subscriber) processChainApplyUpdateHostDB(cau chain.ApplyUpdate) {
	b := cau.Block
	if time.Since(b.Timestamp) > cs.announcementMaxAge {
		return // ignore old announcements
	}
	chain.ForEachHostAnnouncement(b, func(hk types.PublicKey, ha chain.HostAnnouncement) {
		if ha.NetAddress == "" {
			return // ignore
		}
		cs.nextUpdate.HostAnnouncements[hk] = HostAnnouncement{
			Announcement: ha,
			BlockHeight:  cau.State.Index.Height,
			BlockID:      b.ID(),
			Timestamp:    b.Timestamp,
		}
	})
}

func (cs *Subscriber) processChainRevertUpdateHostDB(cru chain.RevertUpdate) {
	// nothing to do, we are not unannouncing hosts
}

func (cs *Subscriber) processChainApplyUpdateContracts(cau chain.ApplyUpdate) {
	type revision struct {
		revisionNumber uint64
		fileSize       uint64
	}

	// generic helper for processing v1 and v2 contracts
	processContract := func(fcid types.FileContractID, rev revision, resolved, valid bool) {
		// ignore unknown contracts
		state, known := cs.knownContracts[fcid]
		if !known {
			return
		}

		// convenience variables
		cu := cs.nextUpdate.ContractUpdate(fcid, state)
		defer func() { cs.knownContracts[fcid] = cu.State }()

		// set contract update
		cu.Size = &rev.fileSize
		cu.RevisionNumber = &rev.revisionNumber
		cu.RevisionHeight = &cau.State.Index.Height

		// update state from 'pending' -> 'active'
		if cu.State == api.ContractStatePending || state == api.ContractStateUnknown {
			cu.State = api.ContractStateActive // 'pending' -> 'active'
			cs.logger.Infow("contract state changed: pending -> active",
				"fcid", fcid,
				"reason", "contract confirmed")
		}

		// renewed: 'active' -> 'complete'
		if rev.revisionNumber == types.MaxRevisionNumber && rev.fileSize == 0 {
			cu.State = api.ContractStateComplete // renewed: 'active' -> 'complete'
			cs.logger.Infow("contract state changed: active -> complete",
				"fcid", fcid,
				"reason", "final revision confirmed")
		}

		// storage proof: 'active' -> 'complete/failed'
		if resolved {
			cu.ProofHeight = &cau.State.Index.Height
			if valid {
				cu.State = api.ContractStateComplete
				cs.logger.Infow("contract state changed: active -> complete",
					"fcid", fcid,
					"reason", "storage proof valid")
			} else {
				cu.State = api.ContractStateFailed
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
		processContract(types.FileContractID(fce.ID), r, resolved, valid)
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
		processContract(types.FileContractID(fce.ID), r, resolved, valid)
	})
}

func (cs *Subscriber) processChainRevertUpdateContracts(cru chain.RevertUpdate) {
	type revision struct {
		revisionNumber uint64
		fileSize       uint64
	}

	// generic helper for processing v1 and v2 contracts
	processContract := func(fcid types.FileContractID, prevRev revision, rev *revision, resolved, valid bool) {
		// ignore unknown contracts
		state, known := cs.knownContracts[fcid]
		if !known {
			return
		}

		// convenience variables
		cu := cs.nextUpdate.ContractUpdate(fcid, state)
		defer func() { cs.knownContracts[fcid] = cu.State }()

		// update state from 'active' -> 'pending'
		if rev == nil {
			cu.State = api.ContractStatePending
		}

		// reverted renewal: 'complete' -> 'active'
		if rev != nil {
			cu.RevisionHeight = &cru.State.Index.Height
			cu.RevisionNumber = &prevRev.revisionNumber
			cu.Size = &prevRev.fileSize
			if cu.State == api.ContractStateComplete {
				cu.State = api.ContractStateActive
				cs.logger.Infow("contract state changed: complete -> active",
					"fcid", fcid,
					"reason", "final revision reverted")
			}
		}

		// reverted storage proof: 'complete/failed' -> 'active'
		if resolved {
			cu.State = api.ContractStateActive // revert from 'complete' to 'active'
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
		processContract(types.FileContractID(fce.ID), prevRev, r, resolved, valid)
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
		processContract(types.FileContractID(fce.ID), prevRev, r, resolved, valid)
	})
}

func (cs *Subscriber) processChainApplyUpdateWallet(cau chain.ApplyUpdate) error {
	return wallet.ApplyChainUpdates(cs.nextUpdate, cs.walletAddress, []chain.ApplyUpdate{cau})
}

func (cs *Subscriber) processChainRevertUpdateWallet(cru chain.RevertUpdate) error {
	return wallet.RevertChainUpdate(cs.nextUpdate, cs.walletAddress, cru)
}

func (cs *Subscriber) sync(index types.ChainIndex) error {
	cs.mu.Lock()
	if cs.syncing {
		cs.mu.Unlock()
		return nil
	}
	cs.syncing = true
	cs.mu.Unlock()

	defer func() {
		cs.mu.Lock()
		cs.syncing = false
		cs.mu.Unlock()
	}()

	for index != cs.cm.Tip() {
		crus, caus, err := cs.cm.UpdatesSince(index, 1000)
		if err != nil {
			return fmt.Errorf("failed to subscribe to chain manager: %w", err)
		}
		for _, cru := range crus {
			if err := cs.ProcessChainRevertUpdate(cru); err != nil {
				return fmt.Errorf("failed to process revert update: %w", err)
			}
			index = cru.State.Index
		}
		for _, cau := range caus {
			if err := cs.ProcessChainApplyUpdate(cau); err != nil {
				return fmt.Errorf("failed to process apply update: %w", err)
			}
			index = cau.State.Index
		}
	}
	return nil
}
