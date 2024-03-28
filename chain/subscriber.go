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

const (
	// updatesBatchSize is the maximum number of updates to fetch in a single
	// call to the chain manager when we request updates since a given index.
	updatesBatchSize = 1000
)

type (
	ChainManager interface {
		Tip() types.ChainIndex
		OnReorg(fn func(types.ChainIndex)) (cancel func())
		UpdatesSince(index types.ChainIndex, max int) (rus []chain.RevertUpdate, aus []chain.ApplyUpdate, err error)
	}

	ChainStore interface {
		ApplyChainUpdate(ctx context.Context, cu *Update) error
		ChainIndex() (types.ChainIndex, error)
		WalletStateElements(ctx context.Context) ([]types.StateElement, error)
	}

	ContractStore interface {
		AddContractStoreSubscriber(context.Context, ContractStoreSubscriber) (map[types.FileContractID]api.ContractState, func(), error)
	}

	ContractStoreSubscriber interface {
		AddContractID(fcid types.FileContractID)
	}

	Subscriber struct {
		cm     ChainManager
		cs     ChainStore
		logger *zap.SugaredLogger

		announcementMaxAge time.Duration
		walletAddress      types.Address

		syncSig         chan struct{}
		csUnsubscribeFn func()

		mu                 sync.Mutex
		closedChan         chan struct{}
		knownContracts     map[types.FileContractID]api.ContractState
		knownStateElements map[types.Hash256]types.StateElement
	}
)

func NewSubscriber(cm ChainManager, cs ChainStore, contracts ContractStore, walletAddress types.Address, announcementMaxAge time.Duration, logger *zap.SugaredLogger) (_ *Subscriber, err error) {
	if announcementMaxAge == 0 {
		return nil, errors.New("announcementMaxAge must be non-zero")
	}

	// create chain subscriber
	subscriber := &Subscriber{
		cm:     cm,
		cs:     cs,
		logger: logger,

		announcementMaxAge: announcementMaxAge,
		walletAddress:      walletAddress,

		syncSig: make(chan struct{}, 1),

		closedChan:         make(chan struct{}),
		knownStateElements: make(map[types.Hash256]types.StateElement),
	}

	// make sure we don't hang
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// subscribe ourselves to receive new contract ids
	subscriber.knownContracts, subscriber.csUnsubscribeFn, err = contracts.AddContractStoreSubscriber(ctx, subscriber)
	if err != nil {
		return nil, err
	}

	// fetch all state elements from the database
	elements, err := cs.WalletStateElements(ctx)
	if err != nil {
		return nil, err
	}
	for _, el := range elements {
		subscriber.knownStateElements[types.Hash256(el.ID)] = el
	}

	return subscriber, nil
}

func (cs *Subscriber) AddContractID(id types.FileContractID) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.knownContracts[id] = api.ContractStatePending
}

func (cs *Subscriber) Close() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// signal we are closing
	close(cs.closedChan)

	// unsubscribe from chain manager
	cs.csUnsubscribeFn()

	return nil
}

func (cs *Subscriber) Run() (func(), error) {
	// perform an initial sync
	index, err := cs.cs.ChainIndex()
	if err != nil {
		return nil, err
	}
	if err := cs.sync(index); err != nil {
		return nil, fmt.Errorf("failed to subscribe to chain manager: %w", err)
	}

	// start sync loop
	go func() {
		for {
			select {
			case <-cs.closedChan:
				return
			case <-cs.syncSig:
			}

			ci, err := cs.cs.ChainIndex()
			if err != nil {
				cs.logger.Errorf("failed to get chain index: %v", err)
				continue
			}
			err = cs.sync(ci)
			if err != nil {
				cs.logger.Errorf("failed to sync: %v", err)
			}
		}
	}()

	// trigger initial sync
	cs.triggerSync()

	// trigger a sync on reorgs
	return cs.cm.OnReorg(func(_ types.ChainIndex) { cs.triggerSync() }), nil
}

func (cs *Subscriber) applyContractUpdates(cu *Update, cau chain.ApplyUpdate) {
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
		c := cu.ContractUpdate(fcid, state)
		defer func() { cs.knownContracts[fcid] = c.State }()

		// set contract update
		if rev.fileSize > 0 {
			c.Size = &rev.fileSize
		}
		if rev.revisionNumber > 0 {
			c.RevisionNumber = &rev.revisionNumber
		}
		c.RevisionHeight = &cau.State.Index.Height

		// update state from 'pending' -> 'active'
		if c.State == api.ContractStatePending || state == api.ContractStateUnknown {
			c.State = api.ContractStateActive // 'pending' -> 'active'
			cs.logger.Infow("contract state changed: pending -> active",
				"fcid", fcid,
				"reason", "contract confirmed")
		}

		// renewed: 'active' -> 'complete'
		if rev.revisionNumber == types.MaxRevisionNumber && rev.fileSize == 0 {
			c.State = api.ContractStateComplete // renewed: 'active' -> 'complete'
			cs.logger.Infow("contract state changed: active -> complete",
				"fcid", fcid,
				"reason", "final revision confirmed")
		}

		// storage proof: 'active' -> 'complete/failed'
		if resolved {
			c.ProofHeight = &cau.State.Index.Height
			if valid {
				c.State = api.ContractStateComplete
				cs.logger.Infow("contract state changed: active -> complete",
					"fcid", fcid,
					"reason", "storage proof valid")
			} else {
				c.State = api.ContractStateFailed
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

func (cs *Subscriber) applyHostUpdates(cu *Update, cau chain.ApplyUpdate) {
	b := cau.Block
	if time.Since(b.Timestamp) > cs.announcementMaxAge {
		return // ignore old announcements
	}
	chain.ForEachHostAnnouncement(b, func(hk types.PublicKey, ha chain.HostAnnouncement) {
		if ha.NetAddress == "" {
			return // ignore
		}
		cu.HostUpdates[hk] = HostUpdate{
			Announcement: ha,
			BlockHeight:  cau.State.Index.Height,
			BlockID:      b.ID(),
			Timestamp:    b.Timestamp,
		}
	})
}

func (cs *Subscriber) applyWalletUpdate(cu *Update, cau chain.ApplyUpdate) error {
	return wallet.ApplyChainUpdates(cu, cs.walletAddress, []chain.ApplyUpdate{cau})
}

func (cs *Subscriber) revertContractUpdate(cu *Update, cru chain.RevertUpdate) {
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
		c := cu.ContractUpdate(fcid, state)
		defer func() { cs.knownContracts[fcid] = c.State }()

		// update state from 'active' -> 'pending'
		if rev == nil {
			c.State = api.ContractStatePending
		}

		// reverted renewal: 'complete' -> 'active'
		if rev != nil {
			c.RevisionHeight = &cru.State.Index.Height
			c.RevisionNumber = &prevRev.revisionNumber
			c.Size = &prevRev.fileSize
			if c.State == api.ContractStateComplete {
				c.State = api.ContractStateActive
				cs.logger.Infow("contract state changed: complete -> active",
					"fcid", fcid,
					"reason", "final revision reverted")
			}
		}

		// reverted storage proof: 'complete/failed' -> 'active'
		if resolved {
			c.State = api.ContractStateActive // revert from 'complete' to 'active'
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

func (cs *Subscriber) revertWalletUpdate(cu *Update, cru chain.RevertUpdate) error {
	return wallet.RevertChainUpdate(cu, cs.walletAddress, cru)
}

func (cs *Subscriber) sync(index types.ChainIndex) error {
	for index != cs.cm.Tip() {
		crus, caus, err := cs.cm.UpdatesSince(index, updatesBatchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch updates: %w", err)
		}

		// lock the subscriber while we apply the updates
		cs.mu.Lock()

		// aggregate updates into one chain update
		cu := NewChainUpdate(cs.knownStateElements)

		// revert chain updates
		for _, cru := range crus {
			cs.revertContractUpdate(cu, cru)
			if err := cs.revertWalletUpdate(cu, cru); err != nil {
				cs.mu.Unlock()
				return fmt.Errorf("failed to revert wallet update: %w", err)
			}
			index = cru.State.Index
		}

		// apply chain updates
		for _, cau := range caus {
			cs.applyContractUpdates(cu, cau)
			cs.applyHostUpdates(cu, cau)
			if err := cs.applyWalletUpdate(cu, cau); err != nil {
				cs.mu.Unlock()
				return fmt.Errorf("failed to apply wallet update: %w", err)
			}
			index = cau.State.Index
		}

		cs.mu.Unlock()

		// update the index
		cu.Index = index
		if err := cs.cs.ApplyChainUpdate(context.Background(), cu); err != nil {
			return fmt.Errorf("failed to apply chain update: %w", err)
		}
	}
	return nil
}

func (cs *Subscriber) triggerSync() {
	select {
	case cs.syncSig <- struct{}{}:
	default:
	}
}
