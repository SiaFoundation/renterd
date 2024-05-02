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

var (
	errClosed = errors.New("subscriber closed")
)

type (
	ChainManager interface {
		Tip() types.ChainIndex
		OnReorg(fn func(types.ChainIndex)) (cancel func())
		UpdatesSince(index types.ChainIndex, max int) (rus []chain.RevertUpdate, aus []chain.ApplyUpdate, err error)
	}

	ChainStore interface {
		ProcessChainUpdate(fn func(ChainUpdateTx) error) error
		ChainIndex() (types.ChainIndex, error)
	}

	ChainUpdateTx interface {
		ContractState(fcid types.FileContractID) (api.ContractState, error)
		UpdateChainIndex(index types.ChainIndex) error
		UpdateContract(fcid types.FileContractID, revisionHeight, revisionNumber, size uint64) error
		UpdateContractState(fcid types.FileContractID, state api.ContractState) error
		UpdateContractProofHeight(fcid types.FileContractID, proofHeight uint64) error
		UpdateFailedContracts(blockHeight uint64) error
		UpdateHost(hk types.PublicKey, ha chain.HostAnnouncement, bh uint64, blockID types.BlockID, ts time.Time) error

		wallet.UpdateTx
	}

	ContractStore interface {
		AddContractStoreSubscriber(context.Context, ContractStoreSubscriber) (map[types.FileContractID]struct{}, func(), error)
	}

	ContractStoreSubscriber interface {
		AddContractID(fcid types.FileContractID)
	}

	Subscriber struct {
		cm              ChainManager
		cs              ChainStore
		csUnsubscribeFn func()
		logger          *zap.SugaredLogger

		announcementMaxAge time.Duration
		retryTxIntervals   []time.Duration
		walletAddress      types.Address

		closedChan chan struct{}
		syncSig    chan struct{}
		wg         sync.WaitGroup

		mu             sync.Mutex
		knownContracts map[types.FileContractID]struct{}
	}

	revision struct {
		revisionNumber uint64
		fileSize       uint64
	}
)

func NewSubscriber(cm ChainManager, cs ChainStore, contracts ContractStore, walletAddress types.Address, announcementMaxAge time.Duration, retryTxIntervals []time.Duration, logger *zap.Logger) (_ *Subscriber, err error) {
	if announcementMaxAge == 0 {
		return nil, errors.New("announcementMaxAge must be non-zero")
	}

	// create chain subscriber
	subscriber := &Subscriber{
		cm:     cm,
		cs:     cs,
		logger: logger.Sugar(),

		announcementMaxAge: announcementMaxAge,
		retryTxIntervals:   retryTxIntervals,
		walletAddress:      walletAddress,

		syncSig: make(chan struct{}, 1),

		closedChan: make(chan struct{}),
	}

	// make sure we don't hang
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// subscribe ourselves to receive new contract ids
	subscriber.knownContracts, subscriber.csUnsubscribeFn, err = contracts.AddContractStoreSubscriber(ctx, subscriber)
	if err != nil {
		return nil, err
	}

	return subscriber, nil
}

func (s *Subscriber) AddContractID(id types.FileContractID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.knownContracts[id] = struct{}{}
}

func (s *Subscriber) Close() error {
	// signal we are closing
	close(s.closedChan)

	// unsubscribe from chain manager
	s.csUnsubscribeFn()

	// wait for sync loop to finish
	s.wg.Wait()

	return nil
}

func (s *Subscriber) Run() (func(), error) {
	// perform an initial sync
	index, err := s.cs.ChainIndex()
	if err != nil {
		return nil, err
	}
	if err := s.sync(index); err != nil {
		return nil, fmt.Errorf("failed to subscribe to chain manager: %w", err)
	}

	// start sync loop in separate goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			select {
			case <-s.closedChan:
				return
			case <-s.syncSig:
			}

			ci, err := s.cs.ChainIndex()
			if err != nil {
				s.logger.Errorf("failed to get chain index: %v", err)
				continue
			}

			if err := s.sync(ci); err != nil && !errors.Is(err, errClosed) {
				s.logger.Errorf("failed to sync: %v", err)
			}
		}
	}()

	// trigger a sync on reorgs
	return s.cm.OnReorg(func(types.ChainIndex) { s.triggerSync() }), nil
}

func (s *Subscriber) applyChainUpdate(tx ChainUpdateTx, cau chain.ApplyUpdate) (err error) {
	// apply host updates
	b := cau.Block
	if time.Since(b.Timestamp) <= s.announcementMaxAge {
		chain.ForEachHostAnnouncement(b, func(hk types.PublicKey, ha chain.HostAnnouncement) {
			if err != nil {
				return // error occurred
			}
			if ha.NetAddress == "" {
				return // ignore
			}
			err = tx.UpdateHost(hk, ha, cau.State.Index.Height, b.ID(), b.Timestamp)
		})
		if err != nil {
			return fmt.Errorf("failed to update host: %w", err)
		}
	}

	// v1 contracts
	cau.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved, valid bool) {
		if err != nil {
			return // error occurred
		}
		curr := &revision{
			revisionNumber: fce.FileContract.RevisionNumber,
			fileSize:       fce.FileContract.Filesize,
		}
		if rev != nil {
			curr.revisionNumber = rev.FileContract.RevisionNumber
			curr.fileSize = rev.FileContract.Filesize
		}
		err = s.updateContract(tx, cau.State.Index, types.FileContractID(fce.ID), nil, curr, resolved, valid)
	})
	if err != nil {
		return fmt.Errorf("failed to process v1 contracts: %w", err)
	}

	// v2 contracts
	cau.ForEachV2FileContractElement(func(fce types.V2FileContractElement, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
		if err != nil {
			return // error occurred
		}
		curr := &revision{
			revisionNumber: fce.V2FileContract.RevisionNumber,
			fileSize:       fce.V2FileContract.Filesize,
		}
		if rev != nil {
			curr.revisionNumber = rev.V2FileContract.RevisionNumber
			curr.fileSize = rev.V2FileContract.Filesize
		}
		resolved, valid := checkFileContract(fce, res)
		err = s.updateContract(tx, cau.State.Index, types.FileContractID(fce.ID), nil, curr, resolved, valid)
	})
	if err != nil {
		return fmt.Errorf("failed to process v2 contracts: %w", err)
	}
	return nil
}

func (s *Subscriber) isKnownContract(fcid types.FileContractID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, known := s.knownContracts[fcid]
	return known
}

func (s *Subscriber) revertChainUpdate(tx ChainUpdateTx, cru chain.RevertUpdate) (err error) {
	// v1 contracts
	cru.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved, valid bool) {
		if err != nil {
			return // error occurred
		}

		var prev, curr *revision
		if rev != nil {
			curr = &revision{
				revisionNumber: rev.FileContract.RevisionNumber,
				fileSize:       rev.FileContract.Filesize,
			}
		}
		prev = &revision{
			revisionNumber: fce.FileContract.RevisionNumber,
			fileSize:       fce.FileContract.Filesize,
		}
		err = s.updateContract(tx, cru.State.Index, types.FileContractID(fce.ID), prev, curr, resolved, valid)
	})
	if err != nil {
		return fmt.Errorf("failed to revert v1 contract: %w", err)
	}

	// v2 contracts
	cru.ForEachV2FileContractElement(func(fce types.V2FileContractElement, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
		if err != nil {
			return // error occurred
		}

		prev := &revision{
			revisionNumber: fce.V2FileContract.RevisionNumber,
			fileSize:       fce.V2FileContract.Filesize,
		}
		var curr *revision
		if rev != nil {
			curr = &revision{
				revisionNumber: rev.V2FileContract.RevisionNumber,
				fileSize:       rev.V2FileContract.Filesize,
			}
		}

		resolved, valid := checkFileContract(fce, res)
		err = s.updateContract(tx, cru.State.Index, types.FileContractID(fce.ID), prev, curr, resolved, valid)
	})
	if err != nil {
		return fmt.Errorf("failed to revert v2 contract: %w", err)
	}

	return nil
}

func (s *Subscriber) sync(index types.ChainIndex) error {
	for index != s.cm.Tip() {
		// check if subscriber was closed
		select {
		case <-s.closedChan:
			return errClosed
		default:
		}

		// fetch updates
		crus, caus, err := s.cm.UpdatesSince(index, updatesBatchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch updates: %w", err)
		}

		// start retry loop
		for i := 1; i <= len(s.retryTxIntervals)+1; i++ {
			// check if subscriber was closed
			select {
			case <-s.closedChan:
				return errClosed
			default:
			}

			// process updates
			index, err = s.processUpdates(crus, caus)
			if err == nil {
				break
			}

			// no more retries left
			if i-1 == len(s.retryTxIntervals) {
				s.logger.Error(fmt.Sprintf("transaction attempt %d/%d failed, err: %v", i, len(s.retryTxIntervals)+1, err))
				return fmt.Errorf("failed to process updates after %d attempts: %w", i, err)
			}

			// sleep
			interval := s.retryTxIntervals[i-1]
			s.logger.Warn(fmt.Sprintf("transaction attempt %d/%d failed, retry in %v, err: %v", i, len(s.retryTxIntervals)+1, interval, err))
			time.Sleep(interval)
		}
	}
	return nil
}

func (s *Subscriber) processUpdates(crus []chain.RevertUpdate, caus []chain.ApplyUpdate) (types.ChainIndex, error) {
	var index types.ChainIndex
	if err := s.cs.ProcessChainUpdate(func(tx ChainUpdateTx) error {
		// process wallet updates
		if err := wallet.UpdateChainState(tx, s.walletAddress, caus, crus); err != nil {
			return fmt.Errorf("failed to process wallet updates: %w", err)
		}

		// process revert updates
		for _, cru := range crus {
			if err := s.revertChainUpdate(tx, cru); err != nil {
				return fmt.Errorf("failed to revert chain update: %w", err)
			}
		}

		// process apply updates
		for _, cau := range caus {
			if err := s.applyChainUpdate(tx, cau); err != nil {
				return fmt.Errorf("failed to apply chain updates: %w", err)
			}
		}

		// update chain index
		index = caus[len(caus)-1].State.Index
		if err := tx.UpdateChainIndex(index); err != nil {
			return fmt.Errorf("failed to update chain index: %w", err)
		}

		// update failed contracts
		if err := tx.UpdateFailedContracts(index.Height); err != nil {
			return fmt.Errorf("failed to update failed contracts: %w", err)
		}

		return nil
	}); err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to process chain update: %w", err)
	}
	return index, nil
}

func (s *Subscriber) triggerSync() {
	select {
	case s.syncSig <- struct{}{}:
	default:
	}
}

func (s *Subscriber) updateContract(tx ChainUpdateTx, index types.ChainIndex, fcid types.FileContractID, prev, curr *revision, resolved, valid bool) error {
	// sanity check at least one is not nil
	if prev == nil && curr == nil {
		return errors.New("both prev and curr revisions are nil") // developer error
	}

	// ignore unknown contracts
	if !s.isKnownContract(fcid) {
		return nil
	}

	// fetch contract state
	state, err := tx.ContractState(fcid)
	if err != nil {
		return fmt.Errorf("failed to get contract state: %w", err)
	}

	// handle reverts
	if prev != nil {
		// update state from 'active' -> 'pending'
		if curr == nil {
			if err := tx.UpdateContractState(fcid, api.ContractStatePending); err != nil {
				return fmt.Errorf("failed to update contract state: %w", err)
			}
		}

		// reverted renewal: 'complete' -> 'active'
		if curr != nil {
			if err := tx.UpdateContract(fcid, index.Height, prev.revisionNumber, prev.fileSize); err != nil {
				return fmt.Errorf("failed to revert contract: %w", err)
			}
			if state == api.ContractStateComplete {
				if err := tx.UpdateContractState(fcid, api.ContractStateActive); err != nil {
					return fmt.Errorf("failed to update contract state: %w", err)
				}
				s.logger.Infow("contract state changed: complete -> active",
					"fcid", fcid,
					"reason", "final revision reverted")
			}
		}

		// reverted storage proof: 'complete/failed' -> 'active'
		if resolved {
			if err := tx.UpdateContractState(fcid, api.ContractStateActive); err != nil {
				return fmt.Errorf("failed to update contract state: %w", err)
			}
			if valid {
				s.logger.Infow("contract state changed: complete -> active",
					"fcid", fcid,
					"reason", "storage proof reverted")
			} else {
				s.logger.Infow("contract state changed: failed -> active",
					"fcid", fcid,
					"reason", "storage proof reverted")
			}
		}

		return nil
	}

	// handle apply
	if err := tx.UpdateContract(fcid, index.Height, curr.revisionNumber, curr.fileSize); err != nil {
		return fmt.Errorf("failed to update contract: %w", err)
	}

	// update state from 'pending' -> 'active'
	if state == api.ContractStatePending || state == api.ContractStateUnknown {
		if err := tx.UpdateContractState(fcid, api.ContractStateActive); err != nil {
			return fmt.Errorf("failed to update contract state: %w", err)
		}
		s.logger.Infow("contract state changed: pending -> active",
			"fcid", fcid,
			"reason", "contract confirmed")
	}

	// renewed: 'active' -> 'complete'
	if curr.revisionNumber == types.MaxRevisionNumber && curr.fileSize == 0 {
		if err := tx.UpdateContractState(fcid, api.ContractStateComplete); err != nil {
			return fmt.Errorf("failed to update contract state: %w", err)
		}
		s.logger.Infow("contract state changed: active -> complete",
			"fcid", fcid,
			"reason", "final revision confirmed")
	}

	// storage proof: 'active' -> 'complete/failed'
	if resolved {
		if err := tx.UpdateContractProofHeight(fcid, index.Height); err != nil {
			return fmt.Errorf("failed to update contract proof height: %w", err)
		}
		if valid {
			if err := tx.UpdateContractState(fcid, api.ContractStateComplete); err != nil {
				return fmt.Errorf("failed to update contract state: %w", err)
			}
			s.logger.Infow("contract state changed: active -> complete",
				"fcid", fcid,
				"reason", "storage proof valid")
		} else {
			if err := tx.UpdateContractState(fcid, api.ContractStateFailed); err != nil {
				return fmt.Errorf("failed to update contract state: %w", err)
			}
			s.logger.Infow("contract state changed: active -> failed",
				"fcid", fcid,
				"reason", "storage proof missed")
		}
	}
	return nil
}

func checkFileContract(fce types.V2FileContractElement, res types.V2FileContractResolutionType) (resolved bool, valid bool) {
	if res == nil {
		return
	}
	resolved = true

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
	return
}
