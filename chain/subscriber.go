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
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

const (
	// updatesBatchSize is the maximum number of updates to fetch in a single
	// call to the chain manager when we request updates since a given index.
	updatesBatchSize = 50
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
		ChainIndex(ctx context.Context) (types.ChainIndex, error)
		ProcessChainUpdate(ctx context.Context, fn func(ChainUpdateTx) error) error
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

	ApplyChainUpdateFn = func(ChainUpdateTx) error

	Subscriber struct {
		cm     ChainManager
		cs     ChainStore
		logger *zap.SugaredLogger

		announcementMaxAge time.Duration
		walletAddress      types.Address

		shutdownCtx       context.Context
		shutdownCtxCancel context.CancelCauseFunc
		syncSig           chan struct{}
		wg                sync.WaitGroup

		mu             sync.Mutex
		knownContracts map[types.FileContractID]bool
	}

	revision struct {
		revisionNumber uint64
		fileSize       uint64
	}

	contractUpdate struct {
		fcid     types.FileContractID
		prev     *revision
		curr     *revision
		resolved bool
		valid    bool
	}

	hostUpdate struct {
		hk types.PublicKey
		ha chain.HostAnnouncement
	}
)

func NewSubscriber(cm ChainManager, cs ChainStore, walletAddress types.Address, announcementMaxAge time.Duration, logger *zap.Logger) (_ *Subscriber, err error) {
	if announcementMaxAge == 0 {
		return nil, errors.New("announcementMaxAge must be non-zero")
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	return &Subscriber{
		cm:     cm,
		cs:     cs,
		logger: logger.Sugar(),

		announcementMaxAge: announcementMaxAge,
		walletAddress:      walletAddress,

		shutdownCtx:       ctx,
		shutdownCtxCancel: cancel,
		syncSig:           make(chan struct{}, 1),

		knownContracts: make(map[types.FileContractID]bool),
	}, nil
}

func (s *Subscriber) Close() error {
	// cancel shutdown context
	s.shutdownCtxCancel(errClosed)

	// wait for sync loop to finish
	s.wg.Wait()
	return nil
}

func (s *Subscriber) Run() (func(), error) {
	// perform an initial sync
	start := time.Now()
	if err := s.sync(); err != nil {
		return nil, fmt.Errorf("initial sync failed: %w", err)
	}
	s.logger.Debugw("initial sync completed", "duration", time.Since(start))

	// start sync loop in separate goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			select {
			case <-s.shutdownCtx.Done():
				return
			case <-s.syncSig:
			}

			if err := s.sync(); errors.Is(err, errClosed) || errors.Is(err, context.Canceled) {
				return
			} else if err != nil {
				s.logger.Panicf("failed to sync: %v", err)
			}
		}
	}()

	// trigger a sync on reorgs
	return s.cm.OnReorg(func(ci types.ChainIndex) {
		select {
		case s.syncSig <- struct{}{}:
			s.logger.Debugw("reorg triggered", "height", ci.Height, "block_id", ci.ID)
		default:
		}
	}), nil
}

func (s *Subscriber) applyChainUpdate(tx ChainUpdateTx, cau chain.ApplyUpdate) error {
	// apply host updates
	b := cau.Block
	if time.Since(b.Timestamp) <= s.announcementMaxAge {
		var hus []hostUpdate
		chain.ForEachHostAnnouncement(b, func(hk types.PublicKey, ha chain.HostAnnouncement) {
			if ha.NetAddress != "" {
				hus = append(hus, hostUpdate{hk, ha})
			}
		})
		for _, hu := range hus {
			if err := tx.UpdateHost(hu.hk, hu.ha, cau.State.Index.Height, b.ID(), b.Timestamp); err != nil {
				return fmt.Errorf("failed to update host: %w", err)
			}
		}
	}

	// v1 contracts
	var cus []contractUpdate
	cau.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved, valid bool) {
		cus = append(cus, v1ContractUpdate(fce, rev, resolved, valid))
	})
	for _, cu := range cus {
		if err := s.updateContract(tx, cau.State.Index, cu.fcid, cu.prev, cu.curr, cu.resolved, cu.valid); err != nil {
			return fmt.Errorf("failed to apply v1 contract update: %w", err)
		}
	}

	// v2 contracts
	cus = cus[:0]
	cau.ForEachV2FileContractElement(func(fce types.V2FileContractElement, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
		cus = append(cus, v2ContractUpdate(fce, rev, res))
	})
	for _, cu := range cus {
		if err := s.updateContract(tx, cau.State.Index, cu.fcid, cu.prev, cu.curr, cu.resolved, cu.valid); err != nil {
			return fmt.Errorf("failed to apply v2 contract update: %w", err)
		}
	}
	return nil
}

func (s *Subscriber) revertChainUpdate(tx ChainUpdateTx, cru chain.RevertUpdate) error {
	// NOTE: host updates are not reverted

	// v1 contracts
	var cus []contractUpdate
	cru.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved, valid bool) {
		cus = append(cus, v1ContractUpdate(fce, rev, resolved, valid))
	})
	for _, cu := range cus {
		if err := s.updateContract(tx, cru.State.Index, cu.fcid, cu.prev, cu.curr, cu.resolved, cu.valid); err != nil {
			return fmt.Errorf("failed to revert v1 contract update: %w", err)
		}
	}

	// v2 contracts
	cus = cus[:0]
	cru.ForEachV2FileContractElement(func(fce types.V2FileContractElement, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
		cus = append(cus, v2ContractUpdate(fce, rev, res))
	})
	for _, cu := range cus {
		if err := s.updateContract(tx, cru.State.Index, cu.fcid, cu.prev, cu.curr, cu.resolved, cu.valid); err != nil {
			return fmt.Errorf("failed to revert v2 contract update: %w", err)
		}
	}

	return nil
}

func (s *Subscriber) sync() error {
	start := time.Now()

	// fetch current chain index
	index, err := s.cs.ChainIndex(s.shutdownCtx)
	if err != nil {
		return fmt.Errorf("failed to get chain index: %w", err)
	}
	s.logger.Debugw("sync started", "height", index.Height, "block_id", index.ID)

	// fetch updates until we're caught up
	var cnt uint64
	for index != s.cm.Tip() && !s.isClosed() {
		// fetch updates
		istart := time.Now()
		crus, caus, err := s.cm.UpdatesSince(index, updatesBatchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch updates: %w", err)
		}
		s.logger.Debugw("fetched updates since", "caus", len(caus), "crus", len(crus), "since_height", index.Height, "since_block_id", index.ID, "ms", time.Since(istart).Milliseconds())

		// process updates
		istart = time.Now()
		index, err = s.processUpdates(s.shutdownCtx, crus, caus)
		if err != nil {
			return fmt.Errorf("failed to process updates: %w", err)
		}
		s.logger.Debugw("processed updates successfully", "new_height", index.Height, "new_block_id", index.ID, "ms", time.Since(istart).Milliseconds())
		cnt++
	}

	s.logger.Debugw("sync completed", "start_height", index.Height, "block_id", index.ID, "ms", time.Since(start).Milliseconds(), "iterations", cnt)
	return nil
}

func (s *Subscriber) processUpdates(ctx context.Context, crus []chain.RevertUpdate, caus []chain.ApplyUpdate) (types.ChainIndex, error) {
	var index types.ChainIndex
	if err := s.cs.ProcessChainUpdate(ctx, func(tx ChainUpdateTx) error {
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
		return types.ChainIndex{}, err
	}
	return index, nil
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
	if err != nil && utils.IsErr(err, api.ErrContractNotFound) {
		s.updateKnownContracts(fcid, false) // ignore unknown contracts
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to get contract state: %w", err)
	} else {
		s.updateKnownContracts(fcid, true) // update known contracts
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

func (s *Subscriber) isClosed() bool {
	select {
	case <-s.shutdownCtx.Done():
		return true
	default:
	}
	return false
}

func (s *Subscriber) isKnownContract(fcid types.FileContractID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	known, ok := s.knownContracts[fcid]
	if !ok {
		return true // assume known
	}
	return known
}

func (s *Subscriber) updateKnownContracts(fcid types.FileContractID, known bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.knownContracts[fcid] = known
}

func v1ContractUpdate(fce types.FileContractElement, rev *types.FileContractElement, resolved, valid bool) contractUpdate {
	curr := &revision{
		revisionNumber: fce.FileContract.RevisionNumber,
		fileSize:       fce.FileContract.Filesize,
	}
	if rev != nil {
		curr.revisionNumber = rev.FileContract.RevisionNumber
		curr.fileSize = rev.FileContract.Filesize
	}
	return contractUpdate{
		fcid:     types.FileContractID(fce.ID),
		prev:     nil,
		curr:     curr,
		resolved: resolved,
		valid:    valid,
	}
}

func v2ContractUpdate(fce types.V2FileContractElement, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) contractUpdate {
	curr := &revision{
		revisionNumber: fce.V2FileContract.RevisionNumber,
		fileSize:       fce.V2FileContract.Filesize,
	}
	if rev != nil {
		curr.revisionNumber = rev.V2FileContract.RevisionNumber
		curr.fileSize = rev.V2FileContract.Filesize
	}

	var resolved, valid bool
	if res != nil {
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
	}

	return contractUpdate{
		fcid:     types.FileContractID(fce.ID),
		prev:     nil,
		curr:     curr,
		resolved: resolved,
		valid:    valid,
	}
}
