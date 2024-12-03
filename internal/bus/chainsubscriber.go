package bus

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
)

const (
	// maxAddrsPerProtocol is the maximum number of announced addresses we will
	// track per host, per protocol for a V2 announcement
	maxAddrsPerProtocol = 2

	// updatesBatchSize is the maximum number of updates to fetch in a single
	// call to the chain manager when we request updates since a given index.
	updatesBatchSize = 100

	// syncUpdateFrequency is the frequency with which we log sync progress.
	syncUpdateFrequency = 1e3 * updatesBatchSize
)

var (
	errClosed = errors.New("subscriber closed")
)

type (
	ChainManager interface {
		OnReorg(fn func(types.ChainIndex)) (cancel func())
		RecommendedFee() types.Currency
		Tip() types.ChainIndex
		UpdatesSince(index types.ChainIndex, max int) (rus []chain.RevertUpdate, aus []chain.ApplyUpdate, err error)
	}

	ChainStore interface {
		ChainIndex(ctx context.Context) (types.ChainIndex, error)
		ProcessChainUpdate(ctx context.Context, applyFn func(sql.ChainUpdateTx) error) error
	}

	WebhookManager interface {
		webhooks.Broadcaster
		Delete(context.Context, webhooks.Webhook) error
		Info() ([]webhooks.Webhook, []webhooks.WebhookQueueInfo)
		Register(context.Context, webhooks.Webhook) error
		Shutdown(context.Context) error
	}

	Wallet interface {
		UpdateChainState(tx wallet.UpdateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error
	}

	chainSubscriber struct {
		cm     ChainManager
		cs     ChainStore
		wm     WebhookManager
		logger *zap.SugaredLogger

		announcementMaxAge time.Duration
		wallet             Wallet

		shutdownCtx       context.Context
		shutdownCtxCancel context.CancelCauseFunc
		syncSig           chan struct{}
		wg                sync.WaitGroup

		mu             sync.Mutex
		knownContracts map[types.FileContractID]bool
		unsubscribeFn  func()
	}
)

type (
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
)

// NewChainSubscriber creates a new chain subscriber that will sync with the
// given chain manager and chain store. The returned subscriber is already
// running and can be stopped by calling Shutdown.
func NewChainSubscriber(whm WebhookManager, cm ChainManager, cs ChainStore, w Wallet, announcementMaxAge time.Duration, logger *zap.Logger) *chainSubscriber {
	logger = logger.Named("chainsubscriber")
	ctx, cancel := context.WithCancelCause(context.Background())
	subscriber := &chainSubscriber{
		cm:     cm,
		cs:     cs,
		wm:     whm,
		logger: logger.Sugar(),

		announcementMaxAge: announcementMaxAge,
		wallet:             w,

		shutdownCtx:       ctx,
		shutdownCtxCancel: cancel,
		syncSig:           make(chan struct{}, 1),

		knownContracts: make(map[types.FileContractID]bool),
	}

	// start the subscriber
	subscriber.run()

	// trigger a sync on reorgs
	subscriber.unsubscribeFn = cm.OnReorg(func(ci types.ChainIndex) {
		select {
		case subscriber.syncSig <- struct{}{}:
			subscriber.logger.Debugw("reorg triggered", "height", ci.Height, "block_id", ci.ID)
		default:
		}
	})

	return subscriber
}

func (s *chainSubscriber) ChainIndex(ctx context.Context) (types.ChainIndex, error) {
	return s.cs.ChainIndex(ctx)
}

func (s *chainSubscriber) Shutdown(ctx context.Context) error {
	// cancel shutdown context
	s.shutdownCtxCancel(errClosed)

	// unsubscribe from the chain manager
	if s.unsubscribeFn != nil {
		s.unsubscribeFn()
	}

	// wait for sync loop to finish
	waitChan := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(waitChan)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitChan:
	}
	return nil
}

func (s *chainSubscriber) applyChainUpdate(tx sql.ChainUpdateTx, cau chain.ApplyUpdate) error {
	// apply host updates
	b := cau.Block
	if time.Since(b.Timestamp) <= s.announcementMaxAge {
		v1Hus := make(map[types.PublicKey]string)
		chain.ForEachHostAnnouncement(b, func(ha chain.HostAnnouncement) {
			if ha.NetAddress != "" {
				v1Hus[ha.PublicKey] = ha.NetAddress
			}
		})
		v2Hus := make(map[types.PublicKey]chain.V2HostAnnouncement)
		chain.ForEachV2HostAnnouncement(b, func(hk types.PublicKey, addrs []chain.NetAddress) {
			filtered := make(map[chain.Protocol][]chain.NetAddress)
			for _, addr := range addrs {
				if addr.Address == "" || addr.Protocol != rhp4.ProtocolTCPSiaMux {
					continue
				} else if len(filtered[addr.Protocol]) < maxAddrsPerProtocol {
					filtered[addr.Protocol] = append(filtered[addr.Protocol], addr)
				}
			}
			for _, addrs := range filtered {
				v2Hus[hk] = append(v2Hus[hk], addrs...)
			}
		})
		// v1 announcements
		for hk, addr := range v1Hus {
			if err := tx.UpdateHost(hk, addr, nil, cau.State.Index.Height, b.ID(), b.Timestamp); err != nil {
				return fmt.Errorf("failed to update host: %w", err)
			}
		}
		// v2 announcements
		for hk, ha := range v2Hus {
			if err := tx.UpdateHost(hk, "", ha, cau.State.Index.Height, b.ID(), b.Timestamp); err != nil {
				return fmt.Errorf("failed to update host: %w", err)
			}
		}
	}

	// v1 contracts
	cus := make(map[types.FileContractID]contractUpdate)
	cau.ForEachFileContractElement(func(fce types.FileContractElement, _ bool, rev *types.FileContractElement, resolved, valid bool) {
		cus[types.FileContractID(fce.ID)] = v1ContractUpdate(fce, rev, resolved, valid)
	})

	// v2 contracts
	cus = make(map[types.FileContractID]contractUpdate)
	var revisedContracts []types.V2FileContractElement
	cau.ForEachV2FileContractElement(func(fce types.V2FileContractElement, created bool, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
		if created {
			revisedContracts = append(revisedContracts, fce) // created
		} else if rev != nil {
			revisedContracts = append(revisedContracts, *rev) // revised
		}
		cus[types.FileContractID(fce.ID)] = v2ContractUpdate(fce, rev, res)
	})

	// updates - this updates the 'known' contracts too so we do this first
	for _, cu := range cus {
		if err := s.updateContract(tx, cau.State.Index, cu.fcid, cu.prev, cu.curr, cu.resolved, cu.valid); err != nil {
			return fmt.Errorf("failed to apply contract updates: %w", err)
		}
	}

	// new contracts - only consider the ones we are interested in
	filtered := revisedContracts[:0]
	for _, fce := range revisedContracts {
		if s.isKnownContract(fce.ID) {
			filtered = append(filtered, fce)
		}
	}
	if err := tx.UpdateFileContractElements(filtered); err != nil {
		return fmt.Errorf("failed to insert v2 file contract elements: %w", err)
	}

	// contract proofs
	if err := tx.UpdateFileContractElementProofs(cau); err != nil {
		return fmt.Errorf("failed to update file contract element proofs: %w", err)
	}

	// prune contracts 144 blocks after window_end
	const fce_prune_window = 144
	if cau.State.Index.Height > fce_prune_window {
		if err := tx.PruneFileContractElements(cau.State.Index.Height - fce_prune_window); err != nil {
			return fmt.Errorf("failed to prune file contract elements: %w", err)
		}
	}
	return nil
}

func (s *chainSubscriber) revertChainUpdate(tx sql.ChainUpdateTx, cru chain.RevertUpdate) error {
	// NOTE: host updates are not reverted

	// v1 contracts
	var cus []contractUpdate
	cru.ForEachFileContractElement(func(fce types.FileContractElement, _ bool, rev *types.FileContractElement, resolved, valid bool) {
		cus = append(cus, v1ContractUpdate(fce, rev, resolved, valid))
	})

	// v2 contracts
	cus = cus[:0]
	var revertedContracts []types.V2FileContractElement
	cru.ForEachV2FileContractElement(func(fce types.V2FileContractElement, created bool, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
		if created {
			revertedContracts = append(revertedContracts, fce)
		} else if rev != nil {
			revertedContracts = append(revertedContracts, *rev)
		}
		cus = append(cus, v2ContractUpdate(fce, rev, res))
	})

	// updates - this updates the 'known' contracts too so we do this first
	for _, cu := range cus {
		if err := s.updateContract(tx, cru.State.Index, cu.fcid, cu.prev, cu.curr, cu.resolved, cu.valid); err != nil {
			return fmt.Errorf("failed to revert v2 contract update: %w", err)
		}
	}

	// reverted contracts - only consider the ones that we are interested in
	filtered := revertedContracts[:0]
	for _, fce := range revertedContracts {
		if s.isKnownContract(fce.ID) {
			filtered = append(filtered, fce)
		}
	}
	if err := tx.UpdateFileContractElements(filtered); err != nil {
		return fmt.Errorf("failed to remove v2 file contract elements: %w", err)
	}

	// contract proofs
	if err := tx.UpdateFileContractElementProofs(cru); err != nil {
		return fmt.Errorf("failed to update file contract element proofs: %w", err)
	}
	return nil
}

func (s *chainSubscriber) run() {
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
}

func (s *chainSubscriber) sync() error {
	start := time.Now()

	// fetch current chain index
	index, err := s.cs.ChainIndex(s.shutdownCtx)
	if err != nil {
		return fmt.Errorf("failed to get chain index: %w", err)
	}
	s.logger.Debugw("sync started", "height", index.Height, "block_id", index.ID)
	sheight := index.Height / syncUpdateFrequency

	// fetch updates until we're caught up
	var cnt uint64
	for index != s.cm.Tip() && index.Height <= s.cm.Tip().Height && !s.isClosed() {
		// fetch updates
		istart := time.Now()
		crus, caus, err := s.cm.UpdatesSince(index, updatesBatchSize)
		if err != nil {
			return fmt.Errorf("failed to fetch updates: %w", err)
		}
		s.logger.Debugw("fetched updates since", "caus", len(caus), "crus", len(crus), "since_height", index.Height, "since_block_id", index.ID, "ms", time.Since(istart).Milliseconds(), "batch_size", updatesBatchSize)

		// process updates
		istart = time.Now()
		index, err = s.processUpdates(s.shutdownCtx, crus, caus)
		if err != nil {
			return fmt.Errorf("failed to process updates: %w", err)
		}
		s.logger.Debugw("processed updates successfully", "new_height", index.Height, "new_block_id", index.ID, "ms", time.Since(istart).Milliseconds())
		cnt++
	}

	s.logger.Debugw("sync completed", "height", index.Height, "block_id", index.ID, "ms", time.Since(start).Milliseconds(), "iterations", cnt)

	// info log sync progress
	if index.Height/syncUpdateFrequency != sheight {
		s.logger.Infow("sync progress", "height", index.Height, "block_id", index.ID)
	}
	return nil
}

func (s *chainSubscriber) processUpdates(ctx context.Context, crus []chain.RevertUpdate, caus []chain.ApplyUpdate) (index types.ChainIndex, err error) {
	err = s.cs.ProcessChainUpdate(ctx, func(tx sql.ChainUpdateTx) error {
		// process wallet updates
		if err := s.wallet.UpdateChainState(tx, crus, caus); err != nil {
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
	})
	return
}

func (s *chainSubscriber) updateContract(tx sql.ChainUpdateTx, index types.ChainIndex, fcid types.FileContractID, prev, curr *revision, resolved, valid bool) error {
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

	// define a helper function to update the contract state
	updateState := func(update api.ContractState) (err error) {
		if state != update {
			err = tx.UpdateContractState(fcid, update)
			if err == nil {
				state = update
			}
		}
		return
	}

	// handle reverts
	if prev != nil {
		// update state from 'active' -> 'pending'
		if curr == nil {
			if err := updateState(api.ContractStatePending); err != nil {
				return fmt.Errorf("failed to update contract state: %w", err)
			}
		}

		// reverted renewal: 'complete' -> 'active'
		if curr != nil {
			if err := tx.UpdateContractRevision(fcid, index.Height, prev.revisionNumber, prev.fileSize); err != nil {
				return fmt.Errorf("failed to revert contract: %w", err)
			}
			if state == api.ContractStateComplete {
				if err := updateState(api.ContractStateActive); err != nil {
					return fmt.Errorf("failed to update contract state: %w", err)
				}
				s.logger.Infow("contract state changed: complete -> active",
					"fcid", fcid,
					"reason", "final revision reverted")
			}
		}

		// reverted storage proof: 'complete/failed' -> 'active'
		if resolved {
			if err := updateState(api.ContractStateActive); err != nil {
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
	if err := tx.UpdateContractRevision(fcid, index.Height, curr.revisionNumber, curr.fileSize); err != nil {
		return fmt.Errorf("failed to update contract %v: %w", fcid, err)
	}

	// update state from 'pending' -> 'active'
	if state == api.ContractStatePending || state == api.ContractStateUnknown {
		if err := updateState(api.ContractStateActive); err != nil {
			return fmt.Errorf("failed to update contract state: %w", err)
		}
		s.logger.Infow("contract state changed: pending -> active",
			"fcid", fcid,
			"reason", "contract confirmed")
	}

	// renewed: 'active' -> 'complete'
	if curr.revisionNumber == types.MaxRevisionNumber && curr.fileSize == 0 {
		if err := updateState(api.ContractStateComplete); err != nil {
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
			if err := updateState(api.ContractStateComplete); err != nil {
				return fmt.Errorf("failed to update contract state: %w", err)
			}
			s.logger.Infow("contract state changed: active -> complete",
				"fcid", fcid,
				"reason", "storage proof valid")
		} else {
			if err := updateState(api.ContractStateFailed); err != nil {
				return fmt.Errorf("failed to update contract state: %w", err)
			}
			s.logger.Infow("contract state changed: active -> failed",
				"fcid", fcid,
				"reason", "storage proof missed")
		}
	}
	return nil
}

func (s *chainSubscriber) isClosed() bool {
	select {
	case <-s.shutdownCtx.Done():
		return true
	default:
	}
	return false
}

func (s *chainSubscriber) isKnownContract(fcid types.FileContractID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	known, ok := s.knownContracts[fcid]
	if !ok {
		return true // assume known
	}
	return known
}

func (s *chainSubscriber) updateKnownContracts(fcid types.FileContractID, known bool) {
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
		switch res.(type) {
		case *types.V2FileContractRenewal:
			valid = true
			// hack to make sure v2 contracts also have the sentinel revision
			// number
			curr.revisionNumber = math.MaxUint64
		case *types.V2StorageProof:
			valid = true
			resolved = true
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
