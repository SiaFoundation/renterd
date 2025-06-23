package bus

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/stores/sql"
	"go.uber.org/zap"
)

const (
	ContractResolutionTxnWeight = 1000
)

const (
	// contractElementPruneWindow is the number of blocks beyond a contract's
	// prune window when we start deleting its file contract elements.
	contractElementPruneWindow = 144

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
		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
		AddV2PoolTransactions(basis types.ChainIndex, txns []types.V2Transaction) (known bool, err error)
		OnReorg(fn func(types.ChainIndex)) (cancel func())
		Tip() types.ChainIndex
		UpdatesSince(index types.ChainIndex, max int) (rus []chain.RevertUpdate, aus []chain.ApplyUpdate, err error)
	}

	ChainStore interface {
		ChainIndex(ctx context.Context) (types.ChainIndex, error)
		ProcessChainUpdate(ctx context.Context, applyFn func(sql.ChainUpdateTx) error) error
		ResetChainState(ctx context.Context) error
	}

	Wallet interface {
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) error
		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		RecommendedFee() types.Currency
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction) error
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
		UpdateChainState(tx wallet.UpdateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error
	}

	chainSubscriber struct {
		cm     ChainManager
		cs     ChainStore
		logger *zap.SugaredLogger

		announcementMaxAge time.Duration
		wallet             Wallet

		shutdownCtx       context.Context
		shutdownCtxCancel context.CancelCauseFunc
		syncSig           chan struct{}
		wg                sync.WaitGroup

		unsubscribeFn func()
	}
)

// NewChainSubscriber creates a new chain subscriber that will sync with the
// given chain manager and chain store. The returned subscriber is already
// running and can be stopped by calling Shutdown.
func NewChainSubscriber(cm ChainManager, cs ChainStore, w Wallet, announcementMaxAge time.Duration, logger *zap.Logger) *chainSubscriber {
	logger = logger.Named("chainsubscriber")
	ctx, cancel := context.WithCancelCause(context.Background())
	subscriber := &chainSubscriber{
		cm:     cm,
		cs:     cs,
		logger: logger.Sugar(),

		announcementMaxAge: announcementMaxAge,
		wallet:             w,

		shutdownCtx:       ctx,
		shutdownCtxCancel: cancel,
		syncSig:           make(chan struct{}, 1),
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
				if addr.Address == "" || addr.Protocol != siamux.Protocol {
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
	for _, diff := range cau.FileContractElementDiffs() {
		fce := diff.FileContractElement
		if known, err := tx.IsKnownContract(fce.ID); err != nil {
			return fmt.Errorf("failed to check if v1 contract is known: %w", err)
		} else if !known {
			continue // only consider known contracts
		}
		if err := s.applyV1ContractUpdate(tx, cau.State.Index, fce, diff.Created, diff.Revision, diff.Resolved, diff.Valid); err != nil {
			return fmt.Errorf("failed to apply v1 contract update: %w", err)
		}
	}

	// v2 contracts
	var revisedContracts []types.V2FileContractElement
	for _, diff := range cau.V2FileContractElementDiffs() {
		fce := diff.V2FileContractElement
		if known, err := tx.IsKnownContract(fce.ID); err != nil {
			return fmt.Errorf("failed to check if v2 contract is known: %w", err)
		} else if !known {
			continue // only consider known contracts
		}
		if rev, ok := diff.V2RevisionElement(); !ok {
			revisedContracts = append(revisedContracts, fce)
		} else {
			revisedContracts = append(revisedContracts, rev)
		}
		if err := s.applyV2ContractUpdate(tx, cau.State.Index, fce, diff.Created, diff.Revision, diff.Resolution); err != nil {
			return fmt.Errorf("failed to apply v2 contract update: %w", err)
		}
	}

	// update revised contracts
	if err := tx.UpdateFileContractElements(revisedContracts); err != nil {
		return fmt.Errorf("failed to insert v2 file contract elements: %w", err)
	}

	// update contract proofs
	if err := tx.UpdateFileContractElementProofs(cau); err != nil {
		return fmt.Errorf("failed to update file contract element proofs: %w", err)
	}

	// broadcast expired file contracts
	s.broadcastExpiredFileContractResolutions(tx, cau)

	if cau.State.Index.Height > contractElementPruneWindow {
		// prune contracts 144 blocks after window_end
		if err := tx.PruneFileContractElements(cau.State.Index.Height - contractElementPruneWindow); err != nil {
			return fmt.Errorf("failed to prune file contract elements: %w", err)
		}
		// mark contracts as failed a prune window after the window end
		if err := tx.UpdateFailedContracts(cau.State.Index.Height - contractElementPruneWindow); err != nil {
			return fmt.Errorf("failed to update failed contracts: %w", err)
		}
	}
	return nil
}

func (s *chainSubscriber) revertChainUpdate(tx sql.ChainUpdateTx, cru chain.RevertUpdate) error {
	// NOTE: host updates are not reverted

	// v1 contracts
	var err error
	for _, diff := range cru.FileContractElementDiffs() {
		fce := diff.FileContractElement
		if known, err := tx.IsKnownContract(fce.ID); err != nil {
			return fmt.Errorf("failed to check if v1 contract is known: %w", err)
		} else if !known {
			continue // only consider known contracts
		}
		if err := s.revertV1ContractUpdate(tx, fce, diff.Created, diff.Revision, diff.Resolved); err != nil {
			return fmt.Errorf("failed to revert v1 contract update: %w", err)
		}
	}

	// v2 contracts
	var revertedContracts []types.V2FileContractElement
	for _, diff := range cru.V2FileContractElementDiffs() {
		fce := diff.V2FileContractElement
		if known, lookupErr := tx.IsKnownContract(fce.ID); lookupErr != nil {
			return fmt.Errorf("failed to check if v2 contract is known: %w", err)
		} else if !known {
			continue // only consider known contracts
		}
		if rev, ok := diff.V2RevisionElement(); !ok {
			revertedContracts = append(revertedContracts, fce)
		} else {
			revertedContracts = append(revertedContracts, rev)
		}
		if err := s.revertV2ContractUpdate(tx, fce, diff.Created, diff.Resolution); err != nil {
			return fmt.Errorf("failed to revert v2 contract update: %w", err)
		}
	}

	// update reverted contracts
	if err := tx.UpdateFileContractElements(revertedContracts); err != nil {
		return fmt.Errorf("failed to remove v2 file contract elements: %w", err)
	}

	// update contract proofs
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
		if errors.Is(err, chain.ErrMissingBlock) {
			s.logger.Warnf("missing block, resetting chain state", "height", index.Height, "block_id", index.ID)
			if err := s.cs.ResetChainState(s.shutdownCtx); err != nil {
				s.logger.Debugw("failed to reset chain state after missing block", zap.Error(err))
				return fmt.Errorf("failed to reset chain state after missing block: %w", err)
			} else if index, err = s.cs.ChainIndex(s.shutdownCtx); err != nil {
				s.logger.Debugw("failed to get chain index after reset", zap.Error(err))
				return fmt.Errorf("failed to get chain index after reset: %w", err)
			}
			continue
		} else if err != nil {
			return fmt.Errorf("failed to fetch updates: %w", err)
		} else if len(crus)+len(caus) == 0 {
			return nil
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
		if len(caus) > 0 {
			index = caus[len(caus)-1].State.Index
		} else {
			index = crus[len(crus)-1].State.Index
		}
		if err := tx.UpdateChainIndex(index); err != nil {
			return fmt.Errorf("failed to update chain index: %w", err)
		}
		return nil
	})
	return
}

func (s *chainSubscriber) broadcastExpiredFileContractResolutions(tx sql.ChainUpdateTx, cau chain.ApplyUpdate) {
	expiredFCEs, err := tx.ExpiredFileContractElements(cau.State.Index.Height)
	if err != nil {
		s.logger.Errorf("failed to get expired file contract elements: %v", err)
		return
	}

	for _, fce := range expiredFCEs {
		go func(fce types.V2FileContractElement) {
			txn := types.V2Transaction{
				MinerFee: s.wallet.RecommendedFee().Mul64(ContractResolutionTxnWeight),
				FileContractResolutions: []types.V2FileContractResolution{
					{
						Parent:     fce,
						Resolution: &types.V2FileContractExpiration{},
					},
				},
			}

			// fund and sign txn
			basis, toSign, err := s.wallet.FundV2Transaction(&txn, txn.MinerFee, true)
			if err != nil {
				s.logger.Errorf("failed to fund contract expiration txn: %v", err)
				return
			}
			s.wallet.SignV2Inputs(&txn, toSign)

			basis, set, err := s.cm.V2TransactionSet(basis, txn)
			if err != nil {
				s.logger.Errorf("failed to get transaction set: %w", err)
				return
			}

			// verify txn and broadcast it
			_, err = s.cm.AddV2PoolTransactions(basis, set)
			err = s.wallet.BroadcastV2TransactionSet(basis, set)
			if err != nil &&
				(strings.Contains(err.Error(), "has already been resolved") ||
					strings.Contains(err.Error(), "not present in the accumulator")) {
				s.wallet.ReleaseInputs(nil, []types.V2Transaction{txn})
				s.logger.With(zap.Error(err)).Debug("failed to add contract expiration txn to pool")
				return
			} else if err != nil {
				s.logger.With(zap.Error(err)).Error("failed to add contract expiration txn to pool")
				s.wallet.ReleaseInputs(nil, []types.V2Transaction{txn})
				return
			}
		}(fce)
	}
}

func (s *chainSubscriber) applyV1ContractUpdate(tx sql.ChainUpdateTx, index types.ChainIndex, fce types.FileContractElement, created bool, rev *types.FileContract, resolved, valid bool) error {
	fcid := fce.ID

	// fetch contract state
	state, err := tx.ContractState(fcid)
	if err != nil {
		return fmt.Errorf("failed to get contract state: %w", err)
	}

	// update revision number and file size
	revisionNumber := fce.FileContract.RevisionNumber
	fileSize := fce.FileContract.Filesize
	if rev != nil {
		revisionNumber = rev.RevisionNumber
		fileSize = rev.Filesize
	}
	if err := tx.UpdateContractRevision(fcid, index.Height, revisionNumber, fileSize); err != nil {
		return fmt.Errorf("failed to update contract %v: %w", fcid, err)
	}

	// consider a contract resolved if it has a max revision number and zero
	// file size
	if rev != nil && rev.RevisionNumber == math.MaxUint64 && rev.Filesize == 0 {
		resolved = true
		valid = true
	}

	// contract was resolved via proof or renewal -> 'complete/failed'
	if resolved {
		if err := tx.UpdateContractProofHeight(fcid, index.Height); err != nil {
			return fmt.Errorf("failed to update contract proof height: %w", err)
		}
		if valid {
			if err := tx.UpdateContractState(fcid, api.ContractStateComplete); err != nil {
				return fmt.Errorf("failed to update contract state: %w", err)
			}
			s.logger.Infow(fmt.Sprintf("contract state changed: %s -> failed", state),
				"fcid", fcid,
				"reason", "storage proof valid")
		} else {
			if err := tx.UpdateContractState(fcid, api.ContractStateFailed); err != nil {
				return fmt.Errorf("failed to update contract state: %w", err)
			}
			s.logger.Infow(fmt.Sprintf("contract state changed: %s -> failed", state),
				"fcid", fcid,
				"reason", "storage proof missed")
		}
		return nil
	}

	// contract was created -> 'active'
	if created {
		if err := tx.UpdateContractState(fcid, api.ContractStateActive); err != nil {
			return fmt.Errorf("failed to update contract state: %w", err)
		}
		s.logger.Infow(fmt.Sprintf("contract state changed: %s -> active", state),
			"fcid", fcid,
			"reason", "contract confirmed")
		return nil
	}

	return nil
}

func (s *chainSubscriber) revertV1ContractUpdate(tx sql.ChainUpdateTx, fce types.FileContractElement, created bool, rev *types.FileContract, resolved bool) error {
	fcid := fce.ID

	// fetch contract state to see if contract is known
	state, err := tx.ContractState(fcid)
	if err != nil {
		return fmt.Errorf("failed to get contract state: %w", err)
	}

	// consider a contract resolved if it has a max revision number and zero
	// file size
	if rev != nil && rev.RevisionNumber == math.MaxUint64 && rev.Filesize == 0 {
		resolved = true
	}

	// contract was reverted -> 'pending'
	if created {
		if err := tx.UpdateContractState(fcid, api.ContractStatePending); err != nil {
			return fmt.Errorf("failed to update contract state: %w", err)
		}
		s.logger.Infow(fmt.Sprintf("contract state changed: %s -> active", state),
			"fcid", fcid,
			"reason", "contract was reverted")
		return nil
	}

	// reverted storage proof -> 'active'
	if resolved {
		if err := tx.UpdateContractState(fcid, api.ContractStateActive); err != nil {
			return fmt.Errorf("failed to update contract state: %w", err)
		}
		s.logger.Infow(fmt.Sprintf("contract state changed: %s -> active", state),
			"fcid", fcid,
			"reason", "storage proof reverted")
		return nil
	}

	return nil
}

func (s *chainSubscriber) applyV2ContractUpdate(tx sql.ChainUpdateTx, index types.ChainIndex, fce types.V2FileContractElement, created bool, rev *types.V2FileContract, res types.V2FileContractResolutionType) error {
	fcid := fce.ID

	// fetch contract state
	state, err := tx.ContractState(fcid)
	if err != nil {
		return fmt.Errorf("failed to get contract state: %w", err)
	}

	// update revision number and file size
	revisionNumber := fce.V2FileContract.RevisionNumber
	fileSize := fce.V2FileContract.Filesize
	if rev != nil {
		revisionNumber = rev.RevisionNumber
		fileSize = rev.Filesize
	}
	if err := tx.UpdateContractRevision(fcid, index.Height, revisionNumber, fileSize); err != nil {
		return fmt.Errorf("failed to update contract %v: %w", fcid, err)
	}

	// resolution -> 'complete/failed'
	if res != nil {
		var newState api.ContractState
		var reason string
		switch res.(type) {
		case *types.V2FileContractRenewal:
			newState = api.ContractStateComplete
			reason = "renewal"

			// link the renewed contract to the new one, this should not be
			// necessary if the renewal was successfully but there is a slim
			// chance that it's not when the renewal was interrupted
			if err := tx.RecordContractRenewal(fcid, fcid.V2RenewalID()); err != nil {
				return fmt.Errorf("failed to record contract renewal: %w", err)
			}

		case *types.V2StorageProof:
			newState = api.ContractStateComplete
			reason = "storage proof"
		case *types.V2FileContractExpiration:
			newState = api.ContractStateFailed
			reason = "expiration"
		default:
			panic("unknown resolution type") // developer error
		}

		// record height of encountering the resolution
		if err := tx.UpdateContractProofHeight(fcid, index.Height); err != nil {
			return fmt.Errorf("failed to update contract proof height: %w", err)
		}

		// record new state
		if err := tx.UpdateContractState(fcid, newState); err != nil {
			return fmt.Errorf("failed to update contract state: %w", err)
		}

		s.logger.Infow(fmt.Sprintf("contract state changed: %s -> %s", state, newState),
			"fcid", fcid,
			"reason", reason)
		return nil
	}

	// contract was created -> 'active'
	if created {
		if err := tx.UpdateContractState(fcid, api.ContractStateActive); err != nil {
			return fmt.Errorf("failed to update contract state: %w", err)
		}
		s.logger.Infow(fmt.Sprintf("contract state changed: %s -> active", state),
			"fcid", fcid,
			"reason", "contract confirmed")
		return nil
	}

	return nil
}

func (s *chainSubscriber) revertV2ContractUpdate(tx sql.ChainUpdateTx, fce types.V2FileContractElement, created bool, res types.V2FileContractResolutionType) error {
	fcid := fce.ID

	// ignore unknown contracts
	if known, err := tx.IsKnownContract(fcid); err != nil {
		return err
	} else if !known {
		return nil
	}

	// fetch contract state to see if contract is known
	state, err := tx.ContractState(fcid)
	if err != nil {
		return fmt.Errorf("failed to get contract state: %w", err)
	}

	// contract was reverted -> 'pending'
	if created {
		if err := tx.UpdateContractState(fcid, api.ContractStatePending); err != nil {
			return fmt.Errorf("failed to update contract state: %w", err)
		}
		s.logger.Infow(fmt.Sprintf("contract state changed: %s -> active", state),
			"fcid", fcid,
			"reason", "contract was reverted")
		return nil
	}

	// reverted resolution -> 'active'
	if res != nil {
		// reset proof height
		if err := tx.UpdateContractProofHeight(fcid, 0); err != nil {
			return fmt.Errorf("failed to update contract proof height: %w", err)
		}

		// record new state
		if err := tx.UpdateContractState(fcid, api.ContractStateActive); err != nil {
			return fmt.Errorf("failed to update contract state: %w", err)
		}

		s.logger.Infow(fmt.Sprintf("contract state changed: %s -> active", state),
			"fcid", fcid,
			"reason", "resolution was reverted")
		return nil
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
