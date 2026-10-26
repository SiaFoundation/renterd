package bus

import (
	"context"
	"errors"
	"fmt"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	cRHP4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/renterd/v2/api"
	ibus "go.sia.tech/renterd/v2/internal/bus"
	"go.sia.tech/renterd/v2/internal/gouging"
	"go.uber.org/zap"
)

func (b *Bus) pruneContract(ctx context.Context, rk types.PrivateKey, cm api.ContractMetadata, hostIP string, gc gouging.Checker, pendingUploads map[types.Hash256]struct{}) (api.ContractPruneResponse, error) {
	start := time.Now()
	log := b.logger.Named("pruneContract").Named(cm.ID.String())
	signer := ibus.NewFormContractSigner(b.w, rk)

	// get latest revision
	rev, err := b.rhp4Client.LatestRevision(ctx, cm.HostKey, hostIP, rk.PublicKey(), cm.ID)
	if err != nil {
		return api.ContractPruneResponse{}, fmt.Errorf("failed to fetch revision for pruning: %w", err)
	}

	// fetch the known contract element, it doesn't exist for contracts that
	// haven't been confirmed yet
	var known *types.V2FileContract
	fce, err := b.store.FileContractElement(ctx, cm.ID)
	if err == nil {
		known = &fce.V2FileContractElement.V2FileContract
	} else if !errors.Is(err, api.ErrContractNotFound) {
		return api.ContractPruneResponse{}, fmt.Errorf("failed to fetch contract element for pruning: %w", err)
	}

	// make sure the host didn't alter the revision
	if err := checkRevision(cm, rk.PublicKey(), known, rev); err != nil {
		return api.ContractPruneResponse{}, fmt.Errorf("invalid revision for pruning: %w", err)
	}

	// get prices
	settings, err := b.rhp4Client.Settings(ctx, cm.HostKey, hostIP)
	if err != nil {
		return api.ContractPruneResponse{}, fmt.Errorf("failed to fetch prices for pruning: %w", err)
	}
	prices := settings.Prices

	// make sure they are sane
	if gb := gc.Check(settings); gb.Gouging() {
		return api.ContractPruneResponse{}, fmt.Errorf("host for pruning is gouging: %v", gb.String())
	}
	log.Debug("attempting to prune contract", zap.Uint64("revision", rev.RevisionNumber), zap.Stringer("hostKey", cm.HostKey), zap.Stringer("protocolVersion", settings.ProtocolVersion))

	// fetch all contract roots
	numsectors := rev.Filesize / rhpv4.SectorSize
	sectorRoots := make([]types.Hash256, 0, numsectors)
	var rootsUsage rhpv4.Usage
	for offset := uint64(0); offset < numsectors; {
		// calculate the batch size
		length := uint64(rhpv4.MaxSectorBatchSize)
		if offset+length > numsectors {
			length = numsectors - offset
		}
		log.Debug("fetching roots for pruning", zap.Uint64("numSectors", numsectors), zap.Uint64("offset", offset), zap.Uint64("length", length), zap.Duration("totalElapsed", time.Since(start)))

		// fetch the batch
		sectorRootsStart := time.Now()
		res, err := b.rhp4Client.SectorRoots(ctx, cm.HostKey, hostIP, b.cm.TipState(), prices, signer, cRHP4.ContractRevision{
			ID:       cm.ID,
			Revision: rev,
		}, offset, length)
		if err != nil {
			log.Debug("failed to fetch sector roots", zap.Error(err), zap.Duration("totalElapsed", time.Since(start)), zap.Duration("elapsed", time.Since(sectorRootsStart)))
			return api.ContractPruneResponse{}, fmt.Errorf("failed to fetch contract sectors: %w", err)
		}
		log.Debug("successfully fetched sector roots", zap.Uint64("roots", uint64(len(res.Roots))), zap.Duration("totalElapsed", time.Since(start)), zap.Duration("elapsed", time.Since(sectorRootsStart)))

		// update revision since it was revised
		rev = res.Revision

		// collect roots
		sectorRoots = append(sectorRoots, res.Roots...)
		offset += uint64(len(res.Roots))

		// update the cost
		rootsUsage = rootsUsage.Add(res.Usage)
	}

	// fetch indices to prune
	prunableRootsStart := time.Now()
	indices, err := b.store.PrunableContractRoots(ctx, cm.ID, sectorRoots)
	if err != nil {
		log.Debug("failed to fetch prunable roots", zap.Error(err), zap.Duration("totalElapsed", time.Since(start)), zap.Duration("elapsed", time.Since(prunableRootsStart)))
		return api.ContractPruneResponse{}, fmt.Errorf("failed to fetch prunable roots: %w", err)
	}
	log.Debug("fetched prunable roots", zap.Int("prunable", len(indices)), zap.Duration("totalElapsed", time.Since(start)), zap.Duration("elapsed", time.Since(prunableRootsStart)))

	// avoid pruning pending uploads
	toPrune := indices[:0]
	for _, index := range indices {
		_, ok := pendingUploads[sectorRoots[index]]
		if !ok {
			toPrune = append(toPrune, index)
		}
	}
	totalToPrune := uint64(len(toPrune))

	// cap at max batch size
	batchSize := rhpv4.MaxSectorBatchSize
	if batchSize > len(toPrune) {
		batchSize = len(toPrune)
	}
	toPrune = toPrune[:batchSize]

	// prune the batch
	freeSectorsStart := time.Now()
	res, err := b.rhp4Client.FreeSectors(ctx, cm.HostKey, hostIP, b.cm.TipState(), prices, rk, cRHP4.ContractRevision{
		ID:       cm.ID,
		Revision: rev,
	}, toPrune)
	if err != nil {
		log.Debug("failed to free sectors", zap.Error(err), zap.Duration("totalElapsed", time.Since(start)), zap.Duration("elapsed", time.Since(freeSectorsStart)))
		return api.ContractPruneResponse{}, fmt.Errorf("failed to free sectors: %w", err)
	}
	log.Debug("successfully freed sectors", zap.Int("pruned", len(toPrune)), zap.Duration("totalElapsed", time.Since(start)), zap.Duration("elapsed", time.Since(freeSectorsStart)))
	deleteUsage := res.Usage
	rev = res.Revision // update rev

	// record spending
	if !rootsUsage.Add(deleteUsage).RenterCost().IsZero() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		b.store.RecordContractSpending(ctx, []api.ContractSpendingRecord{
			{
				ContractSpending: api.ContractSpending{
					Deletions:   deleteUsage.RenterCost(),
					SectorRoots: rootsUsage.RenterCost(),
				},
				ContractID:     cm.ID,
				RevisionNumber: rev.RevisionNumber,
				Size:           rev.Filesize,

				MissedHostPayout:  rev.MissedHostOutput().Value,
				ValidRenterPayout: rev.RenterOutput.Value,
			},
		})
	}

	log.Debug("finished pruning contract", zap.Uint64("pruned", uint64(len(toPrune))*rhpv4.SectorSize), zap.Uint64("remaining", (totalToPrune-uint64(len(toPrune)))*rhpv4.SectorSize), zap.Duration("totalElapsed", time.Since(start)))
	return api.ContractPruneResponse{
		ContractSize: rev.Filesize,
		Pruned:       uint64(len(toPrune) * rhpv4.SectorSize),
		Remaining:    (totalToPrune - uint64(len(toPrune))) * rhpv4.SectorSize,
	}, nil
}

// checkRevision verifies that the revision number of the host's latest revision
// didn't decrease and that the fields which can't change over the lifetime of
// the contract match what the bus knows about the contract. The known contract
// is optional since it only exists for confirmed contracts.
func checkRevision(cm api.ContractMetadata, renterKey types.PublicKey, known *types.V2FileContract, rev types.V2FileContract) error {
	if rev.RevisionNumber < cm.RevisionNumber {
		return fmt.Errorf("revision number %d is less than known revision number %d", rev.RevisionNumber, cm.RevisionNumber)
	} else if rev.HostPublicKey != cm.HostKey {
		return fmt.Errorf("host key %v doesn't match known host key %v", rev.HostPublicKey, cm.HostKey)
	} else if rev.RenterPublicKey != renterKey {
		return fmt.Errorf("renter key %v doesn't match known renter key %v", rev.RenterPublicKey, renterKey)
	} else if rev.ProofHeight != cm.WindowStart {
		return fmt.Errorf("proof height %d doesn't match known proof height %d", rev.ProofHeight, cm.WindowStart)
	} else if rev.ExpirationHeight != cm.WindowEnd {
		return fmt.Errorf("expiration height %d doesn't match known expiration height %d", rev.ExpirationHeight, cm.WindowEnd)
	}
	if known == nil {
		return nil
	}

	knownPayout, overflow := known.RenterOutput.Value.AddWithOverflow(known.HostOutput.Value)
	if overflow {
		return errors.New("known payout overflows")
	}
	payout, overflow := rev.RenterOutput.Value.AddWithOverflow(rev.HostOutput.Value)
	if overflow {
		return errors.New("payout overflows")
	}
	switch {
	case rev.RenterOutput.Address != known.RenterOutput.Address:
		return fmt.Errorf("renter address %v doesn't match known renter address %v", rev.RenterOutput.Address, known.RenterOutput.Address)
	case rev.HostOutput.Address != known.HostOutput.Address:
		return fmt.Errorf("host address %v doesn't match known host address %v", rev.HostOutput.Address, known.HostOutput.Address)
	case !rev.TotalCollateral.Equals(known.TotalCollateral):
		return fmt.Errorf("total collateral %v doesn't match known total collateral %v", rev.TotalCollateral, known.TotalCollateral)
	case !payout.Equals(knownPayout):
		return fmt.Errorf("payout %v doesn't match known payout %v", payout, knownPayout)
	case rev.RenterPublicKey != known.RenterPublicKey:
		return fmt.Errorf("renter key %v doesn't match known renter key %v", rev.RenterPublicKey, known.RenterPublicKey)
	case rev.HostPublicKey != known.HostPublicKey:
		return fmt.Errorf("host key %v doesn't match known host key %v", rev.HostPublicKey, known.HostPublicKey)
	case rev.ProofHeight != known.ProofHeight:
		return fmt.Errorf("proof height %d doesn't match known proof height %d", rev.ProofHeight, known.ProofHeight)
	case rev.ExpirationHeight != known.ExpirationHeight:
		return fmt.Errorf("expiration height %d doesn't match known expiration height %d", rev.ExpirationHeight, known.ExpirationHeight)
	}
	return nil
}
