package bus

import (
	"context"
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
	rev, err := b.rhp4Client.LatestRevision(ctx, cm.HostKey, hostIP, cm.ID)
	if err != nil {
		return api.ContractPruneResponse{}, fmt.Errorf("failed to fetch revision for pruning: %w", err)
	} else if rev.RevisionNumber < cm.RevisionNumber {
		return api.ContractPruneResponse{}, fmt.Errorf("latest known revision %d is less than contract revision %d", rev.RevisionNumber, cm.RevisionNumber)
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

	return api.ContractPruneResponse{
		ContractSize: rev.Filesize,
		Pruned:       uint64(len(toPrune) * rhpv4.SectorSize),
		Remaining:    (totalToPrune - uint64(len(toPrune))) * rhpv4.SectorSize,
	}, nil
}
