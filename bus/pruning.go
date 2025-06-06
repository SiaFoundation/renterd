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
	rhp2 "go.sia.tech/renterd/v2/internal/rhp/v2"
)

func (b *Bus) pruneContractV1(ctx context.Context, rk types.PrivateKey, cm api.ContractMetadata, hostIP string, gc gouging.Checker, pendingUploads map[types.Hash256]struct{}) (api.ContractPruneResponse, error) {
	// prune contract
	rev, spending, pruned, remaining, err := b.rhp2Client.PruneContract(ctx, rk, gc, hostIP, cm.HostKey, cm.ID, cm.RevisionNumber, func(fcid types.FileContractID, roots []types.Hash256) ([]uint64, error) {
		indices, err := b.store.PrunableContractRoots(ctx, fcid, roots)
		if err != nil {
			return nil, err
		} else if len(indices) > len(roots) {
			return nil, fmt.Errorf("selected %d prunable roots but only %d were provided", len(indices), len(roots))
		}

		filtered := indices[:0]
		for _, index := range indices {
			_, ok := pendingUploads[roots[index]]
			if !ok {
				filtered = append(filtered, index)
			}
		}
		indices = filtered
		return indices, nil
	})

	var pruneErr string
	if err != nil && !errors.Is(err, rhp2.ErrNoSectorsToPrune) && !errors.Is(err, context.Canceled) {
		return api.ContractPruneResponse{}, err
	} else if err != nil && !errors.Is(err, rhp2.ErrNoSectorsToPrune) {
		pruneErr = err.Error()
	}

	// record spending
	if !spending.Total().IsZero() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		b.store.RecordContractSpending(ctx, []api.ContractSpendingRecord{
			{
				ContractSpending: spending,
				ContractID:       cm.ID,
				RevisionNumber:   rev.RevisionNumber,
				Size:             rev.Filesize,

				MissedHostPayout:  rev.MissedHostPayout(),
				ValidRenterPayout: rev.ValidRenterPayout(),
			},
		})
	}

	return api.ContractPruneResponse{
		ContractSize: rev.Filesize,
		Pruned:       pruned,
		Remaining:    remaining,
		Error:        pruneErr,
	}, nil
}

func (b *Bus) pruneContractV2(ctx context.Context, rk types.PrivateKey, cm api.ContractMetadata, hostIP string, gc gouging.Checker, pendingUploads map[types.Hash256]struct{}) (api.ContractPruneResponse, error) {
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
	if gb := gc.CheckV2(settings); gb.Gouging() {
		return api.ContractPruneResponse{}, fmt.Errorf("host for pruning is gouging: %v", gb.String())
	}

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

		// fetch the batch
		res, err := b.rhp4Client.SectorRoots(ctx, cm.HostKey, hostIP, b.cm.TipState(), prices, signer, cRHP4.ContractRevision{
			ID:       cm.ID,
			Revision: rev,
		}, offset, length)
		if err != nil {
			return api.ContractPruneResponse{}, err
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
	indices, err := b.store.PrunableContractRoots(ctx, cm.ID, sectorRoots)
	if err != nil {
		return api.ContractPruneResponse{}, err
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
	res, err := b.rhp4Client.FreeSectors(ctx, cm.HostKey, hostIP, b.cm.TipState(), prices, rk, cRHP4.ContractRevision{
		ID:       cm.ID,
		Revision: rev,
	}, toPrune)
	if err != nil {
		return api.ContractPruneResponse{}, err
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
