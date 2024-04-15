package worker

import (
	"context"
	"fmt"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

func (w *worker) migrate(ctx context.Context, s object.Slab, contractSet string, dlContracts, ulContracts []api.ContractMetadata, bh uint64) (int, bool, error) {
	// make a map of good hosts
	goodHosts := make(map[types.PublicKey]map[types.FileContractID]bool)
	for _, c := range ulContracts {
		if goodHosts[c.HostKey] == nil {
			goodHosts[c.HostKey] = make(map[types.FileContractID]bool)
		}
		goodHosts[c.HostKey][c.ID] = false
	}

	// make a map of hosts we can download from
	h2c := make(map[types.PublicKey]types.FileContractID)
	for _, c := range append(dlContracts, ulContracts...) {
		h2c[c.HostKey] = c.ID
	}

	// collect indices of shards that need to be migrated
	usedMap := make(map[types.PublicKey]struct{})
	var shardIndices []int
SHARDS:
	for i, shard := range s.Shards {
		for hk, fcids := range shard.Contracts {
			for _, fcid := range fcids {
				// bad host
				if _, exists := goodHosts[hk]; !exists {
					continue
				}
				// bad contract
				if _, exists := goodHosts[hk][fcid]; !exists {
					continue
				}
				// reused host
				_, exists := usedMap[hk]
				if exists {
					continue
				}
				goodHosts[hk][fcid] = true
				usedMap[hk] = struct{}{}
				continue SHARDS
			}
		}
		// no good host found for shard
		shardIndices = append(shardIndices, i)
	}

	// if all shards are on good hosts, we're done
	if len(shardIndices) == 0 {
		return 0, false, nil
	}

	// calculate the number of missing shards and take into account hosts for
	// which we have a contract (so hosts from which we can download)
	missingShards := len(shardIndices)
	for _, si := range shardIndices {
		if _, exists := h2c[s.Shards[si].LatestHost]; exists {
			missingShards--
			continue
		}
	}

	// perform some sanity checks
	if len(ulContracts) < int(s.MinShards) {
		return 0, false, fmt.Errorf("not enough hosts to repair unhealthy shard to minimum redundancy, %d<%d", len(ulContracts), int(s.MinShards))
	}
	if len(s.Shards)-missingShards < int(s.MinShards) {
		return 0, false, fmt.Errorf("not enough hosts to download unhealthy shard, %d<%d", len(s.Shards)-missingShards, int(s.MinShards))
	}

	// acquire memory for the migration
	mem := w.uploadManager.mm.AcquireMemory(ctx, uint64(len(shardIndices))*rhpv2.SectorSize)
	if mem == nil {
		return 0, false, fmt.Errorf("failed to acquire memory for migration")
	}
	defer mem.Release()

	// download the slab
	shards, surchargeApplied, err := w.downloadManager.DownloadSlab(ctx, s, dlContracts)
	if err != nil {
		return 0, false, fmt.Errorf("failed to download slab for migration: %w", err)
	}
	s.Encrypt(shards)

	// filter it down to the shards we need to migrate
	for i, si := range shardIndices {
		shards[i] = shards[si]
	}
	shards = shards[:len(shardIndices)]

	// filter upload contracts to the ones we haven't used yet
	var allowed []api.ContractMetadata
	for c := range ulContracts {
		if _, exists := usedMap[ulContracts[c].HostKey]; !exists {
			allowed = append(allowed, ulContracts[c])
			usedMap[ulContracts[c].HostKey] = struct{}{}
		}
	}

	// migrate the shards
	err = w.uploadManager.UploadShards(ctx, s, shardIndices, shards, contractSet, allowed, bh, lockingPriorityUpload, mem)
	if err != nil {
		return 0, surchargeApplied, fmt.Errorf("failed to upload slab for migration: %w", err)
	}

	return len(shards), surchargeApplied, nil
}
