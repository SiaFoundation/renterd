package worker

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"go.uber.org/zap"
)

func migrateSlab(ctx context.Context, d *downloadManager, u *uploadManager, s *object.Slab, dlContracts, ulContracts []api.ContractMetadata, bh uint64, logger *zap.SugaredLogger) (map[types.PublicKey]types.FileContractID, int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "migrateSlab")
	defer span.End()

	// make a map of good hosts
	goodHosts := make(map[types.PublicKey]struct{})
	for _, c := range ulContracts {
		goodHosts[c.HostKey] = struct{}{}
	}

	// make a map of host to contract id
	h2c := make(map[types.PublicKey]types.FileContractID)
	for _, c := range append(dlContracts, ulContracts...) {
		h2c[c.HostKey] = c.ID
	}

	// collect indices of shards that need to be migrated
	usedMap := make(map[types.FileContractID]struct{})
	var shardIndices []int
	requiredShards := make([]bool, len(s.Shards))
	for i, shard := range s.Shards {
		// bad host
		if _, exists := goodHosts[shard.Host]; !exists {
			shardIndices = append(shardIndices, i)
			requiredShards[i] = true
			continue
		}

		// reused host
		_, exists := usedMap[h2c[shard.Host]]
		if exists {
			shardIndices = append(shardIndices, i)
			requiredShards[i] = true
			continue
		}
		usedMap[h2c[shard.Host]] = struct{}{}
	}

	// if all shards are on good hosts, we're done
	if len(shardIndices) == 0 {
		used := make(map[types.PublicKey]types.FileContractID)
		for _, shard := range s.Shards {
			used[shard.Host] = h2c[shard.Host]
		}
		return used, 0, nil
	}

	// calculate the number of missing shards and take into account hosts for
	// which we have a contract (so hosts from which we can download)
	missingShards := len(shardIndices)
	for _, si := range shardIndices {
		_, hasContract := h2c[s.Shards[si].Host]
		if hasContract {
			missingShards--
		}
	}

	// perform some sanity checks
	if len(ulContracts) < int(s.MinShards) {
		return nil, 0, fmt.Errorf("not enough hosts to repair unhealthy shard to minimum redundancy, %d<%d", len(ulContracts), int(s.MinShards))
	}
	if len(s.Shards)-missingShards < int(s.MinShards) {
		return nil, 0, fmt.Errorf("not enough hosts to download unhealthy shard, %d<%d", len(s.Shards)-len(shardIndices), int(s.MinShards))
	}

	// download the slab
	shards, err := d.DownloadMissingShards(ctx, *s, dlContracts, requiredShards)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to download slab for migration: %w", err)
	}
	s.Encrypt(shards)

	// filter upload contracts to the ones we haven't used yet
	var allowed []api.ContractMetadata
	for c := range ulContracts {
		if _, exists := usedMap[ulContracts[c].ID]; !exists {
			allowed = append(allowed, ulContracts[c])
		}
	}

	// migrate the shards
	uploaded, used, err := u.UploadShards(ctx, shards, allowed, bh, lockingPriorityUpload)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to upload slab for migration: %w", err)
	}

	// overwrite the unhealthy shards with the newly migrated ones
	for i, si := range shardIndices {
		s.Shards[si] = uploaded[i]
	}

	// loop all shards and extend the used contracts map so it reflects all used
	// contracts, not just the used contracts for the migrated shards
	for _, sector := range s.Shards {
		_, exists := used[sector.Host]
		if !exists {
			if fcid, exists := h2c[sector.Host]; !exists {
				return nil, 0, fmt.Errorf("couldn't find contract for host %v", sector.Host)
			} else {
				used[sector.Host] = fcid
			}
		}
	}

	return used, len(shards), nil
}
