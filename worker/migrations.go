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

func migrateSlab(ctx context.Context, d *downloadManager, u *uploadManager, s *object.Slab, dlContracts, ulContracts []api.ContractMetadata, bh uint64, logger *zap.SugaredLogger) (map[types.PublicKey]types.FileContractID, error) {
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
	for i, shard := range s.Shards {
		// bad host
		if _, exists := goodHosts[shard.Host]; !exists {
			shardIndices = append(shardIndices, i)
			continue
		}

		// reused host
		_, exists := usedMap[h2c[shard.Host]]
		if exists {
			shardIndices = append(shardIndices, i)
			continue
		}
		usedMap[h2c[shard.Host]] = struct{}{}
	}

	// if all shards are on good hosts, we're done
	if len(shardIndices) == 0 {
		return nil, nil
	}

	// subtract the number of shards that are on hosts with contracts and might
	// therefore be used for downloading from.
	missingShards := len(shardIndices)
	for _, si := range shardIndices {
		_, hasContract := h2c[s.Shards[si].Host]
		if hasContract {
			missingShards--
		}
	}

	// perform some sanity checks
	if len(ulContracts) < int(s.MinShards) {
		return nil, fmt.Errorf("not enough hosts to repair unhealthy shard to minimum redundancy, %d<%d", len(ulContracts), int(s.MinShards))
	}
	if len(s.Shards)-missingShards < int(s.MinShards) {
		return nil, fmt.Errorf("not enough hosts to download unhealthy shard, %d<%d", len(s.Shards)-len(shardIndices), int(s.MinShards))
	}

	// download the slab
	shards, err := d.DownloadSlab(ctx, *s, dlContracts)
	if err != nil {
		return nil, fmt.Errorf("failed to download slab for migration: %w", err)
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
		if _, exists := usedMap[ulContracts[c].ID]; !exists {
			allowed = append(allowed, ulContracts[c])
		}
	}

	// migrate the shards
	uploaded, used, err := u.Migrate(ctx, shards, allowed, bh)
	if err != nil {
		return nil, fmt.Errorf("failed to upload slab for migration: %w", err)
	}

	// overwrite the unhealthy shards with the newly migrated ones
	for i, si := range shardIndices {
		s.Shards[si] = uploaded[i]
	}
	return used, nil
}
