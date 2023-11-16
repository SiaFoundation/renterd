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

func migrateSlab(ctx context.Context, d *downloadManager, u *uploadManager, s *object.Slab, dlContracts, ulContracts []api.ContractMetadata, bh uint64, logger *zap.SugaredLogger) (int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "migrateSlab")
	defer span.End()

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
		used := make(map[types.PublicKey]types.FileContractID)
		for hk, contracts := range goodHosts {
			for fcid, inUse := range contracts {
				if inUse {
					used[hk] = fcid
				}
			}
		}
		return 0, nil
	}

	// calculate the number of missing shards and take into account hosts for
	// which we have a contract (so hosts from which we can download)
	missingShards := len(shardIndices)
	for _, si := range shardIndices {
		for hk := range s.Shards[si].Contracts {
			if _, exists := h2c[hk]; exists {
				missingShards--
				break
			}
		}
	}

	// perform some sanity checks
	if len(ulContracts) < int(s.MinShards) {
		return 0, fmt.Errorf("not enough hosts to repair unhealthy shard to minimum redundancy, %d<%d", len(ulContracts), int(s.MinShards))
	}
	if len(s.Shards)-missingShards < int(s.MinShards) {
		return 0, fmt.Errorf("not enough hosts to download unhealthy shard, %d<%d", len(s.Shards)-missingShards, int(s.MinShards))
	}

	// download the slab
	shards, err := d.DownloadSlab(ctx, *s, dlContracts)
	if err != nil {
		return 0, fmt.Errorf("failed to download slab for migration: %w", err)
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
	uploaded, err := u.UploadShards(ctx, shards, allowed, bh, lockingPriorityUpload)
	if err != nil {
		return 0, fmt.Errorf("failed to upload slab for migration: %w", err)
	}

	// overwrite the unhealthy shards with the newly migrated ones
	for i, si := range shardIndices {
		s.Shards[si].LatestHost = uploaded[i].LatestHost

		knownContracts := make(map[types.FileContractID]struct{})
		for _, fcids := range s.Shards[si].Contracts {
			for _, fcid := range fcids {
				knownContracts[fcid] = struct{}{}
			}
		}
		for hk, fcids := range uploaded[i].Contracts {
			for _, fcid := range fcids {
				if _, exists := knownContracts[fcid]; !exists {
					s.Shards[si].Contracts[hk] = append(s.Shards[si].Contracts[hk], fcid)
				}
			}
		}
	}
	return len(shards), nil
}
