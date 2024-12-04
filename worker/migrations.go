package worker

import (
	"context"
	"fmt"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
)

func (w *Worker) migrate(ctx context.Context, s object.Slab, dlHosts []api.HostInfo, ulContracts []api.ContractMetadata, bh uint64) error {
	// map usable hosts
	usableHosts := make(map[types.PublicKey]struct{})
	for _, h := range dlHosts {
		usableHosts[h.PublicKey] = struct{}{}
	}

	// map usable contracts
	usableContracts := make(map[types.FileContractID]struct{})
	for _, c := range ulContracts {
		usableContracts[c.ID] = struct{}{}
	}

	// collect indices of shards that need to be migrated
	seen := make(map[types.PublicKey]struct{})
	var shardIndices []int
SHARDS:
	for i, shard := range s.Shards {
		for hk, fcids := range shard.Contracts {
			for _, fcid := range fcids {
				// bad host
				if _, ok := usableHosts[hk]; !ok {
					continue
				}
				// bad contract
				if _, ok := usableContracts[fcid]; !ok {
					continue
				}
				// reused host
				if _, used := seen[hk]; used {
					continue
				}
				seen[hk] = struct{}{}
				continue SHARDS
			}
		}
		// no good host found for shard
		shardIndices = append(shardIndices, i)
	}

	// if all shards are on good hosts, we're done
	if len(shardIndices) == 0 {
		return nil
	}

	// calculate the number of missing shards and take into account hosts for
	// which we have a contract (so hosts from which we can download)
	missingShards := len(shardIndices)
	for _, si := range shardIndices {
		for hk := range s.Shards[si].Contracts {
			if _, ok := usableHosts[hk]; ok {
				missingShards--
				break
			}
		}
	}

	// perform some sanity checks
	if len(ulContracts) < int(s.MinShards) {
		return fmt.Errorf("not enough hosts to repair unhealthy shard to minimum redundancy, %d<%d", len(ulContracts), int(s.MinShards))
	}
	if len(s.Shards)-missingShards < int(s.MinShards) {
		return fmt.Errorf("not enough hosts to download unhealthy shard, %d<%d", len(s.Shards)-missingShards, int(s.MinShards))
	}

	// acquire memory for the migration
	mem := w.uploadManager.mm.AcquireMemory(ctx, uint64(len(shardIndices))*rhpv2.SectorSize)
	if mem == nil {
		return fmt.Errorf("failed to acquire memory for migration")
	}
	defer mem.Release()

	// download the slab
	shards, err := w.downloadManager.DownloadSlab(ctx, s, dlHosts)
	if err != nil {
		w.logger.Debugw("slab migration failed",
			zap.Error(err),
			zap.Stringer("slab", s.EncryptionKey),
			zap.Int("numShardsMigrated", len(shards)),
		)
		return fmt.Errorf("failed to download slab for migration: %w", err)
	}
	s.Encrypt(shards)

	// filter it down to the shards we need to migrate
	for i, si := range shardIndices {
		shards[i] = shards[si]
	}
	shards = shards[:len(shardIndices)]

	// filter upload contracts to the ones we haven't used yet
	var allowed []api.ContractMetadata
	for _, c := range ulContracts {
		if _, used := seen[c.HostKey]; !used {
			allowed = append(allowed, c)
			seen[c.HostKey] = struct{}{}
		}
	}

	// migrate the shards
	err = w.uploadManager.UploadShards(ctx, s, shardIndices, shards, allowed, bh, mem)
	if err != nil {
		w.logger.Debugw("slab migration failed",
			zap.Error(err),
			zap.Stringer("slab", s.EncryptionKey),
			zap.Int("numShardsMigrated", len(shards)),
		)
		return fmt.Errorf("failed to upload slab for migration: %w", err)
	}

	// debug log migration result
	w.logger.Debugw("slab migration succeeded",
		zap.Stringer("slab", s.EncryptionKey),
		zap.Int("numShardsMigrated", len(shards)),
	)

	return nil
}
