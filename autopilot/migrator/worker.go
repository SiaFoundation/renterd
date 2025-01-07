package migrator

import (
	"context"
	"fmt"

	rhpv2 "go.sia.tech/core/rhp/v2"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/upload"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
)

func (m *Migrator) migrateSlab(ctx context.Context, key object.EncryptionKey) error {
	// fetch slab
	slab, err := m.ss.Slab(ctx, key)
	if err != nil {
		return fmt.Errorf("couldn't fetch slab from bus: %w", err)
	}

	// fetch the upload parameters
	up, err := m.bus.UploadParams(ctx)
	if err != nil {
		return fmt.Errorf("couldn't fetch upload parameters from bus: %w", err)
	}

	// cancel the upload if consensus is not synced
	if !up.ConsensusState.Synced {
		m.logger.Errorf("migration cancelled, err: %v", api.ErrConsensusNotSynced)
		return api.ErrConsensusNotSynced
	}

	// attach gouging checker to the context
	ctx = gouging.WithChecker(ctx, m.bus, up.GougingParams)

	// fetch hosts
	dlHosts, err := m.bus.UsableHosts(ctx)
	if err != nil {
		return fmt.Errorf("couldn't fetch hosts from bus: %w", err)
	}

	hmap := make(map[types.PublicKey]api.HostInfo)
	for _, h := range dlHosts {
		hmap[h.PublicKey] = h
	}

	contracts, err := m.bus.Contracts(ctx, api.ContractsOpts{FilterMode: api.ContractFilterModeGood})
	if err != nil {
		return fmt.Errorf("couldn't fetch contracts from bus: %v", err)
	}

	var ulHosts []upload.HostInfo
	for _, c := range contracts {
		if h, ok := hmap[c.HostKey]; ok {
			ulHosts = append(ulHosts, upload.HostInfo{
				HostInfo:            h,
				ContractEndHeight:   c.WindowEnd,
				ContractID:          c.ID,
				ContractRenewedFrom: c.RenewedFrom,
			})
		}
	}

	// migrate the slab and handle alerts
	err = m.migrate(ctx, slab, dlHosts, ulHosts, up.CurrentHeight)
	if err != nil && !utils.IsErr(err, api.ErrSlabNotFound) {
		var objects []api.ObjectMetadata
		if res, err := m.bus.Objects(ctx, "", api.ListObjectOptions{SlabEncryptionKey: slab.EncryptionKey}); err != nil {
			m.logger.Errorf("failed to list objects for slab key; %v", err)
		} else {
			objects = res.Objects
		}
		m.alerts.RegisterAlert(ctx, newMigrationFailedAlert(slab.EncryptionKey, slab.Health, objects, err))
	} else if err == nil {
		m.alerts.DismissAlerts(ctx, alerts.IDForSlab(alertMigrationID, slab.EncryptionKey))
	}

	if err != nil {
		m.logger.Errorw("failed to migrate slab",
			zap.Error(err),
			zap.Stringer("slab", slab.EncryptionKey),
		)
		return err
	}
	return nil
}

func (m *Migrator) migrate(ctx context.Context, s object.Slab, dlHosts []api.HostInfo, ulHosts []upload.HostInfo, bh uint64) error {
	// map usable hosts
	usableHosts := make(map[types.PublicKey]struct{})
	for _, h := range dlHosts {
		usableHosts[h.PublicKey] = struct{}{}
	}

	// map usable contracts
	usableContracts := make(map[types.FileContractID]struct{})
	for _, c := range ulHosts {
		usableContracts[c.ContractID] = struct{}{}
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
	if len(ulHosts) < int(s.MinShards) {
		return fmt.Errorf("not enough hosts to repair unhealthy shard to minimum redundancy, %d<%d", len(ulHosts), int(s.MinShards))
	}
	if len(s.Shards)-missingShards < int(s.MinShards) {
		return fmt.Errorf("not enough hosts to download unhealthy shard, %d<%d", len(s.Shards)-missingShards, int(s.MinShards))
	}

	// acquire memory for the migration
	mem := m.uploadManager.AcquireMemory(ctx, uint64(len(shardIndices))*rhpv2.SectorSize)
	if mem == nil {
		return fmt.Errorf("failed to acquire memory for migration")
	}
	defer mem.Release()

	// download the slab
	shards, err := m.downloadManager.DownloadSlab(ctx, s, dlHosts)
	if err != nil {
		m.logger.Debugw("slab migration failed",
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
	var allowed []upload.HostInfo
	for _, h := range ulHosts {
		if _, used := seen[h.PublicKey]; !used {
			allowed = append(allowed, h)
			seen[h.PublicKey] = struct{}{}
		}
	}

	// migrate the shards
	err = m.uploadManager.UploadShards(ctx, s, shardIndices, shards, allowed, bh, mem)
	if err != nil {
		m.logger.Debugw("slab migration failed",
			zap.Error(err),
			zap.Stringer("slab", s.EncryptionKey),
			zap.Int("numShardsMigrated", len(shards)),
		)
		return fmt.Errorf("failed to upload slab for migration: %w", err)
	}

	// debug log migration result
	m.logger.Debugw("slab migration succeeded",
		zap.Stringer("slab", s.EncryptionKey),
		zap.Int("numShardsMigrated", len(shards)),
	)

	return nil
}
