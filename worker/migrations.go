package worker

import (
	"context"
	"fmt"
	"io"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"go.uber.org/zap"
)

type hostV2 interface {
	Contract() types.FileContractID
	HostKey() types.PublicKey
	DeleteSectors(ctx context.Context, roots []types.Hash256) error
}

type hostV3 interface {
	hostV2

	DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32) error
	FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error)
	FetchRevision(ctx context.Context, fetchTimeout time.Duration, blockHeight uint64) (types.FileContractRevision, error)
	FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error
	Renew(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, err error)
	SyncAccount(ctx context.Context, rev *types.FileContractRevision) error
	UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) (types.Hash256, error)
}

type hostProvider interface {
	withHostV2(context.Context, types.FileContractID, types.PublicKey, string, func(hostV2) error) (err error)
	newHostV3(types.FileContractID, types.PublicKey, string) (_ hostV3, err error)
}

func migrateSlab(ctx context.Context, d *downloadManager, u *uploadManager, s *object.Slab, dlContracts, ulContracts []api.ContractMetadata, bh uint64, logger *zap.SugaredLogger) error {
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
		return nil
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
		return fmt.Errorf("not enough hosts to repair unhealthy shard to minimum redundancy, %d<%d", len(ulContracts), int(s.MinShards))
	}
	if len(s.Shards)-missingShards < int(s.MinShards) {
		return fmt.Errorf("not enough hosts to download unhealthy shard, %d<%d", len(s.Shards)-len(shardIndices), int(s.MinShards))
	}

	// download the slab
	shards, err := d.DownloadSlab(ctx, *s, dlContracts)
	if err != nil {
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
	for c := range ulContracts {
		if _, exists := usedMap[ulContracts[c].ID]; !exists {
			allowed = append(allowed, ulContracts[c])
		}
	}

	// migrate the shards
	uploaded, err := u.Migrate(ctx, shards, allowed, bh)
	if err != nil {
		return fmt.Errorf("failed to upload slab for migration: %w", err)
	}

	// overwrite the unhealthy shards with the newly migrated ones
	for i, si := range shardIndices {
		s.Shards[si] = uploaded[i]
	}
	return nil
}
