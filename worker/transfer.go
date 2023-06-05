package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"go.uber.org/zap"
)

const (
	defaultSectorDownloadTiming = 200 * time.Millisecond
)

var (
	errGougingHost           = errors.New("host is gouging")
	errDownloadSectorTimeout = errors.New("download sector timed out")
)

type hostV2 interface {
	Contract() types.FileContractID
	HostKey() types.PublicKey
	DeleteSectors(ctx context.Context, roots []types.Hash256) error
}

type hostV3 interface {
	hostV2

	DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) error
	FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error)
	FetchRevision(ctx context.Context, fetchTimeout time.Duration, blockHeight uint64) (types.FileContractRevision, error)
	FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error
	Renew(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, err error)
	SyncAccount(ctx context.Context, rev *types.FileContractRevision) error
	UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) (types.Hash256, error)
}

type hostProvider interface {
	withHostV2(context.Context, types.FileContractID, types.PublicKey, string, func(hostV2) error) (err error)
	newHostV3(context.Context, types.FileContractID, types.PublicKey, string) (_ hostV3, err error)
}

func parallelDownloadSlab(ctx context.Context, hp hostProvider, ss object.SlabSlice, contracts []api.ContractMetadata, downloadSectorTimeout time.Duration, maxOverdrive uint64, logger *zap.SugaredLogger) ([][]byte, []int64, error) {
	// prepopulate the timings with a value for all hosts to ensure unused hosts aren't necessarily favoured in consecutive downloads
	timings := make([]int64, len(contracts))
	for i := 0; i < len(contracts); i++ {
		timings[i] = int64(defaultSectorDownloadTiming)
	}

	// ensure the context is cancelled when the slab is downloaded
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// make sure that there are enough hosts for the download.
	hostMap := make(map[types.PublicKey]struct{})
	for _, h := range contracts {
		hostMap[h.HostKey] = struct{}{}
	}
	availableShards := 0
	for _, s := range ss.Shards {
		if _, found := hostMap[s.Host]; found {
			availableShards++
		}
	}
	if availableShards < int(ss.MinShards) {
		return nil, nil, fmt.Errorf("not enough hosts available to download the slab: %v/%v", availableShards, ss.MinShards)
	}

	// declare types for a download request and response
	type req struct {
		offset     uint64
		length     uint64
		shardIndex int
	}
	type resp struct {
		hostIndex int
		req       req
		shard     []byte
		dur       time.Duration
		err       error
	}

	// declare logic for worker handling a download
	respChan := make(chan resp, 2*len(contracts)) // every host can send up to 2 responses
	worker := func(hostIndex int, r req) {
		start := time.Now()
		doneChan := make(chan struct{})

		// Trace the download.
		ctx, span := tracing.Tracer.Start(ctx, "download-request")
		span.SetAttributes(attribute.Stringer("host", contracts[hostIndex].HostKey))

		go func(r req) {
			defer close(doneChan)
			contract := contracts[hostIndex]
			shard := &ss.Shards[r.shardIndex]

			buf := bytes.NewBuffer(make([]byte, 0, rhpv2.SectorSize))
			err := func() error {
				h, err := hp.newHostV3(ctx, contract.ID, contract.HostKey, contract.SiamuxAddr)
				if err != nil {
					return err
				}
				err = h.DownloadSector(ctx, buf, shard.Root, r.offset, r.length)
				if err != nil {
					span.SetStatus(codes.Error, "downloading the sector failed")
					span.RecordError(err)
				}
				return err
			}()
			respChan <- resp{hostIndex, r, buf.Bytes(), time.Since(start), err}

			var aborted bool
			select {
			case <-ctx.Done():
				aborted = true
			default:
			}
			if err != nil && !errors.Is(err, context.Canceled) && !isBalanceInsufficient(err) && !aborted && !errors.Is(err, os.ErrDeadlineExceeded) {
				logger.Errorf("withHostV3 failed when downloading sector, err: %v", err)
			}
		}(r)

		if downloadSectorTimeout > 0 {
			timer := time.NewTimer(downloadSectorTimeout)
			select {
			case <-timer.C:
				span.SetAttributes(attribute.Bool("slow", true))
				respChan <- resp{
					hostIndex: hostIndex,
					req:       r,
					dur:       time.Since(start),
					err:       errDownloadSectorTimeout}
			case <-doneChan:
				if !timer.Stop() {
					<-timer.C
				}
			}
		}

		<-doneChan
		span.End()
	}

	// track which hosts have tried a shard already.
	type shardInfo struct {
		usedHosts map[types.PublicKey]struct{}
		inflight  int
		done      bool
	}
	shardInfos := make([]shardInfo, len(ss.Shards))
	for i := range ss.Shards {
		shardInfos[i].usedHosts = make(map[types.PublicKey]struct{})
	}

	// helper to find worker for a shard.
	var inflight uint64
	workerForSlab := func() (int, int) {
		for shardIndex := range ss.Shards {
			if shardInfos[shardIndex].done {
				continue // shard is done already
			}
			if shardInfos[shardIndex].inflight > 0 {
				continue // only have 1 worker running per shard
			}
			for hostIndex := range contracts {
				if ss.Shards[shardIndex].Host != contracts[hostIndex].HostKey {
					continue // host is not useful
				}
				if _, used := shardInfos[shardIndex].usedHosts[contracts[hostIndex].HostKey]; used {
					continue // host was already used
				}
				shardInfos[shardIndex].usedHosts[contracts[hostIndex].HostKey] = struct{}{}
				shardInfos[shardIndex].inflight++
				return hostIndex, shardIndex
			}
		}
		return -1, -1
	}

	// helper to launch worker.
	offset, length := ss.SectorRegion()
	launchWorker := func() bool {
		hostIndex, shardIndex := workerForSlab()
		if hostIndex == -1 {
			return false
		}
		go worker(hostIndex, req{
			offset:     offset,
			length:     length,
			shardIndex: shardIndex,
		})
		return true
	}

	// spawn workers for the minimum number of shards
	for i := 0; i < int(ss.MinShards); i++ {
		if !launchWorker() {
			panic("should be able to launch minShards workers")
		}
		inflight++
	}

	// collect responses
	var errs HostErrorSet
	shards := make([][]byte, len(ss.Shards))
	rem := ss.MinShards
	var overdrive uint64
	for inflight > 0 {
		resp := <-respChan

		// only slow downloads might still be in flight
		if !errors.Is(resp.err, errDownloadSectorTimeout) {
			inflight--
		}

		if resp.err != nil {
			errs = append(errs, &HostError{contracts[resp.hostIndex].HostKey, resp.err})

			// make sure non funded or gouging hosts are not used for consecutive downloads
			if errors.Is(resp.err, errBalanceInsufficient) ||
				errors.Is(resp.err, errGougingHost) {
				timings[resp.hostIndex] = math.MaxInt64
			}

			// make sure slow hosts are not not used for consecutive downloads
			if errors.Is(resp.err, errDownloadSectorTimeout) {
				timings[resp.hostIndex] = int64(resp.dur) * 10
				if overdrive < maxOverdrive {
					overdrive++ // add more overdrive
				}
			}
		} else {
			timings[resp.hostIndex] = int64(resp.dur)
			if len(shards[resp.req.shardIndex]) == 0 {
				shards[resp.req.shardIndex] = resp.shard
				rem--
				if rem == 0 {
					break
				}
			}
		}

		// launch more hosts if necessary
		for inflight < uint64(ss.MinShards)+overdrive {
			if !launchWorker() {
				break
			}
			inflight++
		}
	}
	if rem > 0 {
		if errs == nil {
			return nil, nil, fmt.Errorf("rem > 0 (%v) but errs is nil - this should not happen", rem)
		}
		return nil, nil, errs
	}

	return shards, timings, nil
}

func downloadSlab(ctx context.Context, hp hostProvider, out io.Writer, ss object.SlabSlice, contracts []api.ContractMetadata, downloadSectorTimeout time.Duration, maxOverdrive uint64, logger *zap.SugaredLogger) ([]int64, error) {
	ctx, span := tracing.Tracer.Start(ctx, "parallelDownloadSlab")
	defer span.End()

	shards, timings, err := parallelDownloadSlab(ctx, hp, ss, contracts, downloadSectorTimeout, maxOverdrive, logger)
	if err != nil {
		return nil, err
	}
	ss.Decrypt(shards)
	err = ss.Recover(out, shards)
	if err != nil {
		return nil, err
	}
	return timings, nil
}

// slabsForDownload returns the slices that comprise the specified offset-length
// span within slabs.
func slabsForDownload(slabs []object.SlabSlice, offset, length int64) []object.SlabSlice {
	// mutate a copy
	slabs = append([]object.SlabSlice(nil), slabs...)

	firstOffset := offset
	for i, ss := range slabs {
		if firstOffset <= int64(ss.Length) {
			slabs = slabs[i:]
			break
		}
		firstOffset -= int64(ss.Length)
	}
	slabs[0].Offset += uint32(firstOffset)
	slabs[0].Length -= uint32(firstOffset)

	lastLength := length
	for i, ss := range slabs {
		if lastLength <= int64(ss.Length) {
			slabs = slabs[:i+1]
			break
		}
		lastLength -= int64(ss.Length)
	}
	slabs[len(slabs)-1].Length = uint32(lastLength)
	return slabs
}

func deleteSlabs(ctx context.Context, slabs []object.Slab, hosts []hostV2) error {
	rootsByHost := make(map[types.PublicKey][]types.Hash256)
	for _, s := range slabs {
		for _, sector := range s.Shards {
			rootsByHost[sector.Host] = append(rootsByHost[sector.Host], sector.Root)
		}
	}

	errChan := make(chan *HostError)
	for _, h := range hosts {
		go func(h hostV2) {
			// NOTE: if host is not storing any sectors, the map lookup will return
			// nil, making this a no-op
			err := h.DeleteSectors(ctx, rootsByHost[h.HostKey()])
			if err != nil {
				errChan <- &HostError{h.HostKey(), err}
			} else {
				errChan <- nil
			}
		}(h)
	}

	var errs HostErrorSet
	for range hosts {
		if err := <-errChan; err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

func migrateSlab(ctx context.Context, u *uploadManager, hp hostProvider, s *object.Slab, dlContracts, ulContracts []api.ContractMetadata, locker revisionLocker, downloadSectorTimeout, uploadSectorTimeout time.Duration, logger *zap.SugaredLogger) error {
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

	// perform some sanity check
	if len(s.Shards)-len(shardIndices) < int(s.MinShards) {
		return fmt.Errorf("not enough hosts to download unhealthy shard, %d<%d", len(s.Shards)-len(shardIndices), int(s.MinShards))
	}

	// download + reconstruct slab
	ss := object.SlabSlice{
		Slab:   *s,
		Offset: 0,
		Length: uint32(s.MinShards) * rhpv2.SectorSize,
	}
	shards, _, err := parallelDownloadSlab(ctx, hp, ss, dlContracts, downloadSectorTimeout, 0, logger) // no overdrive for downloads
	if err != nil {
		return fmt.Errorf("failed to download slab for migration: %w", err)
	}
	ss.Decrypt(shards)
	if err := s.Reconstruct(shards); err != nil {
		return fmt.Errorf("failed to reconstruct shards downloaded for migration: %w", err)
	}
	s.Encrypt(shards)

	// filter it down to the shards we need to migrate
	for i, si := range shardIndices {
		shards[i] = shards[si]
	}
	shards = shards[:len(shardIndices)]

	// migrate the shards
	uploaded, err := u.Migrate(ctx, shards, usedMap)
	if err != nil {
		return fmt.Errorf("failed to upload slab for migration: %w", err)
	}

	// overwrite the unhealthy shards with the newly migrated ones
	for i, si := range shardIndices {
		s.Shards[si] = uploaded[i]
	}
	return nil
}
