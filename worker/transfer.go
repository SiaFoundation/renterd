package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/tracing"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	contractLockingUploadPriority   = 1
	contractLockingDownloadPriority = 2
	defaultSectorDownloadTiming     = 200 * time.Millisecond
)

var (
	errUnusedHost            = errors.New("host not used")
	errGougingHost           = errors.New("host is gouging")
	errInsufficientBalance   = errors.New("account balance is insufficient")
	errDownloadSectorTimeout = errors.New("download sector timed out")
	errUploadSectorTimeout   = errors.New("upload sector timed out")
)

// A sectorStore stores contract data.
type sectorStore interface {
	Contract() types.FileContractID
	HostKey() types.PublicKey
	UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte) (types.Hash256, error)
	DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) error
	DeleteSectors(ctx context.Context, roots []types.Hash256) error
}

type storeProvider interface {
	withHostV2(context.Context, types.FileContractID, types.PublicKey, string, func(sectorStore) error) (err error)
	withHostV3(context.Context, types.FileContractID, types.PublicKey, string, func(sectorStore) error) (err error)
}

func parallelUploadSlab(ctx context.Context, sp storeProvider, shards [][]byte, contracts []api.ContractMetadata, locker contractLocker, uploadSectorTimeout time.Duration, maxOverdrive int, logger *zap.SugaredLogger) ([]object.Sector, []int, error) {
	// ensure the context is cancelled when the slab is uploaded
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if len(contracts) < len(shards) {
		return nil, nil, fmt.Errorf("not enough hosts to upload slab, %v<%v", len(contracts), len(shards))
	}

	type req struct {
		finishedCtx context.Context
		finishedFn  context.CancelFunc
		hostIndex   int
		shardIndex  int
	}
	type resp struct {
		req  req
		root types.Hash256
		err  error
	}
	respChan := make(chan resp, 2*len(contracts)) // every host can send up to 2 responses
	worker := func(r req) {
		doneChan := make(chan struct{})
		contract := contracts[r.hostIndex]

		// Trace the upload.
		ctx, span := tracing.Tracer.Start(r.finishedCtx, "upload-request")
		span.SetAttributes(attribute.Stringer("host", contract.HostKey))
		span.SetAttributes(attribute.Stringer("contract", contract.ID))

		go func(r req) {
			defer close(doneChan)

			lockID, err := locker.AcquireContract(ctx, contract.ID, contractLockingUploadPriority, time.Minute)
			if err != nil {
				respChan <- resp{r, types.Hash256{}, err}
				span.SetStatus(codes.Error, "acquiring the contract failed")
				span.RecordError(err)
				return
			}

			var res resp
			if err := sp.withHostV2(ctx, contract.ID, contract.HostKey, contract.HostIP, func(ss sectorStore) error {
				root, err := ss.UploadSector(ctx, (*[rhpv2.SectorSize]byte)(shards[r.shardIndex]))
				if err != nil {
					span.SetStatus(codes.Error, "uploading the sector failed")
					span.RecordError(err)
				}
				res = resp{r, root, err}
				return nil // only return the error in the response
			}); err != nil && !errors.Is(err, context.Canceled) {
				logger.Errorf("withHostV2 failed when uploading sector, err: %v", err)
			}

			// NOTE: we release before sending the response to ensure the context isn't cancelled
			if err := locker.ReleaseContract(ctx, contract.ID, lockID); err != nil {
				logger.Errorf("failed to release lock %v on contract %v, err: %v", lockID, contract.ID, err)
			}
			respChan <- res
		}(r)

		if uploadSectorTimeout > 0 {
			timer := time.NewTimer(uploadSectorTimeout)
			select {
			case <-timer.C:
				span.SetAttributes(attribute.Bool("slow", true))
				respChan <- resp{
					req: r,
					err: errUploadSectorTimeout}
			case <-doneChan:
				if !timer.Stop() {
					<-timer.C
				}
			}
		}

		<-doneChan
		span.End()
	}

	// helper to track used hosts.
	hostUsed := make([]bool, len(contracts))
	nextHost := func() int {
		for i := range hostUsed {
			if !hostUsed[i] {
				hostUsed[i] = true
				return i
			}
		}
		return -1
	}

	// helper to determine whether a request is finished.
	isReqFinished := func(r req) bool {
		select {
		case <-r.finishedCtx.Done():
			return true
		default:
			return false
		}
	}

	// helper function for launching worker given an existing request. Returns
	// true if a worker was launched for the request or if the request was
	// already finished.
	inflight := 0
	launchWorker := func(r req) bool {
		if isReqFinished(r) {
			return true
		}
		hostIndex := nextHost()
		if hostIndex == -1 {
			return false
		}
		go worker(req{r.finishedCtx, r.finishedFn, hostIndex, r.shardIndex})
		inflight++
		return true
	}

	// spawn workers and send initial requests
	for i := range shards {
		finishedCtx, finishedFn := context.WithCancel(ctx)
		if !launchWorker(req{finishedCtx, finishedFn, -1, i}) {
			panic("failed to launch worker for initial shard - should never happen")
		}
	}

	// collect responses
	var errs HostErrorSet
	sectors := make([]object.Sector, len(shards))
	rem := len(shards)
	var neededOverdrive []req
	for rem > 0 && inflight > 0 {
		resp := <-respChan
		if !errors.Is(resp.err, errUploadSectorTimeout) {
			inflight--
		}

		// Decide whether to reuse the host or not. We only do that if the
		// worker finished executing and failed due to the request being
		// finished already.
		if resp.err != nil && !errors.Is(resp.err, errUploadSectorTimeout) {
			// Otherwise remember the error and reuse the host if the request
			// was already finished.
			errs = append(errs, &HostError{contracts[resp.req.hostIndex].HostKey, resp.err})
			if isReqFinished(resp.req) {
				hostUsed[resp.req.hostIndex] = false
			}
		}

		if errors.Is(resp.err, errUploadSectorTimeout) {
			// for each slow host we eventually launch an overdrive worker
			neededOverdrive = append(neededOverdrive, resp.req)
		} else if resp.err != nil {
			// host failed, replace it.
			launchWorker(resp.req)
		} else if sectors[resp.req.shardIndex].Root == (types.Hash256{}) {
			// host succeeded.
			sectors[resp.req.shardIndex] = object.Sector{
				Host: contracts[resp.req.hostIndex].HostKey,
				Root: resp.root,
			}
			rem--
			resp.req.finishedFn()
		}

		// Launch overdrive workers as needed.
		for inflight-rem < maxOverdrive && len(neededOverdrive) > 0 {
			if !launchWorker(neededOverdrive[0]) {
				break
			}
			neededOverdrive = neededOverdrive[1:]
		}
	}

	// if rem is still greater 0, we failed to upload the slab.
	if rem > 0 {
		if errs == nil {
			return nil, nil, fmt.Errorf("rem > 0 (%v) but errs is nil - this should not happen", rem)
		}
		return nil, nil, fmt.Errorf("failed to upload slab: rem=%v, inflight=%v, contracts=%v, errs=%w", rem, inflight, len(contracts), errs)
	}

	// make hosts map
	hostsMap := make(map[types.PublicKey]int)
	for i, c := range contracts {
		hostsMap[c.HostKey] = i
	}

	// collect slow host indices
	var slowHosts []int
	for _, he := range errs {
		if errors.Is(he, errUploadSectorTimeout) {
			if _, exists := hostsMap[he.HostKey]; !exists {
				panic("host not found in hostsmap")
			}
			slowHosts = append(slowHosts, hostsMap[he.HostKey])
		}
	}
	return sectors, slowHosts, nil
}

func uploadSlab(ctx context.Context, sp storeProvider, r io.Reader, m, n uint8, contracts []api.ContractMetadata, locker contractLocker, uploadSectorTimeout time.Duration, maxOverdrive int, logger *zap.SugaredLogger) (object.Slab, int, []int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "uploadSlab")
	defer span.End()

	buf := make([]byte, int(m)*rhpv2.SectorSize)
	shards := make([][]byte, n)
	length, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return object.Slab{}, 0, nil, err
	}
	s := object.Slab{
		Key:       object.GenerateEncryptionKey(),
		MinShards: m,
	}
	s.Encode(buf, shards)
	s.Encrypt(shards)

	sectors, slowHosts, err := parallelUploadSlab(ctx, sp, shards, contracts, locker, uploadSectorTimeout, maxOverdrive, logger)
	if err != nil {
		return object.Slab{}, 0, nil, err
	}

	s.Shards = sectors
	return s, length, slowHosts, nil
}

func parallelDownloadSlab(ctx context.Context, sp storeProvider, ss object.SlabSlice, contracts []api.ContractMetadata, downloadSectorTimeout time.Duration, logger *zap.SugaredLogger) ([][]byte, []int64, error) {
	// prepopulate the timings with a value for all contracts to ensure unused hosts aren't necessarily favoured in consecutive downloads
	timings := make([]int64, len(contracts))
	for i := 0; i < len(contracts); i++ {
		timings[i] = int64(defaultSectorDownloadTiming)
	}

	// ensure the context is cancelled when the slab is downloaded
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// check whether we can recover the slab
	if len(contracts) < int(ss.MinShards) {
		return nil, nil, errors.New("not enough hosts to recover slab")
	}

	type req struct {
		hostIndex int
		offset    uint64
		length    uint64
	}
	type resp struct {
		req   req
		shard []byte
		dur   time.Duration
		err   error
	}
	respChan := make(chan resp, 2*len(contracts)) // every host can send up to 2 responses
	worker := func(r req) {
		start := time.Now()
		doneChan := make(chan struct{})

		// Trace the download.
		ctx, span := tracing.Tracer.Start(ctx, "download-request")
		span.SetAttributes(attribute.Stringer("host", contracts[r.hostIndex].HostKey))

		go func(r req) {
			defer close(doneChan)
			c := contracts[r.hostIndex]
			var shard *object.Sector
			for i := range ss.Shards {
				if ss.Shards[i].Host == c.HostKey {
					shard = &ss.Shards[i]
					break
				}
			}
			if shard == nil {
				respChan <- resp{r, nil, 0, fmt.Errorf("host %v, err: %w", c.HostKey, errUnusedHost)}
				return
			}

			if err := sp.withHostV3(ctx, c.ID, c.HostKey, c.SiamuxAddr, func(ss sectorStore) error {
				buf := bytes.NewBuffer(make([]byte, 0, rhpv2.SectorSize))
				err := ss.DownloadSector(ctx, buf, shard.Root, r.offset, r.length)
				if err != nil {
					span.SetStatus(codes.Error, "downloading the sector failed")
					span.RecordError(err)
				}
				respChan <- resp{r, buf.Bytes(), time.Since(start), err}
				return nil // only return the error in the response
			}); err != nil && !errors.Is(err, context.Canceled) {
				logger.Errorf("withHostV3 failed when downloading sector, err: %v", err)
			}
		}(r)

		if downloadSectorTimeout > 0 {
			timer := time.NewTimer(downloadSectorTimeout)
			select {
			case <-timer.C:
				span.SetAttributes(attribute.Bool("slow", true))
				respChan <- resp{
					req: r,
					dur: time.Since(start),
					err: errDownloadSectorTimeout}
			case <-doneChan:
				if !timer.Stop() {
					<-timer.C
				}
			}
		}

		<-doneChan
		span.End()
	}

	// spawn workers and send initial requests
	offset, length := ss.SectorRegion()
	hostIndex := 0
	inflight := 0
	for i := uint8(0); i < ss.MinShards; i++ {
		go worker(req{hostIndex, offset, length})
		hostIndex++
		inflight++
	}
	// collect responses
	var errs HostErrorSet
	shards := make([][]byte, len(ss.Shards))
	rem := ss.MinShards
	for rem > 0 && inflight > 0 {
		resp := <-respChan

		// only slow downloads might still be in flight
		if !errors.Is(resp.err, errDownloadSectorTimeout) {
			inflight--
		}

		if resp.err != nil {
			errs = append(errs, &HostError{contracts[resp.req.hostIndex].HostKey, resp.err})

			// make sure non funded or gouging hosts are not used for consecutive downloads
			if errors.Is(resp.err, errBalanceInsufficient) ||
				errors.Is(resp.err, errGougingHost) {
				timings[resp.req.hostIndex] = math.MaxInt64
			}

			// make sure slow hosts are not not used for consecutive downloads
			if errors.Is(resp.err, errDownloadSectorTimeout) {
				timings[resp.req.hostIndex] = int64(resp.dur) * 10
			}

			// try next host
			if hostIndex < len(contracts) {
				go worker(req{hostIndex, offset, length})
				hostIndex++
				inflight++
			}
		} else {
			timings[resp.req.hostIndex] = int64(resp.dur)
			for i := range ss.Shards {
				if ss.Shards[i].Host == contracts[resp.req.hostIndex].HostKey && len(shards[i]) == 0 {
					shards[i] = resp.shard
					rem--
					break
				}
			}
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

func downloadSlab(ctx context.Context, sp storeProvider, out io.Writer, ss object.SlabSlice, contracts []api.ContractMetadata, downloadSectorTimeout time.Duration, logger *zap.SugaredLogger) ([]int64, error) {
	ctx, span := tracing.Tracer.Start(ctx, "parallelDownloadSlab")
	defer span.End()

	shards, timings, err := parallelDownloadSlab(ctx, sp, ss, contracts, downloadSectorTimeout, logger)
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

func deleteSlabs(ctx context.Context, slabs []object.Slab, hosts []sectorStore) error {
	rootsBysectorStore := make(map[types.PublicKey][]types.Hash256)
	for _, s := range slabs {
		for _, sector := range s.Shards {
			rootsBysectorStore[sector.Host] = append(rootsBysectorStore[sector.Host], sector.Root)
		}
	}

	errChan := make(chan *HostError)
	for _, h := range hosts {
		go func(h sectorStore) {
			// NOTE: if host is not storing any sectors, the map lookup will return
			// nil, making this a no-op
			err := h.DeleteSectors(ctx, rootsBysectorStore[h.HostKey()])
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

func migrateSlab(ctx context.Context, sp storeProvider, s *object.Slab, contracts []api.ContractMetadata, locker contractLocker, downloadSectorTimeout, uploadSectorTimeout time.Duration, logger *zap.SugaredLogger) error {
	ctx, span := tracing.Tracer.Start(ctx, "migrateSlab")
	defer span.End()

	hostsMap := make(map[types.PublicKey]struct{})
	usedMap := make(map[types.PublicKey]struct{})

	// make a map of good hosts
	for _, c := range contracts {
		hostsMap[c.HostKey] = struct{}{}
	}

	// collect indices of shards that need to be migrated
	var shardIndices []int
	for i, shard := range s.Shards {
		// bad host
		if _, exists := hostsMap[shard.Host]; !exists {
			shardIndices = append(shardIndices, i)
			continue
		}

		// reused host
		_, exists := usedMap[shard.Host]
		if exists {
			shardIndices = append(shardIndices, i)
			continue
		}

		usedMap[shard.Host] = struct{}{}
	}

	// if all shards are on good hosts, we're done
	if len(shardIndices) == 0 {
		return nil
	}

	// perform some sanity checks
	if len(s.Shards)-len(shardIndices) < int(s.MinShards) {
		return fmt.Errorf("not enough hosts to download unhealthy shard, %d<%d", len(s.Shards)-len(shardIndices), int(s.MinShards))
	} else if len(shardIndices) > len(contracts) {
		return errors.New("not enough hosts to migrate shard")
	}

	// download + reconstruct slab
	ss := object.SlabSlice{
		Slab:   *s,
		Offset: 0,
		Length: uint32(s.MinShards) * rhpv2.SectorSize,
	}
	shards, _, err := parallelDownloadSlab(ctx, sp, ss, contracts, downloadSectorTimeout, logger)
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

	// filter out the hosts we used already
	filtered := contracts[:0]
	for _, c := range contracts {
		if _, used := usedMap[c.HostKey]; !used {
			filtered = append(filtered, c)
		}
	}

	// randomize order of hosts to make sure we don't migrate to the same hosts all the time
	frand.Shuffle(len(filtered), func(i, j int) { filtered[i], filtered[j] = filtered[j], filtered[i] })

	// reupload those shards. migrations are not time-sensitive, so we can use a
	// max overdrive of 0.
	uploaded, _, err := parallelUploadSlab(ctx, sp, shards, filtered, locker, uploadSectorTimeout, 0, logger)
	if err != nil {
		return fmt.Errorf("failed to upload slab for migration: %w", err)
	}

	// overwrite the unhealthy shards with the newly migrated ones
	for i, si := range shardIndices {
		s.Shards[si] = uploaded[i]
	}
	return nil
}
