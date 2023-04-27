package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"gitlab.com/NebulousLabs/fastrand"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/tracing"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
)

const (
	defaultSectorDownloadTiming = 200 * time.Millisecond
)

var (
	errGougingHost           = errors.New("host is gouging")
	errInsufficientBalance   = errors.New("account balance is insufficient")
	errDownloadSectorTimeout = errors.New("download sector timed out")
	errUploadSectorTimeout   = errors.New("upload sector timed out")
	errHostNotUsed           = errors.New("host was not used")
)

type (
	sectorStore interface {
		Contract() types.FileContractID
		HostKey() types.PublicKey
		UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte) (types.Hash256, error)
		DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) error
		DeleteSectors(ctx context.Context, roots []types.Hash256) error
	}

	storeProvider interface {
		withHostV2(context.Context, types.FileContractID, types.PublicKey, string, func(sectorStore) error) (err error)
		withHostV3(context.Context, types.FileContractID, types.PublicKey, string, func(sectorStore) error) (err error)
	}
)

type (
	uploadReq struct {
		finishedCtx context.Context
		finishedFn  context.CancelFunc
		shardIndex  int
	}

	uploadRes struct {
		req      uploadReq
		root     types.Hash256
		host     types.PublicKey
		doneChan chan error
		err      error
	}
)

func (req uploadReq) isFinished() bool {
	select {
	case <-req.finishedCtx.Done():
		return true
	default:
		return false
	}
}

func parallelUploadSlab(ctx context.Context, sp storeProvider, u Uploader, shards [][]byte, exclude map[types.PublicKey]struct{}, uploadSectorTimeout time.Duration, maxOverdrive int, logger *zap.SugaredLogger) ([]object.Sector, error) {
	// ensure the pool has enough uploaders
	if u.total() < len(shards)+len(exclude) {
		return nil, fmt.Errorf("not enough hosts to upload slab, hosts=%v, exclude=%v, shards=%v", u.total(), len(exclude), len(shards))
	}

	// ensure the upload's state is cleaned up
	var id uploadID
	fastrand.Read(id[:])
	defer u.finish(id)

	// ensure the context is cancelled when the slab is uploaded
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	respChan := make(chan uploadRes)
	worker := func(r uploadReq, contract api.ContractMetadata, signalChan, doneChan chan error) {
		// Trace the upload.
		ctx, span := tracing.Tracer.Start(r.finishedCtx, "upload-request")
		span.SetAttributes(attribute.Stringer("host", contract.HostKey))
		span.SetAttributes(attribute.Stringer("contract", contract.ID))

		// Wait for the signal.
		go func(r uploadReq) {
			var err error
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case err = <-signalChan:
			}

			if err != nil {
				span.SetStatus(codes.Error, "uploading the sector failed")
				span.RecordError(err)
				respChan <- uploadRes{r, types.Hash256{}, contract.HostKey, doneChan, err}
				return
			}

			if err := sp.withHostV2(ctx, contract.ID, contract.HostKey, contract.HostIP, func(ss sectorStore) error {
				root, err := ss.UploadSector(ctx, (*[rhpv2.SectorSize]byte)(shards[r.shardIndex]))
				if err != nil {
					span.SetStatus(codes.Error, "uploading the sector failed")
					span.RecordError(err)
				}
				respChan <- uploadRes{r, root, contract.HostKey, doneChan, err}
				return nil // only return the error in the response
			}); err != nil && !errors.Is(err, context.Canceled) {
				logger.Errorf("withHostV2 failed when uploading sector, err: %v", err)
			}
		}(r)

		if uploadSectorTimeout > 0 {
			timer := time.NewTimer(uploadSectorTimeout)
			select {
			case <-timer.C:
				span.SetAttributes(attribute.Bool("slow", true))
				respChan <- uploadRes{r, types.Hash256{}, contract.HostKey, doneChan, errUploadSectorTimeout}
			case <-doneChan:
				if !timer.Stop() {
					<-timer.C
				}
			}
		}

		<-doneChan
		span.End()
	}

	// helper function for handling requests. Will return true if either a new
	// worker was launched for the request or if the request was already
	// finished.
	inflight := 0
	handleRequest := func(r uploadReq) bool {
		if r.isFinished() {
			return true
		}

		contract, signal, done, err := u.schedule(ctx, id, rhpv2.SectorSize, lockingPriorityUpload, exclude)
		if err != nil {
			return false
		}

		go worker(r, contract, signal, done)
		inflight++
		return true
	}

	// spawn workers and send initial requests
	for i := range shards {
		finishedCtx, finishedFn := context.WithCancel(ctx)
		if !handleRequest(uploadReq{
			finishedCtx: finishedCtx,
			finishedFn:  finishedFn,
			shardIndex:  i,
		}) {
			panic("failed to launch worker for initial shard")
		}
	}

	// collect responses
	var errs HostErrorSet
	sectors := make([]object.Sector, len(shards))
	rem := len(shards)
	var toLaunch []uploadReq
	for inflight > 0 {
		resp := <-respChan
		if !errors.Is(resp.err, errUploadSectorTimeout) {
			inflight--
		}

		if errors.Is(resp.err, errUploadSectorTimeout) {
			// for each slow host we eventually launch an overdrive worker
			toLaunch = append(toLaunch, resp.req)
		} else if resp.err != nil {
			// host failed.
			errs = append(errs, &HostError{resp.host, resp.err})
			if resp.req.isFinished() {
				// if the request was finished, we can reuse the host since it
				// probably failed due to the other host being faster.
				resp.doneChan <- errHostNotUsed
			} else if !handleRequest(resp.req) {
				// if the request wasn't finished we need to replace it. If
				// replacing it failed since no worker is available, we add the
				// request to the front of the overdrive slice.
				toLaunch = append([]uploadReq{resp.req}, toLaunch...)
			}

			// close resp channel
			close(resp.doneChan)
		} else if sectors[resp.req.shardIndex].Root == (types.Hash256{}) {
			// host succeeded.
			sectors[resp.req.shardIndex] = object.Sector{
				Host: resp.host,
				Root: resp.root,
			}
			resp.req.finishedFn()

			rem--
			if rem == 0 {
				// close resp channel
				close(resp.doneChan)
				break // done
			}
		} else {
			// host succeeded but with a duplicate piece. Reuse the host.
			resp.doneChan <- errHostNotUsed
			close(resp.doneChan)
		}

		// Launch overdrive workers as needed.
		for inflight-rem < maxOverdrive && len(toLaunch) > 0 {
			if !handleRequest(toLaunch[0]) {
				break
			}
			toLaunch = toLaunch[1:]
		}
	}

	// if rem is still greater 0, we failed to upload the slab.
	if rem > 0 {
		return nil, fmt.Errorf("failed to upload slab: rem=%v, inflight=%v, errs=%w", rem, inflight, errs)
	}
	return sectors, nil
}

func uploadSlab(ctx context.Context, sp storeProvider, u Uploader, r io.Reader, m, n uint8, uploadSectorTimeout time.Duration, maxOverdrive int, logger *zap.SugaredLogger) (object.Slab, int, error) {
	ctx, span := tracing.Tracer.Start(ctx, "uploadSlab")
	defer span.End()

	buf := make([]byte, int(m)*rhpv2.SectorSize)
	shards := make([][]byte, n)
	length, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return object.Slab{}, 0, err
	}
	s := object.Slab{
		Key:       object.GenerateEncryptionKey(),
		MinShards: m,
	}
	s.Encode(buf, shards)
	s.Encrypt(shards)

	sectors, err := parallelUploadSlab(ctx, sp, u, shards, nil, uploadSectorTimeout, maxOverdrive, logger)
	if err != nil {
		return object.Slab{}, 0, err
	}

	s.Shards = sectors
	return s, length, nil
}

func parallelDownloadSlab(ctx context.Context, sp storeProvider, ss object.SlabSlice, contracts []api.ContractMetadata, downloadSectorTimeout time.Duration, maxOverdrive int, logger *zap.SugaredLogger) ([][]byte, []int64, error) {
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

			if err := sp.withHostV3(ctx, contract.ID, contract.HostKey, contract.SiamuxAddr, func(ss sectorStore) error {
				buf := bytes.NewBuffer(make([]byte, 0, rhpv2.SectorSize))
				err := ss.DownloadSector(ctx, buf, shard.Root, r.offset, r.length)
				if err != nil {
					span.SetStatus(codes.Error, "downloading the sector failed")
					span.RecordError(err)
				}
				respChan <- resp{hostIndex, r, buf.Bytes(), time.Since(start), err}
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
	inflight := 0
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
	var overdrive int
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
		for inflight < int(ss.MinShards)+overdrive {
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

func downloadSlab(ctx context.Context, sp storeProvider, out io.Writer, ss object.SlabSlice, contracts []api.ContractMetadata, downloadSectorTimeout time.Duration, maxOverdrive int, logger *zap.SugaredLogger) ([]int64, error) {
	ctx, span := tracing.Tracer.Start(ctx, "parallelDownloadSlab")
	defer span.End()

	shards, timings, err := parallelDownloadSlab(ctx, sp, ss, contracts, downloadSectorTimeout, maxOverdrive, logger)
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

func migrateSlab(ctx context.Context, sp storeProvider, u Uploader, s *object.Slab, contracts []api.ContractMetadata, downloadSectorTimeout, uploadSectorTimeout time.Duration, logger *zap.SugaredLogger) error {
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
	shards, _, err := parallelDownloadSlab(ctx, sp, ss, contracts, downloadSectorTimeout, 0, logger) // no overdrive for downloads
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

	// reupload those shards. migrations are not time-sensitive, so we can use a
	// max overdrive of 0.
	uploaded, err := parallelUploadSlab(ctx, sp, u, shards, usedMap, uploadSectorTimeout, 0, logger)
	if err != nil {
		return fmt.Errorf("failed to upload slab for migration: %w", err)
	}

	// overwrite the unhealthy shards with the newly migrated ones
	for i, si := range shardIndices {
		s.Shards[si] = uploaded[i]
	}
	return nil
}
