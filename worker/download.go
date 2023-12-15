package worker

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/stats"
	"go.sia.tech/renterd/tracing"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	downloadMemoryLimitDenom       = 6 // 1/6th of the available download memory can be used by a single download
	downloadOverheadB              = 284
	downloadOverpayHealthThreshold = 0.25
	maxConcurrentSectorsPerHost    = 3
)

var (
	errDownloadManagerStopped = errors.New("download manager stopped")
	errDownloadNotEnoughHosts = errors.New("not enough hosts available to download the slab")
)

type (
	// id is a unique identifier used for debugging
	id [8]byte

	downloadManager struct {
		hm     HostManager
		mm     MemoryManager
		os     ObjectStore
		logger *zap.SugaredLogger

		maxOverdrive     uint64
		overdriveTimeout time.Duration

		statsOverdrivePct                *stats.DataPoints
		statsSlabDownloadSpeedBytesPerMS *stats.DataPoints

		shutdownCtx context.Context

		mu            sync.Mutex
		downloaders   map[types.PublicKey]*downloader
		lastRecompute time.Time
	}

	downloader struct {
		host Host

		statsDownloadSpeedBytesPerMS    *stats.DataPoints // keep track of this separately for stats (no decay is applied)
		statsSectorDownloadEstimateInMS *stats.DataPoints

		signalWorkChan chan struct{}
		shutdownCtx    context.Context

		mu                  sync.Mutex
		consecutiveFailures uint64
		numDownloads        uint64
		queue               []*sectorDownloadReq
	}

	downloaderStats struct {
		avgSpeedMBPS float64
		healthy      bool
		numDownloads uint64
	}

	slabDownload struct {
		mgr *downloadManager

		minShards int
		offset    uint32
		length    uint32

		created time.Time
		overpay bool

		mu             sync.Mutex
		lastOverdrive  time.Time
		numCompleted   int
		numInflight    uint64
		numLaunched    uint64
		numOverdriving uint64
		numOverpaid    uint64
		numRelaunched  uint64

		curr          types.PublicKey
		hostToSectors map[types.PublicKey][]sectorInfo
		used          map[types.PublicKey]struct{}

		sectors [][]byte
		errs    HostErrorSet
	}

	slabDownloadResponse struct {
		mem              Memory
		surchargeApplied bool
		shards           [][]byte
		index            int
		err              error
	}

	sectorDownloadReq struct {
		ctx context.Context

		length uint32
		offset uint32
		root   types.Hash256
		hk     types.PublicKey

		overpay     bool
		overdrive   bool
		sectorIndex int
		resps       *sectorResponses
	}

	sectorDownloadResp struct {
		req    *sectorDownloadReq
		sector []byte
		err    error
	}

	sectorResponses struct {
		mu        sync.Mutex
		closed    bool
		responses []*sectorDownloadResp
		c         chan struct{} // signal that a new response is available
	}

	sectorInfo struct {
		object.Sector
		index int
	}

	downloadManagerStats struct {
		avgDownloadSpeedMBPS float64
		avgOverdrivePct      float64
		downloaders          map[types.PublicKey]downloaderStats
	}
)

func (w *worker) initDownloadManager(maxMemory, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.SugaredLogger) {
	if w.downloadManager != nil {
		panic("download manager already initialized") // developer error
	}

	mm := newMemoryManager(logger, maxMemory)
	w.downloadManager = newDownloadManager(w.shutdownCtx, w, mm, w.bus, maxOverdrive, overdriveTimeout, logger)
}

func newDownloadManager(ctx context.Context, hm HostManager, mm MemoryManager, os ObjectStore, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.SugaredLogger) *downloadManager {
	return &downloadManager{
		hm:     hm,
		mm:     mm,
		os:     os,
		logger: logger,

		maxOverdrive:     maxOverdrive,
		overdriveTimeout: overdriveTimeout,

		statsOverdrivePct:                stats.NoDecay(),
		statsSlabDownloadSpeedBytesPerMS: stats.NoDecay(),

		shutdownCtx: ctx,

		downloaders: make(map[types.PublicKey]*downloader),
	}
}

func newDownloader(ctx context.Context, host Host) *downloader {
	return &downloader{
		host: host,

		statsSectorDownloadEstimateInMS: stats.Default(),
		statsDownloadSpeedBytesPerMS:    stats.NoDecay(),

		signalWorkChan: make(chan struct{}, 1),
		shutdownCtx:    ctx,

		queue: make([]*sectorDownloadReq, 0),
	}
}

func (mgr *downloadManager) DownloadObject(ctx context.Context, w io.Writer, o object.Object, offset, length uint64, contracts []api.ContractMetadata) (err error) {
	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "download")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	// create identifier
	id := newID()

	// calculate what slabs we need
	var ss []slabSlice
	for _, s := range o.Slabs {
		ss = append(ss, slabSlice{
			SlabSlice:   s,
			PartialSlab: s.IsPartial(),
		})
	}
	slabs := slabsForDownload(ss, offset, length)
	if len(slabs) == 0 {
		return nil
	}

	// go through the slabs and fetch any partial slab data from the store.
	for i := range slabs {
		if !slabs[i].PartialSlab {
			continue
		}
		data, slab, err := mgr.fetchPartialSlab(ctx, slabs[i].SlabSlice.Key, slabs[i].SlabSlice.Offset, slabs[i].SlabSlice.Length)
		if err != nil {
			return fmt.Errorf("failed to fetch partial slab data: %w", err)
		}
		if slab != nil {
			slabs[i].SlabSlice.Slab = *slab
			slabs[i].PartialSlab = slab.IsPartial()
		} else {
			slabs[i].Data = data
		}
	}

	// refresh the downloaders
	mgr.refreshDownloaders(contracts)

	// build a map to count available shards later
	hosts := make(map[types.PublicKey]struct{})
	for _, c := range contracts {
		hosts[c.HostKey] = struct{}{}
	}

	// buffer the writer
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	// create the cipher writer
	cw := o.Key.Decrypt(bw, offset)

	// create response chan and ensure it's closed properly
	var wg sync.WaitGroup
	responseChan := make(chan *slabDownloadResponse)
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		wg.Wait()
		close(responseChan)
	}()

	// apply a per-download limit to the memory manager
	mm, err := mgr.mm.Limit(mgr.mm.Status().Total / downloadMemoryLimitDenom)
	if err != nil {
		return err
	}

	// launch a goroutine to launch consecutive slab downloads
	wg.Add(1)
	go func() {
		defer wg.Done()

		var slabIndex int
		for slabIndex = 0; slabIndex < len(slabs); slabIndex++ {
			next := slabs[slabIndex]

			// check if we need to abort
			select {
			case <-ctx.Done():
				return
			case <-mgr.shutdownCtx.Done():
				return
			default:
			}

			// check if the next slab is a partial slab.
			if next.PartialSlab {
				responseChan <- &slabDownloadResponse{index: slabIndex}
				continue // handle partial slab separately
			}

			// check if we have enough downloaders
			var available uint8
			for _, s := range next.Shards {
				if _, exists := hosts[s.LatestHost]; exists {
					available++
				}
			}
			if available < next.MinShards {
				responseChan <- &slabDownloadResponse{err: fmt.Errorf("%w: %v/%v", errDownloadNotEnoughHosts, available, next.MinShards)}
				return
			}

			// acquire memory
			mem := mm.AcquireMemory(ctx, uint64(next.Length))
			if mem == nil {
				return // interrupted
			}

			// launch the download
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				shards, surchargeApplied, err := mgr.downloadSlab(ctx, id, next.SlabSlice, false)
				select {
				case responseChan <- &slabDownloadResponse{
					mem:              mem,
					surchargeApplied: surchargeApplied,
					shards:           shards,
					index:            index,
					err:              err,
				}:
				case <-ctx.Done():
					mem.Release() // relase memory if we're interrupted
				}
			}(slabIndex)
		}
	}()

	// collect the response, responses might come in out of order so we keep
	// them in a map and return what we can when we can
	responses := make(map[int]*slabDownloadResponse)
	var respIndex int
outer:
	for {
		var resp *slabDownloadResponse
		select {
		case <-mgr.shutdownCtx.Done():
			return errDownloadManagerStopped
		case <-ctx.Done():
			return errors.New("download timed out")
		case resp = <-responseChan:
		}

		// handle response
		err := func() error {
			if resp.mem != nil {
				defer resp.mem.Release()
			}

			if resp.err != nil {
				mgr.logger.Errorf("download slab %v failed, overpaid %v: %v", resp.index, resp.surchargeApplied, resp.err)
				return resp.err
			} else if resp.surchargeApplied {
				mgr.logger.Warnf("download for slab %v had to overpay to succeed", resp.index)
			}

			responses[resp.index] = resp
			for {
				if next, exists := responses[respIndex]; exists {
					s := slabs[respIndex]
					if s.PartialSlab {
						// Partial slab.
						_, err = cw.Write(s.Data)
						if err != nil {
							mgr.logger.Errorf("failed to send partial slab", respIndex, err)
							return err
						}
					} else {
						// Regular slab.
						slabs[respIndex].Decrypt(next.shards)
						err := slabs[respIndex].Recover(cw, next.shards)
						if err != nil {
							mgr.logger.Errorf("failed to recover slab %v: %v", respIndex, err)
							return err
						}
					}

					next = nil
					delete(responses, respIndex)
					respIndex++

					continue
				} else {
					break
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}

		// exit condition
		if respIndex == len(slabs) {
			break outer
		}
	}

	return nil
}

func (mgr *downloadManager) DownloadSlab(ctx context.Context, slab object.Slab, contracts []api.ContractMetadata) ([][]byte, bool, error) {
	// refresh the downloaders
	mgr.refreshDownloaders(contracts)

	// grab available hosts
	available := make(map[types.PublicKey]struct{})
	for _, c := range contracts {
		available[c.HostKey] = struct{}{}
	}

	// count how many shards we can download (best-case)
	var availableShards uint8
	for _, shard := range slab.Shards {
		if _, exists := available[shard.LatestHost]; exists {
			availableShards++
		}
	}

	// check if we have enough shards
	if availableShards < slab.MinShards {
		return nil, false, fmt.Errorf("not enough hosts available to download the slab: %v/%v", availableShards, slab.MinShards)
	}

	// NOTE: we don't acquire memory here since DownloadSlab is only used for
	// migrations which already have memory acquired

	// create identifier
	id := newID()

	// download the slab
	slice := object.SlabSlice{
		Slab:   slab,
		Offset: 0,
		Length: uint32(slab.MinShards) * rhpv2.SectorSize,
	}
	shards, surchargeApplied, err := mgr.downloadSlab(ctx, id, slice, true)
	if err != nil {
		return nil, false, err
	}

	// decrypt and recover
	slice.Decrypt(shards)
	err = slice.Reconstruct(shards)
	if err != nil {
		return nil, false, err
	}

	return shards, surchargeApplied, err
}

func (mgr *downloadManager) Stats() downloadManagerStats {
	// recompute stats
	mgr.tryRecomputeStats()

	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// collect stats
	stats := make(map[types.PublicKey]downloaderStats)
	for hk, d := range mgr.downloaders {
		stats[hk] = d.stats()
	}

	return downloadManagerStats{
		avgDownloadSpeedMBPS: mgr.statsSlabDownloadSpeedBytesPerMS.Average() * 0.008, // convert bytes per ms to mbps,
		avgOverdrivePct:      mgr.statsOverdrivePct.Average(),
		downloaders:          stats,
	}
}

func (mgr *downloadManager) Stop() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	for _, d := range mgr.downloaders {
		d.Stop()
	}
}

func (mgr *downloadManager) tryRecomputeStats() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if time.Since(mgr.lastRecompute) < statsRecomputeMinInterval {
		return
	}

	for _, d := range mgr.downloaders {
		d.statsSectorDownloadEstimateInMS.Recompute()
		d.statsDownloadSpeedBytesPerMS.Recompute()
	}
	mgr.lastRecompute = time.Now()
}

func (mgr *downloadManager) numDownloaders() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return len(mgr.downloaders)
}

// fetchPartialSlab fetches the data of a partial slab from the bus. It will
// fall back to ask the bus for the slab metadata in case the slab wasn't found
// in the partial slab buffer.
func (mgr *downloadManager) fetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, *object.Slab, error) {
	data, err := mgr.os.FetchPartialSlab(ctx, key, offset, length)
	if err != nil && strings.Contains(err.Error(), api.ErrObjectNotFound.Error()) {
		// Check if slab was already uploaded.
		slab, err := mgr.os.Slab(ctx, key)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to fetch uploaded partial slab: %v", err)
		}
		return nil, &slab, nil
	} else if err != nil {
		return nil, nil, err
	}
	return data, nil, nil
}

func (mgr *downloadManager) refreshDownloaders(contracts []api.ContractMetadata) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// build map
	want := make(map[types.PublicKey]api.ContractMetadata)
	for _, c := range contracts {
		want[c.HostKey] = c
	}

	// prune downloaders
	for hk := range mgr.downloaders {
		_, wanted := want[hk]
		if !wanted {
			mgr.downloaders[hk].Stop()
			delete(mgr.downloaders, hk)
			continue
		}

		delete(want, hk) // remove from want so remainging ones are the missing ones
	}

	// update downloaders
	for _, c := range want {
		// create a host
		host := mgr.hm.Host(c.HostKey, c.ID, c.SiamuxAddr)
		downloader := newDownloader(mgr.shutdownCtx, host)
		mgr.downloaders[c.HostKey] = downloader
		go downloader.processQueue(mgr.hm)
	}
}

func (mgr *downloadManager) newSlabDownload(ctx context.Context, dID id, slice object.SlabSlice, migration bool) *slabDownload {
	// calculate the offset and length
	offset, length := slice.SectorRegion()

	// build sector info
	hostToSectors := make(map[types.PublicKey][]sectorInfo)
	for sI, s := range slice.Shards {
		hostToSectors[s.LatestHost] = append(hostToSectors[s.LatestHost], sectorInfo{s, sI})
	}

	// create slab download
	return &slabDownload{
		mgr: mgr,

		minShards: int(slice.MinShards),
		offset:    offset,
		length:    length,

		created: time.Now(),
		overpay: migration && slice.Health <= downloadOverpayHealthThreshold,

		hostToSectors: hostToSectors,
		used:          make(map[types.PublicKey]struct{}),

		sectors: make([][]byte, len(slice.Shards)),
		errs:    make(HostErrorSet),
	}
}

func (mgr *downloadManager) downloadSlab(ctx context.Context, dID id, slice object.SlabSlice, migration bool) ([][]byte, bool, error) {
	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "downloadSlab")
	defer span.End()

	// prepare new download
	slab := mgr.newSlabDownload(ctx, dID, slice, migration)

	// execute download
	return slab.download(ctx)
}

func (d *downloader) Stop() {
	for {
		download := d.pop()
		if download == nil {
			break
		}
		if !download.done() {
			download.fail(errors.New("downloader stopped"))
		}
	}
}

func (d *downloader) stats() downloaderStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return downloaderStats{
		avgSpeedMBPS: d.statsDownloadSpeedBytesPerMS.Average() * 0.008,
		healthy:      d.consecutiveFailures == 0,
		numDownloads: d.numDownloads,
	}
}

func (d *downloader) fillBatch() (batch []*sectorDownloadReq) {
	for len(batch) < maxConcurrentSectorsPerHost {
		if req := d.pop(); req == nil {
			break
		} else if req.done() {
			continue
		} else {
			batch = append(batch, req)
		}
	}
	return
}

func (d *downloader) processBatch(batch []*sectorDownloadReq) chan struct{} {
	doneChan := make(chan struct{})

	// define some state to keep track of stats
	var mu sync.Mutex
	var start time.Time
	var concurrent int64
	var downloadedB int64
	trackStatsFn := func() {
		if start.IsZero() || time.Since(start).Milliseconds() == 0 || downloadedB == 0 {
			return
		}
		durationMS := time.Since(start).Milliseconds()
		d.statsDownloadSpeedBytesPerMS.Track(float64(downloadedB / durationMS))
		d.statsSectorDownloadEstimateInMS.Track(float64(durationMS))
		start = time.Time{}
		downloadedB = 0
	}

	// define a worker to process download requests
	inflight := uint64(len(batch))
	reqsChan := make(chan *sectorDownloadReq)
	workerFn := func() {
		for req := range reqsChan {
			// check if we need to abort
			select {
			case <-d.shutdownCtx.Done():
				return
			default:
			}

			// update state
			mu.Lock()
			if start.IsZero() {
				start = time.Now()
			}
			concurrent++
			mu.Unlock()

			// execute the request
			err := d.execute(req)
			d.trackFailure(err)

			// update state + potentially track stats
			mu.Lock()
			if err == nil {
				downloadedB += int64(req.length) + downloadOverheadB
				if downloadedB >= maxConcurrentSectorsPerHost*rhpv2.SectorSize || concurrent == maxConcurrentSectorsPerHost {
					trackStatsFn()
				}
			}
			concurrent--
			if concurrent < 0 {
				panic("concurrent can never be less than zero") // developer error
			}
			mu.Unlock()
		}

		// last worker that's done closes the channel and flushes the stats
		if atomic.AddUint64(&inflight, ^uint64(0)) == 0 {
			close(doneChan)
			trackStatsFn()
		}
	}

	// launch workers
	for i := 0; i < len(batch); i++ {
		go workerFn()
	}
	for _, req := range batch {
		reqsChan <- req
	}

	// launch a goroutine to keep the request coming
	go func() {
		defer close(reqsChan)
		for {
			if req := d.pop(); req == nil {
				break
			} else if req.done() {
				continue
			} else {
				reqsChan <- req
			}
		}
	}()

	return doneChan
}

func (d *downloader) processQueue(hp HostManager) {
outer:
	for {
		// wait for work
		select {
		case <-d.signalWorkChan:
		case <-d.shutdownCtx.Done():
			return
		}

		for {
			// try fill a batch of requests
			batch := d.fillBatch()
			if len(batch) == 0 {
				continue outer
			}

			// process the batch
			doneChan := d.processBatch(batch)
			for {
				select {
				case <-d.shutdownCtx.Done():
					return
				case <-doneChan:
					continue outer
				}
			}
		}
	}
}

func (d *downloader) estimate() float64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	// fetch estimated duration per sector
	estimateP90 := d.statsSectorDownloadEstimateInMS.P90()
	if estimateP90 == 0 {
		if avg := d.statsSectorDownloadEstimateInMS.Average(); avg > 0 {
			estimateP90 = avg
		} else {
			estimateP90 = 1
		}
	}

	numSectors := float64(len(d.queue) + 1)
	return numSectors * estimateP90
}

func (d *downloader) enqueue(download *sectorDownloadReq) {
	// add tracing
	span := trace.SpanFromContext(download.ctx)
	span.SetAttributes(attribute.Float64("estimate", d.estimate()))
	span.AddEvent("enqueued")

	// enqueue the job
	d.mu.Lock()
	d.queue = append(d.queue, download)
	d.mu.Unlock()

	// signal there's work
	select {
	case d.signalWorkChan <- struct{}{}:
	default:
	}
}

func (d *downloader) pop() *sectorDownloadReq {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.queue) > 0 {
		j := d.queue[0]
		d.queue[0] = nil
		d.queue = d.queue[1:]
		return j
	}
	return nil
}

func (d *downloader) trackFailure(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err == nil {
		d.consecutiveFailures = 0
		return
	}

	if isBalanceInsufficient(err) ||
		isPriceTableExpired(err) ||
		isPriceTableNotFound(err) ||
		isSectorNotFound(err) {
		return // host is not to blame for these errors
	}

	d.consecutiveFailures++
	d.statsSectorDownloadEstimateInMS.Track(float64(time.Hour.Milliseconds()))
}

func (d *downloader) execute(req *sectorDownloadReq) (err error) {
	// add tracing
	start := time.Now()
	span := trace.SpanFromContext(req.ctx)
	span.AddEvent("execute")
	defer func() {
		elapsed := time.Since(start)
		span.SetAttributes(attribute.Int64("duration", elapsed.Milliseconds()))
		span.RecordError(err)
		span.End()
	}()

	// download the sector
	buf := bytes.NewBuffer(make([]byte, 0, req.length))
	err = d.host.DownloadSector(req.ctx, buf, req.root, req.offset, req.length, req.overpay)
	if err != nil {
		req.fail(err)
		return err
	}

	d.mu.Lock()
	d.numDownloads++
	d.mu.Unlock()

	req.succeed(buf.Bytes())
	return nil
}

func (req *sectorDownloadReq) succeed(sector []byte) {
	req.resps.Add(&sectorDownloadResp{
		req:    req,
		sector: sector,
	})
}

func (req *sectorDownloadReq) fail(err error) {
	req.resps.Add(&sectorDownloadResp{
		req: req,
		err: err,
	})
}

func (req *sectorDownloadReq) done() bool {
	select {
	case <-req.ctx.Done():
		return true
	default:
		return false
	}
}

func (s *slabDownload) overdrive(ctx context.Context, resps *sectorResponses) (resetTimer func()) {
	// overdrive is disabled
	if s.mgr.overdriveTimeout == 0 {
		return func() {}
	}

	// create a helper function that increases the timeout for each overdrive
	timeout := func() time.Duration {
		s.mu.Lock()
		defer s.mu.Unlock()
		return time.Duration(s.numOverdriving+1) * s.mgr.overdriveTimeout
	}

	// create a timer to trigger overdrive
	timer := time.NewTimer(timeout())
	resetTimer = func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
		timer.Reset(timeout())
	}

	// create a function to check whether overdrive is possible
	canOverdrive := func(timeout time.Duration) bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		// overdrive is not due yet
		if time.Since(s.lastOverdrive) < timeout {
			return false
		}

		// overdrive is maxed out
		remaining := s.minShards - s.numCompleted
		if s.numInflight >= s.mgr.maxOverdrive+uint64(remaining) {
			return false
		}

		s.lastOverdrive = time.Now()
		return true
	}

	// try overdriving every time the timer fires
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if canOverdrive(timeout()) {
					for {
						if req := s.nextRequest(ctx, resps, true); req != nil {
							if err := s.launch(req); err != nil {
								continue // try the next request if this fails to launch
							}
						}
						break
					}
				}
				resetTimer()
			}
		}
	}()

	return
}

func (s *slabDownload) nextRequest(ctx context.Context, resps *sectorResponses, overdrive bool) *sectorDownloadReq {
	s.mu.Lock()
	defer s.mu.Unlock()

	// prepare next sectors to download
	if len(s.hostToSectors[s.curr]) == 0 {
		// select all possible hosts
		var hosts []types.PublicKey
		for host, sectors := range s.hostToSectors {
			if len(sectors) == 0 {
				continue // ignore hosts with no more sectors
			} else if _, used := s.used[host]; !used {
				hosts = append(hosts, host)
			}
		}

		// no more sectors to download
		if len(hosts) == 0 {
			return nil
		}

		// make the fastest host the current host
		s.curr = s.mgr.fastest(hosts)
		s.used[s.curr] = struct{}{}
	}

	// pop the next sector
	sector := s.hostToSectors[s.curr][0]
	s.hostToSectors[s.curr] = s.hostToSectors[s.curr][1:]

	// create the span
	sCtx, span := tracing.Tracer.Start(ctx, "sectorDownloadReq")
	span.SetAttributes(attribute.Stringer("hk", sector.LatestHost))
	span.SetAttributes(attribute.Bool("overdrive", overdrive))
	span.SetAttributes(attribute.Int("sector", sector.index))

	// build the request
	return &sectorDownloadReq{
		ctx: sCtx,

		offset: s.offset,
		length: s.length,
		root:   sector.Root,
		hk:     sector.LatestHost,

		// overpay is set to 'true' when a request is retried after the slab
		// download failed and we realise that it might have succeeded if we
		// allowed overpaying for certain sectors, we only do this when trying
		// to migrate a critically low-health slab that might otherwise be
		// unrecoverable
		overpay: false,

		overdrive:   overdrive,
		sectorIndex: sector.index,
		resps:       resps,
	}
}

func (s *slabDownload) download(ctx context.Context) ([][]byte, bool, error) {
	// cancel any sector downloads once the download is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "download")
	defer span.End()

	// create the responses queue
	resps := &sectorResponses{
		c: make(chan struct{}, 1),
	}
	defer resps.Close()

	// launch overdrive
	resetOverdrive := s.overdrive(ctx, resps)

	// launch 'MinShard' requests
	for i := 0; i < int(s.minShards); {
		req := s.nextRequest(ctx, resps, false)
		if req == nil {
			return nil, false, fmt.Errorf("no host available for shard %d", i)
		} else if err := s.launch(req); err == nil {
			i++
		}
	}

	// collect requests that failed due to gouging
	var gouging []*sectorDownloadReq

	// collect responses
	var done bool

loop:
	for s.inflight() > 0 && !done {
		select {
		case <-s.mgr.shutdownCtx.Done():
			return nil, false, errors.New("download stopped")
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-resps.c:
			resetOverdrive()
		}

		for {
			resp := resps.Next()
			if resp == nil {
				break
			}

			// receive the response
			done = s.receive(*resp)
			if done {
				break
			}

			// handle errors
			if resp.err != nil {
				// launch overdrive requests
				for {
					if req := s.nextRequest(ctx, resps, true); req != nil {
						if err := s.launch(req); err != nil {
							continue
						}
					}
					break
				}

				// handle lost sectors
				if isSectorNotFound(resp.err) {
					if err := s.mgr.os.DeleteHostSector(ctx, resp.req.hk, resp.req.root); err != nil {
						s.mgr.logger.Errorw("failed to mark sector as lost", "hk", resp.req.hk, "root", resp.req.root, zap.Error(err))
					} else {
						s.mgr.logger.Infow("successfully marked sector as lost", "hk", resp.req.hk, "root", resp.req.root)
					}
				} else if isPriceTableGouging(resp.err) && s.overpay && !resp.req.overpay {
					resp.req.overpay = true // ensures we don't retry the same request over and over again
					gouging = append(gouging, resp.req)
				}
			}
		}
	}

	if !done && len(gouging) >= s.missing() {
		for _, req := range gouging {
			_ = s.launch(req) // ignore error
		}
		gouging = nil
		goto loop
	}

	// track stats
	s.mgr.statsOverdrivePct.Track(s.overdrivePct())
	s.mgr.statsSlabDownloadSpeedBytesPerMS.Track(float64(s.downloadSpeed()))
	return s.finish()
}

func (s *slabDownload) overdrivePct() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	numOverdrive := (int(s.numLaunched) + int(s.numRelaunched)) - s.minShards
	if numOverdrive < 0 {
		numOverdrive = 0
	}

	return float64(numOverdrive) / float64(s.minShards)
}

func (s *slabDownload) downloadSpeed() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	completedShards := len(s.sectors)
	bytes := completedShards * rhpv2.SectorSize
	ms := time.Since(s.created).Milliseconds()
	if ms == 0 {
		ms = 1 // avoid division by zero
	}
	return int64(bytes) / ms
}

func (s *slabDownload) finish() ([][]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.numCompleted < s.minShards {
		var unused int
		for host := range s.hostToSectors {
			if _, used := s.used[host]; !used {
				unused++
			}
		}

		return nil, s.numOverpaid > 0, fmt.Errorf("failed to download slab: completed=%d inflight=%d launched=%d relaunched=%d overpaid=%d downloaders=%d unused=%d errors=%d %v", s.numCompleted, s.numInflight, s.numLaunched, s.numRelaunched, s.numOverpaid, s.mgr.numDownloaders(), unused, len(s.errs), s.errs)
	}
	return s.sectors, s.numOverpaid > 0, nil
}

func (s *slabDownload) missing() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.numCompleted < s.minShards {
		return s.minShards - s.numCompleted
	}
	return 0
}

func (s *slabDownload) inflight() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numInflight
}

func (s *slabDownload) launch(req *sectorDownloadReq) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// check for nil
	if req == nil {
		return errors.New("no request given")
	}

	// check for completed sector
	if len(s.sectors[req.sectorIndex]) > 0 {
		return errors.New("sector already downloaded")
	}

	// launch the req
	err := s.mgr.launch(req)
	if err != nil {
		span := trace.SpanFromContext(req.ctx)
		span.RecordError(err)
		span.End()
		return err
	}

	// update the state
	s.numInflight++
	if req.overdrive {
		s.numOverdriving++
	}
	if req.overpay {
		s.numRelaunched++
	} else {
		s.numLaunched++
	}
	return nil
}

func (s *slabDownload) receive(resp sectorDownloadResp) (finished bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// update num overdriving
	if resp.req.overdrive {
		s.numOverdriving--
	}

	// failed reqs can't complete the upload
	s.numInflight--
	if resp.err != nil {
		s.errs[resp.req.hk] = resp.err
		return false
	}

	// update num overpaid
	if resp.req.overpay {
		s.numOverpaid++
	}

	// store the sector
	s.sectors[resp.req.sectorIndex] = resp.sector
	s.numCompleted++

	return s.numCompleted >= s.minShards
}

func (mgr *downloadManager) fastest(hosts []types.PublicKey) (fastest types.PublicKey) {
	// recompute stats
	mgr.tryRecomputeStats()

	// return the fastest host
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	lowest := math.MaxFloat64
	for _, h := range hosts {
		if d, ok := mgr.downloaders[h]; !ok {
			continue
		} else if estimate := d.estimate(); estimate < lowest {
			lowest = estimate
			fastest = h
		}
	}
	return
}

func (mgr *downloadManager) launch(req *sectorDownloadReq) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	downloader, exists := mgr.downloaders[req.hk]
	if !exists {
		return fmt.Errorf("no downloader for host %v", req.hk)
	}

	downloader.enqueue(req)
	return nil
}

func newID() id {
	var id id
	frand.Read(id[:])
	return id
}

func (id id) String() string {
	return fmt.Sprintf("%x", id[:])
}

type slabSlice struct {
	object.SlabSlice
	PartialSlab bool
	Data        []byte
}

func slabsForDownload(slabs []slabSlice, offset, length uint64) []slabSlice {
	// declare a helper to cast a uint64 to uint32 with overflow detection. This
	// could should never produce an overflow.
	cast32 := func(in uint64) uint32 {
		if in > math.MaxUint32 {
			panic("slabsForDownload: overflow detected")
		}
		return uint32(in)
	}

	// mutate a copy
	slabs = append([]slabSlice(nil), slabs...)

	firstOffset := offset
	for i, ss := range slabs {
		if firstOffset < uint64(ss.Length) {
			slabs = slabs[i:]
			break
		}
		firstOffset -= uint64(ss.Length)
	}
	slabs[0].Offset += cast32(firstOffset)
	slabs[0].Length -= cast32(firstOffset)

	lastLength := length
	for i, ss := range slabs {
		if lastLength <= uint64(ss.Length) {
			slabs = slabs[:i+1]
			break
		}
		lastLength -= uint64(ss.Length)
	}
	slabs[len(slabs)-1].Length = cast32(lastLength)
	return slabs
}

func (sr *sectorResponses) Add(resp *sectorDownloadResp) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.closed {
		return
	}
	sr.responses = append(sr.responses, resp)
	select {
	case sr.c <- struct{}{}:
	default:
	}
}

func (sr *sectorResponses) Close() error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.closed = true
	sr.responses = nil // clear responses
	close(sr.c)
	return nil
}

func (sr *sectorResponses) Next() *sectorDownloadResp {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if len(sr.responses) == 0 {
		return nil
	}
	resp := sr.responses[0]
	sr.responses = sr.responses[1:]
	return resp
}
