package worker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/stats"
	"go.uber.org/zap"
)

const (
	downloadMemoryLimitDenom       = 6 // 1/6th of the available download memory can be used by a single download
	downloadOverpayHealthThreshold = 0.25
)

var (
	errDownloadNotEnoughHosts = errors.New("not enough hosts available to download the slab")
)

type (
	downloadManager struct {
		hm     HostManager
		mm     MemoryManager
		ms     MetricStore
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

	downloaderStats struct {
		avgSpeedMBPS float64
		healthy      bool
		numDownloads uint64
	}

	download struct {
		mgr *downloadManager

		rs     api.RedundancySettings
		offset uint32
		length uint32

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
	w.downloadManager = newDownloadManager(w.shutdownCtx, w, mm, w.bus, w.bus, maxOverdrive, overdriveTimeout, logger)
}

func newDownloadManager(ctx context.Context, hm HostManager, mm MemoryManager, ms MetricStore, os ObjectStore, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.SugaredLogger) *downloadManager {
	return &downloadManager{
		hm:     hm,
		mm:     mm,
		ms:     ms,
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

func (mgr *downloadManager) DownloadObject(ctx context.Context, w io.Writer, o object.Object, offset, length uint64, contracts []api.ContractMetadata) (err error) {
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
				shards, surchargeApplied, err := mgr.downloadSlab(ctx, next.SlabSlice, false)
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
			return ErrShuttingDown
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

	// download the slab
	slice := object.SlabSlice{
		Slab:   slab,
		Offset: 0,
		Length: uint32(slab.MinShards) * rhpv2.SectorSize,
	}
	shards, surchargeApplied, err := mgr.downloadSlab(ctx, slice, true)
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
		downloader := mgr.newDownloader(c)
		mgr.downloaders[c.HostKey] = downloader
		go downloader.processQueue(mgr.hm)
	}
}

func (mgr *downloadManager) newSlabDownload(ctx context.Context, slice object.SlabSlice, migration bool) *download {
	// calculate the offset and length
	offset, length := slice.SectorRegion()

	// build sector info
	hostToSectors := make(map[types.PublicKey][]sectorInfo)
	for sI, s := range slice.Shards {
		hostToSectors[s.LatestHost] = append(hostToSectors[s.LatestHost], sectorInfo{s, sI})
	}

	// create slab download
	return &download{
		mgr: mgr,

		rs:     api.RedundancySettings{MinShards: int(slice.MinShards), TotalShards: len(slice.Shards)},
		offset: offset,
		length: length,

		created: time.Now(),
		overpay: migration && slice.Health <= downloadOverpayHealthThreshold,

		hostToSectors: hostToSectors,
		used:          make(map[types.PublicKey]struct{}),

		sectors: make([][]byte, len(slice.Shards)),
		errs:    make(HostErrorSet),
	}
}

func (mgr *downloadManager) downloadSlab(ctx context.Context, slice object.SlabSlice, migration bool) ([][]byte, bool, error) {
	// prepare new download
	slab := mgr.newSlabDownload(ctx, slice, migration)

	// execute download
	return slab.download(ctx)
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

func (d *download) overdrive(ctx context.Context, resps *sectorResponses) (resetTimer func()) {
	// overdrive is disabled
	if d.mgr.overdriveTimeout == 0 {
		return func() {}
	}

	// create a helper function that increases the timeout for each overdrive
	timeout := func() time.Duration {
		d.mu.Lock()
		defer d.mu.Unlock()
		return time.Duration(d.numOverdriving+1) * d.mgr.overdriveTimeout
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
		d.mu.Lock()
		defer d.mu.Unlock()

		// overdrive is not due yet
		if time.Since(d.lastOverdrive) < timeout {
			return false
		}

		// overdrive is maxed out
		remaining := d.rs.MinShards - d.numCompleted
		if d.numInflight >= d.mgr.maxOverdrive+uint64(remaining) {
			return false
		}

		d.lastOverdrive = time.Now()
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
						if req := d.nextRequest(ctx, resps, true); req != nil {
							if err := d.launch(req); err != nil {
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

func (d *download) nextRequest(ctx context.Context, resps *sectorResponses, overdrive bool) *sectorDownloadReq {
	d.mu.Lock()
	defer d.mu.Unlock()

	// prepare next sectors to download
	if len(d.hostToSectors[d.curr]) == 0 {
		// select all possible hosts
		var hosts []types.PublicKey
		for host, sectors := range d.hostToSectors {
			if len(sectors) == 0 {
				continue // ignore hosts with no more sectors
			} else if _, used := d.used[host]; !used {
				hosts = append(hosts, host)
			}
		}

		// no more sectors to download
		if len(hosts) == 0 {
			return nil
		}

		// select the fastest host
		fastest := d.mgr.fastest(hosts)
		if fastest == (types.PublicKey{}) {
			return nil // can happen if downloader got stopped
		}

		// make the fastest host the current host
		d.curr = fastest
		d.used[d.curr] = struct{}{}
	}

	// pop the next sector
	sector := d.hostToSectors[d.curr][0]
	d.hostToSectors[d.curr] = d.hostToSectors[d.curr][1:]

	// build the request
	return &sectorDownloadReq{
		ctx: ctx,

		offset: d.offset,
		length: d.length,
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

func (d *download) download(ctx context.Context) ([][]byte, bool, error) {
	// cancel any sector downloads once the download is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create the responses queue
	resps := &sectorResponses{
		c: make(chan struct{}, 1),
	}
	defer resps.Close()

	// launch overdrive
	resetOverdrive := d.overdrive(ctx, resps)

	// launch 'MinShard' requests
	for i := 0; i < int(d.rs.MinShards); {
		req := d.nextRequest(ctx, resps, false)
		if req == nil {
			return nil, false, fmt.Errorf("no host available for shard %d", i)
		} else if err := d.launch(req); err == nil {
			i++
		}
	}

	// collect requests that failed due to gouging
	var gouging []*sectorDownloadReq

	// collect responses
	var done bool

loop:
	for d.inflight() > 0 && !done {
		select {
		case <-d.mgr.shutdownCtx.Done():
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
			done = d.receive(*resp)
			if done {
				break
			}

			// handle errors
			if resp.err != nil {
				// launch overdrive requests
				for {
					if req := d.nextRequest(ctx, resps, true); req != nil {
						if err := d.launch(req); err != nil {
							continue
						}
					}
					break
				}

				// handle lost sectors
				if isSectorNotFound(resp.err) {
					if err := d.mgr.os.DeleteHostSector(ctx, resp.req.hk, resp.req.root); err != nil {
						d.mgr.logger.Errorw("failed to mark sector as lost", "hk", resp.req.hk, "root", resp.req.root, zap.Error(err))
					} else {
						d.mgr.logger.Infow("successfully marked sector as lost", "hk", resp.req.hk, "root", resp.req.root)
					}
				} else if isPriceTableGouging(resp.err) && d.overpay && !resp.req.overpay {
					resp.req.overpay = true // ensures we don't retry the same request over and over again
					gouging = append(gouging, resp.req)
				}
			}
		}
	}

	if !done && len(gouging) >= d.missing() {
		for _, req := range gouging {
			_ = d.launch(req) // ignore error
		}
		gouging = nil
		goto loop
	}

	if err := d.mgr.recordMetrics(d.rs, d.speed(), d.numOverdriveSectors()); err != nil {
		d.mgr.logger.Errorf("failed to record metrics, err %v", err)
	}

	return d.finish()
}

func (d *download) numOverdriveSectors() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	numOverdrive := (int(d.numLaunched) + int(d.numRelaunched)) - d.rs.MinShards
	if numOverdrive < 0 {
		numOverdrive = 0
	}

	return uint64(numOverdrive)
}

func (d *download) speed() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	completedShards := len(d.sectors)
	bytes := completedShards * rhpv2.SectorSize
	ms := time.Since(d.created).Milliseconds()
	if ms == 0 {
		ms = 1 // avoid division by zero
	}
	return uint64(bytes) / uint64(ms)
}

func (d *download) finish() ([][]byte, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.numCompleted < d.rs.MinShards {
		var unused int
		for host := range d.hostToSectors {
			if _, used := d.used[host]; !used {
				unused++
			}
		}

		return nil, d.numOverpaid > 0, fmt.Errorf("failed to download slab: completed=%d inflight=%d launched=%d relaunched=%d overpaid=%d downloaders=%d unused=%d errors=%d %v", d.numCompleted, d.numInflight, d.numLaunched, d.numRelaunched, d.numOverpaid, d.mgr.numDownloaders(), unused, len(d.errs), d.errs)
	}
	return d.sectors, d.numOverpaid > 0, nil
}

func (d *download) missing() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.numCompleted < d.rs.MinShards {
		return d.rs.MinShards - d.numCompleted
	}
	return 0
}

func (d *download) inflight() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.numInflight
}

func (d *download) launch(req *sectorDownloadReq) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// check for nil
	if req == nil {
		return errors.New("no request given")
	}

	// check for completed sector
	if len(d.sectors[req.sectorIndex]) > 0 {
		return errors.New("sector already downloaded")
	}

	// launch the req
	err := d.mgr.launch(req)
	if err != nil {
		return err
	}

	// update the state
	d.numInflight++
	if req.overdrive {
		d.numOverdriving++
	}
	if req.overpay {
		d.numRelaunched++
	} else {
		d.numLaunched++
	}
	return nil
}

func (d *download) receive(resp sectorDownloadResp) (finished bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// update num overdriving
	if resp.req.overdrive {
		d.numOverdriving--
	}

	// failed reqs can't complete the upload
	d.numInflight--
	if resp.err != nil {
		d.errs[resp.req.hk] = resp.err
		return false
	}

	// update num overpaid
	if resp.req.overpay {
		d.numOverpaid++
	}

	// store the sector
	d.sectors[resp.req.sectorIndex] = resp.sector
	d.numCompleted++

	return d.numCompleted >= d.rs.MinShards
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

func (mgr *downloadManager) recordMetrics(rs api.RedundancySettings, speedBytesPerMS, numOverdriveSectors uint64) error {
	// derive timeout ctx from the bg context to ensure we record metrics on shutdown
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// update stats (should be deprecated)
	mgr.statsSlabDownloadSpeedBytesPerMS.Track(float64(speedBytesPerMS))
	mgr.statsOverdrivePct.Track(float64(numOverdriveSectors) / float64(rs.TotalShards))

	return mgr.ms.RecordSlabMetric(ctx, api.SlabMetric{
		Timestamp:       api.TimeNow(),
		Action:          api.SlabActionDownload,
		SpeedBytesPerMS: speedBytesPerMS,
		MinShards:       uint8(rs.MinShards),
		TotalShards:     uint8(rs.TotalShards),
		NumOverdrive:    numOverdriveSectors,
	})
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
