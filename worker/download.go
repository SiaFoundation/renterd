package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/mux/v1"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	maxOngoingSlabDownloads = 10
)

type (
	downloadManager struct {
		hp     hostProvider
		logger *zap.SugaredLogger

		maxOverdrive     uint64
		overdriveTimeout time.Duration

		statsOverdrivePct                *dataPoints
		statsSlabDownloadSpeedBytesPerMS *dataPoints

		stopChan chan struct{}

		mu            sync.Mutex
		ongoing       map[slabID]struct{}
		downloaders   map[types.PublicKey]*downloader
		lastRecompute time.Time
	}

	downloader struct {
		host hostV3

		statsSectorDownloadEstimateInMS    *dataPoints
		statsSectorDownloadSpeedBytesPerMS *dataPoints // keep track of this separately for stats (no decay is applied)

		signalWorkChan chan struct{}
		stopChan       chan struct{}

		mu                  sync.Mutex
		consecutiveFailures uint64
		queue               []*sectorDownloadReq
	}

	slabDownload struct {
		mgr *downloadManager

		sID       slabID
		created   time.Time
		minShards int
		offset    uint32
		length    uint32

		mu            sync.Mutex
		lastOverdrive time.Time
		numInflight   uint64
		numLaunched   uint64
		numCompleted  int

		curr          types.PublicKey
		hostToSectors map[types.PublicKey][]sectorInfo
		used          map[types.PublicKey]struct{}

		sectors [][]byte
		errs    HostErrorSet
	}

	slabDownloadResponse struct {
		shards [][]byte
		index  int
		err    error
	}

	sectorDownloadReq struct {
		ctx context.Context

		length uint32
		offset uint32
		root   types.Hash256
		hk     types.PublicKey

		overdrive    bool
		sectorIndex  int
		responseChan chan sectorDownloadResp
	}

	sectorDownloadResp struct {
		hk          types.PublicKey
		sectorIndex int
		sector      []byte
		err         error
	}

	sectorInfo struct {
		object.Sector
		index int
	}

	downloadManagerStats struct {
		avgSlabDownloadSpeedMBPS float64
		avgOverdrivePct          float64
		healthyDownloaders       uint64
		numDownloaders           uint64
		downloadSpeedsMBPS       map[types.PublicKey]float64
	}
)

func (w *worker) initDownloadManager(maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.SugaredLogger) {
	if w.downloadManager != nil {
		panic("download manager already initialized") // developer error
	}

	w.downloadManager = newDownloadManager(w, maxOverdrive, overdriveTimeout, logger)
}

func newDownloadManager(hp hostProvider, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.SugaredLogger) *downloadManager {
	return &downloadManager{
		hp:     hp,
		logger: logger,

		maxOverdrive:     maxOverdrive,
		overdriveTimeout: overdriveTimeout,

		statsOverdrivePct:                newDataPoints(0),
		statsSlabDownloadSpeedBytesPerMS: newDataPoints(0),

		stopChan: make(chan struct{}),

		ongoing:     make(map[slabID]struct{}),
		downloaders: make(map[types.PublicKey]*downloader),
	}
}

func (mgr *downloadManager) newDownloader(host hostV3) *downloader {
	return &downloader{
		host: host,

		statsSectorDownloadEstimateInMS:    newDataPoints(statsDecayHalfTime),
		statsSectorDownloadSpeedBytesPerMS: newDataPoints(0), // no decay for exposed stats

		signalWorkChan: make(chan struct{}, 1),
		stopChan:       make(chan struct{}),

		queue: make([]*sectorDownloadReq, 0),
	}
}

func (mgr *downloadManager) Download(ctx context.Context, w io.Writer, o object.Object, offset, length uint32, contracts []api.ContractMetadata) (err error) {
	// refresh the downloaders
	mgr.refreshDownloaders(contracts)

	// cancel all in-flight requests when the download is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "download")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	// calculate what slabs we need
	slabs := slabsForDownload(o.Slabs, offset, length)
	if len(slabs) == 0 {
		return nil
	}

	// create the cipher writer
	cw := o.Key.Decrypt(w, offset)

	// create the trigger chan
	nextSlabChan := make(chan struct{}, 1)
	nextSlabChan <- struct{}{}

	// create response channel
	responseChan := make(chan *slabDownloadResponse)
	var slabIndex int

	// responses cache (might come in out of order)
	responses := make(map[int]*slabDownloadResponse)
	var respIndex int

	fmt.Printf("DEBUG PJ: %v | downloading %d slabs\n", o.Key.String(), len(slabs))
loop:
	for {
		select {
		case <-mgr.stopChan:
			return errors.New("manager was stopped")
		case <-ctx.Done():
			return errors.New("download timed out")
		case <-nextSlabChan:
			if slabIndex == len(slabs) {
				continue
			}
			go mgr.downloadSlab(ctx, slabs[slabIndex], slabIndex, responseChan, nextSlabChan)
			slabIndex++
		case resp := <-responseChan:
			fmt.Printf("DEBUG PJ: %v | received slab %d/%d\n", o.Key.String(), resp.index+1, len(slabs))

			// receive the response
			if resp.err != nil {
				return resp.err
			}
			responses[resp.index] = resp

			// slabs might download out of order, therefore we need to keep
			// sending slabs for as long as we have consecutive slabs in the
			// response map
			for {
				next, exists := responses[respIndex]
				if !exists {
					break
				}

				slabs[respIndex].Decrypt(next.shards)
				err := slabs[respIndex].Recover(cw, next.shards)
				if err != nil {
					return err
				}

				next = nil
				delete(responses, respIndex)
				respIndex++
			}

			// exit condition
			if respIndex == slabIndex {
				break loop
			}
		}
	}

	return nil
}
func (mgr *downloadManager) Stats() downloadManagerStats {
	// recompute stats
	mgr.tryRecomputeStats()

	// collect stats
	mgr.mu.Lock()
	var numHealthy uint64
	speeds := make(map[types.PublicKey]float64)
	for hk, d := range mgr.downloaders {
		healthy, mbps := d.stats()
		speeds[hk] = mbps
		if healthy {
			numHealthy++
		}
	}
	mgr.mu.Unlock()

	// prepare stats
	return downloadManagerStats{
		avgSlabDownloadSpeedMBPS: mgr.statsSlabDownloadSpeedBytesPerMS.Average() * 0.008, // convert bytes per ms to mbps,
		avgOverdrivePct:          mgr.statsOverdrivePct.Average(),
		healthyDownloaders:       numHealthy,
		numDownloaders:           uint64(len(mgr.downloaders)),
		downloadSpeedsMBPS:       speeds,
	}
}

func (mgr *downloadManager) Stop() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	close(mgr.stopChan)
	for _, d := range mgr.downloaders {
		close(d.stopChan)
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
		d.statsSectorDownloadSpeedBytesPerMS.Recompute()
	}
	mgr.lastRecompute = time.Now()
}

func (mgr *downloadManager) numDownloaders() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return len(mgr.downloaders)
}

func (mgr *downloadManager) refreshDownloaders(contracts []api.ContractMetadata) {
	fmt.Println("DEBUG PJ: refresh downloaders", len(contracts))

	// build map
	want := make(map[types.PublicKey]api.ContractMetadata)
	for _, c := range contracts {
		want[c.HostKey] = c
	}

	// prune downloaders
	for hk := range mgr.downloaders {
		_, wanted := want[hk]
		if !wanted {
			close(mgr.downloaders[hk].stopChan)
			delete(mgr.downloaders, hk)
			continue
		}

		delete(want, hk) // remove from want so remainging ones are the missing ones
	}

	// update downloaders
	for _, c := range want {
		// create a host
		host, err := mgr.hp.newHostV3(c.ID, c.HostKey, c.SiamuxAddr)
		if err != nil {
			mgr.logger.Errorw(fmt.Sprintf("failed to create downloader, err: %v", err), "hk", c.HostKey, "fcid", c.ID, "address", c.SiamuxAddr)
			continue
		}

		downloader := mgr.newDownloader(host)
		mgr.downloaders[c.HostKey] = downloader
		go downloader.processQueue(mgr.hp)
	}
}

func (mgr *downloadManager) newSlabDownload(ctx context.Context, slice object.SlabSlice) (*slabDownload, func()) {
	// create slab id
	var sID slabID
	frand.Read(sID[:])

	// add slab to ongoing downloads
	mgr.mu.Lock()
	mgr.ongoing[sID] = struct{}{}
	mgr.mu.Unlock()

	// prepare a function to remove it from the ongoing downloads
	finishFn := func() {
		mgr.mu.Lock()
		delete(mgr.ongoing, sID)
		mgr.mu.Unlock()
	}

	// calculate the offset and length
	offset, length := slice.SectorRegion()

	// build sector info
	hostToSectors := make(map[types.PublicKey][]sectorInfo)
	for sI, s := range slice.Shards {
		hostToSectors[s.Host] = append(hostToSectors[s.Host], sectorInfo{s, sI})
	}

	// create slab download
	return &slabDownload{
		mgr: mgr,

		sID:       sID,
		created:   time.Now(),
		minShards: int(slice.MinShards),
		offset:    offset,
		length:    length,

		hostToSectors: hostToSectors,
		used:          make(map[types.PublicKey]struct{}),

		sectors: make([][]byte, len(slice.Shards)),
	}, finishFn
}

func (mgr *downloadManager) ongoingDownloads() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return len(mgr.ongoing)
}

func (mgr *downloadManager) downloadSlab(ctx context.Context, slice object.SlabSlice, index int, responseChan chan *slabDownloadResponse, nextSlabChan chan struct{}) {
	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "downloadSlab")
	defer span.End()

	// prepare the download
	slab, finishFn := mgr.newSlabDownload(ctx, slice)
	defer finishFn()

	// download shards
	resp := &slabDownloadResponse{index: index}
	resp.shards, resp.err = slab.downloadShards(ctx, nextSlabChan)

	// send the response
	select {
	case <-ctx.Done():
	case responseChan <- resp:
	}
}

func (d *downloader) stats() (healthy bool, mbps float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	healthy = d.consecutiveFailures == 0
	mbps = d.statsSectorDownloadSpeedBytesPerMS.Average() * 0.008
	return
}

func (d *downloader) processQueue(hp hostProvider) {
outer:
	for {
		// wait for work
		select {
		case <-d.signalWorkChan:
		case <-d.stopChan:
			return
		}

		for {
			// check if we are stopped
			select {
			case <-d.stopChan:
				return
			default:
			}

			// pop next job
			req := d.pop()
			if req == nil {
				continue outer
			}
			if req.done() {
				continue
			}

			// execute the job
			start := time.Now()
			sector, err := req.execute(d.host)
			elapsed := time.Since(start)

			// handle the response
			if err != nil {
				req.fail(err)
			} else {
				req.succeed(sector)

				// update estimates
				downloadedB := int64(rhpv2.SectorSize)
				durationMS := elapsed.Milliseconds()
				d.statsSectorDownloadSpeedBytesPerMS.Track(float64(downloadedB / durationMS))
				d.statsSectorDownloadEstimateInMS.Track(float64(durationMS))
			}

			// track the failure, ignore gracefully closed streams and canceled overdrives
			isErrClosedStream := errors.Is(err, mux.ErrClosedStream)
			canceledOverdrive := req.done() && req.overdrive && err != nil
			if !canceledOverdrive && !isErrClosedStream {
				d.trackFailure(err)
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
		estimateP90 = 1
	}

	numSectors := math.Sqrt(float64(len(d.queue))) + 1
	return numSectors * estimateP90
}

func (d *downloader) enqueue(download *sectorDownloadReq) {
	// add tracing
	span := trace.SpanFromContext(download.ctx)
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
	if err != nil {
		d.consecutiveFailures++
		d.statsSectorDownloadEstimateInMS.Track(float64(time.Hour.Milliseconds()))
	} else {
		d.consecutiveFailures = 0
	}
}

func (req *sectorDownloadReq) execute(h hostV3) (_ []byte, err error) {
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
	buf := bytes.NewBuffer(make([]byte, 0, rhpv2.SectorSize))
	err = h.DownloadSector(req.ctx, buf, req.root, req.offset, req.length)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (req *sectorDownloadReq) succeed(sector []byte) {
	select {
	case <-req.ctx.Done():
	case req.responseChan <- sectorDownloadResp{
		hk:          req.hk,
		sectorIndex: req.sectorIndex,
		sector:      sector,
	}:
	}
}

func (req *sectorDownloadReq) fail(err error) {
	select {
	case <-req.ctx.Done():
	case req.responseChan <- sectorDownloadResp{
		hk:  req.hk,
		err: err,
	}:
	}
}

func (req *sectorDownloadReq) done() bool {
	select {
	case <-req.ctx.Done():
		return true
	default:
		return false
	}
}

func (s *slabDownload) overdrive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// overdrive is not due yet
	if time.Since(s.lastOverdrive) < s.mgr.overdriveTimeout {
		return false
	}

	// overdrive is maxed out
	if s.numInflight >= s.mgr.maxOverdrive+uint64(s.minShards) {
		return false
	}

	s.lastOverdrive = time.Now()
	return true
}

func (s *slabDownload) nextRequest(ctx context.Context, responseChan chan sectorDownloadResp, overdrive bool) *sectorDownloadReq {
	s.mu.Lock()
	defer s.mu.Unlock()

	// prepare next sectors to download
	if len(s.hostToSectors[s.curr]) == 0 {
		// grab unused hosts
		var hosts []types.PublicKey
		for host := range s.hostToSectors {
			if _, used := s.used[host]; !used {
				hosts = append(hosts, host)
			}
		}

		// sort them and assign the current host
		s.mgr.sort(hosts)
		for _, host := range hosts {
			s.used[host] = struct{}{}
			s.curr = host
			break
		}

		// no more sectors to download
		if len(s.hostToSectors[s.curr]) == 0 {
			return nil
		}
	}

	// pop the next sector
	sector := s.hostToSectors[s.curr][0]
	s.hostToSectors[s.curr] = s.hostToSectors[s.curr][1:]

	// create the span
	sCtx, span := tracing.Tracer.Start(ctx, "sectorDownloadReq")
	span.SetAttributes(attribute.Stringer("hk", sector.Host))
	span.SetAttributes(attribute.Bool("overdrive", overdrive))
	span.SetAttributes(attribute.Int("sector", sector.index))

	// build the request
	return &sectorDownloadReq{
		ctx: sCtx,

		offset: s.offset,
		length: s.length,
		root:   sector.Root,
		hk:     sector.Host,

		overdrive:    overdrive,
		sectorIndex:  sector.index,
		responseChan: responseChan,
	}
}

func (s *slabDownload) downloadShards(ctx context.Context, nextSlabTrigger chan struct{}) ([][]byte, error) {
	// cancel any sector downloads once the download is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "downloadShards")
	defer span.End()

	// create the response channel
	respChan := make(chan sectorDownloadResp)

	// create a timer to trigger overdrive
	timeout := time.NewTimer(s.mgr.overdriveTimeout)
	resetTimeout := func() {
		timeout.Stop()
		select {
		case <-timeout.C:
		default:
		}
		timeout.Reset(s.mgr.overdriveTimeout)
	}

	// launch a goroutine to trigger overdrive
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timeout.C:
				if s.overdrive() {
					_ = s.launch(s.nextRequest(ctx, respChan, true)) // ignore error
				}
				resetTimeout()
			}
		}
	}()

	// launch 'MinShard' requests
	for i := 0; i < int(s.minShards); i++ {
		if err := s.launch(s.nextRequest(ctx, respChan, false)); err != nil {
			return nil, errors.New("no hosts available")
		}
	}

	// collect responses
	var done bool
	var next bool
	var triggered bool
	for s.inflight() > 0 && !done {
		var resp sectorDownloadResp
		select {
		case <-s.mgr.stopChan:
			return nil, errors.New("download stopped")
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp = <-respChan:
			if resp.err == nil {
				resetTimeout()
			}
		}

		done, next = s.receive(resp)
		if !done && resp.err != nil {
			_ = s.launch(s.nextRequest(ctx, respChan, true)) // ignore error
		}

		if !triggered && (done || (next && s.mgr.ongoingDownloads() < maxOngoingSlabDownloads)) {
			select {
			case nextSlabTrigger <- struct{}{}:
				triggered = true
			default:
			}
		}
	}

	// track stats
	s.mgr.statsOverdrivePct.Track(s.overdrivePct())
	s.mgr.statsSlabDownloadSpeedBytesPerMS.Track(float64(s.downloadSpeed()))
	return s.finish()
}

func (s *slabDownload) overdrivePct() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	numOverdrive := int(s.numLaunched) - s.minShards
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
	return int64(bytes) / ms
}

func (s *slabDownload) finish() ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.numCompleted < s.minShards {
		err := fmt.Errorf("failed to download slab: completed=%d, inflight=%d, launched=%d downloaders=%d errors=%w", s.numCompleted, s.numInflight, s.numLaunched, s.mgr.numDownloaders(), s.errs)
		fmt.Println("DEBUG PJ: download failed err", err)
		return nil, err
	}
	return s.sectors, nil
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
	s.numLaunched++
	if req.overdrive {
		s.lastOverdrive = time.Now()
	}

	return nil
}

func (s *slabDownload) receive(resp sectorDownloadResp) (finished bool, next bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// failed reqs can't complete the upload
	s.numInflight--
	if resp.err != nil {
		s.errs = append(s.errs, &HostError{resp.hk, resp.err})
		return false, false
	}

	// store the sector
	s.sectors[resp.sectorIndex] = resp.sector
	s.numCompleted++

	return s.numCompleted >= s.minShards, s.numCompleted+int(s.mgr.maxOverdrive) >= s.minShards
}

func (mgr *downloadManager) sort(hosts []types.PublicKey) {
	// recompute stats
	mgr.tryRecomputeStats()

	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// sort the hosts fastest to slowest
	sort.Slice(hosts, func(i, j int) bool {
		if dI, exists := mgr.downloaders[hosts[i]]; !exists {
			return false
		} else if dJ, exists := mgr.downloaders[hosts[j]]; !exists {
			return true
		} else {
			return dI.estimate() < dJ.estimate()
		}
	})
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
