package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/mux/v1"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"lukechampine.com/frand"
)

// TODO: make this configurable
const maxBatchSize = 4

type (
	downloadManager struct {
		hp hostProvider

		maxOverdrive     uint64
		overdriveTimeout time.Duration

		statsOverdrivePct                *dataPoints
		statsSlabDownloadSpeedBytesPerMS *dataPoints
		stopChan                         chan struct{}

		mu            sync.Mutex
		downloaders   map[types.PublicKey]*downloader
		lastRecompute time.Time
	}

	downloader struct {
		fcid       types.FileContractID
		hk         types.PublicKey
		siamuxAddr string

		statsSectorDownloadEstimateInMS    *dataPoints
		statsSectorDownloadSpeedBytesPerMS *dataPoints // keep track of this separately for stats (no decay is applied)
		signalNewDownload                  chan struct{}
		stopChan                           chan struct{}

		mu                  sync.Mutex
		consecutiveFailures uint64
		queue               []*sectorDownloadReq
	}

	download struct {
		mgr *downloadManager

		nextSlabTrigger chan struct{}

		mu      sync.Mutex
		ongoing []slabID
	}

	slabDownload struct {
		mgr *downloadManager

		sID       slabID
		created   time.Time
		hosts     map[types.PublicKey]int
		minShards int

		mu           sync.Mutex
		numInflight  uint64
		numLaunched  uint64
		numCompleted int

		used    map[types.PublicKey]struct{}
		sectors [][]byte

		lastOverdrive time.Time
		errs          HostErrorSet
	}

	slabDownloadResponse struct {
		shards [][]byte
		index  int
		err    error
	}

	sectorDownloadReq struct {
		sID      slabID
		ctx      context.Context
		download *slabDownload

		length uint32
		offset uint32
		root   types.Hash256
		hk     types.PublicKey

		overdrive    bool
		sectorIndex  int
		responseChan chan sectorDownloadResp

		// these fields are set by the downloader performing the download
		fcid       types.FileContractID
		siamuxAddr string
	}

	sectorDownloadResp struct {
		req    *sectorDownloadReq
		sector []byte
		err    error
	}

	downloadManagerStats struct {
		avgSlabDownloadSpeedMBPS float64
		avgOverdrivePct          float64
		healthyDownloaders       uint64
		numDownloaders           uint64
		downloadSpeedsMBPS       map[types.PublicKey]float64
	}
)

func (w *worker) initDownloadManager(maxOverdrive uint64, overdriveTimeout time.Duration) {
	if w.downloadManager != nil {
		panic("download manager already initialized") // developer error
	}

	w.downloadManager = newDownloadManager(w, maxOverdrive, overdriveTimeout)
}

func newDownloadManager(hp hostProvider, maxOverdrive uint64, overdriveTimeout time.Duration) *downloadManager {
	return &downloadManager{
		hp: hp,

		maxOverdrive:     maxOverdrive,
		overdriveTimeout: overdriveTimeout,

		statsOverdrivePct:                newDataPoints(0),
		statsSlabDownloadSpeedBytesPerMS: newDataPoints(0),

		stopChan: make(chan struct{}),

		downloaders: make(map[types.PublicKey]*downloader),
	}
}

func newDownloader(c api.ContractMetadata) *downloader {
	return &downloader{
		fcid:       c.ID,
		hk:         c.HostKey,
		siamuxAddr: c.SiamuxAddr,

		statsSectorDownloadEstimateInMS:    newDataPoints(statsDecayHalfTime),
		statsSectorDownloadSpeedBytesPerMS: newDataPoints(0), // no decay for exposed stats
		signalNewDownload:                  make(chan struct{}, 1),
		stopChan:                           make(chan struct{}),

		queue: make([]*sectorDownloadReq, 0),
	}
}

func (mgr *downloadManager) Download(ctx context.Context, w io.Writer, o object.Object, offset, length uint32, contracts []api.ContractMetadata) (err error) {
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

	// create the download
	d, err := mgr.newDownload(o, contracts)
	if err != nil {
		return err
	}

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
		case <-d.nextSlabTrigger:
			if slabIndex == len(slabs) {
				continue
			}
			if d.ongoingDownloads() >= 3 {
				continue
			}
			go d.downloadSlab(ctx, slabs[slabIndex], slabIndex, responseChan)
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
	for _, d := range mgr.downloaders {
		healthy, mbps := d.stats()
		speeds[d.hk] = mbps
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

func (mgr *downloadManager) newDownload(o object.Object, contracts []api.ContractMetadata) (*download, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// refresh the downloaders
	mgr.refreshDownloaders(contracts)

	// check if we have enough contracts
	if len(o.Slabs) > 1 {
		// turn the array into a map
		contractsMap := make(map[types.PublicKey]struct{})
		for _, c := range contracts {
			contractsMap[c.HostKey] = struct{}{}
		}

		// count all shards for which we have a contract
		var n uint8
		for _, shard := range o.Slabs[0].Shards {
			if _, exists := contractsMap[shard.Host]; exists {
				n++
			}
		}

		// assert we have at least 'MinShards' contracts
		if n < o.Slabs[0].MinShards {
			return nil, errNotEnoughContracts
		}
	}

	// trigger first slab download
	nextSlabTrigger := make(chan struct{}, 1)
	nextSlabTrigger <- struct{}{}

	return &download{
		mgr:             mgr,
		nextSlabTrigger: nextSlabTrigger,
	}, nil
}

func (mgr *downloadManager) numDownloaders() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return len(mgr.downloaders)
}

func (mgr *downloadManager) refreshDownloaders(contracts []api.ContractMetadata) {
	// build map
	m := make(map[types.PublicKey]api.ContractMetadata)
	for _, contract := range contracts {
		m[contract.HostKey] = contract
	}

	// delete downloaders that are not in the given contracts
	for h := range mgr.downloaders {
		if _, exists := m[h]; !exists {
			mgr.downloaders[h] = nil
			delete(mgr.downloaders, h)
		}
	}

	// add downloaders that are not in the manager yet
	for _, c := range contracts {
		if _, exists := mgr.downloaders[c.HostKey]; !exists {
			downloader := newDownloader(c)
			go downloader.processQueue(mgr.hp)
			mgr.downloaders[c.HostKey] = downloader
		}
	}
}

func (d *download) newSlabDownload(ctx context.Context, slice object.SlabSlice) (*slabDownload, func()) {
	// create slab id
	var sID slabID
	frand.Read(sID[:])

	// add slab to ongoing downloads
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ongoing = append(d.ongoing, sID)

	// prepare a function to remove it from the ongoing downloads
	finishFn := func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		for i, prev := range d.ongoing {
			if prev == sID {
				d.ongoing = append(d.ongoing[:i], d.ongoing[i+1:]...)
				break
			}
		}
	}

	// prepare hosts
	hosts := make(map[types.PublicKey]int)
	for sI, shard := range slice.Shards {
		hosts[shard.Host] = sI
	}

	// create slab download
	return &slabDownload{
		mgr: d.mgr,

		sID:       sID,
		created:   time.Now(),
		minShards: int(slice.MinShards),

		hosts:   hosts,
		used:    make(map[types.PublicKey]struct{}),
		sectors: make([][]byte, len(slice.Shards)),
	}, finishFn
}

func (d *download) ongoingDownloads() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.ongoing)
}

func (d *download) downloadSlab(ctx context.Context, slice object.SlabSlice, index int, responseChan chan *slabDownloadResponse) {
	// cancel any sector downloads once the slab is done.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "downloadSlab")
	defer span.End()

	// prepare the download
	slab, finishFn := d.newSlabDownload(ctx, slice)
	defer finishFn()

	// calculate the offset and length
	offset, length := slice.SectorRegion()

	// download shards
	resp := &slabDownloadResponse{index: index}
	resp.shards, resp.err = slab.downloadShards(ctx, slice.Shards, offset, length, d.nextSlabTrigger)

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
		case <-d.signalNewDownload:
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

			// fill next batch
			var batch []*sectorDownloadReq
			for len(batch) < maxBatchSize {
				req := d.pop()
				if req == nil {
					break
				}
				if req.done() {
					continue
				}
				batch = append(batch, req)
			}
			if len(batch) == 0 {
				continue outer
			}

			start := time.Now()
			var completed uint64
			var wg sync.WaitGroup
			wg.Add(len(batch))
			for _, req := range batch {
				go func(req *sectorDownloadReq) {
					defer wg.Done()

					// execute and handle the response
					sector, err := req.execute(hp)
					if err != nil {
						req.fail(err)
					} else {
						req.succeed(sector)
						atomic.AddUint64(&completed, 1)
					}

					// track the error, ignore gracefully closed streams and canceled overdrives
					isErrClosedStream := errors.Is(err, mux.ErrClosedStream)
					canceledOverdrive := req.done() && req.overdrive && err != nil
					if !canceledOverdrive && !isErrClosedStream {
						d.trackFailure(err)
					}
				}(req)
			}

			wg.Wait()

			if completed == uint64(len(batch)) {
				downloadedB := completed * rhpv2.SectorSize
				durationMS := time.Since(start).Milliseconds()
				durationAvg := durationMS / int64(completed)
				d.statsSectorDownloadSpeedBytesPerMS.Track(float64(downloadedB / uint64(durationMS)))
				d.statsSectorDownloadEstimateInMS.Track(float64(durationAvg))
			} else {
				d.statsSectorDownloadEstimateInMS.Track(float64(time.Hour.Milliseconds()))
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

	numSectors := float64(len(d.queue) + 1)
	return numSectors * estimateP90
}

func (d *downloader) enqueue(download *sectorDownloadReq) {
	// decorate req
	span := trace.SpanFromContext(download.ctx)
	span.SetAttributes(attribute.Stringer("hk", d.hk))
	span.AddEvent("enqueued")
	download.fcid = d.fcid
	download.siamuxAddr = d.siamuxAddr

	d.mu.Lock()
	defer d.mu.Unlock()

	// enqueue the job
	d.queue = append(d.queue, download)

	// signal there's work
	select {
	case d.signalNewDownload <- struct{}{}:
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
	} else {
		d.consecutiveFailures = 0
	}
}

func (download *sectorDownloadReq) execute(hp hostProvider) (_ []byte, err error) {
	// add tracing
	start := time.Now()
	span := trace.SpanFromContext(download.ctx)
	span.AddEvent("execute")
	defer func() {
		elapsed := time.Since(start)
		span.SetAttributes(attribute.Int64("duration", elapsed.Milliseconds()))
		span.RecordError(err)
		span.End()
	}()

	// fetch the host
	h, err := hp.newHostV3(download.ctx, download.fcid, download.hk, download.siamuxAddr)
	if err != nil {
		return nil, err
	}

	// download the sector
	buf := bytes.NewBuffer(make([]byte, 0, rhpv2.SectorSize))
	err = h.DownloadSector(download.ctx, buf, download.root, download.offset, download.length)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (download *sectorDownloadReq) succeed(sector []byte) {
	select {
	case <-download.ctx.Done():
	case download.responseChan <- sectorDownloadResp{
		req:    download,
		sector: sector,
	}:
	}
}

func (download *sectorDownloadReq) fail(err error) {
	select {
	case <-download.ctx.Done():
	case download.responseChan <- sectorDownloadResp{
		req: download,
		err: err,
	}:
	}
}

func (download *sectorDownloadReq) done() bool {
	select {
	case <-download.ctx.Done():
		return true
	default:
		return false
	}
}

func (s *slabDownload) overdrive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	remaining := s.minShards - s.numCompleted
	if remaining < 0 {
		remaining = 0
	}

	// overdrive is not kicking in yet
	if uint64(remaining) >= s.mgr.maxOverdrive {
		return false
	}

	// overdrive is not due yet
	if time.Since(s.lastOverdrive) < s.mgr.overdriveTimeout {
		return false
	}

	// overdrive is maxed out
	if s.numInflight-uint64(remaining) >= s.mgr.maxOverdrive {
		return false
	}

	s.lastOverdrive = time.Now()
	return true
}

func (s *slabDownload) nextHost(hosts []types.PublicKey) types.PublicKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, host := range hosts {
		if _, used := s.used[host]; used {
			continue
		}
		return host
	}
	return types.PublicKey{}
}

func (s *slabDownload) downloadShards(ctx context.Context, shards []object.Sector, offset, length uint32, nextSlabTrigger chan struct{}) ([][]byte, error) {
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

	// build a map of host to shard
	h2shard := make(map[types.PublicKey]object.Sector)
	for _, s := range shards {
		h2shard[s.Host] = s
	}

	// build a list of hosts
	var hosts []types.PublicKey
	for hk := range s.hosts {
		hosts = append(hosts, hk)
	}

	// build a helper function that returns a download req for a given host
	buildRequest := func(hk types.PublicKey, overdrive bool) *sectorDownloadReq {
		if hk == (types.PublicKey{}) {
			return nil
		}
		return &sectorDownloadReq{
			sID:      s.sID,
			download: s,
			ctx:      ctx,

			offset: offset,
			length: length,
			root:   h2shard[hk].Root,
			hk:     h2shard[hk].Host,

			overdrive:    overdrive,
			sectorIndex:  s.hosts[hk],
			responseChan: respChan,
		}
	}

	// launch a goroutine to trigger overdrive
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timeout.C:
				if s.overdrive() {
					s.mgr.sort(hosts)
					_ = s.launch(buildRequest(s.nextHost(hosts), true)) // ignore error
				}
				resetTimeout()
			}
		}
	}()

	// launch 'MinShard' requests
	s.mgr.sort(hosts)
	for i := 0; i < int(s.minShards); i++ {
		if err := s.launch(buildRequest(s.nextHost(hosts), false)); err != nil {
			return nil, errors.New("no hosts available")
		}
	}

	// collect responses
	var done bool
	var next bool
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
		if next {
			select {
			case nextSlabTrigger <- struct{}{}:
			default:
			}
		}
		if !done && resp.err != nil {
			_ = s.launch(buildRequest(s.nextHost(hosts), true)) // ignore error
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
		return nil, fmt.Errorf("failed to download slab: completed=%d, inflight=%d, launched=%d downloaders=%d errors=%w", s.numCompleted, s.numInflight, s.numLaunched, s.mgr.numDownloaders(), s.errs)
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
	s.used[req.hk] = struct{}{}

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
		s.errs = append(s.errs, &HostError{resp.req.hk, resp.err})
		return false, false
	}

	// store the sector
	s.sectors[resp.req.sectorIndex] = resp.sector
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
