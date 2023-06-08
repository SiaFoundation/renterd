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
	"lukechampine.com/frand"
)

type (
	downloadManager struct {
		hp hostProvider

		maxOverdrive     uint64
		overdriveTimeout time.Duration

		statsOverdrive *dataPoints
		statsSpeed     *dataPoints
		stopChan       chan struct{}

		mu               sync.Mutex
		downloaders      []*downloader
		downloadersIndex map[types.PublicKey]int
	}

	downloader struct {
		fcid       types.FileContractID
		hk         types.PublicKey
		siamuxAddr string

		statsSpeed        *dataPoints
		signalNewDownload chan struct{}
		stopChan          chan struct{}

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
		mgr      *downloadManager
		download *download

		sID       slabID
		created   time.Time
		minShards int

		mu          sync.Mutex
		numInflight uint64
		numLaunched uint64

		nextSlabTriggered bool
		lastOverdrive     time.Time
		sectors           [][]byte
		used              map[types.PublicKey]struct{}
		errs              HostErrorSet
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

	sectorInfo struct {
		object.Sector
		index int
	}

	downloadManagerStats struct {
		avgDownloadSpeedMBPS  float64
		healthyDownloaders    uint64
		numDownloaders        uint64
		overdrivePct          float64
		downloadSpeedsP90MBPS map[types.PublicKey]float64
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

		statsOverdrive: newDataPoints(),
		statsSpeed:     newDataPoints(),
		stopChan:       make(chan struct{}),

		downloaders:      make([]*downloader, 0),
		downloadersIndex: make(map[types.PublicKey]int),
	}
}

func newDownloader(c api.ContractMetadata) *downloader {
	return &downloader{
		fcid:       c.ID,
		hk:         c.HostKey,
		siamuxAddr: c.SiamuxAddr,

		statsSpeed:        newDataPoints(),
		signalNewDownload: make(chan struct{}, 1),
		stopChan:          make(chan struct{}),

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

	// calculate what slabs to download
	slabs := slabsForDownload(o.Slabs, offset, length)
	if len(slabs) == 0 {
		return nil
	}

	// create the cipher writer
	cw := o.Key.Decrypt(w, offset)

	fmt.Println("DEBUG PJ: creating download...")
	// create the download
	d, err := mgr.newDownload(o, contracts)
	if err != nil {
		return err
	}

	// create response channel
	responseChan := make(chan *slabDownloadResponse)
	var slabIndex int

	// collect responses
	responses := make(map[int]*slabDownloadResponse)
	var respIndex int

	fmt.Println("DEBUG PJ: starting download...")
loop:
	for {
		select {
		case <-mgr.stopChan:
			return errors.New("manager was stopped")
		case <-ctx.Done():
			return errors.New("download timed out")
		case <-d.nextSlabTrigger:
			if slabIndex < len(slabs) {
				fmt.Println("DEBUG PJ: downloading slab", slabIndex, len(slabs))
				go d.downloadSlab(ctx, slabs[slabIndex], slabIndex, responseChan)
				slabIndex++
			}
		case resp := <-responseChan:
			// receive the response
			if resp.err != nil {
				return resp.err
			}
			responses[resp.index] = resp

			fmt.Println("DEBUG PJ: received slab response")
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
					fmt.Println("DEBUG PJ: recover failed", err, len(next.shards))
					for _, s := range resp.shards {
						fmt.Println("DEBUGP PJ: shard length", len(s))
					}
					return err
				}

				next = nil
				delete(responses, respIndex)
				respIndex++
			}

			// exit condition
			if respIndex == len(slabs) {
				break loop
			}
		}
	}

	return nil
}
func (mgr *downloadManager) Stats() downloadManagerStats {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// prepare stats
	stats := downloadManagerStats{
		avgDownloadSpeedMBPS:  mgr.statsSpeed.recompute() * 0.008, // convert bytes per ms to mbps,
		overdrivePct:          mgr.statsOverdrive.recompute(),
		numDownloaders:        uint64(len(mgr.downloaders)),
		downloadSpeedsP90MBPS: make(map[types.PublicKey]float64),
	}

	// fill in download stats
	for _, d := range mgr.downloaders {
		d.statsSpeed.recompute()
		stats.downloadSpeedsP90MBPS[d.hk] = d.statsSpeed.percentileP90() * 0.008 // convert bytes per ms to mbps
		if d.healthy() {
			stats.healthyDownloaders++
		}
	}

	return stats
}
func (mgr *downloadManager) Stop() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	close(mgr.stopChan)
	for _, d := range mgr.downloaders {
		close(d.stopChan)
	}
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

	download := &download{
		mgr:             mgr,
		nextSlabTrigger: make(chan struct{}, 1),
	}
	download.nextSlabTrigger <- struct{}{} // trigger first download
	return download, nil
}

func (mgr *downloadManager) numDownloaders() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return len(mgr.downloaders)
}

func (mgr *downloadManager) refreshDownloaders(contracts []api.ContractMetadata) {
	// build map
	c2m := make(map[types.FileContractID]api.ContractMetadata)
	for _, c := range contracts {
		c2m[c.ID] = c
	}

	var i int
	for _, downloader := range mgr.downloaders {
		// throw out downloaders not in the given contracts
		_, keep := c2m[downloader.fcid]
		if !keep {
			continue
		}

		// delete it from the map to ensure we don't add it
		delete(c2m, downloader.fcid)
		mgr.downloaders[i] = downloader
		i++
	}
	for j := i; j < len(mgr.downloaders); j++ {
		mgr.downloaders[j] = nil
	}
	mgr.downloaders = mgr.downloaders[:i]

	// add missing downloaders
	for _, contract := range c2m {
		downloader := newDownloader(contract)
		mgr.downloaders = append(mgr.downloaders, downloader)
		go downloader.start(mgr.hp)
	}
}

func (d *download) finishSlabDownload(download *slabDownload) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i, prev := range d.ongoing {
		if prev == download.sID {
			d.ongoing = append(d.ongoing[:i], d.ongoing[i+1:]...)
			break
		}
	}
}

func (d *download) newSlabDownload(ctx context.Context, shards []object.Sector, minShards int) *slabDownload {
	// create slab id
	var sID slabID
	frand.Read(sID[:])

	// add to ongoing downloads
	d.mu.Lock()
	d.ongoing = append(d.ongoing, sID)
	d.mu.Unlock()

	// create slab download
	return &slabDownload{
		mgr:      d.mgr,
		download: d,

		sID:       sID,
		created:   time.Now(),
		minShards: minShards,

		used:    make(map[types.PublicKey]struct{}),
		sectors: make([][]byte, len(shards)),
	}
}

func (d *download) downloadSlab(ctx context.Context, slice object.SlabSlice, index int, responseChan chan *slabDownloadResponse) {
	fmt.Println("DEBUG PJ: downloading slab", index)
	// cancel any sector downloads once the slab is done.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "downloadSlab")
	defer span.End()

	// create the response
	resp := &slabDownloadResponse{index: index}
	resp.shards, resp.err = d.downloadShards(ctx, slice)
	fmt.Println("DEBUG PJ: download resp", len(resp.shards), resp.err)

	// send the response
	select {
	case <-ctx.Done():
	case responseChan <- resp:
	}
}

func (d *downloader) healthy() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.consecutiveFailures == 0
}

func (d *downloader) start(hp hostProvider) {
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

			// pop the next download
			download := d.pop()
			if download == nil {
				continue outer
			}
			fmt.Println("DEBUG PJ: processing download")

			// skip if download is done
			if download.done() {
				continue
			}

			// execute and handle the response
			start := time.Now()
			sector, err := download.execute(hp)
			fmt.Println("DEBUG PJ: download executed", len(sector), err)
			if err != nil {
				download.fail(err)
			} else {
				download.succeed(sector)
			}

			// track the error, ignore gracefully closed streams and canceled overdrives
			isErrClosedStream := errors.Is(err, mux.ErrClosedStream)
			canceledOverdrive := download.done() && download.overdrive && err != nil
			if !canceledOverdrive && !isErrClosedStream {
				d.track(err, time.Since(start))
			}
		}
	}
}

func (d *downloader) estimate() float64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	// fetch average speed
	bytesPerMS := d.statsSpeed.percentileP90()
	if bytesPerMS == 0 {
		bytesPerMS = math.MaxFloat64
	}

	outstanding := (len(d.queue) + 1) * rhpv2.SectorSize
	return float64(outstanding) / bytesPerMS
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
		fmt.Println("could not signal work")
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

func (d *downloader) track(err error, dur time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err != nil {
		d.consecutiveFailures++
		d.statsSpeed.track(1)
	} else {
		d.consecutiveFailures = 0
		d.statsSpeed.track(float64(rhpv2.SectorSize / dur.Milliseconds()))
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

	data := buf.Bytes()
	fmt.Println("DEBUG PJ: received sector data", data[:16])
	return data, nil
}

func (download *sectorDownloadReq) succeed(sector []byte) {
	fmt.Println("DEBUG PJ: trying to send sector resp")
	select {
	case <-download.ctx.Done():
	case download.responseChan <- sectorDownloadResp{
		req:    download,
		sector: sector,
	}:
	}
	fmt.Println("DEBUG PJ: sector resp sent")
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

func (d *download) downloadShards(ctx context.Context, slice object.SlabSlice) ([][]byte, error) {
	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "downloadShards")
	defer span.End()

	// prepare the download
	slab := d.newSlabDownload(ctx, slice.Shards, int(slice.MinShards))
	span.SetAttributes(attribute.Stringer("id", slab.sID))
	defer d.finishSlabDownload(slab)

	// calculate the offset and length
	offset, length := slice.SectorRegion()

	// create the response channel
	respChan := make(chan sectorDownloadResp)

	// create a timer to trigger overdrive
	timeout := time.NewTimer(d.mgr.overdriveTimeout)
	resetTimeout := func() {
		timeout.Stop()
		select {
		case <-timeout.C:
		default:
		}
		timeout.Reset(d.mgr.overdriveTimeout)
	}

	// build a sectors map
	hosts := make([]types.PublicKey, len(slice.Shards))
	sectors := make(map[types.PublicKey]sectorInfo)
	for i, shard := range slice.Shards {
		hosts[i] = shard.Host
		sectors[shard.Host] = sectorInfo{
			shard,
			i,
		}
	}

	// have the manager sort the hosts
	d.mgr.sort(hosts)

	// launch a goroutine to trigger overdrive
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timeout.C:
				if req := slab.overdrive(ctx, hosts, sectors, offset, length, respChan); req != nil {
					_ = slab.launch(req) // ignore error
				}
				resetTimeout()
			}
		}
	}()

	// launch 'MinShard' requests
	for i := 0; i < int(slice.MinShards); i++ {
		sector := sectors[hosts[i]]
		fmt.Println("DEBUG PJ: launch shard", i)
		if err := slab.launch(&sectorDownloadReq{
			sID:      slab.sID,
			download: slab,
			ctx:      ctx,

			offset: offset,
			length: length,
			root:   sector.Root,
			hk:     sector.Host,

			sectorIndex:  sector.index,
			responseChan: respChan,
		}); err != nil {
			return nil, err
		}
		fmt.Println("DEBUG PJ: launched shard", i)
	}
	fmt.Println("DEBUG PJ: all requests launched")

	// collect responses
	var finished bool
	for slab.inflight() > 0 && !finished {
		var resp sectorDownloadResp
		select {
		case <-d.mgr.stopChan:
			return nil, errors.New("download stopped")
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp = <-respChan:
		}
		fmt.Println("DEBUG PJ: received download resp err", resp.err)

		// receive the response
		finished = slab.receive(resp)

		// handle the response
		if resp.err == nil {
			resetTimeout()

			// try and trigger the next slab read
			slab.tryTriggerNextRead()
		}
	}

	// track stats
	d.mgr.statsOverdrive.track(slab.overdrivePct())
	d.mgr.statsSpeed.track(float64(slab.downloadSpeed()))
	return slab.finish()
}

func (s *slabDownload) overdrivePct() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	numOverdrive := int(s.numLaunched) - s.minShards
	if numOverdrive <= 0 {
		return 0
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

	// count whether we have 'MinShards' sectors
	var completed int
	for _, sector := range s.sectors {
		if len(sector) > 0 {
			completed++
		}
	}

	if completed < s.minShards {
		return nil, fmt.Errorf("failed to download slab: completed=%d, inflight=%d, launched=%d downloaders=%d errors=%w", completed, s.numInflight, s.numLaunched, s.mgr.numDownloaders(), s.errs)
	}
	return s.sectors, nil
}

func (s *slabDownload) inflight() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numInflight
}

func (s *slabDownload) overdrive(ctx context.Context, hosts []types.PublicKey, sectors map[types.PublicKey]sectorInfo, offset, length uint32, respChan chan sectorDownloadResp) *sectorDownloadReq {
	s.mgr.sort(hosts)

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, h := range hosts {
		_, used := s.used[h]
		if !used {
			sector := sectors[h]
			return &sectorDownloadReq{
				sID:      s.sID,
				download: s,
				ctx:      ctx,

				offset: offset,
				length: length,
				root:   sector.Root,
				hk:     sector.Host,

				sectorIndex:  sector.index,
				responseChan: respChan,
			}
		}
	}
	return nil
}

func (s *slabDownload) launch(req *sectorDownloadReq) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// launch the req
	hk, err := s.mgr.launch(req)
	if err != nil {
		span := trace.SpanFromContext(req.ctx)
		span.RecordError(err)
		span.End()
		return err
	}
	s.used[hk] = struct{}{}

	// update the state
	s.numInflight++
	s.numLaunched++
	if req.overdrive {
		s.lastOverdrive = time.Now()
	}

	return nil
}

func (s *slabDownload) receive(resp sectorDownloadResp) (finished bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// failed reqs can't complete the upload
	s.numInflight--
	if resp.err != nil {
		s.errs = append(s.errs, &HostError{resp.req.hk, resp.err})
		return false
	}

	// store the sector and call cancel on the sector ctx
	s.sectors[resp.req.sectorIndex] = resp.sector

	// count whether we have 'MinShards' sectors
	var received int
	for _, sector := range s.sectors {
		if len(sector) > 0 {
			received++
			if received >= int(s.minShards) {
				fmt.Println("DEBUG PJ: download complete", received, s.minShards)
				return true
			}
		}
	}
	return false
}

func (s *slabDownload) tryTriggerNextRead() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.nextSlabTriggered && len(s.sectors)+int(s.mgr.maxOverdrive) >= s.minShards {
		select {
		case s.download.nextSlabTrigger <- struct{}{}:
			s.nextSlabTriggered = true
		default:
		}
	}
}

func (mgr *downloadManager) sort(hosts []types.PublicKey) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// recompute the stats first
	for _, downloader := range mgr.downloaders {
		downloader.statsSpeed.recompute()
	}

	// sort the downloaders fastest to slowest
	sort.Slice(mgr.downloaders, func(i, j int) bool {
		return mgr.downloaders[i].estimate() < mgr.downloaders[j].estimate()
	})

	// build a map of host to 'speed index'
	indices := make(map[types.PublicKey]int)
	for i, downloader := range mgr.downloaders {
		indices[downloader.hk] = i
	}
	mgr.downloadersIndex = indices

	// sort the hosts so fastest hosts are first
	sort.Slice(hosts, func(i, j int) bool {
		indexI, exists := indices[hosts[i]]
		if !exists {
			return false
		}
		indexJ, exists := indices[hosts[j]]
		if !exists {
			return false
		}
		return indexI < indexJ
	})
}

func (mgr *downloadManager) launch(req *sectorDownloadReq) (types.PublicKey, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	index, exists := mgr.downloadersIndex[req.hk]
	if !exists {
		return types.PublicKey{}, errors.New("downloader not found")
	}
	downloader := mgr.downloaders[index]

	fmt.Println("DEBUG PJ: enqueueing req to downloader", downloader.hk)
	downloader.enqueue(req)
	return req.hk, nil
}
