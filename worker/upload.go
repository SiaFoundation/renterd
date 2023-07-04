package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	statsDecayHalfTime        = 10 * time.Minute
	statsDecayThreshold       = 5 * time.Minute
	statsRecomputeMinInterval = 3 * time.Second
)

var (
	errNoFreeUploader     = errors.New("no free uploader")
	errNotEnoughContracts = errors.New("not enough contracts to support requested redundancy")
)

type (
	slabID [8]byte

	uploadManager struct {
		hp     hostProvider
		rl     revisionLocker
		logger *zap.SugaredLogger

		maxOverdrive     uint64
		overdriveTimeout time.Duration

		statsOverdrivePct              *dataPoints
		statsSlabUploadSpeedBytesPerMS *dataPoints
		stopChan                       chan struct{}

		mu            sync.Mutex
		uploaders     []*uploader
		lastRecompute time.Time
	}

	uploader struct {
		host       hostV3
		fcid       types.FileContractID
		hk         types.PublicKey
		siamuxAddr string
		endHeight  uint64

		statsSectorUploadEstimateInMS    *dataPoints
		statsSectorUploadSpeedBytesPerMS *dataPoints // keep track of this separately for stats (no decay is applied)
		signalNewUpload                  chan struct{}
		stopChan                         chan struct{}

		mu                  sync.Mutex
		bh                  uint64
		consecutiveFailures uint64
		queue               []*sectorUploadReq
	}

	upload struct {
		mgr *uploadManager

		allowed          map[types.FileContractID]struct{}
		nextSlabTrigger  chan struct{}
		doneShardTrigger chan struct{}

		mu      sync.Mutex
		ongoing []slabID
		used    map[slabID]map[types.FileContractID]struct{}
	}

	slabUpload struct {
		mgr    *uploadManager
		upload *upload

		sID     slabID
		created time.Time
		shards  [][]byte

		mu          sync.Mutex
		numInflight uint64
		numLaunched uint64

		lastOverdrive time.Time
		overdriving   map[int]int
		remaining     map[int]sectorCtx
		sectors       []object.Sector
		errs          HostErrorSet
	}

	slabUploadResponse struct {
		slab  object.SlabSlice
		index int
		err   error
	}

	sectorCtx struct {
		ctx    context.Context
		cancel context.CancelFunc
	}

	sectorUploadReq struct {
		upload *upload

		sID slabID
		ctx context.Context

		overdrive    bool
		sector       *[rhpv2.SectorSize]byte
		sectorIndex  int
		responseChan chan sectorUploadResp

		// these fields are set by the uploader performing the upload
		fcid       types.FileContractID
		hk         types.PublicKey
		siamuxAddr string
	}

	sectorUploadResp struct {
		req  *sectorUploadReq
		root types.Hash256
		err  error
	}

	uploadManagerStats struct {
		avgSlabUploadSpeedMBPS float64
		avgOverdrivePct        float64
		healthyUploaders       uint64
		numUploaders           uint64
		uploadSpeedsMBPS       map[types.PublicKey]float64
	}

	dataPoints struct {
		stats.Float64Data
		halfLife time.Duration
		size     int

		mu            sync.Mutex
		cnt           int
		p90           float64
		lastDatapoint time.Time
		lastDecay     time.Time
	}
)

func (w *worker) initUploadManager(maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.SugaredLogger) {
	if w.uploadManager != nil {
		panic("upload manager already initialized") // developer error
	}

	w.uploadManager = newUploadManager(w, w, maxOverdrive, overdriveTimeout, logger)
}

func newDataPoints(halfLife time.Duration) *dataPoints {
	return &dataPoints{
		size:        20,
		Float64Data: make([]float64, 0),
		halfLife:    halfLife,
		lastDecay:   time.Now(),
	}
}

func newUploadManager(hp hostProvider, rl revisionLocker, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.SugaredLogger) *uploadManager {
	return &uploadManager{
		hp:     hp,
		rl:     rl,
		logger: logger,

		maxOverdrive:     maxOverdrive,
		overdriveTimeout: overdriveTimeout,

		statsOverdrivePct:              newDataPoints(0),
		statsSlabUploadSpeedBytesPerMS: newDataPoints(0),

		stopChan: make(chan struct{}),

		uploaders: make([]*uploader, 0),
	}
}

func newUploader(c api.ContractMetadata, h hostV3) *uploader {
	return &uploader{
		host: h,

		fcid:       c.ID,
		hk:         c.HostKey,
		siamuxAddr: c.SiamuxAddr,
		endHeight:  c.WindowEnd,

		queue:           make([]*sectorUploadReq, 0),
		signalNewUpload: make(chan struct{}, 1),

		statsSectorUploadEstimateInMS:    newDataPoints(statsDecayHalfTime),
		statsSectorUploadSpeedBytesPerMS: newDataPoints(0), // no decay for exposed stats
		stopChan:                         make(chan struct{}),
	}
}

func (mgr *uploadManager) Migrate(ctx context.Context, shards [][]byte, contracts []api.ContractMetadata, bh uint64) ([]object.Sector, error) {
	// initiate the upload
	upload, err := mgr.newUpload(len(shards), contracts, bh)
	if err != nil {
		return nil, err
	}

	// upload the shards
	return upload.uploadShards(ctx, shards)
}

func (mgr *uploadManager) Stats() uploadManagerStats {
	// recompute stats
	mgr.tryRecomputeStats()

	// collect stats
	mgr.mu.Lock()
	var numHealthy uint64
	speeds := make(map[types.PublicKey]float64)
	for _, u := range mgr.uploaders {
		healthy, mbps := u.stats()
		speeds[u.hk] = mbps
		if healthy {
			numHealthy++
		}
	}
	mgr.mu.Unlock()

	// prepare stats
	return uploadManagerStats{
		avgSlabUploadSpeedMBPS: mgr.statsSlabUploadSpeedBytesPerMS.Average() * 0.008, // convert bytes per ms to mbps,
		avgOverdrivePct:        mgr.statsOverdrivePct.Average(),
		healthyUploaders:       numHealthy,
		numUploaders:           uint64(len(speeds)),
		uploadSpeedsMBPS:       speeds,
	}
}

func (mgr *uploadManager) Stop() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	close(mgr.stopChan)
	for _, u := range mgr.uploaders {
		close(u.stopChan)
	}
}

func (mgr *uploadManager) Upload(ctx context.Context, r io.Reader, rs api.RedundancySettings, contracts []api.ContractMetadata, bh uint64) (_ object.Object, err error) {
	// cancel all in-flight requests when the upload is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "upload")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	// create the object
	o := object.NewObject()

	// create the cipher reader
	cr := o.Encrypt(r)

	// create the upload
	u, err := mgr.newUpload(rs.TotalShards, contracts, bh)
	if err != nil {
		return object.Object{}, err
	}

	// create the response channel
	respChan := make(chan slabUploadResponse)

	// collect the responses
	var responses []slabUploadResponse
	var slabIndex int
	numSlabs := -1

	// prepare slab size
	size := int64(rs.MinShards) * rhpv2.SectorSize
loop:
	for {
		select {
		case <-mgr.stopChan:
			return object.Object{}, errors.New("manager was stopped")
		case <-ctx.Done():
			return object.Object{}, errors.New("upload timed out")
		case <-u.nextSlabTrigger:
			// read next slab's data
			data := make([]byte, size)
			length, err := io.ReadFull(io.LimitReader(cr, size), data)
			if err == io.EOF {
				if slabIndex == 0 {
					break loop
				}
				numSlabs = slabIndex
				continue
			} else if err != nil && err != io.ErrUnexpectedEOF {
				return object.Object{}, err
			}
			go u.uploadSlab(ctx, rs, data, length, slabIndex, respChan)
			slabIndex++
		case res := <-respChan:
			if res.err != nil {
				return object.Object{}, res.err
			}
			responses = append(responses, res)
			if len(responses) == numSlabs {
				close(respChan)
				break loop
			}
		}
	}

	// sort the slabs by index
	sort.Slice(responses, func(i, j int) bool {
		return responses[i].index < responses[j].index
	})

	// decorate the object with the slabs
	for _, resp := range responses {
		o.Slabs = append(o.Slabs, resp.slab)
	}
	return o, nil
}

func (mgr *uploadManager) launch(s *sectorUploadReq) error {
	// recompute stats
	mgr.tryRecomputeStats()

	// find a free uploader
	uploader := mgr.uploader(s)
	if uploader == nil {
		return errNoFreeUploader
	}
	uploader.enqueue(s)

	s.upload.registerUsedUploader(s.sID, uploader.fcid)
	return nil
}

func (mgr *uploadManager) newUpload(totalShards int, contracts []api.ContractMetadata, bh uint64) (*upload, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// refresh the uploaders
	mgr.refreshUploaders(contracts, bh)

	// check if we have enough contracts
	if len(contracts) < totalShards {
		return nil, errNotEnoughContracts
	}

	// create allowed map
	allowed := make(map[types.FileContractID]struct{})
	for _, c := range contracts {
		allowed[c.ID] = struct{}{}
	}

	// create upload
	upload := &upload{
		mgr: mgr,

		allowed:          allowed,
		nextSlabTrigger:  make(chan struct{}, 1),
		doneShardTrigger: make(chan struct{}, 1),

		ongoing: make([]slabID, 0),
		used:    make(map[slabID]map[types.FileContractID]struct{}),
	}
	upload.nextSlabTrigger <- struct{}{} // trigger first read
	return upload, nil
}

func (mgr *uploadManager) numUploaders() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return len(mgr.uploaders)
}

func (mgr *uploadManager) uploader(req *sectorUploadReq) *uploader {
	// fetch candidates
	candidates := func() []*uploader {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()

		// sort the uploaders by their estimate
		sort.Slice(mgr.uploaders, func(i, j int) bool {
			return mgr.uploaders[i].estimate() < mgr.uploaders[j].estimate()
		})

		// select top ten candidates
		var candidates []*uploader
		for _, uploader := range mgr.uploaders {
			if req.upload.canUseUploader(uploader, req.sID) {
				candidates = append(candidates, uploader)
				if len(candidates) == 10 {
					break
				}
			}
		}
		return candidates
	}()

	// return early if we have no queues left
	if len(candidates) == 0 {
		return nil
	}

loop:
	for {
		// if this slab does not have more than 1 parent, we return the best
		// candidate
		if len(req.upload.parents(req.sID)) <= 1 {
			return candidates[0]
		}

		// otherwise we wait, allowing the parents to complete, after which we
		// re-sort the candidates
		select {
		case <-req.upload.doneShardTrigger:
			sort.Slice(candidates, func(i, j int) bool {
				return candidates[i].estimate() < candidates[j].estimate()
			})
			continue loop
		case <-req.ctx.Done():
			break loop
		}
	}

	return nil
}

func (mgr *uploadManager) refreshUploaders(contracts []api.ContractMetadata, bh uint64) {
	// build map
	c2m := make(map[types.FileContractID]api.ContractMetadata)
	for _, c := range contracts {
		c2m[c.ID] = c
	}

	// purge the expired uploaders
	var i int
	for _, uploader := range mgr.uploaders {
		if bh > uploader.endHeight {
			continue
		}
		delete(c2m, uploader.fcid)
		mgr.uploaders[i] = uploader
		i++
	}
	for j := i; j < len(mgr.uploaders); j++ {
		close(mgr.uploaders[j].stopChan)
		mgr.uploaders[j] = nil
	}
	mgr.uploaders = mgr.uploaders[:i]

	// add missing uploaders
	for _, c := range c2m {
		// create uploader
		h := mgr.hp.newHostV3(c.ID, c.HostKey, c.SiamuxAddr)
		uploader := newUploader(c, h)
		mgr.uploaders = append(mgr.uploaders, uploader)
		go uploader.start(mgr.hp, mgr.rl)
	}

	// update blockheight
	for _, u := range mgr.uploaders {
		u.updateBlockHeight(bh)
	}
}

func (mgr *uploadManager) tryRecomputeStats() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if time.Since(mgr.lastRecompute) < statsRecomputeMinInterval {
		return
	}

	for _, u := range mgr.uploaders {
		u.statsSectorUploadEstimateInMS.Recompute()
		u.statsSectorUploadSpeedBytesPerMS.Recompute()
	}
	mgr.lastRecompute = time.Now()
}

func (u *upload) parents(sID slabID) []slabID {
	u.mu.Lock()
	defer u.mu.Unlock()

	var parents []slabID
	for _, ongoing := range u.ongoing {
		if ongoing == sID {
			break
		}
		parents = append(parents, ongoing)
	}
	return parents
}

func (u *upload) finishSlabUpload(upload *slabUpload) {
	// update ongoing slab history
	u.mu.Lock()
	for i, prev := range u.ongoing {
		if prev == upload.sID {
			u.ongoing = append(u.ongoing[:i], u.ongoing[i+1:]...)
			break
		}
	}
	u.mu.Unlock()

	// cleanup contexts
	upload.mu.Lock()
	for _, shard := range upload.remaining {
		shard.cancel()
	}
	upload.mu.Unlock()
}

func (u *upload) newSlabUpload(ctx context.Context, shards [][]byte) (*slabUpload, []*sectorUploadReq, chan sectorUploadResp) {
	// create slab id
	var sID slabID
	frand.Read(sID[:])

	// add to ongoing uploads
	u.mu.Lock()
	u.ongoing = append(u.ongoing, sID)
	u.mu.Unlock()

	// create slab upload
	slab := &slabUpload{
		mgr: u.mgr,

		upload:  u,
		sID:     sID,
		created: time.Now(),
		shards:  shards,

		overdriving: make(map[int]int, len(shards)),
		remaining:   make(map[int]sectorCtx, len(shards)),
		sectors:     make([]object.Sector, len(shards)),
	}

	// prepare sector uploads
	responseChan := make(chan sectorUploadResp)
	requests := make([]*sectorUploadReq, len(shards))
	for sI, shard := range shards {
		// create the sector upload's cancel func
		sCtx, cancel := context.WithCancel(ctx)
		slab.remaining[sI] = sectorCtx{ctx: sCtx, cancel: cancel}

		// create the upload's span
		sCtx, span := tracing.Tracer.Start(sCtx, "uploadSector")
		span.SetAttributes(attribute.Bool("overdrive", false))
		span.SetAttributes(attribute.Int("sector", sI))

		// create the sector upload
		requests[sI] = &sectorUploadReq{
			upload: u,
			sID:    sID,
			ctx:    sCtx,

			sector:       (*[rhpv2.SectorSize]byte)(shard),
			sectorIndex:  sI,
			responseChan: responseChan,
		}
	}

	return slab, requests, responseChan
}

func (u *upload) canUseUploader(ul *uploader, sID slabID) bool {
	u.mu.Lock()
	defer u.mu.Unlock()

	_, allowed := u.allowed[ul.fcid]
	if !allowed {
		return false
	}

	_, used := u.used[sID][ul.fcid]
	return !used
}

func (u *upload) uploadSlab(ctx context.Context, rs api.RedundancySettings, data []byte, length, index int, respChan chan slabUploadResponse) {
	// cancel any sector uploads once the slab is done.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "uploadSlab")
	defer span.End()

	// create the response
	resp := slabUploadResponse{
		slab: object.SlabSlice{
			Slab:   object.NewSlab(uint8(rs.MinShards)),
			Offset: 0,
			Length: uint32(length),
		},
		index: index,
	}

	// create the shards
	shards := make([][]byte, rs.TotalShards)
	resp.slab.Slab.Encode(data, shards)
	resp.slab.Slab.Encrypt(shards)

	// upload the shards
	resp.slab.Slab.Shards, resp.err = u.uploadShards(ctx, shards)

	// send the response
	select {
	case <-ctx.Done():
	case respChan <- resp:
	}
}

func (u *upload) registerUsedUploader(sID slabID, fcid types.FileContractID) {
	u.mu.Lock()
	defer u.mu.Unlock()

	_, exists := u.used[sID]
	if !exists {
		u.used[sID] = make(map[types.FileContractID]struct{})
	}
	u.used[sID][fcid] = struct{}{}
}

func (u *upload) uploadShards(ctx context.Context, shards [][]byte) ([]object.Sector, error) {
	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "uploadShards")
	defer span.End()

	// prepare the upload
	slab, requests, respChan := u.newSlabUpload(ctx, shards)
	span.SetAttributes(attribute.Stringer("id", slab.sID))
	defer u.finishSlabUpload(slab)

	// launch all shard uploads
	for _, upload := range requests {
		if err := slab.launch(upload); err != nil {
			return nil, err
		}
	}

	// launch overdrive
	resetOverdrive := slab.overdrive(ctx, respChan)

	// collect responses
	var done bool
	var next bool
	var triggered bool
	for slab.inflight() > 0 && !done {
		var resp sectorUploadResp
		select {
		case <-u.mgr.stopChan:
			return nil, errors.New("upload stopped")
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp = <-respChan:
		}

		resetOverdrive()

		// receive the response
		done, next = slab.receive(resp)

		// try and trigger next slab
		if next && !triggered {
			select {
			case u.nextSlabTrigger <- struct{}{}:
				triggered = true
			default:
			}
		}

		// relaunch non-overdrive uploads
		if !done && resp.err != nil && !resp.req.overdrive {
			if err := slab.launch(resp.req); err != nil {
				break // fail the download
			}
		}

		// handle the response
		if resp.err == nil {
			// signal the upload a shard was received
			select {
			case u.doneShardTrigger <- struct{}{}:
			default:
			}
		}
	}

	// make sure next slab is triggered
	if done && !triggered {
		select {
		case u.nextSlabTrigger <- struct{}{}:
		default:
		}
	}

	// register the amount of overdrive sectors
	span.SetAttributes(attribute.Int("overdrive", slab.overdriveCnt()))

	// track stats
	u.mgr.statsOverdrivePct.Track(slab.overdrivePct())
	u.mgr.statsSlabUploadSpeedBytesPerMS.Track(float64(slab.uploadSpeed()))
	return slab.finish()
}

func (u *uploader) execute(req *sectorUploadReq, rev types.FileContractRevision) (types.Hash256, error) {
	// fetch span from context
	span := trace.SpanFromContext(req.ctx)
	span.AddEvent("execute")

	// upload the sector
	start := time.Now()
	root, err := u.host.UploadSector(req.ctx, req.sector, rev)
	if err != nil {
		return types.Hash256{}, err
	}

	// update span
	elapsed := time.Since(start)
	span.SetAttributes(attribute.Int64("duration", elapsed.Milliseconds()))
	span.RecordError(err)
	span.End()
	return root, nil
}
func (u *uploader) start(hp hostProvider, rl revisionLocker) {
outer:
	for {
		// wait for work
		select {
		case <-u.signalNewUpload:
		case <-u.stopChan:
			return
		}

		for {
			// check if we are stopped
			select {
			case <-u.stopChan:
				return
			default:
			}

			// pop the next upload
			upload := u.pop()
			if upload == nil {
				continue outer
			}

			// skip if upload is done
			if upload.done() {
				continue
			}

			// execute it
			var root types.Hash256
			start := time.Now()
			err := rl.withRevision(upload.ctx, defaultRevisionFetchTimeout, u.fcid, u.hk, u.siamuxAddr, lockingPriorityUpload, u.blockHeight(), func(rev types.FileContractRevision) error {
				var err error
				root, err = u.execute(upload, rev)
				return err
			})

			// send the response
			if err != nil {
				upload.fail(err)
			} else {
				upload.succeed(root)
			}

			// track the error, ignore gracefully closed streams and canceled overdrives
			canceledOverdrive := upload.done() && upload.overdrive && err != nil
			if !canceledOverdrive && !isClosedStream(err) {
				u.trackSectorUpload(err, time.Since(start))
			}
		}
	}
}

func (u *uploader) blockHeight() uint64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.bh
}

func (u *uploader) estimate() float64 {
	u.mu.Lock()
	defer u.mu.Unlock()

	// fetch estimated duration per sector
	estimateP90 := u.statsSectorUploadEstimateInMS.P90()
	if estimateP90 == 0 {
		estimateP90 = 1
	}

	// calculate estimated time
	numSectors := float64(len(u.queue) + 1)
	return numSectors * estimateP90
}

func (u *uploader) stats() (healthy bool, mbps float64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	healthy = u.consecutiveFailures == 0
	mbps = u.statsSectorUploadSpeedBytesPerMS.Average() * 0.008
	return
}

func (u *uploader) enqueue(upload *sectorUploadReq) {
	// decorate req
	span := trace.SpanFromContext(upload.ctx)
	span.SetAttributes(attribute.Stringer("hk", u.hk))
	span.AddEvent("enqueued")
	upload.fcid = u.fcid
	upload.hk = u.hk
	upload.siamuxAddr = u.siamuxAddr

	u.mu.Lock()
	defer u.mu.Unlock()

	// enqueue the job
	u.queue = append(u.queue, upload)

	// signal there's work
	select {
	case u.signalNewUpload <- struct{}{}:
	default:
	}
}

func (u *uploader) trackSectorUpload(err error, d time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err != nil {
		u.consecutiveFailures++
		u.statsSectorUploadEstimateInMS.Track(float64(time.Hour.Milliseconds()))
	} else {
		ms := d.Milliseconds()
		u.consecutiveFailures = 0
		u.statsSectorUploadEstimateInMS.Track(float64(ms))                       // duration in ms
		u.statsSectorUploadSpeedBytesPerMS.Track(float64(rhpv2.SectorSize / ms)) // bytes per ms
	}
}

func (u *uploader) updateBlockHeight(bh uint64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.bh = bh
}

func (u *uploader) pop() *sectorUploadReq {
	u.mu.Lock()
	defer u.mu.Unlock()

	if len(u.queue) > 0 {
		j := u.queue[0]
		u.queue[0] = nil
		u.queue = u.queue[1:]
		return j
	}
	return nil
}

func (upload *sectorUploadReq) succeed(root types.Hash256) {
	select {
	case <-upload.ctx.Done():
	case upload.responseChan <- sectorUploadResp{
		req:  upload,
		root: root,
	}:
	}
}

func (upload *sectorUploadReq) fail(err error) {
	select {
	case <-upload.ctx.Done():
	case upload.responseChan <- sectorUploadResp{
		req: upload,
		err: err,
	}:
	}
}

func (upload *sectorUploadReq) done() bool {
	select {
	case <-upload.ctx.Done():
		return true
	default:
		return false
	}
}

func (s *slabUpload) uploadSpeed() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	totalShards := len(s.sectors)
	completedShards := totalShards - len(s.remaining)
	bytes := completedShards * rhpv2.SectorSize
	ms := time.Since(s.created).Milliseconds()
	return int64(bytes) / ms
}

func (s *slabUpload) finish() ([]object.Sector, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	remaining := len(s.remaining)
	if remaining > 0 {
		return nil, fmt.Errorf("failed to upload slab: remaining=%d, inflight=%d, launched=%d uploaders=%d errors=%w", remaining, s.numInflight, s.numLaunched, s.mgr.numUploaders(), s.errs)
	}
	return s.sectors, nil
}

func (s *slabUpload) inflight() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numInflight
}

func (s *slabUpload) launch(req *sectorUploadReq) error {
	s.mu.Lock()
	defer s.mu.Unlock()

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
		s.overdriving[req.sectorIndex]++
	}

	return nil
}

func (s *slabUpload) overdrive(ctx context.Context, respChan chan sectorUploadResp) (resetTimer func()) {
	// overdrive is disabled
	if s.mgr.overdriveTimeout == 0 {
		return func() {}
	}

	// create a timer to trigger overdrive
	timer := time.NewTimer(s.mgr.overdriveTimeout)
	resetTimer = func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
		timer.Reset(s.mgr.overdriveTimeout)
	}

	// create a function to check whether overdrive is possible
	canOverdrive := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		// overdrive is not kicking in yet
		if uint64(len(s.remaining)) >= s.mgr.maxOverdrive {
			return false
		}

		// overdrive is not due yet
		if time.Since(s.lastOverdrive) < s.mgr.overdriveTimeout {
			return false
		}

		// overdrive is maxed out
		if s.numInflight-uint64(len(s.remaining)) >= s.mgr.maxOverdrive {
			return false
		}

		return true
	}

	// try overdriving every time the timer fires
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if canOverdrive() {
					req := s.nextRequest(respChan)
					if req != nil {
						_ = s.launch(req) // ignore error
					}
				}
				resetTimer()
			}
		}
	}()

	return
}

func (s *slabUpload) nextRequest(responseChan chan sectorUploadResp) *sectorUploadReq {
	s.mu.Lock()
	defer s.mu.Unlock()

	// overdrive the remaining sector with the least number of overdrives
	lowestSI := -1
	s.overdriving[lowestSI] = math.MaxInt
	for sI := range s.remaining {
		if s.overdriving[sI] < s.overdriving[lowestSI] {
			lowestSI = sI
		}
	}
	if lowestSI == -1 {
		return nil
	}

	return &sectorUploadReq{
		upload: s.upload,
		sID:    s.sID,
		ctx:    s.remaining[lowestSI].ctx,

		overdrive:    true,
		responseChan: responseChan,

		sectorIndex: lowestSI,
		sector:      (*[rhpv2.SectorSize]byte)(s.shards[lowestSI]),
	}
}

func (s *slabUpload) overdriveCnt() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.numLaunched) - len(s.sectors)
}

func (s *slabUpload) overdrivePct() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	numOverdrive := int(s.numLaunched) - len(s.sectors)
	if numOverdrive <= 0 {
		return 0
	}

	return float64(numOverdrive) / float64(len(s.sectors))
}

func (s *slabUpload) receive(resp sectorUploadResp) (finished bool, next bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// failed reqs can't complete the upload
	s.numInflight--
	if resp.err != nil {
		s.errs = append(s.errs, &HostError{resp.req.hk, resp.err})
		return false, false
	}

	// redundant sectors can't complete the upload
	if s.sectors[resp.req.sectorIndex].Root != (types.Hash256{}) {
		return false, false
	}

	// store the sector and call cancel on the sector ctx
	s.sectors[resp.req.sectorIndex] = object.Sector{
		Host: resp.req.hk,
		Root: resp.root,
	}
	s.remaining[resp.req.sectorIndex].cancel()

	// update remaining sectors
	delete(s.remaining, resp.req.sectorIndex)
	finished = len(s.remaining) == 0
	next = len(s.remaining) <= int(s.mgr.maxOverdrive)
	return
}

func (a *dataPoints) Average() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	avg, err := a.Mean()
	if err != nil {
		avg = 0
	}
	return avg
}

func (a *dataPoints) P90() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.p90
}

func (a *dataPoints) Recompute() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// apply decay
	a.tryDecay()

	// recalculate the p90
	p90, err := a.Percentile(90)
	if err != nil {
		p90 = 0
	}
	a.p90 = p90
}

func (a *dataPoints) Track(p float64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.cnt < a.size {
		a.Float64Data = append(a.Float64Data, p)
	} else {
		a.Float64Data[a.cnt%a.size] = p
	}

	a.lastDatapoint = time.Now()
	a.cnt++
}

func (a *dataPoints) tryDecay() {
	// return if decay is disabled
	if a.halfLife == 0 {
		return
	}

	// return if decay is not needed
	if time.Since(a.lastDatapoint) < statsDecayThreshold {
		return
	}

	// return if decay is not due
	decayFreq := a.halfLife / 5
	timePassed := time.Since(a.lastDecay)
	if timePassed < decayFreq {
		return
	}

	// calculate decay and apply it
	strength := float64(timePassed) / float64(a.halfLife)
	decay := math.Floor(math.Pow(0.5, strength)*100) / 100 // round down to 2 decimals
	for i := range a.Float64Data {
		a.Float64Data[i] *= decay
	}

	// update the last decay time
	a.lastDecay = time.Now()
}

func (sID slabID) String() string {
	return fmt.Sprintf("%x", sID[:])
}
