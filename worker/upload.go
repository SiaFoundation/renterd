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
	"go.sia.tech/mux/v1"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"lukechampine.com/frand"
)

var (
	errNoFreeQueue    = errors.New("no free queue")
	errNotEnoughHosts = errors.New("not enough hosts to support requested redundancy")
)

type (
	uploadID [8]byte
	slabID   [8]byte

	uploadManager struct {
		hp hostProvider
		rl revisionLocker

		maxOverdrive     uint64
		overdriveTimeout time.Duration

		statsOverdrive *dataPoints
		statsSpeed     *dataPoints
		stopChan       chan struct{}

		mu        sync.Mutex
		uploaders []*uploader
		uploads   map[uploadID]*upload
	}

	uploader struct {
		fcid       types.FileContractID
		hk         types.PublicKey
		siamuxAddr string

		mu                  sync.Mutex
		bh                  uint64
		consecutiveFailures uint64
		queueChan           chan struct{}
		queue               []*shardUpload

		statsSpeed *dataPoints
		stopChan   chan struct{}
	}

	upload struct {
		mgr *uploadManager
		id  uploadID

		excluded          map[types.FileContractID]struct{}
		nextReadTrigger   chan struct{}
		doneSectorTrigger chan struct{}

		mu      sync.Mutex
		ongoing []slabID
		used    map[slabID]map[types.FileContractID]struct{}
	}

	slabUpload struct {
		u       *upload
		created time.Time
		sID     slabID

		mu           sync.Mutex
		numCompleted uint64
		numInflight  uint64
		numLaunched  uint64

		nextReadTriggered bool
		lastOverdrive     time.Time
		overdriving       map[int]int
		remaining         map[int]shardCtx
		sectors           []object.Sector
		errs              HostErrorSet
	}

	slabData struct {
		data   []byte
		length int
		err    error
	}

	slabResponse struct {
		slab  object.SlabSlice
		index int
		err   error
	}

	shardCtx struct {
		ctx    context.Context
		cancel context.CancelFunc
	}

	shardUpload struct {
		u   *upload
		sID slabID
		ctx context.Context

		overdrive    bool
		responseChan chan shardResp

		sector      *[rhpv2.SectorSize]byte
		sectorIndex int
		uploader    *uploader
	}

	shardResp struct {
		req  *shardUpload
		root types.Hash256
		err  error
	}

	uploadManagerStats struct {
		overdrivePct      float64
		uploadersHealthy  uint64
		uploadersSpeedAvg float64
		uploadersTotal    uint64
		uploadersStats    map[types.PublicKey]uploaderStats
	}

	uploaderStats struct {
		estimate float64
		speedP90 float64
	}

	dataPoints struct {
		mu  sync.Mutex
		pts [20]float64
		cnt uint64
		p90 float64
	}
)

func (w *worker) initUploadManager() {
	if w.uploadManager != nil {
		panic("uploader already initialized") // developer error
	}

	w.uploadManager = newUploadManager(w, w, w.uploadMaxOverdrive, w.uploadSectorTimeout)
}

func newDataPoints() *dataPoints {
	return &dataPoints{
		pts: [20]float64{},
	}
}

func newUploadManager(hp hostProvider, rl revisionLocker, maxOverdrive uint64, sectorTimeout time.Duration) *uploadManager {
	return &uploadManager{
		hp: hp,
		rl: rl,

		maxOverdrive:     maxOverdrive,
		overdriveTimeout: sectorTimeout,

		statsOverdrive: newDataPoints(),
		statsSpeed:     newDataPoints(),

		stopChan: make(chan struct{}),

		uploaders: make([]*uploader, 0),
		uploads:   make(map[uploadID]*upload, 0),
	}
}

func (mgr *uploadManager) Stats() uploadManagerStats {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// prepare stats
	stats := uploadManagerStats{
		overdrivePct:      mgr.statsOverdrive.recompute(),
		uploadersSpeedAvg: mgr.statsSpeed.recompute(),
		uploadersTotal:    uint64(len(mgr.uploaders)),
		uploadersStats:    make(map[types.PublicKey]uploaderStats),
	}

	// fill in uploader stats
	for _, u := range mgr.uploaders {
		u.statsSpeed.recompute()
		stats.uploadersStats[u.hk] = uploaderStats{
			estimate: u.estimate(),
			speedP90: u.statsSpeed.percentileP90() * 0.008, // convert bytes per ms to mbps
		}
		if u.healthy() {
			stats.uploadersHealthy++
		}
	}

	return stats
}

func (mgr *uploadManager) Stop() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	close(mgr.stopChan)
	for _, u := range mgr.uploaders {
		close(u.stopChan)
	}
}

func (mgr *uploadManager) enqueue(j *shardUpload) error {
	queue := mgr.uploader(j)
	if queue == nil {
		return errNoFreeQueue
	}
	queue.push(j)
	return nil
}

func (mgr *uploadManager) finishUpload(u *upload) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	delete(mgr.uploads, u.id)
}

func (mgr *uploadManager) migrateShards(ctx context.Context, shards [][]byte, excluded map[types.FileContractID]struct{}) ([]object.Sector, error) {
	// initiate the upload
	upload, err := mgr.newUpload(len(shards), excluded)
	if err != nil {
		return nil, err
	}
	defer mgr.finishUpload(upload)

	// upload the shards
	return upload.uploadShards(ctx, shards, 0)
}

func (mgr *uploadManager) newUpload(totalShards int, excluded map[types.FileContractID]struct{}) (*upload, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// check if we have enough uploaders
	var usable int
	for _, u := range mgr.uploaders {
		if _, exclude := excluded[u.fcid]; exclude {
			continue
		}
		usable++
	}
	if usable < totalShards {
		return nil, errNotEnoughHosts
	}

	// create id
	var id uploadID
	frand.Read(id[:])

	// create upload
	mgr.uploads[id] = &upload{
		mgr: mgr,
		id:  id,

		excluded:          excluded,
		nextReadTrigger:   make(chan struct{}, 1),
		doneSectorTrigger: make(chan struct{}, 1),

		ongoing: make([]slabID, 0),
		used:    make(map[slabID]map[types.FileContractID]struct{}),
	}

	mgr.uploads[id].triggerNextRead()
	return mgr.uploads[id], nil
}

func (mgr *uploadManager) newUploader(c api.ContractMetadata) *uploader {
	return &uploader{
		fcid:       c.ID,
		hk:         c.HostKey,
		siamuxAddr: c.SiamuxAddr,

		queue:     make([]*shardUpload, 0),
		queueChan: make(chan struct{}, 1),

		statsSpeed: newDataPoints(),
		stopChan:   make(chan struct{}),
	}
}

func (mgr *uploadManager) numUploaders() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return len(mgr.uploaders)
}

func (mgr *uploadManager) update(contracts []api.ContractMetadata, bh uint64) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// build map
	c2m := make(map[types.FileContractID]api.ContractMetadata)
	for _, c := range contracts {
		c2m[c.ID] = c
	}

	// recreate the pool
	var i int
	for _, q := range mgr.uploaders {
		if _, keep := c2m[q.fcid]; !keep {
			continue
		}
		delete(c2m, q.fcid)
		mgr.uploaders[i] = q
		i++
	}
	for j := i; j < len(mgr.uploaders); j++ {
		mgr.uploaders[j] = nil
	}
	mgr.uploaders = mgr.uploaders[:i]

	// add missing uploaders
	for _, contract := range c2m {
		uploader := mgr.newUploader(contract)
		mgr.uploaders = append(mgr.uploaders, uploader)
		uploader.start(mgr.hp, mgr.rl)
	}

	// update blockheight
	for _, u := range mgr.uploaders {
		u.updateBlockHeight(bh)
	}
}

func (mgr *uploadManager) upload(ctx context.Context, r io.Reader, rs api.RedundancySettings) (_ object.Object, err error) {
	// add cancel
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
	u, err := mgr.newUpload(rs.TotalShards, nil)
	if err != nil {
		return object.Object{}, err
	}
	defer mgr.finishUpload(u)
	fmt.Printf("DEBUG PJ: %v | started\n", u.id)

	// launch the upload
	slabsChan := u.start(ctx, cr, rs)

	// collect the slabs
	var responses []slabResponse
	for res := range slabsChan {
		if res.err != nil {
			return object.Object{}, res.err
		}
		responses = append(responses, res)
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

func (mgr *uploadManager) uploader(j *shardUpload) *uploader {
	mgr.mu.Lock()
	if len(mgr.uploaders) == 0 {
		mgr.mu.Unlock()
		return nil
	}

	// recompute the stats first
	for _, q := range mgr.uploaders {
		q.statsSpeed.recompute()
	}

	// sort the uploaders by their estimate
	sort.Slice(mgr.uploaders, func(i, j int) bool {
		return mgr.uploaders[i].estimate() < mgr.uploaders[j].estimate()
	})

	// filter queues
	var candidates []*uploader
	for _, q := range mgr.uploaders {
		if j.u.canUseQueue(q, j.sID) {
			candidates = append(candidates, q)
		}
	}
	mgr.mu.Unlock()
	// return early if we have no queues left
	if len(candidates) == 0 {
		return nil
	}

loop:
	for {
		// grab ongoing slab uploads
		j.u.mu.Lock()
		ongoing := j.u.ongoing
		j.u.mu.Unlock()

		// grab the slabs parents
		var parents []slabID
		for _, sID := range ongoing {
			if sID == j.sID {
				break
			}
			parents = append(parents, sID)
		}

		// if this slab does not have more than 2 parents, we return the first
		// (and thus best) candidate
		if len(parents) < 3 {
			return candidates[0]
		}

		fmt.Printf("DEBUG PJ: %v | %v | no queue yet for sector %d, overdrive %v, candidates %d waiting on %d shards to complete (%v)\n", j.u.id, j.sID, j.sectorIndex, j.overdrive, len(candidates), len(parents), parents)

		// otherwise we wait, allowing the parents to complete
		select {
		case <-j.u.doneSectorTrigger:
			continue loop
		case <-j.ctx.Done():
			break loop
		}
	}

	return nil
}

func (u *upload) newSlabUpload(ctx context.Context, shards [][]byte) (*slabUpload, []*shardUpload, chan shardResp) {
	u.mu.Lock()
	defer u.mu.Unlock()

	// prepare slab id
	var sID slabID
	frand.Read(sID[:])
	u.ongoing = append(u.ongoing, sID)

	// prepare slab upload
	slab := &slabUpload{
		u:       u,
		sID:     sID,
		created: time.Now(),

		overdriving: make(map[int]int, len(shards)),
		remaining:   make(map[int]shardCtx, len(shards)),
		sectors:     make([]object.Sector, len(shards)),
	}

	// prepare requests
	responseChan := make(chan shardResp)
	jobs := make([]*shardUpload, len(shards))
	for sI, shard := range shards {
		// create the sector upload's cancel func
		uCtx, cancel := context.WithCancel(ctx)
		slab.remaining[sI] = shardCtx{ctx: uCtx, cancel: cancel}

		// create the job's span
		uCtx, span := tracing.Tracer.Start(uCtx, "uploadShard")
		span.SetAttributes(attribute.Bool("overdrive", false))
		span.SetAttributes(attribute.Int("sector", sI))

		// create the job
		jobs[sI] = &shardUpload{
			u:            u,
			sID:          sID,
			ctx:          uCtx,
			responseChan: responseChan,

			sector:      (*[rhpv2.SectorSize]byte)(shard),
			sectorIndex: sI,
		}
	}

	return slab, jobs, responseChan
}

func (u *upload) canUseQueue(q *uploader, sID slabID) bool {
	u.mu.Lock()
	defer u.mu.Unlock()

	_, excluded := u.excluded[q.fcid]
	if excluded {
		return false
	}

	_, used := u.used[sID][q.fcid]
	return !used
}

func (u *upload) start(ctx context.Context, r io.Reader, rs api.RedundancySettings) chan slabResponse {
	// create the response channel
	slabsChan := make(chan slabResponse)

	// launch a goroutine uploading the slabs
	go func() {
		var wg sync.WaitGroup
		defer func() {
			wg.Wait()
			close(slabsChan)
		}()

		var slabIndex int
		for res := range u.read(ctx, r, rs) {
			if res.err != nil {
				slabsChan <- slabResponse{err: res.err}
				return
			}

			wg.Add(1)
			go func(data []byte, length, index int) {
				defer wg.Done()

				// add tracing
				ctx, span := tracing.Tracer.Start(ctx, "createShards")
				defer span.End()

				// create slab
				s := object.NewSlab(uint8(rs.MinShards))

				// create the shards
				shards := make([][]byte, rs.TotalShards)
				s.Encode(data, shards)
				s.Encrypt(shards)

				// upload the shards
				var err error
				s.Shards, err = u.uploadShards(ctx, shards, index)
				if err != nil {
					select {
					case slabsChan <- slabResponse{err: err}:
					default:
						fmt.Printf("DEBUG PJ: %v | failed to send slab response, err %v\n", u.id, err)
					}
					return
				}

				// send the slab
				select {
				case slabsChan <- slabResponse{
					slab: object.SlabSlice{
						Slab:   s,
						Offset: 0,
						Length: uint32(length),
					},
					index: index,
				}:
				default:
					fmt.Printf("DEBUG PJ: %v | failed to send slab response\n", u.id)
				}
			}(res.data, res.length, slabIndex)
			slabIndex++
		}
	}()
	return slabsChan
}

func (u *upload) registerUsedQueue(sID slabID, fcid types.FileContractID) {
	u.mu.Lock()
	defer u.mu.Unlock()

	_, exists := u.used[sID]
	if !exists {
		u.used[sID] = make(map[types.FileContractID]struct{})
	}
	u.used[sID][fcid] = struct{}{}
}

func (u *upload) registerCompletedSector(sID slabID, fcid types.FileContractID, done bool) {
	u.mu.Lock()
	defer u.mu.Unlock()

	// register completed sector
	u.triggerDoneSector()

	// update ongoing uploads if the slab upload is done
	if done {
		for i, prev := range u.ongoing {
			if prev == sID {
				u.ongoing = append(u.ongoing[:i], u.ongoing[i+1:]...)
				break
			}
		}
		fmt.Printf("DEBUG PJ: %v | %v | updating history %v\n", u.id, sID, u.ongoing)
	}
}

func (u *upload) read(ctx context.Context, r io.Reader, rs api.RedundancySettings) chan slabData {
	size := int64(rs.MinShards) * rhpv2.SectorSize
	data := make(chan slabData)

	go func() {
		for {
			select {
			case <-u.mgr.stopChan:
				data <- slabData{err: errors.New("manager was stopped")}
				return
			case <-ctx.Done():
				data <- slabData{err: errors.New("upload timed out")}
				return
			case <-u.nextReadTrigger:
			}
			fmt.Printf("DEBUG PJ: %v | slab read triggered\n", u.id)

			buf := make([]byte, size)
			length, err := io.ReadFull(io.LimitReader(r, size), buf)
			if err == io.EOF {
				fmt.Printf("DEBUG PJ: %v | slab reads done\n", u.id)
				close(data)
				return
			} else if err != nil && err != io.ErrUnexpectedEOF {
				data <- slabData{err: errors.New("data read failed")}
				return
			}

			data <- slabData{data: buf, length: length}
		}
	}()

	return data
}

func (u *upload) triggerDoneSector() {
	select {
	case u.doneSectorTrigger <- struct{}{}:
	default:
	}
}

func (u *upload) triggerNextRead() {
	select {
	case u.nextReadTrigger <- struct{}{}:
	default:
	}
}

func (u *upload) uploadShards(ctx context.Context, shards [][]byte, index int) ([]object.Sector, error) {
	ctx, span := tracing.Tracer.Start(ctx, "uploadShards")
	defer span.End()

	// convenience variables
	mgr := u.mgr

	// prepare the upload
	slab, reqs, sectorChan := u.newSlabUpload(ctx, shards)
	span.SetAttributes(attribute.Stringer("id", slab.sID))
	defer slab.cleanup()

	fmt.Printf("DEBUG PJ: %v | %v | slab %d started \n", slab.u.id, slab.sID, index)
	defer fmt.Printf("DEBUG PJ: %v | %v | slab %d finished \n", slab.u.id, slab.sID, index)

	// launch all reqs
	for _, req := range reqs {
		if err := slab.launch(req); err != nil {
			return nil, err
		}
	}

	// create a timer to trigger overdrive
	timeout := time.NewTimer(mgr.overdriveTimeout)
	resetTimeout := func() {
		timeout.Stop()
		select {
		case <-timeout.C:
		default:
		}
		timeout.Reset(mgr.overdriveTimeout)
	}

	// launch a goroutine to trigger overdrive
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timeout.C:
				if req := slab.overdrive(sectorChan, shards); req != nil {
					_ = slab.launch(req) // ignore error
				}
				resetTimeout()
			}
		}
	}()

	// collect responses
	for slab.inflight() > 0 {
		var resp shardResp
		select {
		case <-mgr.stopChan:
			return nil, errors.New("upload stopped")
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp = <-sectorChan:
			if resp.err == nil {
				resetTimeout()
			}
		}

		// handle the response
		if done := slab.receive(resp); done {
			break
		}

		// relaunch regular reqs
		if resp.err != nil && !resp.req.overdrive {
			if err := slab.launch(resp.req); err != nil {
				break // fail the download
			}
		}
	}

	// register the amount of overdrive sectors
	span.SetAttributes(attribute.Int("overdrive", slab.overdriveCnt()))

	// track stats
	mgr.statsOverdrive.track(slab.overdrivePct())
	mgr.statsSpeed.track(float64(slab.bytesPerMS()))
	return slab.finish()
}

func (u *uploader) start(hp hostProvider, rl revisionLocker) {
	go func() {
	outer:
		for {
			// wait for work
			select {
			case <-u.queueChan:
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

				// pop the next req
				req := u.pop()
				if req == nil {
					continue outer
				}

				// skip if req is done
				if req.done() {
					continue
				}

				// execute it
				var root types.Hash256
				start := time.Now()
				err := rl.withRevision(req.ctx, defaultRevisionFetchTimeout, u.fcid, u.hk, u.siamuxAddr, lockingPriorityUpload, u.blockHeight(), func(rev types.FileContractRevision) error {
					var err error
					root, err = req.execute(hp, rev)
					return err
				})

				// send the response
				if err != nil {
					req.fail(err)
				} else {
					req.succeed(root)
				}

				// track the error, ignore gracefully closed streams and canceled overdrives
				isErrClosedStream := errors.Is(err, mux.ErrClosedStream)
				canceledOverdrive := req.done() && req.overdrive && err != nil
				if !canceledOverdrive && !isErrClosedStream {
					u.track(err, time.Since(start))
				}
			}
		}
	}()
}

func (u *uploader) blockHeight() uint64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.bh
}

func (u *uploader) estimate() float64 {
	u.mu.Lock()
	defer u.mu.Unlock()

	// fetch average speed
	bytesPerMS := int(u.statsSpeed.percentileP90())
	if bytesPerMS == 0 {
		bytesPerMS = math.MaxInt64
	}

	outstanding := (len(u.queue) + 1) * rhpv2.SectorSize
	return float64(outstanding / bytesPerMS)
}

func (u *uploader) push(req *shardUpload) {
	// decorate req
	span := trace.SpanFromContext(req.ctx)
	span.SetAttributes(attribute.Stringer("hk", u.hk))
	span.AddEvent("enqueued")
	req.uploader = u
	req.u.registerUsedQueue(req.sID, u.fcid)

	u.mu.Lock()
	defer u.mu.Unlock()

	// enqueue the job
	u.queue = append(u.queue, req)

	// signal there's work
	select {
	case u.queueChan <- struct{}{}:
	default:
	}
}

func (u *uploader) healthy() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.consecutiveFailures == 0
}

func (u *uploader) track(err error, d time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err != nil {
		u.consecutiveFailures++
		u.statsSpeed.track(1)
		fmt.Println("TRACK ERROR", u.hk, err)
	} else {
		u.consecutiveFailures = 0
		u.statsSpeed.track(float64(rhpv2.SectorSize / d.Milliseconds()))
	}
}

func (u *uploader) updateBlockHeight(bh uint64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.bh = bh
}

func (u *uploader) pop() *shardUpload {
	u.mu.Lock()
	defer u.mu.Unlock()

	if len(u.queue) > 0 {
		j := u.queue[0]
		u.queue = u.queue[1:]
		return j
	}
	return nil
}

func (req *shardUpload) execute(hp hostProvider, rev types.FileContractRevision) (types.Hash256, error) {
	// fetch span from context
	span := trace.SpanFromContext(req.ctx)
	span.AddEvent("execute")

	// create a host
	h, err := hp.newHostV3(req.ctx, req.uploader.fcid, req.uploader.hk, req.uploader.siamuxAddr)
	if err != nil {
		return types.Hash256{}, err
	}

	// upload the sector
	start := time.Now()
	root, err := h.UploadSector(req.ctx, req.sector, rev)
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

func (req *shardUpload) succeed(root types.Hash256) {
	select {
	case <-req.ctx.Done():
	case req.responseChan <- shardResp{
		req:  req,
		root: root,
	}:
	}
}

func (req *shardUpload) fail(err error) {
	select {
	case <-req.ctx.Done():
	case req.responseChan <- shardResp{
		req: req,
		err: err,
	}:
	}
}

func (req *shardUpload) done() bool {
	select {
	case <-req.ctx.Done():
		return true
	default:
		return false
	}
}

func (s *slabUpload) bytesPerMS() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	bytes := int64(s.numCompleted) * rhpv2.SectorSize
	ms := time.Since(s.created).Milliseconds()
	return bytes / ms
}

func (s *slabUpload) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sCtx := range s.remaining {
		sCtx.cancel()
	}
}

func (s *slabUpload) finish() ([]object.Sector, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	remaining := len(s.remaining)
	if remaining > 0 {
		return nil, fmt.Errorf("failed to upload slab: remaining=%d, inflight=%d, completed=%d launched=%d contracts=%d errors=%w", remaining, s.numInflight, s.numCompleted, s.numLaunched, s.u.mgr.numUploaders(), s.errs)
	}
	return s.sectors, nil
}

func (s *slabUpload) inflight() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numInflight
}

func (s *slabUpload) launch(req *shardUpload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// convenience variables
	mgr := s.u.mgr

	// enqueue the job
	err := mgr.enqueue(req)
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
		fmt.Printf("DEBUG PJ: %v | %v | launching overdrive for sector %d\n", s.u.id, s.sID, req.sectorIndex)
		s.lastOverdrive = time.Now()
		s.overdriving[req.sectorIndex]++
	}

	return nil
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

func (s *slabUpload) overdrive(responseChan chan shardResp, shards [][]byte) *shardUpload {
	s.mu.Lock()
	defer s.mu.Unlock()

	// overdrive is not kicking in yet
	if uint64(len(s.remaining)) >= s.u.mgr.maxOverdrive {
		return nil
	}

	// overdrive is not due yet
	if time.Since(s.lastOverdrive) < s.u.mgr.overdriveTimeout {
		return nil
	}

	// overdrive is maxed out
	if s.numInflight-uint64(len(s.remaining)) >= s.u.mgr.maxOverdrive {
		return nil
	}

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

	return &shardUpload{
		u:   s.u,
		sID: s.sID,
		ctx: s.remaining[lowestSI].ctx,

		overdrive:    true,
		responseChan: responseChan,

		sectorIndex: lowestSI,
		sector:      (*[rhpv2.SectorSize]byte)(shards[lowestSI]),
	}
}

func (s *slabUpload) receive(resp shardResp) (completed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// convenience variables
	mgr := s.u.mgr

	// failed reqs can't complete the upload
	s.numInflight--
	if resp.err != nil {
		s.errs = append(s.errs, &HostError{resp.req.uploader.hk, resp.err})
		return false
	}

	defer func() {
		s.u.registerCompletedSector(s.sID, resp.req.uploader.fcid, completed)

		// trigger next slab
		if !s.nextReadTriggered {
			if len(s.remaining) < int(mgr.maxOverdrive) {
				s.nextReadTriggered = true
				s.u.triggerNextRead()
			} else if completed {
				s.nextReadTriggered = true
				s.u.triggerNextRead()
			}
		}
	}()

	// redundant sectors can't complete the upload
	s.numCompleted++
	if s.sectors[resp.req.sectorIndex].Root != (types.Hash256{}) {
		return false
	}

	// store the sector and call cancel on the sector ctx
	s.sectors[resp.req.sectorIndex] = object.Sector{
		Host: resp.req.uploader.hk,
		Root: resp.root,
	}
	s.remaining[resp.req.sectorIndex].cancel()

	// count the sector as complete and check if we're done
	delete(s.remaining, resp.req.sectorIndex)

	if len(s.remaining)%5 == 0 || len(s.remaining) < 5 {
		fmt.Printf("DEBUG PJ: %v | %v | remaining sectors %d\n", s.u.id, s.sID, len(s.remaining))
	}
	return len(s.remaining) == 0
}

func (a *dataPoints) percentileP90() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.p90
}

func (a *dataPoints) recompute() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	// calculate sums
	var data stats.Float64Data
	for _, p := range a.pts {
		if p > 0 {
			data = append(data, p)
		}
	}
	p90, err := data.Percentile(90)
	if err != nil {
		return 0
	}
	a.p90 = p90
	return p90
}

func (a *dataPoints) track(p float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pts[a.cnt%uint64(len(a.pts))] = p
	a.cnt++
}

func (uID uploadID) String() string {
	return fmt.Sprintf("%x", uID[:])
}

func (sID slabID) String() string {
	return fmt.Sprintf("%x", sID[:])
}
