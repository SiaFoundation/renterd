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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"lukechampine.com/frand"
)

// TODO: add priority queues

const defaultAvgUpdateInterval = 10 * time.Second

var errNoQueue = errors.New("no queue")

type (
	migrator interface {
		uploadSectors(ctx context.Context, shards [][]byte, contracts []api.ContractMetadata, used map[types.FileContractID]struct{}, blockHeight uint64) ([]object.Sector, error)
	}

	uploadID [8]byte

	uploader struct {
		w *worker

		statsOverdrive *average
		statsStopChan  chan struct{}

		poolMu  sync.Mutex
		pool    []*uploadQueue
		exclude map[uploadID]map[types.FileContractID]struct{}
		bh      uint64
	}

	uploadQueue struct {
		u  *uploader
		hp hostProvider
		rl revisionLocker

		fcid       types.FileContractID
		hk         types.PublicKey
		siamuxAddr string

		queueMu        sync.Mutex
		queueChan      chan struct{}
		queueChanClose *sync.Once
		queueUploads   map[uploadID]struct{}
		queue          []*uploadJob

		statsSpeed        *average
		statsEstimateDiff *average
		stopChan          chan struct{}
	}

	uploadJob struct {
		requestCtx  context.Context
		requestSpan trace.Span

		overdriveChan chan int
		responseChan  chan uploadResponse

		id            uploadID
		sector        *[rhpv2.SectorSize]byte
		sectorIndex   int
		sectorTimeout time.Duration
		estimate      time.Duration
		queue         *uploadQueue
	}

	uploadResponse struct {
		job  *uploadJob
		root types.Hash256
		err  error
	}

	uploadState struct {
		mu           sync.Mutex
		infl         uint64
		rem          uint64
		maxOverdrive uint64
		numOverdrive uint64

		pending     []int
		overdriving map[int]struct{}
		sectors     []object.Sector
	}

	average struct {
		mu             sync.Mutex
		avg            float64
		pts            [1e3]float64
		cnt            uint64
		updateInterval time.Duration
		lastUpdate     time.Time
	}
)

func (u *uploader) newQueue(c api.ContractMetadata) *uploadQueue {
	return &uploadQueue{
		u:  u,
		hp: u.w,
		rl: u.w,

		fcid:       c.ID,
		hk:         c.HostKey,
		siamuxAddr: c.SiamuxAddr,

		queue:        make([]*uploadJob, 0),
		queueChan:    make(chan struct{}),
		queueUploads: make(map[uploadID]struct{}),

		statsSpeed:        newAverage(defaultAvgUpdateInterval),
		statsEstimateDiff: newAverage(defaultAvgUpdateInterval),
		stopChan:          make(chan struct{}),
	}
}

func newAverage(updateInterval time.Duration) *average {
	return &average{
		updateInterval: updateInterval,
		pts:            [1e3]float64{},
	}
}

func (w *worker) initUploader() {
	if w.uploader != nil {
		panic("uploader already initialized") // developer error
	}

	w.uploader = &uploader{
		statsOverdrive: newAverage(defaultAvgUpdateInterval),
		statsStopChan:  make(chan struct{}),

		w:       w,
		pool:    make([]*uploadQueue, 0),
		exclude: make(map[uploadID]map[types.FileContractID]struct{}),
	}
}

func (u *uploader) Stop() {
	u.poolMu.Lock()
	defer u.poolMu.Unlock()
	for _, q := range u.pool {
		close(q.stopChan)
	}
	close(u.statsStopChan)
}

func (u *uploader) blockHeight() uint64 {
	u.poolMu.Lock()
	defer u.poolMu.Unlock()
	return u.bh
}

func (u *uploader) newUpload(exclude map[types.FileContractID]struct{}, totalShards uint64) (uploadID, error) {
	u.poolMu.Lock()
	defer u.poolMu.Unlock()

	// sanity check redundancy
	var remaining uint64
	for _, q := range u.pool {
		if _, exclude := exclude[q.fcid]; !exclude {
			remaining++
		}
	}
	if totalShards > remaining {
		return uploadID{}, errors.New("not enough contracts to meet redundancy")
	}

	// generate upload id and keep track of the exclude list
	var id uploadID
	frand.Read(id[:])
	u.exclude[id] = exclude
	return id, nil
}

func (u *uploader) finishUpload(id uploadID) {
	u.poolMu.Lock()
	defer u.poolMu.Unlock()

	delete(u.exclude, id)
	for _, q := range u.pool {
		q.finish(id)
	}
}

func (u *uploader) upload(ctx context.Context, r io.Reader, contracts []api.ContractMetadata, rs api.RedundancySettings, blockHeight uint64) (o object.Object, used map[types.PublicKey]types.FileContractID, err error) {
	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "uploader.upload")
	defer func() {
		if err != nil {
			span.RecordError(err)
		}
		span.End()
	}()

	// keep track of used hosts
	used = make(map[types.PublicKey]types.FileContractID)

	// build contracts map
	h2c := make(map[types.PublicKey]types.FileContractID)
	for _, c := range contracts {
		h2c[c.HostKey] = c.ID
	}

	// initialize the encryption key
	o.Key = object.GenerateEncryptionKey()
	cr := o.Key.Encrypt(r)

	for {
		proceed, err := func() (bool, error) {
			// ensure the upload ctx is cancelled
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			// create span
			ctx, span := tracing.Tracer.Start(ctx, "uploader.slab")
			defer span.End()

			// create slab
			slab := object.Slab{
				Key:       object.GenerateEncryptionKey(),
				MinShards: uint8(rs.MinShards),
			}

			// read slab data
			buf := make([]byte, slab.Length())
			length, err := io.ReadFull(io.LimitReader(cr, int64(slab.Length())), buf)
			if err == io.EOF {
				return false, nil
			} else if err != nil && err != io.ErrUnexpectedEOF {
				return false, err
			}
			span.AddEvent("shards read")

			// encode and encrypt the shards
			shards := make([][]byte, rs.TotalShards)
			slab.Encode(buf, shards)
			span.AddEvent("shards encoded")
			slab.Encrypt(shards)
			span.AddEvent("shards encrypted")

			// upload the shards
			slab.Shards, err = u.uploadSectors(ctx, shards, contracts, nil, blockHeight)
			if err != nil {
				return false, err
			}

			// add the slab
			o.Slabs = append(o.Slabs, object.SlabSlice{
				Slab:   slab,
				Offset: 0,
				Length: uint32(length),
			})

			// update used hosts
			for _, ss := range slab.Shards {
				if _, ok := used[ss.Host]; !ok {
					used[ss.Host] = h2c[ss.Host]
				}
			}
			return true, nil
		}()
		if err != nil {
			return object.Object{}, nil, err
		}
		if !proceed {
			break
		}
	}

	return o, used, nil
}

func (u *uploader) uploadSectors(ctx context.Context, shards [][]byte, contracts []api.ContractMetadata, used map[types.FileContractID]struct{}, blockHeight uint64) ([]object.Sector, error) {
	ctx, span := tracing.Tracer.Start(ctx, "uploader.uploadSectors")
	defer span.End()

	// refresh contracts
	u.updatePool(contracts, blockHeight)

	// initialize upload
	id, err := u.newUpload(used, uint64(len(shards)))
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer u.finishUpload(id)
	span.SetAttributes(attribute.Stringer("id", id))

	// prepare state
	state := &uploadState{
		maxOverdrive: u.w.uploadMaxOverdrive,
		pending:      make([]int, 0, len(shards)),
		overdriving:  make(map[int]struct{}, len(shards)),
		sectors:      make([]object.Sector, len(shards)),
		rem:          uint64(len(shards)),
	}

	// prepare launch function
	launch := func(job *uploadJob, overdrive bool) error {
		// set the job's span
		_, jobSpan := tracing.Tracer.Start(ctx, "uploader.uploadJob")
		job.requestSpan = jobSpan
		job.requestSpan.SetAttributes(attribute.Bool("overdrive", overdrive))

		// schedule the job
		if err := u.enqueue(job); err != nil {
			job.requestSpan.RecordError(err)
			return err
		}
		state.launched(overdrive)
		return nil
	}

	// create a timer to trigger overdrive
	timeout := time.NewTimer(u.w.uploadSectorTimeout)
	resetTimeout := func() {
		timeout.Stop()
		select {
		case <-timeout.C:
		default:
		}
		timeout.Reset(u.w.uploadSectorTimeout)
	}

	// launch a goroutine that handles overdrive
	overdriveChan := make(chan int)
	responseChan := make(chan uploadResponse)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case i := <-overdriveChan:
				if state.canOverdrive(i) {
					_ = launch(&uploadJob{
						overdriveChan: overdriveChan,
						responseChan:  responseChan,
						requestCtx:    ctx,
						sector:        (*[rhpv2.SectorSize]byte)(shards[i]),
						sectorIndex:   i,
						sectorTimeout: u.w.uploadSectorTimeout,
						id:            id,
					}, true)
				}
				resetTimeout()
			}
		}
	}()

	// launch all shards
	for i, shard := range shards {
		if err := launch(&uploadJob{
			overdriveChan: overdriveChan,
			responseChan:  responseChan,
			requestCtx:    ctx,
			sector:        (*[rhpv2.SectorSize]byte)(shard),
			sectorIndex:   i,
			sectorTimeout: u.w.uploadSectorTimeout,
			id:            id,
		}, false); err != nil {
			return nil, err
		}
	}

	// collect responses
	var errs HostErrorSet
	for state.inflight() > 0 {
		var resp uploadResponse
		select {
		case <-timeout.C:
			nxt := state.nextOverdrive()
			if nxt != -1 && state.canOverdrive(nxt) {
				_ = launch(&uploadJob{
					overdriveChan: overdriveChan,
					responseChan:  responseChan,
					requestCtx:    ctx,
					sectorIndex:   nxt,
					sector:        (*[rhpv2.SectorSize]byte)(shards[nxt]),
					id:            id,
				}, true)
			}
			resetTimeout()
			continue
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp = <-responseChan:
			state.received()
			resetTimeout()
		}

		hk := resp.job.queue.hk
		if resp.err != nil {
			errs = append(errs, &HostError{hk, resp.err})
			if err := launch(resp.job, false); err != nil {
				u.w.logger.Debugf("failed to re-launch job: %v, host %v failed with err %v", err, resp.job.queue.hk, resp.err)
				state.schedule(resp.job.sectorIndex)
			}
			continue
		}

		if state.complete(resp.job.sectorIndex, hk, resp.root) {
			break
		}

		for {
			nxt := state.nextOverdrive()
			if nxt == -1 {
				break
			}

			if err := launch(&uploadJob{
				overdriveChan: overdriveChan,
				responseChan:  responseChan,
				requestCtx:    ctx,
				sectorIndex:   nxt,
				sector:        (*[rhpv2.SectorSize]byte)(shards[nxt]),
				id:            id,
			}, true); err != nil {
				break
			}
		}
	}

	// register the amount of overdrive sectors
	span.SetAttributes(attribute.Int("overdrive", int(state.numOverdrive)))

	// if there are remaining sectors, fail with an error message
	if state.remaining() > 0 {
		return nil, fmt.Errorf("failed to upload slab: rem=%v, inflight=%v, errs=%w", state.remaining(), state.inflight(), errs)
	}

	// track overdrive pct
	overdrivePct := float64(state.numOverdrive+uint64(len(shards))) / float64(len(shards))
	u.statsOverdrive.track(overdrivePct)

	state.mu.Lock()
	defer state.mu.Unlock()
	return state.sectors, nil
}

func (u *uploader) enqueue(j *uploadJob) error {
	queue := u.queue(j.id)
	if queue == nil {
		return errNoQueue
	}
	queue.push(j)
	return nil
}

func (u *uploader) queue(id uploadID) *uploadQueue {
	u.poolMu.Lock()
	defer u.poolMu.Unlock()
	if len(u.pool) == 0 {
		return nil
	}

	// sort the pool by estimate
	sort.Slice(u.pool, func(i, j int) bool {
		return u.pool[i].estimate() < u.pool[j].estimate()
	})

	// return the first unused queue
	for _, q := range u.pool {
		if _, exclude := u.exclude[id][q.fcid]; exclude {
			continue
		}
		if !q.used(id) {
			return q
		}
	}
	return nil
}

func (u *uploader) updatePool(contracts []api.ContractMetadata, bh uint64) {
	u.poolMu.Lock()
	defer u.poolMu.Unlock()

	// update blockheight
	u.bh = bh

	// build map
	c2m := make(map[types.FileContractID]api.ContractMetadata)
	for _, c := range contracts {
		c2m[c.ID] = c
	}

	// recreate the pool
	var i int
	for _, q := range u.pool {
		if _, keep := c2m[q.fcid]; !keep {
			continue
		}
		delete(c2m, q.fcid)
		u.pool[i] = q
		i++
	}
	for j := i; j < len(u.pool); j++ {
		u.pool[j] = nil
	}
	u.pool = u.pool[:i]

	// add missing uploaders
	for _, contract := range c2m {
		queue := u.newQueue(contract)
		u.pool = append(u.pool, queue)
		go queue.processJobs()
	}
}

func (q *uploadQueue) finish(id uploadID) {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	delete(q.queueUploads, id)
}

func (q *uploadQueue) used(id uploadID) bool {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()
	_, used := q.queueUploads[id]
	return used
}

func (q *uploadQueue) withRevision(ctx context.Context, fn func(rev types.FileContractRevision) error) error {
	return q.rl.withRevision(ctx, defaultRevisionFetchTimeout, q.fcid, q.hk, q.siamuxAddr, lockingPriorityUpload, q.u.blockHeight(), fn)
}

func (q *uploadQueue) processJobs() {
	for {
		// pop a job
		job := q.pop()
		if job == nil {
			select {
			case <-q.queueChan:
				job = q.pop()
			case <-q.stopChan:
				return
			}
		}

		// skip if job is done
		if job.done() {
			continue
		}

		// execute it
		_ = q.withRevision(job.requestCtx, func(rev types.FileContractRevision) error {
			return job.execute(q.hp, rev)
		})
	}
}

func (q *uploadQueue) estimate() float64 {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	// fetch average speed
	speed := q.statsSpeed.average()
	if speed == 0 {
		random := time.Duration(frand.Intn(30)+1) * time.Second
		speed = mbps(rhpv2.SectorSize, random.Seconds())
	}

	data := (len(q.queue) + 1) * rhpv2.SectorSize
	return float64(data) * 0.000008 / speed
}

func (q *uploadQueue) push(j *uploadJob) {
	// decorate job
	j.requestSpan.SetAttributes(attribute.Stringer("hk", q.hk))
	j.estimate = time.Duration(q.estimate()) * time.Second
	j.queue = q

	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	// enqueue the job
	q.queue = append(q.queue, j)
	q.queueUploads[j.id] = struct{}{}
	q.queueChanClose.Do(func() { close(q.queueChan) })
}

func (q *uploadQueue) pop() *uploadJob {
	q.queueMu.Lock()
	defer q.queueMu.Unlock()

	if len(q.queue) > 0 {
		j := q.queue[0]
		q.queue = q.queue[1:]
		return j
	}

	// recreate the channel
	q.queueChan = make(chan struct{})
	q.queueChanClose = new(sync.Once)
	return nil
}

func (j *uploadJob) execute(hp hostProvider, rev types.FileContractRevision) (err error) {
	j.requestSpan.AddEvent("execute")

	// track performance
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)

		// update span
		j.requestSpan.SetAttributes(attribute.Int64("duration", elapsed.Milliseconds()))
		j.requestSpan.RecordError(err)

		// update stats
		if err == nil {
			speed := mbps(rhpv2.SectorSize, elapsed.Seconds())
			j.queue.statsSpeed.track(speed)
			j.queue.statsEstimateDiff.track(float64((elapsed - j.estimate).Milliseconds()))
		} else {
			j.queue.statsSpeed.track(math.SmallestNonzeroFloat64)
		}
	}()

	// schedule overdrive
	doneChan := make(chan struct{})
	defer close(doneChan)
	go func() {
		select {
		case <-time.After(j.sectorTimeout):
			j.requestSpan.AddEvent("overdrive")
			select {
			case j.overdriveChan <- j.sectorIndex:
			default:
			}
		case <-j.requestCtx.Done():
		case <-doneChan:
		}
	}()

	// upload sector
	var root types.Hash256
	if err = hp.withHostV3(j.requestCtx, j.queue.fcid, j.queue.hk, j.queue.siamuxAddr, func(h hostV3) error {
		j.requestSpan.AddEvent("hostready")
		root, err = h.UploadSector(j.requestCtx, j.sector, rev)
		j.requestSpan.AddEvent("uploaded")
		return err
	}); err != nil {
		j.fail(err)
	} else {
		j.succeed(root)
	}

	return
}

func (j *uploadJob) succeed(root types.Hash256) {
	select {
	case j.responseChan <- uploadResponse{
		job:  j,
		root: root,
	}:
	case <-time.After(time.Second):
	}
	j.requestSpan.End()
}

func (j *uploadJob) fail(err error) {
	select {
	case j.responseChan <- uploadResponse{
		job: j,
		err: err,
	}:
	case <-time.After(time.Second):
	}
	j.requestSpan.End()
}

func (j *uploadJob) done() bool {
	select {
	case <-j.requestCtx.Done():
		return true
	default:
		return false
	}
}

func (s *uploadState) launched(overdrive bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.infl++
	if overdrive {
		s.numOverdrive++
	}
}

func (s *uploadState) received() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.infl--
}

func (s *uploadState) complete(index int, hk types.PublicKey, root types.Hash256) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sectors[index] = object.Sector{
		Host: hk,
		Root: root,
	}
	s.rem--
	return s.rem == 0
}

func (s *uploadState) inflight() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.infl
}

func (s *uploadState) remaining() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rem
}

func (s *uploadState) schedule(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, index)
}

func (s *uploadState) canOverdrive(index int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.infl-s.rem < s.maxOverdrive {
		return true
	}

	// schedule overdrive for later
	s.pending = append(s.pending, index)
	return false
}

func (s *uploadState) nextOverdrive() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	// try pending overdrives first
	for len(s.pending) > 0 {
		index := s.pending[0]
		s.pending = s.pending[1:]
		if s.sectors[index].Root == (types.Hash256{}) {
			return index
		}
	}

	// try overdriving a sector that we have not overdrived before
	for index, sector := range s.sectors {
		_, ongoing := s.overdriving[index]
		if sector.Root == (types.Hash256{}) && !ongoing {
			s.overdriving[index] = struct{}{}
			return index
		}
	}

	// randomly overdrive a sector we overdrived before, but is not finished yet
	for index := range s.overdriving {
		if s.sectors[index].Root == (types.Hash256{}) {
			return index
		}
	}
	return -1
}

func (a *average) average() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.update() // try update
	return a.avg
}

func (a *average) track(p float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pts[a.cnt%uint64(len(a.pts))] = p
	a.cnt++
}

func (a *average) update() {
	if a.avg > 0 && time.Since(a.lastUpdate) < a.updateInterval {
		return
	}

	// calculate sums
	var sum float64
	var nonzero float64
	for _, p := range a.pts {
		sum += p
		if p > 0 {
			nonzero++
		}
	}
	if nonzero == 0 {
		nonzero = 1 // avoid division by zero
	}
	a.avg = sum / nonzero
	a.lastUpdate = time.Now()
}

func (id uploadID) String() string {
	return fmt.Sprintf("%x", id[:])
}

func mbps(b int64, s float64) float64 {
	bps := float64(b) / s
	return bps * 0.000008
}
