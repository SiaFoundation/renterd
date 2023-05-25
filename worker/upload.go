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
		uploadShards(ctx context.Context, shards [][]byte, contracts []api.ContractMetadata, used map[types.FileContractID]struct{}, blockHeight uint64) ([]object.Sector, error)
	}

	uploadID [8]byte

	uploader struct {
		hp hostProvider
		rl revisionLocker

		sectorTimeout time.Duration
		maxOverdrive  uint64

		statsOverdrive *average
		statsSpeed     *average

		mu        sync.Mutex
		contracts []*uploadQueue
		exclude   map[uploadID]map[types.FileContractID]struct{}
	}

	uploadQueue struct {
		fcid       types.FileContractID
		hk         types.PublicKey
		siamuxAddr string

		mu                  sync.Mutex
		bh                  uint64
		consecutiveFailures uint64
		queueChan           chan struct{}
		queueUploads        map[uploadID]struct{}
		queue               []*uploadJob

		statsSpeed *average
		stopChan   chan struct{}
	}

	uploadJob struct {
		requestCtx context.Context

		overdrive    bool
		responseChan chan uploadResponse

		id          uploadID
		sector      *[rhpv2.SectorSize]byte
		sectorIndex int
		estimate    time.Duration
		queue       *uploadQueue
	}

	uploadResponse struct {
		job  *uploadJob
		root types.Hash256
		err  error
	}

	uploadState struct {
		maxOverdrive  uint64
		sectorTimeout time.Duration

		mu           sync.Mutex
		numCompleted uint64
		numInflight  uint64
		numLaunched  uint64

		lastOverdrive time.Time
		overdriving   map[int]int
		remaining     map[int]context.CancelFunc
		sectors       []object.Sector
	}

	uploadStats struct {
		overdrivePct   float64
		queuesHealthy  uint64
		queuesSpeedAvg float64
		queuesTotal    uint64
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
		fcid:       c.ID,
		hk:         c.HostKey,
		siamuxAddr: c.SiamuxAddr,

		queue:        make([]*uploadJob, 0),
		queueChan:    make(chan struct{}, 1),
		queueUploads: make(map[uploadID]struct{}),

		statsSpeed: newAverage(defaultAvgUpdateInterval),
		stopChan:   make(chan struct{}),
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

	w.uploader = newUploader(w, w, w.uploadMaxOverdrive, w.uploadSectorTimeout)
}

func newUploader(hp hostProvider, rl revisionLocker, maxOverdrive uint64, sectorTimeout time.Duration) *uploader {
	return &uploader{
		hp: hp,
		rl: rl,

		maxOverdrive:  maxOverdrive,
		sectorTimeout: sectorTimeout,

		statsOverdrive: newAverage(defaultAvgUpdateInterval),
		statsSpeed:     newAverage(defaultAvgUpdateInterval),

		contracts: make([]*uploadQueue, 0),
		exclude:   make(map[uploadID]map[types.FileContractID]struct{}),
	}
}

func (u *uploader) Stats() uploadStats {
	u.mu.Lock()
	defer u.mu.Unlock()

	var healthy uint64
	for _, q := range u.contracts {
		if q.consecutiveFailures == 0 {
			healthy++
		}
	}

	return uploadStats{
		overdrivePct:   u.statsOverdrive.average(),
		queuesHealthy:  healthy,
		queuesSpeedAvg: u.statsSpeed.average(),
		queuesTotal:    uint64(len(u.contracts)),
	}
}

func (u *uploader) Stop() {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, q := range u.contracts {
		close(q.stopChan)
	}
}

func (u *uploader) newUpload(exclude map[types.FileContractID]struct{}, totalShards uint64) (uploadID, error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	// sanity check redundancy
	var remaining uint64
	for _, q := range u.contracts {
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
	u.mu.Lock()
	defer u.mu.Unlock()

	delete(u.exclude, id)
	for _, q := range u.contracts {
		q.finish(id)
	}
}

func (u *uploader) upload(ctx context.Context, r io.Reader, contracts []api.ContractMetadata, rs api.RedundancySettings, blockHeight uint64) (o object.Object, used map[types.PublicKey]types.FileContractID, err error) {
	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "uploader.upload")
	defer func() {
		span.RecordError(err)
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

			// encode and encrypt the shards
			span.AddEvent("shards read")
			shards := make([][]byte, rs.TotalShards)
			slab.Encode(buf, shards)
			span.AddEvent("shards encoded")
			slab.Encrypt(shards)
			span.AddEvent("shards encrypted")

			// upload the shards
			slab.Shards, err = u.uploadShards(ctx, shards, contracts, nil, blockHeight)
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

func (u *uploader) uploadShards(ctx context.Context, shards [][]byte, contracts []api.ContractMetadata, used map[types.FileContractID]struct{}, blockHeight uint64) ([]object.Sector, error) {
	ctx, span := tracing.Tracer.Start(ctx, "uploader.uploadShards")
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
		maxOverdrive:  u.maxOverdrive,
		sectorTimeout: u.sectorTimeout,

		overdriving: make(map[int]int, len(shards)),
		remaining:   make(map[int]context.CancelFunc, len(shards)),
		sectors:     make([]object.Sector, len(shards)),
	}

	// prepare launch function
	launch := func(job *uploadJob) error {
		// set the job's span
		_, jobSpan := tracing.Tracer.Start(ctx, "uploader.uploadJob")
		jobSpan.SetAttributes(attribute.Bool("overdrive", job.overdrive))
		jobSpan.SetAttributes(attribute.Int("sector", job.sectorIndex))
		job.requestCtx = trace.ContextWithSpan(job.requestCtx, jobSpan)

		// schedule the job
		if err := u.enqueue(job); err != nil {
			jobSpan.RecordError(err)
			jobSpan.End()
			return err
		}

		// keep state
		state.launch(job)
		return nil
	}

	// launch all shards
	start := time.Now()
	responseChan := make(chan uploadResponse)
	for i, shard := range shards {
		requestCtx, cancel := context.WithCancel(ctx)
		state.remaining[i] = cancel
		if err := launch(&uploadJob{
			overdrive:    false,
			responseChan: responseChan,
			requestCtx:   requestCtx,
			sector:       (*[rhpv2.SectorSize]byte)(shard),
			sectorIndex:  i,
			id:           id,
		}); err != nil {
			return nil, err
		}
	}

	// collect responses
	var errs HostErrorSet
	for state.inflight() > 0 {
		var resp uploadResponse
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp = <-responseChan:
		}

		// handle the response
		if done := state.receive(resp); done {
			break
		}

		// see if we need to relaunch the job
		if resp.err != nil {
			errs = append(errs, &HostError{resp.job.queue.hk, resp.err})
			if !resp.job.overdrive {
				if err := launch(resp.job); err != nil {
					break // download failed, not enough hosts
				}
			}
			continue
		}

		// launch an overdrive worker if possible
		if sI := state.overdrive(); sI != -1 {
			if err := launch(&uploadJob{
				overdrive:    true,
				responseChan: responseChan,
				requestCtx:   ctx,
				sectorIndex:  sI,
				sector:       (*[rhpv2.SectorSize]byte)(shards[sI]),
				id:           id,
			}); err == nil {
				continue
			}
		}
	}

	// register the amount of overdrive sectors

	span.SetAttributes(attribute.Int("overdrive", int(state.numLaunched)-len(state.sectors)))

	// track stats
	u.statsOverdrive.track(state.overdrivePct())
	u.statsSpeed.track(mbps(state.downloaded(), time.Since(start).Seconds()))

	return state.finish(errs)
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
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.contracts) == 0 {
		return nil
	}

	// sort the pool by estimate
	sort.Slice(u.contracts, func(i, j int) bool {
		return u.contracts[i].estimate() < u.contracts[j].estimate()
	})

	// return the first unused queue
	for _, q := range u.contracts {
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
	u.mu.Lock()
	defer u.mu.Unlock()

	// build map
	c2m := make(map[types.FileContractID]api.ContractMetadata)
	for _, c := range contracts {
		c2m[c.ID] = c
	}

	// recreate the pool
	var i int
	for _, q := range u.contracts {
		if _, keep := c2m[q.fcid]; !keep {
			continue
		}
		delete(c2m, q.fcid)
		u.contracts[i] = q
		i++
	}
	for j := i; j < len(u.contracts); j++ {
		u.contracts[j] = nil
	}
	u.contracts = u.contracts[:i]

	// add missing uploaders
	for _, contract := range c2m {
		queue := u.newQueue(contract)
		u.contracts = append(u.contracts, queue)
		go processQueue(u.hp, u.rl, queue)
	}

	// update queue blockheight
	for _, q := range u.contracts {
		q.updateBlockHeight(bh)
	}
}

func processQueue(hp hostProvider, rl revisionLocker, q *uploadQueue) {
outer:
	for {
		// wait for work
		select {
		case <-q.queueChan:
		case <-q.stopChan:
			return
		}

		for {
			// check if we are stopped
			select {
			case <-q.stopChan:
				return
			default:
			}

			// pop the next job
			job := q.pop()
			if job == nil {
				continue outer
			}

			// skip if job is done
			if job.done() {
				continue
			}

			// execute it
			_ = rl.withRevision(job.requestCtx, defaultRevisionFetchTimeout, q.fcid, q.hk, q.siamuxAddr, lockingPriorityUpload, q.blockHeight(), func(rev types.FileContractRevision) error {
				return job.execute(hp, rev)
			})
		}
	}
}

func (q *uploadQueue) finish(id uploadID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.queueUploads, id)
}

func (q *uploadQueue) used(id uploadID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, used := q.queueUploads[id]
	return used
}

func (q *uploadQueue) blockHeight() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bh
}

func (q *uploadQueue) estimate() float64 {
	q.mu.Lock()
	defer q.mu.Unlock()

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
	span := trace.SpanFromContext(j.requestCtx)
	span.SetAttributes(attribute.Stringer("hk", q.hk))
	j.estimate = time.Duration(q.estimate()) * time.Second
	j.queue = q

	q.mu.Lock()
	defer q.mu.Unlock()

	// enqueue the job
	q.queue = append(q.queue, j)
	q.queueUploads[j.id] = struct{}{}

	// signal there's work
	select {
	case q.queueChan <- struct{}{}:
	default:
	}
}

func (q *uploadQueue) track(err error, d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err != nil {
		q.consecutiveFailures++
		q.statsSpeed.track(math.SmallestNonzeroFloat64)
	} else {
		q.consecutiveFailures = 0
		q.statsSpeed.track(mbps(rhpv2.SectorSize, d.Seconds()))
	}
}

func (q *uploadQueue) updateBlockHeight(bh uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.bh = bh
}

func (q *uploadQueue) pop() *uploadJob {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.queue) > 0 {
		j := q.queue[0]
		q.queue = q.queue[1:]
		return j
	}
	return nil
}

func (j *uploadJob) execute(hp hostProvider, rev types.FileContractRevision) (err error) {
	// fetch span from context
	span := trace.SpanFromContext(j.requestCtx)
	span.AddEvent("execute")

	// create a host
	h, err := hp.newHostV3(j.requestCtx, j.queue.fcid, j.queue.hk, j.queue.siamuxAddr)
	if err != nil {
		j.fail(err)
		return
	}

	// upload the sector
	start := time.Now()
	root, err := h.UploadSector(j.requestCtx, j.sector, rev)
	if err != nil {
		j.fail(err)
	} else {
		j.succeed(root)
	}

	// update span
	elapsed := time.Since(start)
	span.SetAttributes(attribute.Int64("duration", elapsed.Milliseconds()))
	span.RecordError(err)
	span.End()

	// update queue
	j.queue.track(err, elapsed)
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
}

func (j *uploadJob) fail(err error) {
	select {
	case j.responseChan <- uploadResponse{
		job: j,
		err: err,
	}:
	case <-time.After(time.Second):
	}
}

func (j *uploadJob) done() bool {
	select {
	case <-j.requestCtx.Done():
		return true
	default:
		return false
	}
}

func (s *uploadState) launch(job *uploadJob) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.numInflight++
	s.numLaunched++
	if job.overdrive {
		s.lastOverdrive = time.Now()
		s.overdriving[job.sectorIndex]++
	}
}

func (s *uploadState) receive(resp uploadResponse) bool {
	fmt.Printf("DEBUG PJ: %+x receive resp for sector %d, err %v\n", resp.job.id, resp.job.sectorIndex, resp.err)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.numInflight--

	if resp.err == nil {
		if s.sectors[resp.job.sectorIndex].Root == (types.Hash256{}) {
			s.sectors[resp.job.sectorIndex] = object.Sector{
				Host: resp.job.queue.hk,
				Root: resp.root,
			}
			s.remaining[resp.job.sectorIndex]()
			delete(s.remaining, resp.job.sectorIndex)
		}
		s.numCompleted++
	}

	return len(s.remaining) == 0
}

func (s *uploadState) inflight() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numInflight
}

func (s *uploadState) downloaded() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(s.numCompleted) * rhpv2.SectorSize
}

func (s *uploadState) overdrivePct() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	numOverdrive := int(s.numLaunched) - len(s.sectors)
	if numOverdrive <= 0 {
		return 0
	}

	return float64(numOverdrive) / float64(len(s.sectors))
}

func (s *uploadState) overdrive() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	// overdrive is not kicking in yet
	if uint64(len(s.remaining)) >= s.maxOverdrive {
		return -1
	}

	// overdrive is not due yet
	if time.Since(s.lastOverdrive) < s.sectorTimeout {
		return -1
	}

	// overdrive is maxed out
	if s.numInflight-uint64(len(s.remaining)) >= s.maxOverdrive {
		return -1
	}

	// create a map of remaining sectors
	remaining := make(map[int]struct{})
	for sI, sector := range s.sectors {
		if sector.Root == (types.Hash256{}) {
			remaining[sI] = struct{}{}
		}
	}

	// loop remaining sectors and find a good candidate
	currSI := -1
	lowest := math.MaxInt
	for sI := range remaining {
		timesOverdriven := s.overdriving[sI]
		if timesOverdriven < lowest {
			lowest = int(timesOverdriven)
			currSI = sI
		}
	}

	return currSI
}

func (s *uploadState) finish(hes HostErrorSet) ([]object.Sector, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	remaining := len(s.remaining)
	if remaining > 0 {
		for _, cancel := range s.remaining {
			cancel()
		}
		return nil, fmt.Errorf("failed to upload slab: remaining=%v, inflight=%v, errors=%w", remaining, s.numInflight, hes)
	}
	return s.sectors, nil
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
