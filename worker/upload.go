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
	"lukechampine.com/frand"
)

var (
	errNoFreeQueue = errors.New("no free queue")
)

type (
	migrator interface {
		uploadShards(ctx context.Context, rs api.RedundancySettings, shards [][]byte, contracts []api.ContractMetadata, used map[types.FileContractID]struct{}, blockHeight uint64) ([]object.Sector, error)
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
		queue       *uploadQueue
	}

	uploadResponse struct {
		job  *uploadJob
		root types.Hash256
		hk   types.PublicKey
		err  error
	}

	uploadState struct {
		u       *uploader
		id      uploadID
		created time.Time

		mu           sync.Mutex
		numCompleted uint64
		numInflight  uint64
		numLaunched  uint64

		lastOverdrive time.Time
		overdriving   map[int]int
		remaining     map[int]sectorCtx
		sectors       []object.Sector
		errs          HostErrorSet
	}

	sectorCtx struct {
		ctx    context.Context
		cancel context.CancelFunc
	}

	uploadStats struct {
		overdrivePct   float64
		queuesHealthy  uint64
		queuesSpeedAvg float64
		queuesTotal    uint64
	}

	average struct {
		mu             sync.Mutex
		pts            [20]float64
		cnt            uint64
		updateInterval time.Duration
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

		statsSpeed: newAverage(10 * time.Second),
		stopChan:   make(chan struct{}),
	}
}

func newAverage(updateInterval time.Duration) *average {
	return &average{
		updateInterval: updateInterval,
		pts:            [20]float64{},
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

		statsOverdrive: newAverage(10 * time.Second),
		statsSpeed:     newAverage(10 * time.Second),

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

func (u *uploader) newUpload(rs api.RedundancySettings, exclude map[types.FileContractID]struct{}) (*uploadState, error) {
	// check redundancy
	if !u.supportsRedundancy(rs, exclude) {
		return nil, errors.New("not enough contracts to meet redundancy")
	}

	// prepare id
	var id uploadID
	frand.Read(id[:])

	// prepare excludes
	u.mu.Lock()
	u.exclude[id] = exclude
	u.mu.Unlock()

	// return state
	return &uploadState{
		u:       u,
		id:      id,
		created: time.Now(),

		overdriving: make(map[int]int, rs.TotalShards),
		remaining:   make(map[int]sectorCtx, rs.TotalShards),
		sectors:     make([]object.Sector, rs.TotalShards),
	}, nil
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
			slab.Shards, err = u.uploadShards(ctx, rs, shards, contracts, nil, blockHeight)
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

func (u *uploader) uploadShards(ctx context.Context, rs api.RedundancySettings, shards [][]byte, contracts []api.ContractMetadata, exclude map[types.FileContractID]struct{}, blockHeight uint64) ([]object.Sector, error) {
	ctx, span := tracing.Tracer.Start(ctx, "uploader.uploadShards")
	defer span.End()

	// refresh uploader contracts
	u.updateContracts(contracts, blockHeight)

	// initialize upload
	state, err := u.newUpload(rs, exclude)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	defer state.cleanup()
	defer u.finishUpload(state.id)
	span.SetAttributes(attribute.Stringer("id", state.id))

	// prepare the upload
	responseChan, jobs := state.prepare(ctx, shards)

	// launch all jobs
	for _, job := range jobs {
		if err := state.launch(job); err != nil {
			return nil, err
		}
	}

	// create a timer to trigger overdrive
	timeout := time.NewTimer(u.sectorTimeout)
	resetTimeout := func() {
		timeout.Stop()
		select {
		case <-timeout.C:
		default:
		}
		timeout.Reset(u.sectorTimeout)
	}

	// launch a goroutine to trigger overdrive
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timeout.C:
				state.overdrive(responseChan, shards)
				resetTimeout()
			}
		}
	}()

	// collect responses
	for state.inflight() > 0 {
		var resp uploadResponse
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp = <-responseChan:
			resetTimeout()
		}

		// handle the response
		if done := state.receive(resp); done {
			break
		}

		// relaunch non-overdrive jobs and break on failure
		if resp.err != nil && !resp.job.overdrive {
			if err := state.launch(resp.job); err != nil {
				break // download failed, not enough hosts
			}
		}
	}

	// register the amount of overdrive sectors
	span.SetAttributes(attribute.Int("overdrive", state.overdriveCnt()))

	// track stats
	u.statsOverdrive.track(state.overdrivePct())
	u.statsSpeed.track(state.performance())
	return state.finish()
}

func (u *uploader) enqueue(j *uploadJob) error {
	queue := u.queue(j.id)
	if queue == nil {
		return errNoFreeQueue
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

func (u *uploader) updateContracts(contracts []api.ContractMetadata, bh uint64) {
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

func (u *uploader) supportsRedundancy(rs api.RedundancySettings, exclude map[types.FileContractID]struct{}) bool {
	u.mu.Lock()
	defer u.mu.Unlock()

	var usable int
	for _, q := range u.contracts {
		if _, ok := exclude[q.fcid]; !ok {
			usable++
		}
	}
	return usable >= rs.TotalShards
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
			var root types.Hash256
			start := time.Now()
			err := rl.withRevision(job.requestCtx, defaultRevisionFetchTimeout, q.fcid, q.hk, q.siamuxAddr, lockingPriorityUpload, q.blockHeight(), func(rev types.FileContractRevision) error {
				var err error
				root, err = job.execute(hp, &rev)
				return err
			})
			var canceledOverdrive bool
			select {
			case <-job.requestCtx.Done():
				canceledOverdrive = err != nil && job.overdrive
			default:
			}
			if !canceledOverdrive {
				q.track(err, time.Since(start))
			}
			if err != nil {
				job.fail(err)
			} else {
				job.succeed(root)
			}
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
		speed = math.MaxFloat64
	}

	data := (len(q.queue) + 1) * rhpv2.SectorSize
	return float64(data) * 0.000008 / speed
}

func (q *uploadQueue) push(j *uploadJob) {
	// decorate job
	span := trace.SpanFromContext(j.requestCtx)
	span.SetAttributes(attribute.Stringer("hk", q.hk))
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
		fmt.Println("TRACK ERROR", q.hk, err)
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

func (j *uploadJob) execute(hp hostProvider, rev *types.FileContractRevision) (types.Hash256, error) {
	// fetch span from context
	span := trace.SpanFromContext(j.requestCtx)
	span.AddEvent("execute")

	// create a host
	h, err := hp.newHostV3(j.requestCtx, j.queue.fcid, j.queue.hk, j.queue.siamuxAddr)
	if err != nil {
		return types.Hash256{}, err
	}

	// upload the sector
	start := time.Now()
	root, err := h.UploadSector(j.requestCtx, j.sector, rev)
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

func (j *uploadJob) succeed(root types.Hash256) {
	// on success we try and send the response regardless of whether the ctx is
	// closed (so we can keep track of overdrive), if nobody is listening we
	// assert the ctx was cancelled
	select {
	case <-j.requestCtx.Done():
	case j.responseChan <- uploadResponse{
		job:  j,
		root: root,
	}:
	}
}

func (j *uploadJob) fail(err error) {
	// on failure we try and send but if the ctx is done we don't care
	select {
	case <-j.requestCtx.Done():
	case j.responseChan <- uploadResponse{
		job: j,
		err: err,
	}:
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

func (s *uploadState) launch(job *uploadJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// enqueue the job
	err := s.u.enqueue(job)
	if err != nil {
		span := trace.SpanFromContext(job.requestCtx)
		span.RecordError(err)
		span.End()
		return err
	}

	// update the state
	s.numInflight++
	s.numLaunched++
	if job.overdrive {
		s.lastOverdrive = time.Now()
		s.overdriving[job.sectorIndex]++
	}

	return nil
}

func (s *uploadState) receive(resp uploadResponse) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// failed jobs can't complete the upload
	s.numInflight--
	if resp.err != nil {
		s.errs = append(s.errs, &HostError{resp.job.queue.hk, resp.err})
		return false
	}

	// redundant sectors can't complete the upload
	s.numCompleted++
	if s.sectors[resp.job.sectorIndex].Root != (types.Hash256{}) {
		return false
	}

	// store the sector and call cancel on the sector ctx
	s.sectors[resp.job.sectorIndex] = object.Sector{
		Host: resp.job.queue.hk,
		Root: resp.root,
	}
	s.remaining[resp.job.sectorIndex].cancel()

	// count the sector as complete and check if we're done
	delete(s.remaining, resp.job.sectorIndex)
	return len(s.remaining) == 0
}

func (s *uploadState) prepare(ctx context.Context, shards [][]byte) (chan uploadResponse, []*uploadJob) {
	s.mu.Lock()
	defer s.mu.Unlock()
	responseChan := make(chan uploadResponse)

	jobs := make([]*uploadJob, len(shards))
	for sI, shard := range shards {
		// create the sector's cancel func
		ctx, cancel := context.WithCancel(ctx)
		s.remaining[sI] = sectorCtx{ctx: ctx, cancel: cancel}

		// create the job's span
		_, span := tracing.Tracer.Start(ctx, "uploader.uploadJob")
		span.SetAttributes(attribute.Bool("overdrive", false))
		span.SetAttributes(attribute.Int("sector", sI))

		jobs[sI] = &uploadJob{
			requestCtx:   ctx,
			responseChan: responseChan,
			id:           s.id,
			sector:       (*[rhpv2.SectorSize]byte)(shard),
			sectorIndex:  sI,
		}
	}

	return responseChan, jobs
}

func (s *uploadState) inflight() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numInflight
}

func (s *uploadState) performance() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	downloaded := int64(s.numCompleted) * rhpv2.SectorSize
	elapsed := time.Since(s.created)
	return mbps(downloaded, elapsed.Seconds())
}

func (s *uploadState) overdriveCnt() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.numLaunched) - len(s.sectors)
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

func (s *uploadState) overdrive(responseChan chan uploadResponse, shards [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// overdrive is not kicking in yet
	if uint64(len(s.remaining)) >= s.u.maxOverdrive {
		return
	}

	// overdrive is not due yet
	if time.Since(s.lastOverdrive) < s.u.sectorTimeout {
		return
	}

	// overdrive is maxed out
	if s.numInflight-uint64(len(s.remaining)) >= s.u.maxOverdrive {
		return
	}

	// find a good overdrive candidate
	currSI := -1
	lowest := math.MaxInt
	for sI := range s.remaining {
		timesOverdriven := s.overdriving[sI]
		if timesOverdriven < lowest {
			lowest = int(timesOverdriven)
			currSI = sI
		}
	}
	if currSI == -1 {
		return
	}

	// enqueue the overdrive job
	_ = s.u.enqueue(&uploadJob{
		requestCtx: s.remaining[currSI].ctx,

		overdrive:    true,
		responseChan: responseChan,

		sectorIndex: currSI,
		sector:      (*[rhpv2.SectorSize]byte)(shards[currSI]),
		id:          s.id,
	})
}

func (s *uploadState) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sCtx := range s.remaining {
		sCtx.cancel()
	}
}

func (s *uploadState) finish() ([]object.Sector, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	remaining := len(s.remaining)
	if remaining > 0 {
		return nil, fmt.Errorf("failed to upload slab: remaining=%v, inflight=%v, errors=%w", remaining, s.numInflight, s.errs)
	}
	return s.sectors, nil
}

func (a *average) average() float64 {
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
	return p90
}

func (a *average) track(p float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pts[a.cnt%uint64(len(a.pts))] = p
	a.cnt++
}

func (id uploadID) String() string {
	return fmt.Sprintf("%x", id[:])
}

func mbps(b int64, s float64) float64 {
	bps := float64(b) / s
	return bps * 0.000008
}
