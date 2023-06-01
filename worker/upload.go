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
	errNoFreeQueue    = errors.New("no free queue")
	errNotEnoughHosts = errors.New("not enough hosts to support requested redundancy")
)

type (
	uploadID [8]byte

	uploader struct {
		hp hostProvider
		rl revisionLocker

		overdriveTimeout time.Duration
		maxOverdrive     uint64

		statsOverdrive *dataPoints
		statsSpeed     *dataPoints

		stopChan chan struct{}

		mu        sync.Mutex
		contracts []*uploadQueue
		completed map[uploadID]map[uploadID]map[types.FileContractID]struct{}
		excluded  map[uploadID]map[types.FileContractID]struct{}
		history   map[uploadID][]uploadID
		used      map[uploadID]map[uploadID]map[types.FileContractID]struct{}

		nextSlabTriggers        map[uploadID]chan struct{}
		sectorCompletedTriggers map[uploadID]chan struct{}
	}

	uploadStats struct {
		overdrivePct   float64
		queuesHealthy  uint64
		queuesSpeedAvg float64
		queuesTotal    uint64

		hostStats []hostStats
	}

	hostStats struct {
		hk       types.PublicKey
		estimate float64
		speedAvg float64
	}

	uploadQueue struct {
		fcid       types.FileContractID
		hk         types.PublicKey
		siamuxAddr string

		mu                  sync.Mutex
		bh                  uint64
		consecutiveFailures uint64
		queueChan           chan struct{}
		queue               []*uploadJob

		statsSpeed *dataPoints
		stopChan   chan struct{}
	}

	uploadState struct {
		u        *uploader
		shardID  uploadID
		uploadID uploadID
		created  time.Time

		mu           sync.Mutex
		numCompleted uint64
		numInflight  uint64
		numLaunched  uint64

		nextReadTriggered bool
		lastOverdrive     time.Time
		overdriving       map[int]int
		remaining         map[int]sectorCtx
		sectors           []object.Sector
		errs              HostErrorSet
	}

	uploadJob struct {
		requestCtx context.Context

		overdrive    bool
		responseChan chan sectorResponse

		shardID     uploadID
		uploadID    uploadID
		sector      *[rhpv2.SectorSize]byte
		sectorIndex int
		queue       *uploadQueue
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

	sectorCtx struct {
		ctx    context.Context
		cancel context.CancelFunc
	}

	sectorResponse struct {
		job  *uploadJob
		root types.Hash256
		err  error
	}

	dataPoints struct {
		mu  sync.Mutex
		pts [20]float64
		cnt uint64
		avg float64
	}
)

func newUploadID() uploadID {
	var id uploadID
	frand.Read(id[:])
	return id
}

func (u *uploader) newQueue(c api.ContractMetadata) *uploadQueue {
	return &uploadQueue{
		fcid:       c.ID,
		hk:         c.HostKey,
		siamuxAddr: c.SiamuxAddr,

		queue:     make([]*uploadJob, 0),
		queueChan: make(chan struct{}, 1),

		statsSpeed: newDataPoints(),
		stopChan:   make(chan struct{}),
	}
}

func (u *uploader) numContracts() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.contracts)
}

func (u *uploader) supportsRedundancy(n int, excluded map[types.FileContractID]struct{}) bool {
	u.mu.Lock()
	defer u.mu.Unlock()

	var usable int
	for _, q := range u.contracts {
		if _, ok := excluded[q.fcid]; !ok {
			usable++
		}
	}
	return usable >= n
}

func newDataPoints() *dataPoints {
	return &dataPoints{
		pts: [20]float64{},
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

		maxOverdrive:     maxOverdrive,
		overdriveTimeout: sectorTimeout,

		statsOverdrive: newDataPoints(),
		statsSpeed:     newDataPoints(),

		stopChan: make(chan struct{}),

		contracts: make([]*uploadQueue, 0),
		completed: make(map[uploadID]map[uploadID]map[types.FileContractID]struct{}),
		excluded:  make(map[uploadID]map[types.FileContractID]struct{}),
		history:   make(map[uploadID][]uploadID),
		used:      make(map[uploadID]map[uploadID]map[types.FileContractID]struct{}),

		nextSlabTriggers:        make(map[uploadID]chan struct{}),
		sectorCompletedTriggers: make(map[uploadID]chan struct{}),
	}
}

func (u *uploader) Stats() uploadStats {
	u.mu.Lock()
	defer u.mu.Unlock()

	// prepare stats
	stats := uploadStats{
		overdrivePct:   u.statsOverdrive.recompute(),
		queuesSpeedAvg: u.statsSpeed.recompute(),
		queuesTotal:    uint64(len(u.contracts)),
		hostStats:      make([]hostStats, len(u.contracts)),
	}

	// fill in host stats
	for i, q := range u.contracts {
		stats.hostStats[i] = hostStats{
			hk:       q.hk,
			estimate: q.statsSpeed.recompute(),
			speedAvg: q.statsSpeed.average(),
		}
		stats.queuesHealthy++
	}

	// sort the host stats by speed
	sort.Slice(stats.hostStats, func(i, j int) bool {
		return stats.hostStats[i].speedAvg > stats.hostStats[j].speedAvg
	})

	return stats
}

func (u *uploader) Stop() {
	u.mu.Lock()
	defer u.mu.Unlock()
	close(u.stopChan)
	for _, q := range u.contracts {
		close(q.stopChan)
	}
}

func (u *uploader) newUpload() uploadID {
	u.mu.Lock()
	defer u.mu.Unlock()

	id := newUploadID()
	u.completed[id] = make(map[uploadID]map[types.FileContractID]struct{})
	u.excluded = make(map[uploadID]map[types.FileContractID]struct{})
	u.history[id] = make([]uploadID, 0)
	u.nextSlabTriggers[id] = make(chan struct{}, 1)
	u.nextSlabTriggers[id] <- struct{}{}
	u.sectorCompletedTriggers[id] = make(chan struct{}, 1)
	u.used[id] = make(map[uploadID]map[types.FileContractID]struct{})
	return id
}

func (u *uploader) finishUpload(id uploadID) {
	u.mu.Lock()
	defer u.mu.Unlock()
	delete(u.completed, id)
	delete(u.excluded, id)
	delete(u.history, id)
	delete(u.nextSlabTriggers, id)
	delete(u.sectorCompletedTriggers, id)
	delete(u.used, id)
}

func (u *uploader) prepareUpload(ctx context.Context, uID uploadID, shards [][]byte) (*uploadState, chan sectorResponse, []*uploadJob) {
	u.mu.Lock()
	defer u.mu.Unlock()

	// prepare id
	var id uploadID
	frand.Read(id[:])

	// append the id to the upload history
	u.history[uID] = append(u.history[uID], id)

	// prepare state
	state := &uploadState{
		u:        u,
		shardID:  id,
		uploadID: uID,
		created:  time.Now(),

		overdriving: make(map[int]int, len(shards)),
		remaining:   make(map[int]sectorCtx, len(shards)),
		sectors:     make([]object.Sector, len(shards)),
	}

	// prepare jobs
	responseChan := make(chan sectorResponse)
	jobs := make([]*uploadJob, len(shards))
	for sI, shard := range shards {
		// create the sector's cancel func
		sCtx, cancel := context.WithCancel(ctx)
		state.remaining[sI] = sectorCtx{ctx: sCtx, cancel: cancel}

		// create the job's span
		sCtx, span := tracing.Tracer.Start(sCtx, "uploader.uploadJob")
		span.SetAttributes(attribute.Bool("overdrive", false))
		span.SetAttributes(attribute.Int("sector", sI))

		// create the job
		jobs[sI] = &uploadJob{
			requestCtx:   sCtx,
			responseChan: responseChan,
			shardID:      id,
			uploadID:     uID,
			sector:       (*[rhpv2.SectorSize]byte)(shard),
			sectorIndex:  sI,
		}
	}

	return state, responseChan, jobs
}

func (u *uploader) triggerNextSlab(id uploadID) {
	select {
	case u.nextSlabTriggers[id] <- struct{}{}:
	default:
	}
}

func (u *uploader) triggerCompletedSector(id uploadID) {
	select {
	case u.sectorCompletedTriggers[id] <- struct{}{}:
	default:
	}
}

func (u *uploader) read(ctx context.Context, r io.Reader, rs api.RedundancySettings, id uploadID) chan slabData {
	size := int64(rs.MinShards) * rhpv2.SectorSize
	data := make(chan slabData)

	go func() {
		var err error
		for {
			select {
			case <-u.stopChan:
				err = errors.New("upload stopped")
			case <-ctx.Done():
				err = errors.New("upload timed out")
			case <-u.nextSlabTriggers[id]:
				fmt.Printf("DEBUG PJ: %v | slab read triggered\n", id)
			}

			if err != nil {
				data <- slabData{err: err}
				return
			}

			buf := make([]byte, size)
			length, err := io.ReadFull(io.LimitReader(r, size), buf)
			if err == io.EOF {
				fmt.Printf("DEBUG PJ: %v | slab reads done\n", id)
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

func (u *uploader) upload(ctx context.Context, r io.Reader, rs api.RedundancySettings) (_ object.Object, err error) {
	// sanity check redundancy
	if !u.supportsRedundancy(rs.TotalShards, nil) {
		return object.Object{}, errNotEnoughHosts
	}

	// add cancel
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "uploader.upload")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	// create the id
	id := u.newUpload()
	defer u.finishUpload(id)

	fmt.Printf("DEBUG PJ: %v | started\n", id)

	// create the object
	o := object.NewObject()

	// create the response channel
	slabsChan := make(chan slabResponse)

	// launch the upload
	go func() {
		var wg sync.WaitGroup
		defer func() {
			wg.Wait()
			close(slabsChan)
		}()

		var slabIndex int
		for data := range u.read(ctx, o.Encrypt(r), rs, id) {
			if data.err != nil {
				slabsChan <- slabResponse{err: data.err}
				return
			}

			wg.Add(1)
			go func(buf []byte, length, index int) {
				defer wg.Done()

				// add tracing
				ctx, span := tracing.Tracer.Start(ctx, "uploader.slab")
				defer span.End()

				// create slab
				s := object.NewSlab(uint8(rs.MinShards))

				// create the shards
				shards := make([][]byte, rs.TotalShards)
				s.Encode(buf, shards)
				s.Encrypt(shards)

				// upload the shards
				s.Shards, err = u.uploadShards(ctx, id, shards, index)
				if err != nil {
					select {
					case slabsChan <- slabResponse{err: err}:
					default:
						fmt.Printf("DEBUG PJ: %v | failed to send slab response, err %v\n", id, err)
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
					fmt.Printf("DEBUG PJ: %v | failed to send slab response\n", id)
				}
			}(data.data, data.length, slabIndex)
			slabIndex++
		}
	}()

	// collect the slabs
	var responses []slabResponse
	for res := range slabsChan {
		if res.err != nil {
			return object.Object{}, res.err
		}
		responses = append(responses, res)
	}

	// sort the responses and append the slabs
	sort.Slice(responses, func(i, j int) bool {
		return responses[i].index < responses[j].index
	})
	for _, resp := range responses {
		o.Slabs = append(o.Slabs, resp.slab)
	}
	return o, nil
}

func (u *uploader) migrateShards(ctx context.Context, shards [][]byte, exclude map[types.FileContractID]struct{}) ([]object.Sector, error) {
	// sanity check redundancy
	if !u.supportsRedundancy(len(shards), exclude) {
		return nil, errNotEnoughHosts
	}

	// create the id
	id := u.newUpload()
	defer u.finishUpload(id)

	// exclude the given contracts
	u.mu.Lock()
	u.excluded[id] = exclude
	u.mu.Unlock()

	// upload the shards
	return u.uploadShards(ctx, id, shards, 0)
}

func (u *uploader) uploadShards(ctx context.Context, id uploadID, shards [][]byte, index int) ([]object.Sector, error) {
	ctx, span := tracing.Tracer.Start(ctx, "uploader.uploadShards")
	defer span.End()

	// prepare the upload
	state, sectorChan, jobs := u.prepareUpload(ctx, id, shards)
	span.SetAttributes(attribute.Stringer("id", state.shardID))
	defer state.cleanup()

	fmt.Printf("DEBUG PJ: %v | %v | slab %d started \n", id, state.shardID, index)
	defer fmt.Printf("DEBUG PJ: %v | %v | slab %d finished \n", id, state.shardID, index)

	// launch all jobs
	for _, job := range jobs {
		if err := state.launch(job); err != nil {
			return nil, err
		}
	}

	// create a timer to trigger overdrive
	timeout := time.NewTimer(u.overdriveTimeout)
	resetTimeout := func() {
		timeout.Stop()
		select {
		case <-timeout.C:
		default:
		}
		timeout.Reset(u.overdriveTimeout)
	}

	// launch a goroutine to trigger overdrive
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timeout.C:
				if job := state.overdrive(sectorChan, shards); job != nil {
					_ = state.launch(job) // ignore error
				}
				resetTimeout()
			}
		}
	}()

	// collect responses
	for state.inflight() > 0 {
		var resp sectorResponse
		select {
		case <-u.stopChan:
			return nil, errors.New("upload stopped")
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp = <-sectorChan:
			if resp.err == nil {
				resetTimeout()
			}
		}

		// handle the response
		if done := state.receive(resp); done {
			break
		}

		// relaunch regular jobs
		if resp.err != nil && !resp.job.overdrive {
			if err := state.launch(resp.job); err != nil {
				break // fail the download
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

func (u *uploader) registerCompletedSector(uID, shardID uploadID, fcid types.FileContractID, last bool) {
	u.mu.Lock()
	defer u.mu.Unlock()

	// register completed sector
	_, exists := u.completed[uID][shardID]
	if !exists {
		u.completed[uID][shardID] = make(map[types.FileContractID]struct{})
	}
	u.completed[uID][shardID][fcid] = struct{}{}
	u.triggerCompletedSector(uID)

	// if last sector is completed, we can remove the shard from the history
	if last {
		for i, prev := range u.history[uID] {
			if prev == shardID {
				u.history[uID] = append(u.history[uID][:i], u.history[uID][i+1:]...)
				break
			}
		}
		fmt.Printf("DEBUG PJ: %v | %v | updating history %v\n", uID, shardID, u.history[uID])
	}
}

func (u *uploader) enqueue(j *uploadJob) error {
	queue := u.queue(j)
	if queue == nil {
		return errNoFreeQueue
	}
	queue.push(j)

	u.mu.Lock()
	defer u.mu.Unlock()

	// register queue as used
	_, exists := u.used[j.uploadID][j.shardID]
	if !exists {
		u.used[j.uploadID][j.shardID] = make(map[types.FileContractID]struct{})
	}
	u.used[j.uploadID][j.shardID][queue.fcid] = struct{}{}

	return nil
}

func (u *uploader) queue(j *uploadJob) *uploadQueue {
	u.mu.Lock()
	if len(u.contracts) == 0 {
		u.mu.Unlock()
		return nil
	}

	// recompute the stats first
	for _, q := range u.contracts {
		q.statsSpeed.recompute()
	}

	// sort the contracts by their estimate
	sort.Slice(u.contracts, func(i, j int) bool {
		return u.contracts[i].estimate() < u.contracts[j].estimate()
	})

	// filter queues
	var allowed []*uploadQueue
	for _, q := range u.contracts {
		// filter excluded contracts
		if _, excluded := u.excluded[j.uploadID][q.fcid]; excluded {
			continue
		}
		// filter used queue
		if _, used := u.used[j.uploadID][j.shardID][q.fcid]; used {
			continue
		}
		allowed = append(allowed, q)
	}
	u.mu.Unlock()

	// return early if we have no queues left
	if len(allowed) == 0 {
		return nil
	}

loop:
	for {
		// grab upload history
		u.mu.Lock()
		history := u.history[j.uploadID]
		u.mu.Unlock()

		// limit consecutive slab uploads
		if len(history) < 3 {
			return allowed[0]
		}

		fmt.Printf("DEBUG PJ: %v | %v | no queue yet for sector %d, overdrive %v, allowed %d waiting on %d shards to complete (%v)\n", j.uploadID, j.shardID, j.sectorIndex, j.overdrive, len(allowed), len(history)-1, history)

		// otherwise keep waiting
		select {
		case <-u.sectorCompletedTriggers[j.uploadID]:
			continue loop
		case <-j.requestCtx.Done():
			break loop
		}
	}

	return nil
}

func (u *uploader) update(contracts []api.ContractMetadata, bh uint64) {
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
			var root types.Hash256
			start := time.Now()
			err := rl.withRevision(job.requestCtx, defaultRevisionFetchTimeout, q.fcid, q.hk, q.siamuxAddr, lockingPriorityUpload, q.blockHeight(), func(rev types.FileContractRevision) error {
				var err error
				root, err = job.execute(hp, rev)
				return err
			})

			// track the error, but only if the job is not a cancelled overdrive
			canceledOverdrive := job.done() && job.overdrive && err != nil
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
	span.AddEvent("enqueued")
	j.queue = q

	q.mu.Lock()
	defer q.mu.Unlock()

	// enqueue the job
	q.queue = append(q.queue, j)

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

func (j *uploadJob) execute(hp hostProvider, rev types.FileContractRevision) (types.Hash256, error) {
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
	select {
	case <-j.requestCtx.Done():
	case j.responseChan <- sectorResponse{
		job:  j,
		root: root,
	}:
	}
}

func (j *uploadJob) fail(err error) {
	select {
	case <-j.requestCtx.Done():
	case j.responseChan <- sectorResponse{
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

func (s *uploadState) receive(resp sectorResponse) (completed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// failed jobs can't complete the upload
	s.numInflight--
	if resp.err != nil {
		s.errs = append(s.errs, &HostError{resp.job.queue.hk, resp.err})
		return false
	}

	defer func() {
		s.u.registerCompletedSector(s.uploadID, s.shardID, resp.job.queue.fcid, completed)

		// trigger next slab
		if !s.nextReadTriggered {
			if s.u.numContracts()-len(s.remaining)+int(s.u.maxOverdrive) > len(s.sectors) {
				s.nextReadTriggered = true
				s.u.triggerNextSlab(s.uploadID)
			} else if completed {
				s.nextReadTriggered = true
				s.u.triggerNextSlab(s.uploadID)
			}
		}
	}()

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

	if len(s.remaining)%5 == 0 || len(s.remaining) < 5 {
		fmt.Printf("DEBUG PJ: %v | %v | remaining sectors %d\n", s.uploadID, s.shardID, len(s.remaining))
	}
	return len(s.remaining) == 0
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

func (s *uploadState) overdrive(responseChan chan sectorResponse, shards [][]byte) *uploadJob {
	s.mu.Lock()
	defer s.mu.Unlock()

	// overdrive is not kicking in yet
	if uint64(len(s.remaining)) >= s.u.maxOverdrive {
		return nil
	}

	// overdrive is not due yet
	if time.Since(s.lastOverdrive) < s.u.overdriveTimeout {
		return nil
	}

	// overdrive is maxed out
	if s.numInflight-uint64(len(s.remaining)) >= s.u.maxOverdrive {
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

	fmt.Printf("DEBUG PJ: %v | %v | launching overdrive for sector %d\n", s.uploadID, s.shardID, lowestSI)
	return &uploadJob{
		requestCtx: s.remaining[lowestSI].ctx,

		overdrive:    true,
		responseChan: responseChan,

		sectorIndex: lowestSI,
		sector:      (*[rhpv2.SectorSize]byte)(shards[lowestSI]),
		uploadID:    s.uploadID,
		shardID:     s.shardID,
	}
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
		return nil, fmt.Errorf("failed to upload slab: remaining=%d, inflight=%d, completed=%d launched=%d contracts=%d errors=%w", remaining, s.numInflight, s.numCompleted, s.numLaunched, s.u.numContracts(), s.errs)
	}
	return s.sectors, nil
}

func (id uploadID) String() string {
	return fmt.Sprintf("%x", id[:])
}

func (a *dataPoints) average() float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.avg
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
	a.avg = p90
	return p90
}

func (a *dataPoints) track(p float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pts[a.cnt%uint64(len(a.pts))] = p
	a.cnt++
}

func mbps(b int64, s float64) float64 {
	bps := float64(b) / s
	return bps * 0.000008
}
