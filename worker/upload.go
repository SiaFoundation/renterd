package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/stats"
	"go.sia.tech/renterd/tracing"
	"go.uber.org/zap"
)

const (
	statsRecomputeMinInterval = 3 * time.Second

	defaultPackedSlabsLockDuration  = 10 * time.Minute
	defaultPackedSlabsUploadTimeout = 10 * time.Minute
)

var (
	errNoCandidateUploader = errors.New("no candidate uploader found")
	errNotEnoughContracts  = errors.New("not enough contracts to support requested redundancy")
	errWorkerShutDown      = errors.New("worker was shut down")
)

type (
	uploadManager struct {
		b           Bus
		hp          hostProvider
		rl          revisionLocker
		mm          MemoryManager
		logger      *zap.SugaredLogger
		shutdownCtx context.Context

		maxOverdrive     uint64
		overdriveTimeout time.Duration

		statsOverdrivePct              *stats.DataPoints
		statsSlabUploadSpeedBytesPerMS *stats.DataPoints

		mu        sync.Mutex
		uploaders []*uploader
	}

	uploadManagerStats struct {
		avgSlabUploadSpeedMBPS float64
		avgOverdrivePct        float64
		healthyUploaders       uint64
		numUploaders           uint64
		uploadSpeedsMBPS       map[types.PublicKey]float64
	}

	upload struct {
		id           api.UploadID
		allowed      map[types.PublicKey]struct{}
		lockPriority int
		shutdownCtx  context.Context
	}

	slabUpload struct {
		uploadID         api.UploadID
		created          time.Time
		lockPriority     int
		maxOverdrive     uint64
		mem              *acquiredMemory
		overdriveTimeout time.Duration

		sectors    []*sectorUpload
		candidates []*candidate // sorted by upload estimate
		shards     [][]byte

		mu             sync.Mutex
		numInflight    uint64
		numLaunched    uint64
		numOverdriving uint64
		numUploaded    uint64

		lastOverdrive time.Time
		errs          HostErrorSet
	}

	candidate struct {
		uploader *uploader

		used        bool
		overdriving int // sector index
	}

	slabUploadResponse struct {
		slab  object.SlabSlice
		index int
		err   error
	}

	sectorUpload struct {
		data     *[rhpv2.SectorSize]byte
		index    int
		root     types.Hash256
		uploaded object.Sector

		ctx    context.Context
		cancel context.CancelFunc
	}

	sectorUploadReq struct {
		lockPriority int
		overdrive    bool
		responseChan chan sectorUploadResp
		sector       *sectorUpload
		uploadID     api.UploadID

		// set by the uploader performing the upload
		fcid types.FileContractID
		hk   types.PublicKey
	}

	sectorUploadResp struct {
		req  *sectorUploadReq
		root types.Hash256
		err  error
	}
)

func (w *worker) initUploadManager(maxMemory, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.SugaredLogger) {
	if w.uploadManager != nil {
		panic("upload manager already initialized") // developer error
	}

	mm := newMemoryManager(logger, maxMemory)
	w.uploadManager = newUploadManager(w.bus, w, w, mm, maxOverdrive, overdriveTimeout, w.shutdownCtx, logger)
}

func (w *worker) upload(ctx context.Context, r io.Reader, contracts []api.ContractMetadata, up uploadParameters, opts ...UploadOption) (_ string, err error) {
	// apply the options
	for _, opt := range opts {
		opt(&up)
	}

	// if not given, try decide on a mime type using the file extension
	if !up.multipart && up.mimeType == "" {
		up.mimeType = mime.TypeByExtension(filepath.Ext(up.path))

		// if mime type is still not known, wrap the reader with a mime reader
		if up.mimeType == "" {
			up.mimeType, r, err = newMimeReader(r)
			if err != nil {
				return
			}
		}
	}

	// perform the upload
	bufferSizeLimitReached, eTag, err := w.uploadManager.Upload(ctx, r, contracts, up, lockingPriorityUpload)
	if err != nil {
		return "", err
	}

	// if packing was enabled try uploading packed slabs
	if up.packing {
		if err := w.tryUploadPackedSlabs(ctx, up.rs, up.contractSet, bufferSizeLimitReached); err != nil {
			w.logger.Errorf("couldn't upload packed slabs, err: %v", err)
		}
	}
	return eTag, nil
}

func (w *worker) threadedUploadPackedSlabs(rs api.RedundancySettings, contractSet string, lockPriority int) {
	key := fmt.Sprintf("%d-%d_%s", rs.MinShards, rs.TotalShards, contractSet)

	w.uploadsMu.Lock()
	if w.uploadingPackedSlabs[key] {
		w.uploadsMu.Unlock()
		return
	}
	w.uploadingPackedSlabs[key] = true
	w.uploadsMu.Unlock()

	// make sure we mark uploading packed slabs as false when we're done
	defer func() {
		w.uploadsMu.Lock()
		w.uploadingPackedSlabs[key] = false
		w.uploadsMu.Unlock()
	}()

	// keep uploading packed slabs until we're done
	ctx := context.WithValue(w.shutdownCtx, keyInteractionRecorder, w)
	for {
		uploaded, err := w.uploadPackedSlabs(ctx, defaultPackedSlabsLockDuration, rs, contractSet, lockPriority)
		if err != nil {
			w.logger.Errorf("couldn't upload packed slabs, err: %v", err)
			return
		} else if uploaded == 0 {
			return
		}
	}
}

func (w *worker) tryUploadPackedSlabs(ctx context.Context, rs api.RedundancySettings, contractSet string, block bool) (err error) {
	// if we want to block, try and upload one packed slab synchronously, we use
	// a slightly higher upload priority to avoid reaching the context deadline
	if block {
		_, err = w.uploadPackedSlabs(ctx, defaultPackedSlabsLockDuration, rs, contractSet, lockingPriorityBlockedUpload)
	}

	// make sure there's a goroutine uploading the remainder of the packed slabs
	go w.threadedUploadPackedSlabs(rs, contractSet, lockingPriorityBackgroundUpload)
	return
}

func (w *worker) uploadPackedSlabs(ctx context.Context, lockingDuration time.Duration, rs api.RedundancySettings, contractSet string, lockPriority int) (uploaded int, err error) {
	// upload packed slabs
	var mu sync.Mutex
	var errs error

	var wg sync.WaitGroup
	totalSize := uint64(rs.TotalShards) * rhpv2.SectorSize

	// derive a context that we can use as an interrupt in case of an error.
	interruptCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		// block until we have memory for a slab or until we are interrupted
		mem := w.uploadManager.mm.AcquireMemory(interruptCtx, totalSize)
		if mem == nil {
			break // interrupted
		}

		// fetch packed slabs to upload
		var packedSlabs []api.PackedSlab
		packedSlabs, err = w.bus.PackedSlabsForUpload(ctx, lockingDuration, uint8(rs.MinShards), uint8(rs.TotalShards), contractSet, 1)
		if err != nil {
			err = fmt.Errorf("couldn't fetch packed slabs from bus: %v", err)
			mem.Release()
			break
		} else if len(packedSlabs) == 0 {
			mem.Release()
			break // no more slabs
		}
		ps := packedSlabs[0]

		// launch upload for slab
		wg.Add(1)
		go func(ps api.PackedSlab) {
			defer mem.Release()
			defer wg.Done()
			err := w.uploadPackedSlab(ctx, ps, rs, contractSet, lockPriority, mem)
			mu.Lock()
			if err != nil {
				errs = errors.Join(errs, err)
				cancel() // prevent new uploads from being launched
			} else {
				uploaded++
			}
			mu.Unlock()
		}(ps)
	}

	// wait for all threads to finish
	wg.Wait()

	// return collected errors
	err = errors.Join(err, errs)
	return
}

func (w *worker) uploadPackedSlab(ctx context.Context, ps api.PackedSlab, rs api.RedundancySettings, contractSet string, lockPriority int, mem *acquiredMemory) error {
	// create a context with sane timeout
	ctx, cancel := context.WithTimeout(ctx, defaultPackedSlabsUploadTimeout)
	defer cancel()

	// fetch contracts
	contracts, err := w.bus.ContractSetContracts(ctx, contractSet)
	if err != nil {
		return fmt.Errorf("couldn't fetch packed slabs from bus: %v", err)
	}

	// fetch upload params
	up, err := w.bus.UploadParams(ctx)
	if err != nil {
		return fmt.Errorf("couldn't fetch upload params from bus: %v", err)
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// upload packed slab
	err = w.uploadManager.UploadPackedSlab(ctx, rs, ps, contracts, up.CurrentHeight, lockPriority, mem)
	if err != nil {
		return fmt.Errorf("couldn't upload packed slab, err: %v", err)
	}

	return nil
}

func newUploadManager(b Bus, hp hostProvider, rl revisionLocker, mm MemoryManager, maxOverdrive uint64, overdriveTimeout time.Duration, shutdownCtx context.Context, logger *zap.SugaredLogger) *uploadManager {
	return &uploadManager{
		b:      b,
		hp:     hp,
		rl:     rl,
		logger: logger,
		mm:     mm,

		maxOverdrive:     maxOverdrive,
		overdriveTimeout: overdriveTimeout,

		statsOverdrivePct:              stats.NoDecay(),
		statsSlabUploadSpeedBytesPerMS: stats.NoDecay(),

		shutdownCtx: shutdownCtx,

		uploaders: make([]*uploader, 0),
	}
}

func (mgr *uploadManager) newUploader(b Bus, hp hostProvider, c api.ContractMetadata, bh uint64) *uploader {
	return &uploader{
		b: b,

		// static
		hk:              c.HostKey,
		siamuxAddr:      c.SiamuxAddr,
		shutdownCtx:     mgr.shutdownCtx,
		signalNewUpload: make(chan struct{}, 1),

		// stats
		statsSectorUploadEstimateInMS:    stats.Default(),
		statsSectorUploadSpeedBytesPerMS: stats.NoDecay(),

		// covered by mutex
		host:      hp.newHostV3(c.ID, c.HostKey, c.SiamuxAddr),
		bh:        bh,
		fcid:      c.ID,
		endHeight: c.WindowEnd,
		queue:     make([]*sectorUploadReq, 0),
	}
}

func (mgr *uploadManager) MigrateShards(ctx context.Context, s *object.Slab, shardIndices []int, shards [][]byte, contractSet string, contracts []api.ContractMetadata, bh uint64, lockPriority int, mem *acquiredMemory) (err error) {
	// cancel all in-flight requests when the upload is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "MigrateShards")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	// create the upload
	upload, err := mgr.newUpload(ctx, len(shards), contracts, bh, lockPriority)
	if err != nil {
		return err
	}

	// track the upload in the bus
	if err := mgr.b.TrackUpload(ctx, upload.id); err != nil {
		return fmt.Errorf("failed to track upload '%v', err: %w", upload.id, err)
	}

	// defer a function that finishes the upload
	defer func() {
		ctx, cancel := context.WithTimeout(mgr.shutdownCtx, time.Minute)
		if err := mgr.b.FinishUpload(ctx, upload.id); err != nil {
			mgr.logger.Errorf("failed to mark upload %v as finished: %v", upload.id, err)
		}
		cancel()
	}()

	// upload the shards
	uploaded, overdrivePct, overdriveSpeed, err := upload.uploadShards(ctx, shards, mgr.candidates(upload.allowed), mem, mgr.maxOverdrive, mgr.overdriveTimeout)
	if err != nil {
		return err
	}

	// track stats
	mgr.statsOverdrivePct.Track(overdrivePct)
	mgr.statsSlabUploadSpeedBytesPerMS.Track(float64(overdriveSpeed))

	// overwrite the shards with the newly uploaded ones
	for i, si := range shardIndices {
		s.Shards[si].LatestHost = uploaded[i].LatestHost

		knownContracts := make(map[types.FileContractID]struct{})
		for _, fcids := range s.Shards[si].Contracts {
			for _, fcid := range fcids {
				knownContracts[fcid] = struct{}{}
			}
		}
		for hk, fcids := range uploaded[i].Contracts {
			for _, fcid := range fcids {
				if _, exists := knownContracts[fcid]; !exists {
					if s.Shards[si].Contracts == nil {
						s.Shards[si].Contracts = make(map[types.PublicKey][]types.FileContractID)
					}
					s.Shards[si].Contracts[hk] = append(s.Shards[si].Contracts[hk], fcid)
				}
			}
		}
	}

	// update the slab
	return mgr.b.UpdateSlab(ctx, *s, contractSet)
}

func (mgr *uploadManager) Stats() uploadManagerStats {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	var numHealthy uint64
	speeds := make(map[types.PublicKey]float64)
	for _, u := range mgr.uploaders {
		speeds[u.hk] = u.statsSectorUploadSpeedBytesPerMS.Average() * 0.008
		if u.Healthy() {
			numHealthy++
		}
	}

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
	for _, u := range mgr.uploaders {
		u.Stop()
	}
}

func (mgr *uploadManager) Upload(ctx context.Context, r io.Reader, contracts []api.ContractMetadata, up uploadParameters, lockPriority int) (bufferSizeLimitReached bool, eTag string, err error) {
	// cancel all in-flight requests when the upload is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "Upload")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	// create the object
	o := object.NewObject(up.ec)

	// create the hash reader
	hr := newHashReader(r)

	// create the cipher reader
	cr, err := o.Encrypt(hr, up.encryptionOffset)
	if err != nil {
		return false, "", err
	}

	// create the upload
	upload, err := mgr.newUpload(ctx, up.rs.TotalShards, contracts, up.bh, lockPriority)
	if err != nil {
		return false, "", err
	}

	// track the upload in the bus
	if err := mgr.b.TrackUpload(ctx, upload.id); err != nil {
		return false, "", fmt.Errorf("failed to track upload '%v', err: %w", upload.id, err)
	}

	// defer a function that finishes the upload
	defer func() {
		ctx, cancel := context.WithTimeout(mgr.shutdownCtx, time.Minute)
		if err := mgr.b.FinishUpload(ctx, upload.id); err != nil {
			mgr.logger.Errorf("failed to mark upload %v as finished: %v", upload.id, err)
		}
		cancel()
	}()

	// create the response channel
	respChan := make(chan slabUploadResponse)

	// channel to notify main thread of the number of slabs to wait for
	numSlabsChan := make(chan int, 1)

	// prepare slab size
	size := int64(up.rs.MinShards) * rhpv2.SectorSize
	redundantSize := uint64(up.rs.TotalShards) * rhpv2.SectorSize
	var partialSlab []byte

	// launch uploads in a separate goroutine
	go func() {
		var slabIndex int
		for {
			select {
			case <-mgr.shutdownCtx.Done():
				return // interrupted
			case <-ctx.Done():
				return // interrupted
			default:
			}
			// acquire memory
			mem := mgr.mm.AcquireMemory(ctx, redundantSize)
			if mem == nil {
				return // interrupted
			}

			// read next slab's data
			data := make([]byte, size)
			length, err := io.ReadFull(io.LimitReader(cr, size), data)
			if err == io.EOF {
				mem.Release()

				// no more data to upload, notify main thread of the number of
				// slabs to wait for
				numSlabs := slabIndex
				if partialSlab != nil && slabIndex > 0 {
					numSlabs-- // don't wait on partial slab
				}
				numSlabsChan <- numSlabs
				return
			} else if err != nil && err != io.ErrUnexpectedEOF {
				mem.Release()

				// unexpected error, notify main thread
				select {
				case respChan <- slabUploadResponse{err: err}:
				case <-ctx.Done():
				}
				return
			} else if up.packing && errors.Is(err, io.ErrUnexpectedEOF) {
				mem.Release()

				// uploadPacking is true, we return the partial slab without
				// uploading.
				partialSlab = data[:length]
			} else {
				// regular upload
				go func(rs api.RedundancySettings, data []byte, length, slabIndex int) {
					upload.uploadSlab(ctx, rs, data, length, slabIndex, respChan, mgr.candidates(upload.allowed), mem, mgr.maxOverdrive, mgr.overdriveTimeout)
					mem.Release()
				}(up.rs, data, length, slabIndex)
			}
			slabIndex++
		}
	}()

	// collect responses
	var responses []slabUploadResponse
	numSlabs := math.MaxInt32
	for len(responses) < numSlabs {
		select {
		case <-mgr.shutdownCtx.Done():
			return false, "", errWorkerShutDown
		case numSlabs = <-numSlabsChan:
		case res := <-respChan:
			if res.err != nil {
				return false, "", res.err
			}
			responses = append(responses, res)
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

	// calculate the eTag
	eTag = hr.Hash()

	// add partial slabs
	if len(partialSlab) > 0 {
		var pss []object.SlabSlice
		pss, bufferSizeLimitReached, err = mgr.b.AddPartialSlab(ctx, partialSlab, uint8(up.rs.MinShards), uint8(up.rs.TotalShards), up.contractSet)
		if err != nil {
			return false, "", err
		}
		o.Slabs = append(o.Slabs, pss...)
	}

	if up.multipart {
		// persist the part
		err = mgr.b.AddMultipartPart(ctx, up.bucket, up.path, up.contractSet, eTag, up.uploadID, up.partNumber, o.Slabs)
		if err != nil {
			return bufferSizeLimitReached, "", fmt.Errorf("couldn't add multi part: %w", err)
		}
	} else {
		// persist the object
		err = mgr.b.AddObject(ctx, up.bucket, up.path, up.contractSet, o, api.AddObjectOptions{MimeType: up.mimeType, ETag: eTag})
		if err != nil {
			return bufferSizeLimitReached, "", fmt.Errorf("couldn't add object: %w", err)
		}
	}

	return
}

func (mgr *uploadManager) UploadPackedSlab(ctx context.Context, rs api.RedundancySettings, ps api.PackedSlab, contracts []api.ContractMetadata, bh uint64, lockPriority int, mem *acquiredMemory) (err error) {
	// cancel all in-flight requests when the upload is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "UploadPackedSlab")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	// build the shards
	shards := encryptPartialSlab(ps.Data, ps.Key, uint8(rs.MinShards), uint8(rs.TotalShards))

	// create the upload
	upload, err := mgr.newUpload(ctx, len(shards), contracts, bh, lockPriority)
	if err != nil {
		return err
	}

	// track the upload in the bus
	if err := mgr.b.TrackUpload(ctx, upload.id); err != nil {
		return fmt.Errorf("failed to track upload '%v', err: %w", upload.id, err)
	}

	// defer a function that finishes the upload
	defer func() {
		ctx, cancel := context.WithTimeout(mgr.shutdownCtx, time.Minute)
		if err := mgr.b.FinishUpload(ctx, upload.id); err != nil {
			mgr.logger.Errorf("failed to mark upload %v as finished: %v", upload.id, err)
		}
		cancel()
	}()

	// upload the shards
	sectors, overdrivePct, overdriveSpeed, err := upload.uploadShards(ctx, shards, mgr.candidates(upload.allowed), mem, mgr.maxOverdrive, mgr.overdriveTimeout)
	if err != nil {
		return err
	}

	// track stats
	mgr.statsOverdrivePct.Track(overdrivePct)
	mgr.statsSlabUploadSpeedBytesPerMS.Track(float64(overdriveSpeed))

	// mark packed slab as uploaded
	slab := api.UploadedPackedSlab{BufferID: ps.BufferID, Shards: sectors}
	err = mgr.b.MarkPackedSlabsUploaded(ctx, []api.UploadedPackedSlab{slab})
	if err != nil {
		return fmt.Errorf("couldn't mark packed slabs uploaded, err: %v", err)
	}

	return nil
}

func (mgr *uploadManager) candidates(allowed map[types.PublicKey]struct{}) (candidates []*uploader) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for _, u := range mgr.uploaders {
		if _, allowed := allowed[u.hk]; allowed {
			candidates = append(candidates, u)
		}
	}

	// sort candidates by upload estimate
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].estimate() < candidates[j].estimate()
	})
	return
}

func (mgr *uploadManager) newUpload(ctx context.Context, totalShards int, contracts []api.ContractMetadata, bh uint64, lockPriority int) (*upload, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// refresh the uploaders
	mgr.refreshUploaders(contracts, bh)

	// check if we have enough contracts
	if len(contracts) < totalShards {
		return nil, fmt.Errorf("%v < %v: %w", len(contracts), totalShards, errNotEnoughContracts)
	}

	// create allowed map
	allowed := make(map[types.PublicKey]struct{})
	for _, c := range contracts {
		allowed[c.HostKey] = struct{}{}
	}

	// create upload
	return &upload{
		id:           api.NewUploadID(),
		allowed:      allowed,
		lockPriority: lockPriority,
		shutdownCtx:  mgr.shutdownCtx,
	}, nil
}

func (mgr *uploadManager) refreshUploaders(contracts []api.ContractMetadata, bh uint64) {
	// build map of contracts to keep and what contracts got renewed
	toKeep := make(map[types.FileContractID]api.ContractMetadata)
	renewedTo := make(map[types.FileContractID]api.ContractMetadata)
	for _, c := range contracts {
		toKeep[c.ID] = c
		if c.RenewedFrom != (types.FileContractID{}) {
			renewedTo[c.RenewedFrom] = c
		}
	}

	// keep list of uploaders uploaders
	var uploaders []*uploader
	for _, uploader := range mgr.uploaders {
		renewal, renewed := renewedTo[uploader.ContractID()]
		if _, keep := toKeep[uploader.ContractID()]; !(keep || renewed) {
			uploader.Stop()
			continue
		} else if renewed {
			uploader.Renew(mgr.hp, renewal, bh)
		}

		// delete current fcid from toKeep, by doing so it becomes a list of the
		// contracts we want to add
		delete(toKeep, uploader.ContractID())

		uploader.UpdateBlockHeight(bh)
		uploader.tryRecomputeStats()
		uploaders = append(uploaders, uploader)
	}

	for _, c := range toKeep {
		uploader := mgr.newUploader(mgr.b, mgr.hp, c, bh)
		uploaders = append(uploaders, uploader)
		go uploader.Start(mgr.hp, mgr.rl)
	}

	mgr.uploaders = uploaders
}

func (u *upload) newSlabUpload(ctx context.Context, shards [][]byte, uploaders []*uploader, mem *acquiredMemory, maxOverdrive uint64, overdriveTimeout time.Duration) (*slabUpload, []*sectorUploadReq, chan sectorUploadResp) {
	// prepare response channel
	responseChan := make(chan sectorUploadResp)

	// prepare sectors
	sectors := make([]*sectorUpload, len(shards))
	for sI, shard := range shards {
		// create the ctx
		sCtx, sCancel := context.WithCancel(ctx)

		// attach the upload's span
		sCtx, span := tracing.Tracer.Start(sCtx, "uploadSector")
		span.SetAttributes(attribute.Bool("overdrive", false))
		span.SetAttributes(attribute.Int("sector", sI))

		// create the sector
		sectors[sI] = &sectorUpload{
			data:   (*[rhpv2.SectorSize]byte)(shard),
			index:  sI,
			root:   rhpv2.SectorRoot((*[rhpv2.SectorSize]byte)(shard)),
			ctx:    sCtx,
			cancel: sCancel,
		}
	}

	// prepare requests
	requests := make([]*sectorUploadReq, len(shards))
	for sI := range shards {
		requests[sI] = &sectorUploadReq{
			lockPriority: u.lockPriority,
			overdrive:    false,
			responseChan: responseChan,
			sector:       sectors[sI],
			uploadID:     u.id,
		}
	}

	// prepare candidates
	candidates := make([]*candidate, len(uploaders))
	for i, uploader := range uploaders {
		candidates[i] = &candidate{uploader: uploader, used: false, overdriving: -1}
	}

	// create slab upload
	return &slabUpload{
		lockPriority:     u.lockPriority,
		uploadID:         u.id,
		created:          time.Now(),
		maxOverdrive:     maxOverdrive,
		mem:              mem,
		overdriveTimeout: overdriveTimeout,

		sectors:    sectors,
		candidates: candidates,
		shards:     shards,

		errs: make(HostErrorSet),
	}, requests, responseChan
}

func (u *upload) uploadSlab(ctx context.Context, rs api.RedundancySettings, data []byte, length, index int, respChan chan slabUploadResponse, candidates []*uploader, mem *acquiredMemory, maxOverdrive uint64, overdriveTimeout time.Duration) (overdrivePct float64, overdriveSpeed int64) {
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
	resp.slab.Slab.Shards, overdrivePct, overdriveSpeed, resp.err = u.uploadShards(ctx, shards, candidates, mem, maxOverdrive, overdriveTimeout)

	// send the response
	select {
	case <-ctx.Done():
	case respChan <- resp:
	}

	return
}

func (u *upload) uploadShards(ctx context.Context, shards [][]byte, candidates []*uploader, mem *acquiredMemory, maxOverdrive uint64, overdriveTimeout time.Duration) ([]object.Sector, float64, int64, error) {
	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "uploadShards")
	defer span.End()

	// ensure inflight uploads get cancelled
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// prepare the upload
	slab, requests, respChan := u.newSlabUpload(ctx, shards, candidates, mem, maxOverdrive, overdriveTimeout)

	// launch all shard uploads
	for _, upload := range requests {
		if _, err := slab.launch(upload); err != nil {
			return nil, 0, 0, err
		}
	}

	// create an overdrive timer
	if overdriveTimeout == 0 {
		overdriveTimeout = time.Duration(math.MaxInt64)
	}
	timer := time.NewTimer(overdriveTimeout)

	// collect responses
	var done bool
loop:
	for slab.inflight() > 0 && !done {
		select {
		case <-u.shutdownCtx.Done():
			return nil, 0, 0, errors.New("upload stopped")
		case <-ctx.Done():
			return nil, 0, 0, ctx.Err()
		case resp := <-respChan:
			// receive the response
			done = slab.receive(resp)

			// relaunch non-overdrive uploads
			if !done && resp.err != nil && !resp.req.overdrive {
				if overdriving, err := slab.launch(resp.req); err != nil {
					if !overdriving {
						break loop // fail the upload
					}
				}
			}

			// try overdriving a sector
			if slab.canOverdrive() {
				_, _ = slab.launch(slab.nextRequest(respChan)) // ignore result
			}
		case <-timer.C:
			// try overdriving a sector
			if slab.canOverdrive() {
				_, _ = slab.launch(slab.nextRequest(respChan)) // ignore result
			}
		}

		// reset the overdrive timer
		if overdriveTimeout != math.MaxInt64 {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(overdriveTimeout)
		}
	}

	// register the amount of overdrive sectors
	span.SetAttributes(attribute.Int("overdrive", slab.overdriveCnt()))

	sectors, err := slab.finish()
	return sectors, slab.overdrivePct(), slab.uploadSpeed(), err
}

func (s *slabUpload) canOverdrive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// overdrive is not kicking in yet
	remaining := uint64(len(s.shards)) - s.numUploaded
	if remaining >= s.maxOverdrive {
		return false
	}

	// overdrive is not due yet
	if time.Since(s.lastOverdrive) < s.overdriveTimeout {
		return false
	}

	// overdrive is maxed out
	if s.numInflight-remaining >= s.maxOverdrive {
		return false
	}

	return true
}

func (s *slabUpload) finish() (sectors []object.Sector, _ error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.numUploaded < uint64(len(s.shards)) {
		remaining := uint64(len(s.shards)) - s.numUploaded
		return nil, fmt.Errorf("failed to upload slab: launched=%d uploaded=%d remaining=%d inflight=%d uploaders=%d errors=%d %w", s.numLaunched, s.numUploaded, remaining, s.numInflight, len(s.candidates), len(s.errs), s.errs)
	}

	for i := 0; i < len(s.shards); i++ {
		sectors = append(sectors, s.sectors[i].uploaded)
	}
	return
}

func (s *slabUpload) inflight() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numInflight
}

func (s *slabUpload) ongoingOverdrive(sI int) bool {
	for _, candidate := range s.candidates {
		if candidate.used && candidate.overdriving == sI {
			return true
		}
	}
	return false
}

func (s *slabUpload) launch(req *sectorUploadReq) (interrupt bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// nothing to do
	if req == nil {
		return false, nil
	}

	// find candidate
	var candidate *candidate
	for _, c := range s.candidates {
		if c.used {
			continue
		}
		candidate = c
		break
	}

	// no candidate found
	if candidate == nil {
		err = errNoCandidateUploader
		interrupt = !req.overdrive && !s.ongoingOverdrive(req.sector.index)
		span := trace.SpanFromContext(req.sector.ctx)
		span.RecordError(err)
		span.End()
		return
	}

	// update the candidate
	candidate.used = true
	if req.overdrive {
		candidate.overdriving = req.sector.index
		s.lastOverdrive = time.Now()
		s.numOverdriving++
	}

	// update the state
	s.numInflight++
	s.numLaunched++

	// enqueue the req
	candidate.uploader.enqueue(req)
	return
}

func (s *slabUpload) nextRequest(responseChan chan sectorUploadResp) *sectorUploadReq {
	s.mu.Lock()
	defer s.mu.Unlock()

	// count overdrives
	overdriveCnts := make(map[int]int)
	for _, c := range s.candidates {
		if c.used && c.overdriving != -1 {
			overdriveCnts[c.overdriving]++
		}
	}

	// overdrive the sector with the least amount of overdrives
	lowestNumOverdrives := math.MaxInt
	var nextSector *sectorUpload
	for sI, sector := range s.sectors {
		if !sector.isUploaded() && overdriveCnts[sI] < lowestNumOverdrives {
			lowestNumOverdrives = overdriveCnts[sI]
			nextSector = sector
		}
	}
	if nextSector == nil {
		return nil
	}

	return &sectorUploadReq{
		lockPriority: s.lockPriority,
		overdrive:    true,
		responseChan: responseChan,
		sector:       nextSector,
		uploadID:     s.uploadID,
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

func (s *slabUpload) receive(resp sectorUploadResp) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// convenience variable
	req := resp.req
	sector := req.sector

	// update the state
	if req.overdrive {
		s.numOverdriving--
	}
	s.numInflight--

	// failed reqs can't complete the upload
	if resp.err != nil {
		s.errs[req.hk] = resp.err
		return false
	}

	// redundant sectors can't complete the upload
	if sector.uploaded.Root != (types.Hash256{}) {
		return false
	}

	// store the sector
	sector.uploaded = object.Sector{
		Contracts:  map[types.PublicKey][]types.FileContractID{req.hk: {req.fcid}},
		LatestHost: req.hk,
		Root:       resp.root,
	}

	// update uploaded sectors
	s.numUploaded++

	// cancel the sector context
	sector.cancel()

	// release hosts that are overdriving this sector
	for _, candidate := range s.candidates {
		if candidate.overdriving == sector.index {
			candidate.overdriving = -1
			candidate.used = false
		}
	}

	// release memory
	sector.data = nil
	s.shards[sector.index] = nil
	s.mem.ReleaseSome(rhpv2.SectorSize)

	return s.numUploaded == uint64(len(s.shards))
}

func (s *slabUpload) uploadSpeed() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	bytes := s.numUploaded * rhpv2.SectorSize
	ms := time.Since(s.created).Milliseconds()
	return int64(bytes) / ms
}

func (s *sectorUpload) isUploaded() bool {
	return s.uploaded.Root != (types.Hash256{})
}

func (req *sectorUploadReq) done() bool {
	select {
	case <-req.sector.ctx.Done():
		return true
	default:
		return false
	}
}

func (req *sectorUploadReq) fail(err error) {
	select {
	case <-req.sector.ctx.Done():
	case req.responseChan <- sectorUploadResp{
		req: req,
		err: err,
	}:
	}
}

func (req *sectorUploadReq) succeed(root types.Hash256) {
	select {
	case <-req.sector.ctx.Done():
	case req.responseChan <- sectorUploadResp{
		req:  req,
		root: root,
	}:
	}
}
