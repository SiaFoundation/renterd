package worker

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/stats"
	"go.sia.tech/renterd/tracing"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	statsRecomputeMinInterval = 3 * time.Second

	defaultPackedSlabsLockDuration  = 10 * time.Minute
	defaultPackedSlabsUploadTimeout = 10 * time.Minute
)

var (
	errUploadManagerStopped = errors.New("upload manager stopped")
	errNoCandidateUploader  = errors.New("no candidate uploader found")
	errNotEnoughContracts   = errors.New("not enough contracts to support requested redundancy")
)

type uploadParameters struct {
	bucket string
	path   string

	multipart  bool
	uploadID   string
	partNumber int

	ec               object.EncryptionKey
	encryptionOffset uint64

	rs          api.RedundancySettings
	bh          uint64
	contractSet string
	packing     bool
	mimeType    string
}

func defaultParameters(bucket, path string) uploadParameters {
	return uploadParameters{
		bucket: bucket,
		path:   path,

		ec:               object.GenerateEncryptionKey(), // random key
		encryptionOffset: 0,                              // from the beginning

		rs: build.DefaultRedundancySettings,
	}
}

func multipartParameters(bucket, path, uploadID string, partNumber int) uploadParameters {
	return uploadParameters{
		bucket: bucket,
		path:   path,

		multipart:  true,
		uploadID:   uploadID,
		partNumber: partNumber,

		ec:               object.GenerateEncryptionKey(), // random key
		encryptionOffset: 0,                              // from the beginning

		rs: build.DefaultRedundancySettings,
	}
}

type UploadOption func(*uploadParameters)

func WithBlockHeight(bh uint64) UploadOption {
	return func(up *uploadParameters) {
		up.bh = bh
	}
}

func WithContractSet(contractSet string) UploadOption {
	return func(up *uploadParameters) {
		up.contractSet = contractSet
	}
}

func WithCustomKey(ec object.EncryptionKey) UploadOption {
	return func(up *uploadParameters) {
		up.ec = ec
	}
}

func WithCustomEncryptionOffset(offset uint64) UploadOption {
	return func(up *uploadParameters) {
		up.encryptionOffset = offset
	}
}

func WithMimeType(mimeType string) UploadOption {
	return func(up *uploadParameters) {
		up.mimeType = mimeType
	}
}

func WithPacking(packing bool) UploadOption {
	return func(up *uploadParameters) {
		up.packing = packing
	}
}

func WithRedundancySettings(rs api.RedundancySettings) UploadOption {
	return func(up *uploadParameters) {
		up.rs = rs
	}
}

type (
	slabID [8]byte

	uploadManager struct {
		b      Bus
		hp     hostProvider
		rl     revisionLocker
		logger *zap.SugaredLogger
		mm     *memoryManager

		maxOverdrive     uint64
		overdriveTimeout time.Duration

		statsOverdrivePct              *stats.DataPoints
		statsSlabUploadSpeedBytesPerMS *stats.DataPoints
		stopChan                       chan struct{}

		mu            sync.Mutex
		uploaders     []*uploader
		lastRecompute time.Time
	}

	uploader struct {
		mgr *uploadManager

		hk         types.PublicKey
		siamuxAddr string

		statsSectorUploadEstimateInMS    *stats.DataPoints
		statsSectorUploadSpeedBytesPerMS *stats.DataPoints // keep track of this separately for stats (no decay is applied)
		signalNewUpload                  chan struct{}
		stopChan                         chan struct{}

		mu                  sync.Mutex
		host                hostV3
		fcid                types.FileContractID
		renewedFrom         types.FileContractID
		endHeight           uint64
		bh                  uint64
		consecutiveFailures uint64
		queue               []*sectorUploadReq
	}

	upload struct {
		id  api.UploadID
		mgr *uploadManager

		allowed      map[types.FileContractID]struct{}
		lockPriority int

		mu   sync.Mutex
		used map[slabID]map[types.FileContractID]struct{}
	}

	slabUpload struct {
		mgr    *uploadManager
		mem    *acquiredMemory
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

		// set by the uploader performing the upload
		hk types.PublicKey
	}

	sectorUploadResp struct {
		req  *sectorUploadReq
		fcid types.FileContractID
		hk   types.PublicKey
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
)

func (w *worker) initUploadManager(mm *memoryManager, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.SugaredLogger) {
	if w.uploadManager != nil {
		panic("upload manager already initialized") // developer error
	}

	w.uploadManager = newUploadManager(w.bus, w, w, mm, maxOverdrive, overdriveTimeout, logger)
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

func newUploadManager(b Bus, hp hostProvider, rl revisionLocker, mm *memoryManager, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.SugaredLogger) *uploadManager {
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

		stopChan: make(chan struct{}),

		uploaders: make([]*uploader, 0),
	}
}

func (mgr *uploadManager) newUploader(c api.ContractMetadata) *uploader {
	return &uploader{
		mgr:  mgr,
		host: mgr.hp.newHostV3(c.ID, c.HostKey, c.SiamuxAddr),

		fcid:       c.ID,
		hk:         c.HostKey,
		siamuxAddr: c.SiamuxAddr,
		endHeight:  c.WindowEnd,

		queue:           make([]*sectorUploadReq, 0),
		signalNewUpload: make(chan struct{}, 1),

		statsSectorUploadEstimateInMS:    stats.Default(),
		statsSectorUploadSpeedBytesPerMS: stats.NoDecay(),
		stopChan:                         make(chan struct{}),
	}
}

func (mgr *uploadManager) Stats() uploadManagerStats {
	// recompute stats
	mgr.tryRecomputeStats()

	// collect stats
	mgr.mu.Lock()
	var numHealthy uint64
	speeds := make(map[types.PublicKey]float64)
	for _, u := range mgr.uploaders {
		healthy, mbps := u.Stats()
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
		u.Stop()
	}
}

func (mgr *uploadManager) Upload(ctx context.Context, r io.Reader, contracts []api.ContractMetadata, up uploadParameters, lockPriority int) (bufferSizeLimitReached bool, eTag string, err error) {
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
	o := object.NewObject(up.ec)

	// create the hash reader
	hr := newHashReader(r)

	// create the cipher reader
	cr, err := o.Encrypt(hr, up.encryptionOffset)
	if err != nil {
		return false, "", err
	}

	// create the upload
	u, finishFn, err := mgr.newUpload(ctx, up.rs.TotalShards, contracts, up.bh, lockPriority)
	if err != nil {
		return false, "", err
	}
	defer finishFn()

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
			case <-mgr.stopChan:
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
					u.uploadSlab(ctx, rs, data, length, slabIndex, respChan, mem)
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
		case <-mgr.stopChan:
			return false, "", errUploadManagerStopped
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
		pss, bufferSizeLimitReached, err = u.mgr.b.AddPartialSlab(ctx, partialSlab, uint8(up.rs.MinShards), uint8(up.rs.TotalShards), up.contractSet)
		if err != nil {
			return false, "", err
		}
		o.Slabs = append(o.Slabs, pss...)
	}

	if up.multipart {
		// persist the part
		err = u.mgr.b.AddMultipartPart(ctx, up.bucket, up.path, up.contractSet, eTag, up.uploadID, up.partNumber, o.Slabs)
		if err != nil {
			return bufferSizeLimitReached, "", fmt.Errorf("couldn't add multi part: %w", err)
		}
	} else {
		// persist the object
		err = u.mgr.b.AddObject(ctx, up.bucket, up.path, up.contractSet, o, api.AddObjectOptions{MimeType: up.mimeType, ETag: eTag})
		if err != nil {
			return bufferSizeLimitReached, "", fmt.Errorf("couldn't add object: %w", err)
		}
	}

	return
}

func (mgr *uploadManager) UploadPackedSlab(ctx context.Context, rs api.RedundancySettings, ps api.PackedSlab, contracts []api.ContractMetadata, bh uint64, lockPriority int, mem *acquiredMemory) error {
	// build the shards
	shards := encryptPartialSlab(ps.Data, ps.Key, uint8(rs.MinShards), uint8(rs.TotalShards))

	// initiate the upload
	upload, finishFn, err := mgr.newUpload(ctx, len(shards), contracts, bh, lockPriority)
	if err != nil {
		return err
	}
	defer finishFn()

	// upload the shards
	sectors, err := upload.uploadShards(ctx, shards, mem)
	if err != nil {
		return err
	}

	// mark packed slab as uploaded
	slab := api.UploadedPackedSlab{BufferID: ps.BufferID, Shards: sectors}
	err = mgr.b.MarkPackedSlabsUploaded(ctx, []api.UploadedPackedSlab{slab})
	if err != nil {
		return fmt.Errorf("couldn't mark packed slabs uploaded, err: %v", err)
	}

	return nil
}

func (mgr *uploadManager) MigrateShards(ctx context.Context, s *object.Slab, shardIndices []int, shards [][]byte, contractSet string, contracts []api.ContractMetadata, bh uint64, lockPriority int, mem *acquiredMemory) error {
	// initiate the upload
	upload, finishFn, err := mgr.newUpload(ctx, len(shards), contracts, bh, lockPriority)
	if err != nil {
		return err
	}
	defer finishFn()

	// upload the shards
	uploaded, err := upload.uploadShards(ctx, shards, mem)
	if err != nil {
		return err
	}

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

func (mgr *uploadManager) launch(req *sectorUploadReq) error {
	// recompute stats
	mgr.tryRecomputeStats()

	// find a candidate uploader
	uploader := mgr.candidate(req)
	if uploader == nil {
		return errNoCandidateUploader
	}
	uploader.enqueue(req)
	return nil
}

func (mgr *uploadManager) newUpload(ctx context.Context, totalShards int, contracts []api.ContractMetadata, bh uint64, lockPriority int) (*upload, func(), error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// refresh the uploaders
	mgr.refreshUploaders(contracts, bh)

	// check if we have enough contracts
	if len(contracts) < totalShards {
		return nil, func() {}, fmt.Errorf("%v < %v: %w", len(contracts), totalShards, errNotEnoughContracts)
	}

	// create allowed map
	allowed := make(map[types.FileContractID]struct{})
	for _, c := range contracts {
		allowed[c.ID] = struct{}{}
	}

	// track the upload in the bus
	id := api.NewUploadID()
	if err := mgr.b.TrackUpload(ctx, id); err != nil {
		mgr.logger.Errorf("failed to track upload '%v', err: %v", id, err)
	}

	// create a finish function to finish the upload
	finishFn := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := mgr.b.FinishUpload(ctx, id); err != nil {
			mgr.logger.Errorf("failed to mark upload %v as finished: %v", id, err)
		}
	}

	// create upload
	return &upload{
		id:  id,
		mgr: mgr,

		allowed:      allowed,
		lockPriority: lockPriority,

		used: make(map[slabID]map[types.FileContractID]struct{}),
	}, finishFn, nil
}

func (mgr *uploadManager) numUploaders() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return len(mgr.uploaders)
}

func (mgr *uploadManager) candidate(req *sectorUploadReq) *uploader {
	// fetch candidate
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// select candidate with the best estimate
	var candidate *uploader
	for _, uploader := range mgr.uploaders {
		if !req.upload.canUseUploader(req.sID, uploader) {
			continue // ignore
		} else if candidate == nil || uploader.estimate() < candidate.estimate() {
			candidate = uploader
		}
	}
	return candidate
}

func (mgr *uploadManager) renewUploader(u *uploader) {
	// fetch renewed contract
	fcid, _, _ := u.contractInfo()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	renewed, err := mgr.b.RenewedContract(ctx, fcid)
	cancel()

	// remove the uploader if we can't renew it
	mgr.mu.Lock()
	if err != nil {
		mgr.logger.Errorf("failed to fetch renewed contract for uploader %v: %v", fcid, err)
		for i := 0; i < len(mgr.uploaders); i++ {
			if mgr.uploaders[i] == u {
				mgr.uploaders = append(mgr.uploaders[:i], mgr.uploaders[i+1:]...)
				u.Stop()
				break
			}
		}
		mgr.mu.Unlock()
		return
	}
	mgr.mu.Unlock()

	// update the uploader if we found the renewed contract
	u.mu.Lock()
	u.host = mgr.hp.newHostV3(renewed.ID, renewed.HostKey, renewed.SiamuxAddr)
	u.fcid = renewed.ID
	u.renewedFrom = renewed.RenewedFrom
	u.endHeight = renewed.WindowEnd
	u.mu.Unlock()

	u.SignalWork()
}

func (mgr *uploadManager) refreshUploaders(contracts []api.ContractMetadata, bh uint64) {
	// build map
	c2m := make(map[types.FileContractID]api.ContractMetadata)
	c2r := make(map[types.FileContractID]struct{})
	for _, c := range contracts {
		c2m[c.ID] = c
		c2r[c.RenewedFrom] = struct{}{}
	}

	// prune expired or renewed contracts
	var refreshed []*uploader
	for _, uploader := range mgr.uploaders {
		fcid, _, endHeight := uploader.contractInfo()
		_, renewed := c2r[fcid]
		if renewed || bh > endHeight {
			uploader.Stop()
			continue
		}
		refreshed = append(refreshed, uploader)
		delete(c2m, fcid)
	}

	// create new uploaders for missing contracts
	for _, c := range c2m {
		uploader := mgr.newUploader(c)
		refreshed = append(refreshed, uploader)
		go uploader.Start(mgr.hp, mgr.rl)
	}

	// update blockheight
	for _, u := range refreshed {
		u.updateBlockHeight(bh)
	}
	mgr.uploaders = refreshed
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

func (u *upload) finishSlabUpload(upload *slabUpload) {
	// cleanup contexts
	upload.mu.Lock()
	for _, shard := range upload.remaining {
		shard.cancel()
	}
	upload.mu.Unlock()
}

func (u *upload) newSlabUpload(ctx context.Context, shards [][]byte, mem *acquiredMemory) (*slabUpload, []*sectorUploadReq, chan sectorUploadResp) {
	// create slab id
	var sID slabID
	frand.Read(sID[:])

	// create slab upload
	slab := &slabUpload{
		mgr: u.mgr,
		mem: mem,

		upload:  u,
		sID:     sID,
		created: time.Now(),
		shards:  shards,

		overdriving: make(map[int]int, len(shards)),
		remaining:   make(map[int]sectorCtx, len(shards)),
		sectors:     make([]object.Sector, len(shards)),
		errs:        make(HostErrorSet),
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

func (u *upload) canUseUploader(sID slabID, ul *uploader) bool {
	fcid, renewedFrom, _ := ul.contractInfo()

	u.mu.Lock()
	defer u.mu.Unlock()

	// check if the uploader is allowed
	_, allowed := u.allowed[fcid]
	if !allowed {
		_, allowed = u.allowed[renewedFrom]
	}
	if !allowed {
		return false
	}

	// check whether we've used it already
	_, used := u.used[sID][fcid]
	if !used {
		_, used = u.used[sID][renewedFrom]
	}
	return !used
}

func (u *upload) uploadSlab(ctx context.Context, rs api.RedundancySettings, data []byte, length, index int, respChan chan slabUploadResponse, mem *acquiredMemory) {
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
	resp.slab.Slab.Shards, resp.err = u.uploadShards(ctx, shards, mem)

	// send the response
	select {
	case <-ctx.Done():
	case respChan <- resp:
	}
}

func (u *upload) markUsed(sID slabID, fcid types.FileContractID) {
	u.mu.Lock()
	defer u.mu.Unlock()

	_, exists := u.used[sID]
	if !exists {
		u.used[sID] = make(map[types.FileContractID]struct{})
	}
	u.used[sID][fcid] = struct{}{}
}

func (u *upload) uploadShards(ctx context.Context, shards [][]byte, mem *acquiredMemory) ([]object.Sector, error) {
	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "uploadShards")
	defer span.End()

	// prepare the upload
	slab, requests, respChan := u.newSlabUpload(ctx, shards, mem)
	span.SetAttributes(attribute.Stringer("id", slab.sID))
	defer u.finishSlabUpload(slab)

	// launch all shard uploads
	for _, upload := range requests {
		if _, err := slab.launch(upload); err != nil {
			return nil, err
		}
	}

	// launch overdrive
	resetOverdrive := slab.overdrive(ctx, respChan)

	// collect responses
	var done bool
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
		done = slab.receive(resp)

		// relaunch non-overdrive uploads
		if !done && resp.err != nil && !resp.req.overdrive {
			if overdriving, err := slab.launch(resp.req); err != nil {
				u.mgr.logger.Errorf("failed to relaunch a sector upload, err %v", err)
				if !overdriving {
					break // fail the upload
				}
			}
		}
	}

	// register the amount of overdrive sectors
	span.SetAttributes(attribute.Int("overdrive", slab.overdriveCnt()))

	// track stats
	u.mgr.statsOverdrivePct.Track(slab.overdrivePct())
	u.mgr.statsSlabUploadSpeedBytesPerMS.Track(float64(slab.uploadSpeed()))
	return slab.finish()
}

func (u *uploader) contractInfo() (types.FileContractID, types.FileContractID, uint64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.fcid, u.renewedFrom, u.endHeight
}

func (u *uploader) SignalWork() {
	select {
	case u.signalNewUpload <- struct{}{}:
	default:
	}
}

func (u *uploader) Start(hp hostProvider, rl revisionLocker) {
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

			// pop the next upload req
			req := u.pop()
			if req == nil {
				continue outer
			}

			// skip if upload is done
			if req.done() {
				continue
			}

			// execute it
			var root types.Hash256
			start := time.Now()
			fcid, _, _ := u.contractInfo()
			err := rl.withRevision(req.ctx, defaultRevisionFetchTimeout, fcid, u.hk, u.siamuxAddr, req.upload.lockPriority, u.blockHeight(), func(rev types.FileContractRevision) error {
				if rev.RevisionNumber == math.MaxUint64 {
					return errMaxRevisionReached
				}

				var err error
				root, err = u.execute(req, rev)
				return err
			})

			// the uploader's contract got renewed, requeue the request, try and refresh the contract
			if errors.Is(err, errMaxRevisionReached) {
				u.requeue(req)
				u.mgr.renewUploader(u)
				continue outer
			}

			// send the response
			if err != nil {
				req.fail(err)
			} else {
				req.succeed(root, u.hk, u.fcid)
			}

			// track the error, ignore gracefully closed streams and canceled overdrives
			canceledOverdrive := req.done() && req.overdrive && err != nil
			if !canceledOverdrive && !isClosedStream(err) {
				u.trackSectorUpload(err, time.Since(start))
			}
		}
	}
}

func (u *uploader) Stop() {
	close(u.stopChan)

	// clear the queue
	for {
		upload := u.pop()
		if upload == nil {
			break
		}
		if !upload.done() {
			upload.fail(errors.New("uploader stopped"))
		}
	}
}

func (u *uploader) Stats() (healthy bool, mbps float64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	healthy = u.consecutiveFailures == 0
	mbps = u.statsSectorUploadSpeedBytesPerMS.Average() * 0.008
	return
}

func (u *uploader) execute(req *sectorUploadReq, rev types.FileContractRevision) (types.Hash256, error) {
	u.mu.Lock()
	host := u.host
	fcid := u.fcid
	u.mu.Unlock()

	// fetch span from context
	span := trace.SpanFromContext(req.ctx)
	span.AddEvent("execute")

	// update the bus
	if err := u.mgr.b.AddUploadingSector(req.ctx, req.upload.id, fcid, rhpv2.SectorRoot(req.sector)); err != nil {
		return types.Hash256{}, fmt.Errorf("failed to add uploading sector to contract %v, err: %v", fcid, err)
	}

	// upload the sector
	start := time.Now()
	root, err := host.UploadSector(req.ctx, req.sector, rev)
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

func (u *uploader) requeue(req *sectorUploadReq) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.queue = append([]*sectorUploadReq{req}, u.queue...)
}

func (u *uploader) enqueue(req *sectorUploadReq) {
	// trace the request
	span := trace.SpanFromContext(req.ctx)
	span.SetAttributes(attribute.Stringer("hk", u.hk))
	span.AddEvent("enqueued")

	// set the host key and enqueue the request
	u.mu.Lock()
	req.hk = u.hk
	u.queue = append(u.queue, req)
	u.mu.Unlock()

	// mark as used
	fcid, _, _ := u.contractInfo()
	req.upload.markUsed(req.sID, fcid)

	// signal there's work
	u.SignalWork()
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

func (req *sectorUploadReq) succeed(root types.Hash256, hk types.PublicKey, fcid types.FileContractID) {
	select {
	case <-req.ctx.Done():
	case req.responseChan <- sectorUploadResp{
		fcid: fcid,
		hk:   hk,
		req:  req,
		root: root,
	}:
	}
}

func (req *sectorUploadReq) fail(err error) {
	select {
	case <-req.ctx.Done():
	case req.responseChan <- sectorUploadResp{
		req: req,
		err: err,
	}:
	}
}

func (req *sectorUploadReq) done() bool {
	select {
	case <-req.ctx.Done():
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
		return nil, fmt.Errorf("failed to upload slab: remaining=%d, inflight=%d, launched=%d uploaders=%d errors=%d %w", remaining, s.numInflight, s.numLaunched, s.mgr.numUploaders(), len(s.errs), s.errs)
	}
	return s.sectors, nil
}

func (s *slabUpload) inflight() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numInflight
}

func (s *slabUpload) launch(req *sectorUploadReq) (overdriving bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// nothing to do
	if req == nil {
		return false, nil
	}

	// launch the req
	err = s.mgr.launch(req)
	if err != nil {
		overdriving = req.overdrive && s.overdriving[req.sectorIndex] > 0
		span := trace.SpanFromContext(req.ctx)
		span.RecordError(err)
		span.End()
		return
	}

	// update the state
	s.numInflight++
	s.numLaunched++
	if req.overdrive {
		s.lastOverdrive = time.Now()
		s.overdriving[req.sectorIndex]++
		overdriving = true
	}
	return
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
					_, _ = s.launch(s.nextRequest(respChan)) // ignore result
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

func (s *slabUpload) receive(resp sectorUploadResp) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// update the state
	if resp.req.overdrive {
		s.overdriving[resp.req.sectorIndex]--
	}

	// failed reqs can't complete the upload
	s.numInflight--
	if resp.err != nil {
		s.errs[resp.req.hk] = resp.err
		return false
	}

	// redundant sectors can't complete the upload
	if s.sectors[resp.req.sectorIndex].Root != (types.Hash256{}) {
		return false
	}

	// store the sector and call cancel on the sector ctx
	s.sectors[resp.req.sectorIndex] = object.Sector{
		Contracts: map[types.PublicKey][]types.FileContractID{
			resp.hk: {
				resp.fcid,
			},
		},
		LatestHost: resp.req.hk,
		Root:       resp.root,
	}
	s.remaining[resp.req.sectorIndex].cancel()

	// update remaining sectors
	delete(s.remaining, resp.req.sectorIndex)

	// release memory
	resp.req.sector = nil
	s.shards[resp.req.sectorIndex] = nil
	s.mem.ReleaseSome(rhpv2.SectorSize)

	return len(s.remaining) == 0
}

func (sID slabID) String() string {
	return fmt.Sprintf("%x", sID[:])
}

func newMimeReader(r io.Reader) (mimeType string, recycled io.Reader, err error) {
	buf := bytes.NewBuffer(nil)
	mtype, err := mimetype.DetectReader(io.TeeReader(r, buf))
	recycled = io.MultiReader(buf, r)
	return mtype.String(), recycled, err
}

type hashReader struct {
	r io.Reader
	h *types.Hasher
}

func newHashReader(r io.Reader) *hashReader {
	return &hashReader{
		r: r,
		h: types.NewHasher(),
	}
}

func (e *hashReader) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if _, wErr := e.h.E.Write(p[:n]); wErr != nil {
		return 0, wErr
	}
	return n, err
}

func (e *hashReader) Hash() string {
	sum := e.h.Sum()
	return hex.EncodeToString(sum[:])
}
