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
	defaultPackedSlabsLimit         = 1
)

var (
	errNoCandidateUploader = errors.New("no candidate uploader found")
	errNotEnoughContracts  = errors.New("not enough contracts to support requested redundancy")
)

type uploadParameters struct {
	ec               object.EncryptionKey
	encryptionOffset uint64
	mimeType         string

	rs          api.RedundancySettings
	bh          uint64
	contractSet string
	packing     bool
}

func defaultParameters() uploadParameters {
	return uploadParameters{
		ec:               object.GenerateEncryptionKey(), // random key
		encryptionOffset: 0,                              // from the beginning
		rs:               build.DefaultRedundancySettings,
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

		allowed          map[types.FileContractID]struct{}
		doneShardTrigger chan struct{}
		lockPriority     int

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

func (w *worker) initUploadManager(maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.SugaredLogger) {
	if w.uploadManager != nil {
		panic("upload manager already initialized") // developer error
	}

	w.uploadManager = newUploadManager(w.bus, w, w, maxOverdrive, overdriveTimeout, logger)
}

func (w *worker) upload(ctx context.Context, r io.Reader, bucket, path string, opts ...UploadOption) (string, error) {
	//  build upload parameters
	up := defaultParameters()
	for _, opt := range opts {
		opt(&up)
	}

	// if not given, try decide on a mime type using the file extension
	mimeType := up.mimeType
	if mimeType == "" {
		mimeType = mime.TypeByExtension(filepath.Ext(path))

		// if mime type is still not known, wrap the reader with a mime reader
		if mimeType == "" {
			var err error
			mimeType, r, err = newMimeReader(r)
			if err != nil {
				return "", err
			}
		}
	}

	// perform the upload
	obj, partialSlabData, eTag, err := w.uploadManager.Upload(ctx, r, up, lockingPriorityUpload)
	if err != nil {
		return "", fmt.Errorf("couldn't upload object: %w", err)
	}

	// add partial slabs
	var bufferSizeLimitReached bool
	if len(partialSlabData) > 0 {
		obj.PartialSlabs, bufferSizeLimitReached, err = w.bus.AddPartialSlab(ctx, partialSlabData, uint8(up.rs.MinShards), uint8(up.rs.TotalShards), up.contractSet)
		if err != nil {
			return "", err
		}
	}

	// persist the object
	err = w.bus.AddObject(ctx, bucket, path, up.contractSet, obj, api.AddObjectOptions{MimeType: mimeType, ETag: eTag})
	if err != nil {
		return "", fmt.Errorf("couldn't add object: %w", err)
	}

	// if packing was enabled try uploading packed slabs
	if up.packing {
		if err := w.tryUploadPackedSlabs(ctx, up.rs, up.contractSet, bufferSizeLimitReached); err != nil {
			w.logger.Errorf("couldn't upload packed slabs, err: %v", err)
		}
	}
	return eTag, nil
}

func (w *worker) uploadMultiPart(ctx context.Context, r io.Reader, bucket, path, uploadID string, partNumber int, opts ...UploadOption) (string, error) {
	//  build upload parameters
	up := defaultParameters()
	for _, opt := range opts {
		opt(&up)
	}

	// upload the part
	obj, partialSlabData, eTag, err := w.uploadManager.Upload(ctx, r, up, lockingPriorityUpload)
	if err != nil {
		return "", fmt.Errorf("couldn't upload object: %w", err)
	}

	// add parital slabs
	var bufferSizeLimitReached bool
	if len(partialSlabData) > 0 {
		obj.PartialSlabs, bufferSizeLimitReached, err = w.bus.AddPartialSlab(ctx, partialSlabData, uint8(up.rs.MinShards), uint8(up.rs.TotalShards), up.contractSet)
		if err != nil {
			return "", err
		}
	}

	// persist the part
	err = w.bus.AddMultipartPart(ctx, bucket, path, up.contractSet, eTag, uploadID, partNumber, obj.Slabs, obj.PartialSlabs)
	if err != nil {
		return "", fmt.Errorf("couldn't add multi part: %w", err)
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
	for {
		uploaded, err := w.uploadPackedSlabs(context.Background(), defaultPackedSlabsLockDuration, rs, contractSet, defaultPackedSlabsLimit, lockPriority)
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
		_, err = w.uploadPackedSlabs(ctx, defaultPackedSlabsLockDuration, rs, contractSet, defaultPackedSlabsLimit, lockingPriorityBlockedUpload)
	}

	// make sure there's a goroutine uploading the remainder of the packed slabs
	go w.threadedUploadPackedSlabs(rs, contractSet, lockingPriorityBackgroundUpload)
	return
}

func (w *worker) uploadPackedSlabs(ctx context.Context, lockingDuration time.Duration, rs api.RedundancySettings, contractSet string, limit, lockPriority int) (uploaded int, err error) {
	// fetch packed slabs
	packedSlabs, err := w.bus.PackedSlabsForUpload(ctx, lockingDuration, uint8(rs.MinShards), uint8(rs.TotalShards), contractSet, limit)
	if err != nil {
		return 0, fmt.Errorf("couldn't fetch packed slabs from bus: %v", err)
	}

	// upload packed slabs
	for _, ps := range packedSlabs {
		err = w.uploadPackedSlab(ctx, ps, rs, contractSet, lockPriority)
		if err != nil {
			return
		}
		uploaded++
	}
	return
}

func (w *worker) uploadPackedSlab(ctx context.Context, ps api.PackedSlab, rs api.RedundancySettings, contractSet string, lockPriority int) error {
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
	shards := encryptPartialSlab(ps.Data, ps.Key, uint8(rs.MinShards), uint8(rs.TotalShards))
	sectors, err := w.uploadManager.UploadShards(ctx, shards, contracts, up.CurrentHeight, lockPriority)
	if err != nil {
		return fmt.Errorf("couldn't upload packed slab, err: %v", err)
	}

	// mark packed slab as uploaded
	slab := api.UploadedPackedSlab{BufferID: ps.BufferID, Shards: sectors}
	err = w.bus.MarkPackedSlabsUploaded(ctx, []api.UploadedPackedSlab{slab})
	if err != nil {
		return fmt.Errorf("couldn't mark packed slabs uploaded, err: %v", err)
	}

	return nil
}

func newUploadManager(b Bus, hp hostProvider, rl revisionLocker, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.SugaredLogger) *uploadManager {
	return &uploadManager{
		b:      b,
		hp:     hp,
		rl:     rl,
		logger: logger,

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

func (mgr *uploadManager) Upload(ctx context.Context, r io.Reader, up uploadParameters, lockPriority int) (_ object.Object, partialSlab []byte, eTag string, err error) {
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
		return object.Object{}, nil, "", err
	}

	// fetch contracts
	contracts, err := mgr.b.ContractSetContracts(ctx, up.contractSet)
	if err != nil {
		return object.Object{}, nil, "", fmt.Errorf("couldn't fetch contracts from bus: %w", err)
	}

	// create the upload
	u, finishFn, err := mgr.newUpload(ctx, up.rs.TotalShards, contracts, up.bh, lockPriority)
	if err != nil {
		return object.Object{}, nil, "", err
	}
	defer finishFn()

	// create the next slab channel
	nextSlabChan := make(chan struct{}, 1)
	defer close(nextSlabChan)

	// create the response channel
	respChan := make(chan slabUploadResponse)

	// collect the responses
	var responses []slabUploadResponse
	var slabIndex int
	numSlabs := -1

	// prepare slab size
	size := int64(up.rs.MinShards) * rhpv2.SectorSize
loop:
	for {
		select {
		case <-mgr.stopChan:
			return object.Object{}, nil, "", errors.New("manager was stopped")
		case <-ctx.Done():
			return object.Object{}, nil, "", errors.New("upload timed out")
		case nextSlabChan <- struct{}{}:
			// read next slab's data
			data := make([]byte, size)
			length, err := io.ReadFull(io.LimitReader(cr, size), data)
			if err == io.EOF {
				if slabIndex == 0 {
					break loop
				}
				numSlabs = slabIndex
				if partialSlab != nil {
					numSlabs-- // don't wait on partial slab
				}
				if len(responses) == numSlabs {
					break loop
				}
				continue
			} else if err != nil && err != io.ErrUnexpectedEOF {
				return object.Object{}, nil, "", err
			}
			if up.packing && errors.Is(err, io.ErrUnexpectedEOF) {
				// If uploadPacking is true, we return the partial slab without
				// uploading.
				partialSlab = data[:length]
				<-nextSlabChan // trigger next iteration
			} else {
				// Otherwise we upload it.
				go func(rs api.RedundancySettings, data []byte, length, slabIndex int) {
					u.uploadSlab(ctx, rs, data, length, slabIndex, respChan, nextSlabChan)
				}(up.rs, data, length, slabIndex)
			}
			slabIndex++
		case res := <-respChan:
			if res.err != nil {
				return object.Object{}, nil, "", res.err
			}

			// collect the response and potentially break out of the loop
			responses = append(responses, res)
			if len(responses) == numSlabs {
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
	return o, partialSlab, hr.Hash(), nil
}

func (mgr *uploadManager) UploadShards(ctx context.Context, shards [][]byte, contracts []api.ContractMetadata, bh uint64, lockPriority int) ([]object.Sector, error) {
	// initiate the upload
	upload, finishFn, err := mgr.newUpload(ctx, len(shards), contracts, bh, lockPriority)
	if err != nil {
		return nil, err
	}
	defer finishFn()

	// upload the shards
	sectors, err := upload.uploadShards(ctx, shards, nil)
	if err != nil {
		return nil, err
	}

	// build host to contract map
	h2c := make(map[types.PublicKey]types.FileContractID)
	for _, contract := range contracts {
		h2c[contract.HostKey] = contract.ID
	}
	return sectors, nil
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

		allowed:          allowed,
		doneShardTrigger: make(chan struct{}, 1),
		lockPriority:     lockPriority,

		ongoing: make([]slabID, 0),
		used:    make(map[slabID]map[types.FileContractID]struct{}),
	}, finishFn, nil
}

func (mgr *uploadManager) numUploaders() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return len(mgr.uploaders)
}

func (mgr *uploadManager) candidate(req *sectorUploadReq) *uploader {
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
			if req.upload.canUseUploader(req.sID, uploader) {
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

func (mgr *uploadManager) renewalsMap() map[types.FileContractID]types.FileContractID {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	renewals := make(map[types.FileContractID]types.FileContractID)
	for _, u := range mgr.uploaders {
		fcid, renewedFrom, _ := u.contractInfo()
		if renewedFrom != (types.FileContractID{}) {
			renewals[renewedFrom] = fcid
		}
	}
	return renewals
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

func (u *upload) uploadSlab(ctx context.Context, rs api.RedundancySettings, data []byte, length, index int, respChan chan slabUploadResponse, nextSlabChan chan struct{}) {
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
	resp.slab.Slab.Shards, resp.err = u.uploadShards(ctx, shards, nextSlabChan)

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

func (u *upload) uploadShards(ctx context.Context, shards [][]byte, nextSlabChan chan struct{}) ([]object.Sector, error) {
	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "uploadShards")
	defer span.End()

	// prepare the upload
	slab, requests, respChan := u.newSlabUpload(ctx, shards)
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
			case <-nextSlabChan:
				triggered = true
			default:
			}
		}

		// relaunch non-overdrive uploads
		if !done && resp.err != nil && !resp.req.overdrive {
			if overdriving, err := slab.launch(resp.req); err != nil {
				u.mgr.logger.Errorf("failed to relaunch a sector upload, err %v", err)
				if !overdriving {
					break // fail the upload
				}
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
		case <-nextSlabChan:
			triggered = true
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

func (s *slabUpload) receive(resp sectorUploadResp) (finished bool, next bool) {
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
		return false, false
	}

	// redundant sectors can't complete the upload
	if s.sectors[resp.req.sectorIndex].Root != (types.Hash256{}) {
		return false, false
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
	finished = len(s.remaining) == 0
	next = len(s.remaining) <= int(s.mgr.maxOverdrive)
	return
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
