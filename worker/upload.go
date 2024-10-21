package worker

import (
	"context"
	"crypto/md5"
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

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
)

const (
	statsRecomputeMinInterval = 3 * time.Second

	defaultPackedSlabsLockDuration  = 10 * time.Minute
	defaultPackedSlabsUploadTimeout = 10 * time.Minute
)

var (
	errContractExpired      = errors.New("contract expired")
	errNoCandidateUploader  = errors.New("no candidate uploader found")
	errNotEnoughContracts   = errors.New("not enough contracts to support requested redundancy")
	errUploadInterrupted    = errors.New("upload was interrupted")
	errSectorUploadFinished = errors.New("sector upload already finished")
)

type (
	uploadManager struct {
		hm     HostManager
		mm     MemoryManager
		os     ObjectStore
		cl     ContractLocker
		cs     ContractStore
		logger *zap.SugaredLogger

		contractLockDuration time.Duration

		maxOverdrive     uint64
		overdriveTimeout time.Duration

		statsOverdrivePct              *utils.DataPoints
		statsSlabUploadSpeedBytesPerMS *utils.DataPoints

		shutdownCtx context.Context

		mu        sync.Mutex
		uploaders []*uploader
	}

	// TODO: should become a metric
	uploadManagerStats struct {
		avgSlabUploadSpeedMBPS float64
		avgOverdrivePct        float64
		healthyUploaders       uint64
		numUploaders           uint64
		uploadSpeedsMBPS       map[types.PublicKey]float64
	}

	upload struct {
		id api.UploadID

		allowed map[types.PublicKey]struct{}

		contractLockPriority int
		contractLockDuration time.Duration

		shutdownCtx context.Context
	}

	slabUpload struct {
		uploadID             api.UploadID
		contractLockPriority int
		contractLockDuration time.Duration

		maxOverdrive  uint64
		lastOverdrive time.Time

		sectors    []*sectorUpload
		candidates []*candidate // sorted by upload estimate

		numLaunched    uint64
		numInflight    uint64
		numOverdriving uint64
		numUploaded    uint64
		numSectors     uint64

		mem Memory

		errs HostErrorSet
	}

	candidate struct {
		uploader *uploader
		req      *sectorUploadReq
	}

	slabUploadResponse struct {
		slab  object.SlabSlice
		index int
		err   error
	}

	sectorUpload struct {
		index int
		root  types.Hash256

		ctx    context.Context
		cancel context.CancelCauseFunc

		mu       sync.Mutex
		uploaded object.Sector
		data     *[rhpv2.SectorSize]byte
	}

	sectorUploadReq struct {
		// upload fields
		uploadID             api.UploadID
		contractLockDuration time.Duration
		contractLockPriority int

		sector       *sectorUpload
		overdrive    bool
		responseChan chan sectorUploadResp

		// set by the uploader performing the upload
		fcid types.FileContractID
		hk   types.PublicKey
	}

	sectorUploadResp struct {
		req *sectorUploadReq
		err error
	}
)

func (w *Worker) initUploadManager(maxMemory, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.Logger) {
	if w.uploadManager != nil {
		panic("upload manager already initialized") // developer error
	}

	w.uploadManager = newUploadManager(w.shutdownCtx, w, w.bus, w.bus, w.bus, maxMemory, maxOverdrive, overdriveTimeout, w.contractLockingDuration, logger)
}

func (w *Worker) upload(ctx context.Context, bucket, path string, rs api.RedundancySettings, r io.Reader, contracts []api.ContractMetadata, opts ...UploadOption) (_ string, err error) {
	// apply the options
	up := defaultParameters(bucket, path, rs)
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

	// return early if worker was shut down or if we don't have to consider
	// packed uploads
	if w.isStopped() || !up.packing {
		return eTag, nil
	}

	// try and upload one slab synchronously
	if bufferSizeLimitReached {
		mem := w.uploadManager.mm.AcquireMemory(ctx, up.rs.SlabSize())
		if mem != nil {
			defer mem.Release()

			// fetch packed slab to upload
			packedSlabs, err := w.bus.PackedSlabsForUpload(ctx, defaultPackedSlabsLockDuration, uint8(up.rs.MinShards), uint8(up.rs.TotalShards), up.contractSet, 1)
			if err != nil {
				w.logger.With(zap.Error(err)).Error("couldn't fetch packed slabs from bus")
			} else if len(packedSlabs) > 0 {
				// upload packed slab
				if err := w.tryUploadPackedSlab(ctx, mem, packedSlabs[0], up.rs, up.contractSet, lockingPriorityBlockedUpload); err != nil {
					w.logger.With(zap.Error(err)).Error("failed to upload packed slab")
				}
			}
		}
	}

	// make sure there's a goroutine uploading any packed slabs
	go w.threadedUploadPackedSlabs(up.rs, up.contractSet, lockingPriorityBackgroundUpload)

	return eTag, nil
}

func (w *Worker) threadedUploadPackedSlabs(rs api.RedundancySettings, contractSet string, lockPriority int) {
	key := fmt.Sprintf("%d-%d_%s", rs.MinShards, rs.TotalShards, contractSet)
	w.uploadsMu.Lock()
	if _, ok := w.uploadingPackedSlabs[key]; ok {
		w.uploadsMu.Unlock()
		return
	}
	w.uploadingPackedSlabs[key] = struct{}{}
	w.uploadsMu.Unlock()

	// make sure we mark uploading packed slabs as false when we're done
	defer func() {
		w.uploadsMu.Lock()
		delete(w.uploadingPackedSlabs, key)
		w.uploadsMu.Unlock()
	}()

	// derive a context that we can use as an interrupt in case of an error or shutdown.
	interruptCtx, interruptCancel := context.WithCancel(w.shutdownCtx)
	defer interruptCancel()

	var wg sync.WaitGroup
	for {
		// block until we have memory
		mem := w.uploadManager.mm.AcquireMemory(interruptCtx, rs.SlabSize())
		if mem == nil {
			break // interrupted
		}

		// fetch packed slab to upload
		packedSlabs, err := w.bus.PackedSlabsForUpload(interruptCtx, defaultPackedSlabsLockDuration, uint8(rs.MinShards), uint8(rs.TotalShards), contractSet, 1)
		if err != nil {
			w.logger.Errorf("couldn't fetch packed slabs from bus: %v", err)
			mem.Release()
			break
		}

		// no more packed slabs to upload
		if len(packedSlabs) == 0 {
			mem.Release()
			break
		}

		wg.Add(1)
		go func(ps api.PackedSlab) {
			defer wg.Done()
			defer mem.Release()

			// we use the background context here, but apply a sane timeout,
			// this ensures ongoing uploads are handled gracefully during
			// shutdown
			ctx, cancel := context.WithTimeout(context.Background(), defaultPackedSlabsUploadTimeout)
			defer cancel()

			// try to upload a packed slab, if there were no packed slabs left to upload ok is false
			if err := w.tryUploadPackedSlab(ctx, mem, ps, rs, contractSet, lockPriority); err != nil {
				w.logger.Error(err)
				interruptCancel() // prevent new uploads from being launched
			}
		}(packedSlabs[0])
	}

	// wait for all threads to finish
	wg.Wait()
}

func (w *Worker) tryUploadPackedSlab(ctx context.Context, mem Memory, ps api.PackedSlab, rs api.RedundancySettings, contractSet string, lockPriority int) error {
	// fetch contracts
	contracts, err := w.bus.Contracts(ctx, api.ContractsOpts{ContractSet: contractSet})
	if err != nil {
		return fmt.Errorf("couldn't fetch contracts from bus: %v", err)
	}

	// fetch upload params
	up, err := w.bus.UploadParams(ctx)
	if err != nil {
		return fmt.Errorf("couldn't fetch upload params from bus: %v", err)
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// upload packed slab
	err = w.uploadManager.UploadPackedSlab(ctx, rs, ps, mem, contracts, up.CurrentHeight, lockPriority)
	if err != nil {
		return fmt.Errorf("couldn't upload packed slab, err: %v", err)
	}

	return nil
}

func newUploadManager(ctx context.Context, hm HostManager, os ObjectStore, cl ContractLocker, cs ContractStore, maxMemory, maxOverdrive uint64, overdriveTimeout time.Duration, contractLockDuration time.Duration, logger *zap.Logger) *uploadManager {
	logger = logger.Named("uploadmanager")
	return &uploadManager{
		hm:     hm,
		mm:     newMemoryManager(maxMemory, logger),
		os:     os,
		cl:     cl,
		cs:     cs,
		logger: logger.Sugar(),

		contractLockDuration: contractLockDuration,

		maxOverdrive:     maxOverdrive,
		overdriveTimeout: overdriveTimeout,

		statsOverdrivePct:              utils.NewDataPoints(0),
		statsSlabUploadSpeedBytesPerMS: utils.NewDataPoints(0),

		shutdownCtx: ctx,

		uploaders: make([]*uploader, 0),
	}
}

func (mgr *uploadManager) newUploader(os ObjectStore, cl ContractLocker, cs ContractStore, hm HostManager, c api.ContractMetadata) *uploader {
	return &uploader{
		os:     os,
		cl:     cl,
		cs:     cs,
		hm:     hm,
		logger: mgr.logger,

		// static
		hk:              c.HostKey,
		siamuxAddr:      c.SiamuxAddr,
		shutdownCtx:     mgr.shutdownCtx,
		signalNewUpload: make(chan struct{}, 1),

		// stats
		statsSectorUploadEstimateInMS:    utils.NewDataPoints(10 * time.Minute),
		statsSectorUploadSpeedBytesPerMS: utils.NewDataPoints(0),

		// covered by mutex
		host:      hm.Host(c.HostKey, c.ID, c.SiamuxAddr),
		fcid:      c.ID,
		endHeight: c.WindowEnd,
		queue:     make([]*sectorUploadReq, 0),
	}
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
		u.Stop(ErrShuttingDown)
	}
}

func (mgr *uploadManager) Upload(ctx context.Context, r io.Reader, contracts []api.ContractMetadata, up uploadParameters, lockPriority int) (bufferSizeLimitReached bool, eTag string, err error) {
	// cancel all in-flight requests when the upload is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create the object
	o := object.NewObject(up.ec)

	// create the md5 hasher for the etag
	// NOTE: we use md5 since it's s3 compatible and clients expect it to be md5
	hasher := md5.New()
	r = io.TeeReader(r, hasher)

	// create the cipher reader
	cr, err := o.Encrypt(r, up.encryptionOffset)
	if err != nil {
		return false, "", err
	}

	// create the upload
	upload, err := mgr.newUpload(up.rs.TotalShards, contracts, up.bh, lockPriority)
	if err != nil {
		return false, "", err
	}

	// track the upload in the bus
	if err := mgr.os.TrackUpload(ctx, upload.id); err != nil {
		return false, "", fmt.Errorf("failed to track upload '%v', err: %w", upload.id, err)
	}

	// defer a function that finishes the upload
	defer func() {
		ctx, cancel := context.WithTimeout(mgr.shutdownCtx, time.Minute)
		if err := mgr.os.FinishUpload(ctx, upload.id); err != nil && !errors.Is(err, context.Canceled) {
			mgr.logger.Errorf("failed to mark upload %v as finished: %v", upload.id, err)
		}
		cancel()
	}()

	// create the response channel
	respChan := make(chan slabUploadResponse)

	// channel to notify main thread of the number of slabs to wait for
	numSlabsChan := make(chan int, 1)

	// prepare slab sizes
	slabSizeNoRedundancy := up.rs.SlabSizeNoRedundancy()
	slabSize := up.rs.SlabSize()
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
			mem := mgr.mm.AcquireMemory(ctx, slabSize)
			if mem == nil {
				return // interrupted
			}

			// read next slab's data
			data := make([]byte, slabSizeNoRedundancy)
			length, err := io.ReadFull(io.LimitReader(cr, int64(slabSizeNoRedundancy)), data)
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
					uploadSpeed, overdrivePct := upload.uploadSlab(ctx, rs, data, length, slabIndex, respChan, mgr.candidates(upload.allowed), mem, mgr.maxOverdrive, mgr.overdriveTimeout)

					// track stats
					mgr.statsSlabUploadSpeedBytesPerMS.Track(float64(uploadSpeed))
					mgr.statsOverdrivePct.Track(overdrivePct)

					// release memory
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
			return false, "", ErrShuttingDown
		case <-ctx.Done():
			return false, "", errUploadInterrupted
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

	// compute etag
	eTag = hex.EncodeToString(hasher.Sum(nil))

	// add partial slabs
	if len(partialSlab) > 0 {
		var pss []object.SlabSlice
		pss, bufferSizeLimitReached, err = mgr.os.AddPartialSlab(ctx, partialSlab, uint8(up.rs.MinShards), uint8(up.rs.TotalShards), up.contractSet)
		if err != nil {
			return false, "", err
		}
		o.Slabs = append(o.Slabs, pss...)
	}

	if up.multipart {
		// persist the part
		err = mgr.os.AddMultipartPart(ctx, up.bucket, up.path, up.contractSet, eTag, up.uploadID, up.partNumber, o.Slabs)
		if err != nil {
			return bufferSizeLimitReached, "", fmt.Errorf("couldn't add multi part: %w", err)
		}
	} else {
		// persist the object
		err = mgr.os.AddObject(ctx, up.bucket, up.path, up.contractSet, o, api.AddObjectOptions{MimeType: up.mimeType, ETag: eTag, Metadata: up.metadata})
		if err != nil {
			return bufferSizeLimitReached, "", fmt.Errorf("couldn't add object: %w", err)
		}
	}

	return
}

func (mgr *uploadManager) UploadPackedSlab(ctx context.Context, rs api.RedundancySettings, ps api.PackedSlab, mem Memory, contracts []api.ContractMetadata, bh uint64, lockPriority int) (err error) {
	// cancel all in-flight requests when the upload is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// build the shards
	shards := encryptPartialSlab(ps.Data, ps.Key, uint8(rs.MinShards), uint8(rs.TotalShards))

	// create the upload
	upload, err := mgr.newUpload(len(shards), contracts, bh, lockPriority)
	if err != nil {
		return err
	}

	// track the upload in the bus
	if err := mgr.os.TrackUpload(ctx, upload.id); err != nil {
		return fmt.Errorf("failed to track upload '%v', err: %w", upload.id, err)
	}

	// defer a function that finishes the upload
	defer func() {
		ctx, cancel := context.WithTimeout(mgr.shutdownCtx, time.Minute)
		if err := mgr.os.FinishUpload(ctx, upload.id); err != nil {
			mgr.logger.Errorf("failed to mark upload %v as finished: %v", upload.id, err)
		}
		cancel()
	}()

	// upload the shards
	sectors, uploadSpeed, overdrivePct, err := upload.uploadShards(ctx, shards, mgr.candidates(upload.allowed), mem, mgr.maxOverdrive, mgr.overdriveTimeout)
	if err != nil {
		return err
	}

	// track stats
	mgr.statsSlabUploadSpeedBytesPerMS.Track(float64(uploadSpeed))
	mgr.statsOverdrivePct.Track(overdrivePct)

	// mark packed slab as uploaded
	slab := api.UploadedPackedSlab{BufferID: ps.BufferID, Shards: sectors}
	err = mgr.os.MarkPackedSlabsUploaded(ctx, []api.UploadedPackedSlab{slab})
	if err != nil {
		return fmt.Errorf("couldn't mark packed slabs uploaded, err: %v", err)
	}

	return nil
}

func (mgr *uploadManager) UploadShards(ctx context.Context, s object.Slab, shardIndices []int, shards [][]byte, contractSet string, contracts []api.ContractMetadata, bh uint64, lockPriority int, mem Memory) (err error) {
	// cancel all in-flight requests when the upload is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create the upload
	upload, err := mgr.newUpload(len(shards), contracts, bh, lockPriority)
	if err != nil {
		return err
	}

	// track the upload in the bus
	if err := mgr.os.TrackUpload(ctx, upload.id); err != nil {
		return fmt.Errorf("failed to track upload '%v', err: %w", upload.id, err)
	}

	// defer a function that finishes the upload
	defer func() {
		ctx, cancel := context.WithTimeout(mgr.shutdownCtx, time.Minute)
		if err := mgr.os.FinishUpload(ctx, upload.id); err != nil {
			mgr.logger.Errorf("failed to mark upload %v as finished: %v", upload.id, err)
		}
		cancel()
	}()

	// upload the shards
	uploaded, uploadSpeed, overdrivePct, err := upload.uploadShards(ctx, shards, mgr.candidates(upload.allowed), mem, mgr.maxOverdrive, mgr.overdriveTimeout)
	if err != nil {
		return err
	}

	// track stats
	mgr.statsOverdrivePct.Track(overdrivePct)
	mgr.statsSlabUploadSpeedBytesPerMS.Track(float64(uploadSpeed))

	// overwrite the shards with the newly uploaded ones
	for i, si := range shardIndices {
		s.Shards[si].LatestHost = uploaded[i].LatestHost
		s.Shards[si].Contracts = make(map[types.PublicKey][]types.FileContractID)
		for hk, fcids := range uploaded[i].Contracts {
			s.Shards[si].Contracts[hk] = append(s.Shards[si].Contracts[hk], fcids...)
		}
	}

	// update the slab
	return mgr.os.UpdateSlab(ctx, s, contractSet)
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

func (mgr *uploadManager) newUpload(totalShards int, contracts []api.ContractMetadata, bh uint64, lockPriority int) (*upload, error) {
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
		id:                   api.NewUploadID(),
		allowed:              allowed,
		contractLockDuration: mgr.contractLockDuration,
		contractLockPriority: lockPriority,
		shutdownCtx:          mgr.shutdownCtx,
	}, nil
}

func (mgr *uploadManager) refreshUploaders(contracts []api.ContractMetadata, bh uint64) {
	// build map of renewals
	renewals := make(map[types.FileContractID]api.ContractMetadata)
	for _, c := range contracts {
		if c.RenewedFrom != (types.FileContractID{}) {
			renewals[c.RenewedFrom] = c
		}
	}

	// refresh uploaders
	var refreshed []*uploader
	existing := make(map[types.FileContractID]struct{})
	for _, uploader := range mgr.uploaders {
		// refresh uploaders that got renewed
		if renewal, renewed := renewals[uploader.ContractID()]; renewed {
			uploader.Refresh(renewal)
		}

		// stop uploaders that expired
		if uploader.Expired(bh) {
			uploader.Stop(errContractExpired)
			continue
		}

		// recompute the stats
		uploader.tryRecomputeStats()

		// add to the list
		refreshed = append(refreshed, uploader)
		existing[uploader.ContractID()] = struct{}{}
	}

	// add missing uploaders
	for _, c := range contracts {
		if _, exists := existing[c.ID]; !exists && bh < c.WindowEnd {
			uploader := mgr.newUploader(mgr.os, mgr.cl, mgr.cs, mgr.hm, c)
			refreshed = append(refreshed, uploader)
			go uploader.Start()
		}
	}

	mgr.uploaders = refreshed
}

func (u *upload) newSlabUpload(ctx context.Context, shards [][]byte, uploaders []*uploader, mem Memory, maxOverdrive uint64) (*slabUpload, chan sectorUploadResp) {
	// prepare response channel
	responseChan := make(chan sectorUploadResp)

	// prepare sectors
	var wg sync.WaitGroup
	sectors := make([]*sectorUpload, len(shards))
	for sI := range shards {
		wg.Add(1)
		go func(idx int) {
			// create the ctx
			sCtx, sCancel := context.WithCancelCause(ctx)

			// create the sector
			// NOTE: we are computing the sector root here and pass it all the
			// way down to the RPC to avoid having to recompute it for the proof
			// verification. This is necessary because we need it ahead of time
			// for the call to AddUploadingSector in uploader.go
			// Once we upload to temp storage we don't need AddUploadingSector
			// anymore and can move it back to the RPC.
			sectors[idx] = &sectorUpload{
				data:   (*[rhpv2.SectorSize]byte)(shards[idx]),
				index:  idx,
				root:   rhpv2.SectorRoot((*[rhpv2.SectorSize]byte)(shards[idx])),
				ctx:    sCtx,
				cancel: sCancel,
			}
			wg.Done()
		}(sI)
	}
	wg.Wait()

	// prepare candidates
	candidates := make([]*candidate, len(uploaders))
	for i, uploader := range uploaders {
		candidates[i] = &candidate{uploader: uploader}
	}

	// create slab upload
	return &slabUpload{
		uploadID: u.id,

		contractLockPriority: u.contractLockPriority,
		contractLockDuration: u.contractLockDuration,

		maxOverdrive: maxOverdrive,
		mem:          mem,

		sectors:    sectors,
		candidates: candidates,
		numSectors: uint64(len(shards)),

		errs: make(HostErrorSet),
	}, responseChan
}

func (u *upload) uploadSlab(ctx context.Context, rs api.RedundancySettings, data []byte, length, index int, respChan chan slabUploadResponse, candidates []*uploader, mem Memory, maxOverdrive uint64, overdriveTimeout time.Duration) (uploadSpeed int64, overdrivePct float64) {
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
	resp.slab.Slab.Shards, uploadSpeed, overdrivePct, resp.err = u.uploadShards(ctx, shards, candidates, mem, maxOverdrive, overdriveTimeout)

	// send the response
	select {
	case <-ctx.Done():
	case respChan <- resp:
	}

	return
}

func (u *upload) uploadShards(ctx context.Context, shards [][]byte, candidates []*uploader, mem Memory, maxOverdrive uint64, overdriveTimeout time.Duration) (sectors []object.Sector, uploadSpeed int64, overdrivePct float64, err error) {
	// ensure inflight uploads get cancelled
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// prepare the upload
	slab, respChan := u.newSlabUpload(ctx, shards, candidates, mem, maxOverdrive)

	// prepare requests
	requests := make([]*sectorUploadReq, len(shards))
	for sI := range shards {
		requests[sI] = &sectorUploadReq{
			uploadID:             slab.uploadID,
			sector:               slab.sectors[sI],
			contractLockPriority: slab.contractLockPriority,
			contractLockDuration: slab.contractLockDuration,
			overdrive:            false,
			responseChan:         respChan,
		}
	}

	// launch all requests
	for _, upload := range requests {
		if err := slab.launch(upload); err != nil {
			return nil, 0, 0, err
		}
	}

	// create an overdrive timer
	if overdriveTimeout == 0 {
		overdriveTimeout = time.Duration(math.MaxInt64)
	}
	timer := time.NewTimer(overdriveTimeout)

	// create a request buffer
	var buffer []*sectorUploadReq

	// start the timer after the upload has started
	// newSlabUpload is quite slow due to computing the sector roots
	start := time.Now()

	// collect responses
	var used bool
	var done bool
loop:
	for slab.numInflight > 0 && !done {
		select {
		case <-u.shutdownCtx.Done():
			return nil, 0, 0, ErrShuttingDown
		case <-ctx.Done():
			return nil, 0, 0, context.Cause(ctx)
		case resp := <-respChan:
			// receive the response
			used, done = slab.receive(resp)
			if done {
				break loop
			}

			// relaunch non-overdrive uploads
			if resp.err != nil && !resp.req.overdrive {
				if err := slab.launch(resp.req); err != nil {
					// a failure to relaunch non-overdrive uploads is bad, but
					// we need to keep them around because an overdrive upload
					// might've been redundant, in which case we can re-use the
					// host to launch this request
					buffer = append(buffer, resp.req)
				}
			} else if resp.err == nil && !used {
				if len(buffer) > 0 {
					// relaunch buffered upload request
					if err := slab.launch(buffer[0]); err == nil {
						buffer = buffer[1:]
					}
				} else if slab.canOverdrive(overdriveTimeout) {
					// or try overdriving a sector
					_ = slab.launch(slab.nextRequest(respChan))
				}
			}
		case <-timer.C:
			// try overdriving a sector
			if slab.canOverdrive(overdriveTimeout) {
				_ = slab.launch(slab.nextRequest(respChan)) // ignore result
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

	// calculate the upload speed
	bytes := slab.numUploaded * rhpv2.SectorSize
	ms := time.Since(start).Milliseconds()
	if ms == 0 {
		ms = 1
	}
	uploadSpeed = int64(bytes) / ms

	// calculate overdrive pct
	var numOverdrive uint64
	if slab.numLaunched > slab.numSectors {
		numOverdrive = slab.numLaunched - slab.numSectors
	}
	overdrivePct = float64(numOverdrive) / float64(slab.numSectors)

	if slab.numUploaded < slab.numSectors {
		remaining := slab.numSectors - slab.numUploaded
		err = fmt.Errorf("failed to upload slab: launched=%d uploaded=%d remaining=%d inflight=%d pending=%d uploaders=%d errors=%d %w", slab.numLaunched, slab.numUploaded, remaining, slab.numInflight, len(buffer), len(slab.candidates), len(slab.errs), slab.errs)
		return
	}

	// collect the sectors
	for _, sector := range slab.sectors {
		sectors = append(sectors, sector.uploaded)
	}
	return
}

func (s *slabUpload) canOverdrive(overdriveTimeout time.Duration) bool {
	// overdrive is not kicking in yet
	remaining := s.numSectors - s.numUploaded
	if remaining > s.maxOverdrive {
		return false
	}

	// overdrive is not due yet
	if time.Since(s.lastOverdrive) < overdriveTimeout {
		return false
	}

	// overdrive is maxed out
	if s.numInflight-remaining >= s.maxOverdrive {
		return false
	}

	return true
}

func (s *slabUpload) launch(req *sectorUploadReq) error {
	// nothing to do
	if req == nil {
		return nil
	}

	// find candidate
	var candidate *candidate
	for _, c := range s.candidates {
		if c.req != nil {
			continue
		}
		candidate = c
		break
	}

	// no candidate found
	if candidate == nil {
		return errNoCandidateUploader
	}

	// update the candidate
	candidate.req = req
	if req.overdrive {
		s.lastOverdrive = time.Now()
		s.numOverdriving++
	}
	// update the state
	s.numInflight++
	s.numLaunched++

	// enqueue the req
	candidate.uploader.enqueue(req)
	return nil
}

func (s *slabUpload) nextRequest(responseChan chan sectorUploadResp) *sectorUploadReq {
	// count overdrives
	overdriveCnts := make(map[int]int)
	for _, c := range s.candidates {
		if c.req != nil && c.req.overdrive {
			overdriveCnts[c.req.sector.index]++
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
		contractLockDuration: s.contractLockDuration,
		contractLockPriority: s.contractLockPriority,
		overdrive:            true,
		responseChan:         responseChan,
		sector:               nextSector,
		uploadID:             s.uploadID,
	}
}

func (s *slabUpload) receive(resp sectorUploadResp) (bool, bool) {
	// convenience variable
	req := resp.req
	sector := req.sector

	// update the state
	if req.overdrive {
		s.numOverdriving--
	}
	s.numInflight--

	// redundant sectors can't complete the upload
	if sector.isUploaded() {
		// release the candidate
		for _, candidate := range s.candidates {
			if candidate.req == req {
				candidate.req = nil
				break
			}
		}
		return false, false
	}

	// failed reqs can't complete the upload, we do this after the isUploaded
	// check since any error returned for a redundant sector is probably a
	// result of the sector ctx being closed
	if resp.err != nil {
		s.errs[req.hk] = resp.err
		return false, false
	}

	// store the sector
	sector.finish(object.Sector{
		Contracts:  map[types.PublicKey][]types.FileContractID{req.hk: {req.fcid}},
		LatestHost: req.hk,
		Root:       req.sector.root,
	})

	// update uploaded sectors
	s.numUploaded++

	// release memory
	s.mem.ReleaseSome(rhpv2.SectorSize)

	return true, s.numUploaded == s.numSectors
}

func (s *sectorUpload) finish(sector object.Sector) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancel(errSectorUploadFinished)
	s.uploaded = sector
	s.data = nil
}

func (s *sectorUpload) isUploaded() bool {
	return s.uploaded.Root != (types.Hash256{})
}

func (s *sectorUpload) sectorData() *[rhpv2.SectorSize]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data
}

func (req *sectorUploadReq) done() bool {
	select {
	case <-req.sector.ctx.Done():
		return true
	default:
		return false
	}
}

func (req *sectorUploadReq) finish(err error) {
	select {
	case <-req.sector.ctx.Done():
	case req.responseChan <- sectorUploadResp{
		req: req,
		err: err,
	}:
	}
}
