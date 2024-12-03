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
	"go.sia.tech/renterd/internal/host"
	"go.sia.tech/renterd/internal/memory"
	"go.sia.tech/renterd/internal/upload/uploader"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
)

const (
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
		hm        host.HostManager
		mm        memory.MemoryManager
		os        ObjectStore
		cl        ContractLocker
		cs        ContractStore
		uploadKey *utils.UploadKey
		logger    *zap.SugaredLogger

		maxOverdrive     uint64
		overdriveTimeout time.Duration

		statsOverdrivePct              *utils.DataPoints
		statsSlabUploadSpeedBytesPerMS *utils.DataPoints

		shutdownCtx context.Context

		mu        sync.Mutex
		uploaders []*uploader.Uploader
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
		id          api.UploadID
		allowed     map[types.PublicKey]struct{}
		os          ObjectStore
		shutdownCtx context.Context
	}

	uploadedSector struct {
		hk   types.PublicKey
		fcid types.FileContractID
		root types.Hash256
	}

	slabUpload struct {
		uploadID api.UploadID

		maxOverdrive  uint64
		lastOverdrive time.Time

		sectors    []*sectorUpload
		candidates []*candidate // sorted by upload estimate

		numLaunched    uint64
		numInflight    uint64
		numOverdriving uint64
		numUploaded    uint64
		numSectors     uint64

		mem memory.Memory

		errs utils.HostErrorSet
	}

	candidate struct {
		uploader *uploader.Uploader
		req      *uploader.SectorUploadReq
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
		uploaded uploadedSector
		data     *[rhpv2.SectorSize]byte
	}
)

func (us uploadedSector) toObjectSector() object.Sector {
	return object.Sector{
		Contracts: map[types.PublicKey][]types.FileContractID{us.hk: {us.fcid}},
		Root:      us.root,
	}
}

func (w *Worker) initUploadManager(uploadKey *utils.UploadKey, maxMemory, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.Logger) {
	if w.uploadManager != nil {
		panic("upload manager already initialized") // developer error
	}

	w.uploadManager = newUploadManager(w.shutdownCtx, uploadKey, w, w.bus, w.bus, w.bus, maxMemory, maxOverdrive, overdriveTimeout, logger)
}

func (w *Worker) upload(ctx context.Context, bucket, key string, rs api.RedundancySettings, r io.Reader, contracts []api.ContractMetadata, opts ...UploadOption) (_ string, err error) {
	// apply the options
	up := defaultParameters(bucket, key, rs)
	for _, opt := range opts {
		opt(&up)
	}

	// if not given, try decide on a mime type using the file extension
	if !up.multipart && up.mimeType == "" {
		up.mimeType = mime.TypeByExtension(filepath.Ext(up.key))

		// if mime type is still not known, wrap the reader with a mime reader
		if up.mimeType == "" {
			up.mimeType, r, err = newMimeReader(r)
			if err != nil {
				return
			}
		}
	}

	// perform the upload
	bufferSizeLimitReached, eTag, err := w.uploadManager.Upload(ctx, r, contracts, up)
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
			packedSlabs, err := w.bus.PackedSlabsForUpload(ctx, defaultPackedSlabsLockDuration, uint8(up.rs.MinShards), uint8(up.rs.TotalShards), 1)
			if err != nil {
				w.logger.With(zap.Error(err)).Error("couldn't fetch packed slabs from bus")
			} else if len(packedSlabs) > 0 {
				// upload packed slab
				if err := w.uploadPackedSlab(ctx, mem, packedSlabs[0], up.rs); err != nil {
					w.logger.With(zap.Error(err)).Error("failed to upload packed slab")
				}
			}
		}
	}

	// make sure there's a goroutine uploading any packed slabs
	go w.threadedUploadPackedSlabs(up.rs)

	return eTag, nil
}

func (w *Worker) threadedUploadPackedSlabs(rs api.RedundancySettings) {
	key := fmt.Sprintf("%d-%d", rs.MinShards, rs.TotalShards)
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
		packedSlabs, err := w.bus.PackedSlabsForUpload(interruptCtx, defaultPackedSlabsLockDuration, uint8(rs.MinShards), uint8(rs.TotalShards), 1)
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

			// upload packed slab
			if err := w.uploadPackedSlab(ctx, mem, ps, rs); err != nil {
				w.logger.Error(err)
				interruptCancel() // prevent new uploads from being launched
			}
		}(packedSlabs[0])
	}

	// wait for all threads to finish
	wg.Wait()
}

func (w *Worker) uploadPackedSlab(ctx context.Context, mem memory.Memory, ps api.PackedSlab, rs api.RedundancySettings) error {
	// fetch contracts
	contracts, err := w.bus.Contracts(ctx, api.ContractsOpts{FilterMode: api.ContractFilterModeGood})
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
	err = w.uploadManager.UploadPackedSlab(ctx, rs, ps, mem, contracts, up.CurrentHeight)
	if err != nil {
		return fmt.Errorf("couldn't upload packed slab, err: %v", err)
	}

	return nil
}

func newUploadManager(ctx context.Context, uploadKey *utils.UploadKey, hm host.HostManager, os ObjectStore, cl ContractLocker, cs ContractStore, maxMemory, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.Logger) *uploadManager {
	logger = logger.Named("uploadmanager")
	return &uploadManager{
		hm:        hm,
		mm:        memory.NewManager(maxMemory, logger),
		os:        os,
		cl:        cl,
		cs:        cs,
		uploadKey: uploadKey,
		logger:    logger.Sugar(),

		maxOverdrive:     maxOverdrive,
		overdriveTimeout: overdriveTimeout,

		statsOverdrivePct:              utils.NewDataPoints(0),
		statsSlabUploadSpeedBytesPerMS: utils.NewDataPoints(0),

		shutdownCtx: ctx,

		uploaders: make([]*uploader.Uploader, 0),
	}
}

func (mgr *uploadManager) newUploader(cl ContractLocker, cs ContractStore, hm host.HostManager, c api.ContractMetadata) *uploader.Uploader {
	return uploader.New(mgr.shutdownCtx, cl, cs, hm, c, mgr.logger)
}

func (mgr *uploadManager) Stats() uploadManagerStats {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	var numHealthy uint64
	speeds := make(map[types.PublicKey]float64)
	for _, u := range mgr.uploaders {
		speeds[u.PublicKey()] = u.AvgUploadSpeedBytesPerMS() * 0.008
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

func (mgr *uploadManager) Upload(ctx context.Context, r io.Reader, contracts []api.ContractMetadata, up uploadParameters) (bufferSizeLimitReached bool, eTag string, err error) {
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
	cr, err := o.Encrypt(r, object.EncryptionOptions{
		Offset: up.encryptionOffset,
		Key:    mgr.uploadKey,
	})
	if err != nil {
		return false, "", err
	}

	// create the upload
	upload, err := mgr.newUpload(up.rs.TotalShards, contracts, up.bh)
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
		pss, bufferSizeLimitReached, err = mgr.os.AddPartialSlab(ctx, partialSlab, uint8(up.rs.MinShards), uint8(up.rs.TotalShards))
		if err != nil {
			return false, "", err
		}
		o.Slabs = append(o.Slabs, pss...)
	}

	if up.multipart {
		// persist the part
		err = mgr.os.AddMultipartPart(ctx, up.bucket, up.key, eTag, up.uploadID, up.partNumber, o.Slabs)
		if err != nil {
			return bufferSizeLimitReached, "", fmt.Errorf("couldn't add multi part: %w", err)
		}
	} else {
		// persist the object
		err = mgr.os.AddObject(ctx, up.bucket, up.key, o, api.AddObjectOptions{MimeType: up.mimeType, ETag: eTag, Metadata: up.metadata})
		if err != nil {
			return bufferSizeLimitReached, "", fmt.Errorf("couldn't add object: %w", err)
		}
	}

	return
}

func (mgr *uploadManager) UploadPackedSlab(ctx context.Context, rs api.RedundancySettings, ps api.PackedSlab, mem memory.Memory, contracts []api.ContractMetadata, bh uint64) (err error) {
	// cancel all in-flight requests when the upload is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// build the shards
	shards := encryptPartialSlab(ps.Data, ps.EncryptionKey, uint8(rs.MinShards), uint8(rs.TotalShards))

	// create the upload
	upload, err := mgr.newUpload(len(shards), contracts, bh)
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

	// build sectors
	var sectors []api.UploadedSector
	for _, sector := range uploaded {
		sectors = append(sectors, api.UploadedSector{
			ContractID: sector.fcid,
			Root:       sector.root,
		})
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

func (mgr *uploadManager) UploadShards(ctx context.Context, s object.Slab, shardIndices []int, shards [][]byte, contracts []api.ContractMetadata, bh uint64, mem memory.Memory) (err error) {
	// cancel all in-flight requests when the upload is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create the upload
	upload, err := mgr.newUpload(len(shards), contracts, bh)
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

	// build sectors
	var sectors []api.UploadedSector
	for _, sector := range uploaded {
		sectors = append(sectors, api.UploadedSector{
			ContractID: sector.fcid,
			Root:       sector.root,
		})
	}

	// track stats
	mgr.statsOverdrivePct.Track(overdrivePct)
	mgr.statsSlabUploadSpeedBytesPerMS.Track(float64(uploadSpeed))

	// update the slab
	return mgr.os.UpdateSlab(ctx, s.EncryptionKey, sectors)
}

func (mgr *uploadManager) candidates(allowed map[types.PublicKey]struct{}) (candidates []*uploader.Uploader) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for _, u := range mgr.uploaders {
		if _, allowed := allowed[u.PublicKey()]; allowed {
			candidates = append(candidates, u)
		}
	}

	// sort candidates by upload estimate
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Estimate() < candidates[j].Estimate()
	})
	return
}

func (mgr *uploadManager) newUpload(totalShards int, contracts []api.ContractMetadata, bh uint64) (*upload, error) {
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
		id:          api.NewUploadID(),
		allowed:     allowed,
		os:          mgr.os,
		shutdownCtx: mgr.shutdownCtx,
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
	var refreshed []*uploader.Uploader
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
		uploader.TryRecomputeStats()

		// add to the list
		refreshed = append(refreshed, uploader)
		existing[uploader.ContractID()] = struct{}{}
	}

	// add missing uploaders
	for _, c := range contracts {
		if _, exists := existing[c.ID]; !exists && bh < c.WindowEnd {
			uploader := mgr.newUploader(mgr.cl, mgr.cs, mgr.hm, c)
			refreshed = append(refreshed, uploader)
			go uploader.Start()
		}
	}

	mgr.uploaders = refreshed
}

func (u *upload) newSlabUpload(ctx context.Context, shards [][]byte, uploaders []*uploader.Uploader, mem memory.Memory, maxOverdrive uint64) (*slabUpload, chan uploader.SectorUploadResp) {
	// prepare response channel
	responseChan := make(chan uploader.SectorUploadResp)

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

		maxOverdrive: maxOverdrive,
		mem:          mem,

		sectors:    sectors,
		candidates: candidates,
		numSectors: uint64(len(shards)),

		errs: make(utils.HostErrorSet),
	}, responseChan
}

func (u *upload) uploadSlab(ctx context.Context, rs api.RedundancySettings, data []byte, length, index int, respChan chan slabUploadResponse, candidates []*uploader.Uploader, mem memory.Memory, maxOverdrive uint64, overdriveTimeout time.Duration) (int64, float64) {
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
	uploaded, uploadSpeed, overdrivePct, err := u.uploadShards(ctx, shards, candidates, mem, maxOverdrive, overdriveTimeout)

	// build the sectors
	var sectors []object.Sector
	for _, sector := range uploaded {
		sectors = append(sectors, sector.toObjectSector())
	}

	// decorate the response
	resp.err = err
	resp.slab.Shards = sectors

	// send the response
	select {
	case <-ctx.Done():
	case respChan <- resp:
	}

	return uploadSpeed, overdrivePct
}

func (u *upload) uploadShards(ctx context.Context, shards [][]byte, candidates []*uploader.Uploader, mem memory.Memory, maxOverdrive uint64, overdriveTimeout time.Duration) (sectors []uploadedSector, uploadSpeed int64, overdrivePct float64, err error) {
	// ensure inflight uploads get cancelled
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// prepare the upload
	slab, respChan := u.newSlabUpload(ctx, shards, candidates, mem, maxOverdrive)

	// prepare requests
	requests := make([]*uploader.SectorUploadReq, len(shards))
	roots := make([]types.Hash256, len(shards))
	for sI := range shards {
		s := slab.sectors[sI]
		requests[sI] = uploader.NewUploadRequest(s.ctx, s.data, sI, respChan, s.root, false)
		roots[sI] = slab.sectors[sI].root
	}

	// notify bus about roots
	if err := u.os.AddUploadingSectors(ctx, u.id, roots); err != nil {
		return nil, 0, 0, fmt.Errorf("failed to add sector to uploading sectors: %w", err)
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
	var buffer []*uploader.SectorUploadReq

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
			if resp.Err != nil && !resp.Req.Overdrive {
				if err := slab.launch(resp.Req); err != nil {
					// a failure to relaunch non-overdrive uploads is bad, but
					// we need to keep them around because an overdrive upload
					// might've been redundant, in which case we can re-use the
					// host to launch this request
					buffer = append(buffer, resp.Req)
				}
			} else if resp.Err == nil && !used {
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

func (s *slabUpload) launch(req *uploader.SectorUploadReq) error {
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
	if req.Overdrive {
		s.lastOverdrive = time.Now()
		s.numOverdriving++
	}
	// update the state
	s.numInflight++
	s.numLaunched++

	// enqueue the req
	candidate.uploader.Enqueue(req)
	return nil
}

func (s *slabUpload) nextRequest(responseChan chan uploader.SectorUploadResp) *uploader.SectorUploadReq {
	// count overdrives
	overdriveCnts := make(map[int]int)
	for _, c := range s.candidates {
		if c.req != nil && c.req.Overdrive {
			overdriveCnts[c.req.Idx]++
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

	return uploader.NewUploadRequest(nextSector.ctx, nextSector.data, nextSector.index, responseChan, nextSector.root, true)
}

func (s *slabUpload) receive(resp uploader.SectorUploadResp) (bool, bool) {
	// convenience variable
	req := resp.Req
	sector := s.sectors[req.Idx]

	// update the state
	if req.Overdrive {
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
	if resp.Err != nil {
		s.errs[resp.HK] = resp.Err
		return false, false
	}

	// store the sector
	sector.finish(resp)

	// update uploaded sectors
	s.numUploaded++

	// release memory
	s.mem.ReleaseSome(rhpv2.SectorSize)

	return true, s.numUploaded == s.numSectors
}

func (s *sectorUpload) finish(resp uploader.SectorUploadResp) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancel(errSectorUploadFinished)
	s.uploaded = uploadedSector{
		hk:   resp.HK,
		fcid: resp.FCID,
		root: resp.Req.Root,
	}
	s.data = nil
}

func (s *sectorUpload) isUploaded() bool {
	return s.uploaded.root != (types.Hash256{})
}
