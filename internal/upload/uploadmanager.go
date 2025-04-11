package upload

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/hosts"
	"go.sia.tech/renterd/v2/internal/memory"
	"go.sia.tech/renterd/v2/internal/upload/uploader"
	"go.sia.tech/renterd/v2/internal/utils"
	"go.sia.tech/renterd/v2/object"
	"go.uber.org/zap"
)

var (
	ErrContractExpired      = errors.New("contract expired")
	ErrNoCandidateUploader  = errors.New("no candidate uploader found")
	ErrShuttingDown         = errors.New("upload manager is shutting down")
	ErrUploadCancelled      = errors.New("upload was cancelled")
	ErrUploadNotEnoughHosts = errors.New("not enough hosts to support requested upload redundancy")
)

type (
	ContractLocker interface {
		AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error)
		KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error)
		ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error)
	}

	ObjectStore interface {
		AddMultipartPart(ctx context.Context, bucket, key, ETag, uploadID string, partNumber int, slices []object.SlabSlice) (err error)
		AddObject(ctx context.Context, bucket, key string, o object.Object, opts api.AddObjectOptions) error
		AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8) (slabs []object.SlabSlice, slabBufferMaxSizeSoftReached bool, err error)
		AddUploadingSectors(ctx context.Context, uID api.UploadID, root []types.Hash256) error
		FinishUpload(ctx context.Context, uID api.UploadID) error
		MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error
		Objects(ctx context.Context, prefix string, opts api.ListObjectOptions) (resp api.ObjectsResponse, err error)
		TrackUpload(ctx context.Context, uID api.UploadID) error
		UpdateSlab(ctx context.Context, key object.EncryptionKey, sectors []api.UploadedSector) error
	}
)

type (
	HostInfo struct {
		api.HostInfo

		// contract info
		ContractEndHeight   uint64
		ContractID          types.FileContractID
		ContractRenewedFrom types.FileContractID
	}

	Manager struct {
		hm        hosts.Manager
		mm        memory.MemoryManager
		os        ObjectStore
		cl        ContractLocker
		cs        uploader.ContractStore
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

	Stats struct {
		AvgSlabUploadSpeedMBPS float64
		AvgOverdrivePct        float64
		HealthyUploaders       uint64
		NumUploaders           uint64
		UploadSpeedsMBPS       map[types.PublicKey]float64
	}
)

type (
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
		uploadCtx context.Context
		uploadID  api.UploadID

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

func NewManager(ctx context.Context, uploadKey *utils.UploadKey, hm hosts.Manager, mm memory.MemoryManager, os ObjectStore, cl ContractLocker, cs uploader.ContractStore, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.Logger) *Manager {
	logger = logger.Named("uploadmanager")
	return &Manager{
		hm:        hm,
		mm:        mm,
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

func (mgr *Manager) AcquireMemory(ctx context.Context, amt uint64) memory.Memory {
	return mgr.mm.AcquireMemory(ctx, amt)
}

func (mgr *Manager) MemoryStatus() memory.Status {
	return mgr.mm.Status()
}

func (mgr *Manager) Stats() Stats {
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
	return Stats{
		AvgSlabUploadSpeedMBPS: mgr.statsSlabUploadSpeedBytesPerMS.Average() * 0.008, // convert bytes per ms to mbps,
		AvgOverdrivePct:        mgr.statsOverdrivePct.Average(),
		HealthyUploaders:       numHealthy,
		NumUploaders:           uint64(len(speeds)),
		UploadSpeedsMBPS:       speeds,
	}
}

func (mgr *Manager) Stop() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	for _, u := range mgr.uploaders {
		u.Stop(ErrShuttingDown)
	}
}

func (mgr *Manager) Upload(ctx context.Context, r io.Reader, hosts []HostInfo, up Parameters) (bufferSizeLimitReached bool, eTag string, err error) {
	// cancel all in-flight requests when the upload is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create the object
	o := object.NewObject(up.EC)

	// create the md5 hasher for the etag
	// NOTE: we use md5 since it's s3 compatible and clients expect it to be md5
	hasher := md5.New()
	r = io.TeeReader(r, hasher)

	// create the cipher reader
	cr, err := o.Encrypt(r, object.EncryptionOptions{
		Offset: up.EncryptionOffset,
		Key:    mgr.uploadKey,
	})
	if err != nil {
		return false, "", err
	}

	// create the upload
	upload, err := mgr.newUpload(up.RS.TotalShards, hosts, up.BH)
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
	slabSizeNoRedundancy := up.RS.SlabSizeNoRedundancy()
	slabSize := up.RS.SlabSize()
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
			} else if up.Packing && errors.Is(err, io.ErrUnexpectedEOF) {
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
				}(up.RS, data, length, slabIndex)
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
			return false, "", ErrUploadCancelled
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
		pss, bufferSizeLimitReached, err = mgr.os.AddPartialSlab(ctx, partialSlab, uint8(up.RS.MinShards), uint8(up.RS.TotalShards))
		if err != nil {
			return false, "", err
		}
		o.Slabs = append(o.Slabs, pss...)
	}

	if up.Multipart {
		// persist the part
		err = mgr.os.AddMultipartPart(ctx, up.Bucket, up.Key, eTag, up.UploadID, up.PartNumber, o.Slabs)
		if err != nil {
			return bufferSizeLimitReached, "", fmt.Errorf("couldn't add multi part: %w", err)
		}
	} else {
		// persist the object
		err = mgr.os.AddObject(ctx, up.Bucket, up.Key, o, api.AddObjectOptions{MimeType: up.MimeType, ETag: eTag, Metadata: up.Metadata})
		if err != nil {
			return bufferSizeLimitReached, "", fmt.Errorf("couldn't add object: %w", err)
		}
	}

	return
}

func (mgr *Manager) UploadPackedSlab(ctx context.Context, rs api.RedundancySettings, ps api.PackedSlab, mem memory.Memory, hosts []HostInfo, bh uint64) (err error) {
	// cancel all in-flight requests when the upload is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// build the shards
	shards := encryptPartialSlab(ps.Data, ps.EncryptionKey, uint8(rs.MinShards), uint8(rs.TotalShards))

	// create the upload
	upload, err := mgr.newUpload(len(shards), hosts, bh)
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

func (mgr *Manager) UploadShards(ctx context.Context, s object.Slab, shards [][]byte, hosts []HostInfo, bh uint64, mem memory.Memory) (err error) {
	// cancel all in-flight requests when the upload is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create the upload
	upload, err := mgr.newUpload(len(shards), hosts, bh)
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

	// build sectors
	var sectors []api.UploadedSector
	for _, sector := range uploaded {
		sectors = append(sectors, api.UploadedSector{
			ContractID: sector.fcid,
			Root:       sector.root,
		})
	}
	if len(sectors) > 0 {
		if err := mgr.os.UpdateSlab(ctx, s.EncryptionKey, sectors); err != nil {
			return fmt.Errorf("couldn't update slab: %w", err)
		}
	}

	// check error
	if err != nil {
		return err
	}

	// track stats
	mgr.statsOverdrivePct.Track(overdrivePct)
	mgr.statsSlabUploadSpeedBytesPerMS.Track(float64(uploadSpeed))

	return nil
}

func (mgr *Manager) candidates(allowed map[types.PublicKey]struct{}) (candidates []*uploader.Uploader) {
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

func (mgr *Manager) newUpload(totalShards int, hosts []HostInfo, bh uint64) (*upload, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// refresh the uploaders
	mgr.refreshUploaders(hosts, bh)

	// check if we have enough contracts
	if len(hosts) < totalShards {
		return nil, fmt.Errorf("%v < %v: %w", len(hosts), totalShards, ErrUploadNotEnoughHosts)
	}

	// create allowed map
	allowed := make(map[types.PublicKey]struct{})
	for _, h := range hosts {
		allowed[h.PublicKey] = struct{}{}
	}

	// create upload
	return &upload{
		id:          api.NewUploadID(),
		allowed:     allowed,
		os:          mgr.os,
		shutdownCtx: mgr.shutdownCtx,
	}, nil
}

func (mgr *Manager) refreshUploaders(hosts []HostInfo, bh uint64) {
	// build table to lookup lookup
	lookup := make(map[types.FileContractID]HostInfo)
	for _, h := range hosts {
		if h.ContractRenewedFrom != (types.FileContractID{}) {
			lookup[h.ContractRenewedFrom] = h
		}
	}

	// refresh uploaders
	var refreshed []*uploader.Uploader
	existing := make(map[types.FileContractID]struct{})
	for _, uploader := range mgr.uploaders {
		// refresh uploaders that got renewed
		if renewal, renewed := lookup[uploader.ContractID()]; renewed {
			uploader.Refresh(&renewal.HostInfo, renewal.ContractID, renewal.ContractEndHeight)
		}

		// stop uploaders that expired
		if uploader.Expired(bh) {
			go uploader.Stop(ErrContractExpired) // unblock caller
			continue
		}

		// recompute the stats
		uploader.TryRecomputeStats()

		// add to the list
		refreshed = append(refreshed, uploader)
		existing[uploader.ContractID()] = struct{}{}
	}

	// add missing uploaders
	for _, h := range hosts {
		if _, exists := existing[h.ContractID]; !exists && bh < h.ContractEndHeight {
			uploader := uploader.New(mgr.shutdownCtx, mgr.cl, mgr.cs, mgr.hm, h.HostInfo, h.ContractID, h.ContractEndHeight, mgr.logger)
			refreshed = append(refreshed, uploader)
			go uploader.Start()
		}
	}

	mgr.uploaders = refreshed
	return
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
			// for the call to AddUploadingSector in upload.go
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
		uploadCtx: ctx,
		uploadID:  u.id,

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

// uploadShards uploads the shards to the provided candidates. It returns an
// error if it fails to upload all shards but len(sectors) will be > 0 if some
// shards were uploaded successfully.
func (u *upload) uploadShards(ctx context.Context, shards [][]byte, candidates []*uploader.Uploader, mem memory.Memory, maxOverdrive uint64, overdriveTimeout time.Duration) (sectors []uploadedSector, uploadSpeed int64, overdrivePct float64, err error) {
	// ensure inflight uploads get cancelled
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// prepare the upload
	slab, respChan := u.newSlabUpload(ctx, shards, candidates, mem, maxOverdrive)
	defer close(respChan)

	// prepare requests
	requests := make([]*uploader.SectorUploadReq, len(shards))
	roots := make([]types.Hash256, len(shards))
	for sI := range shards {
		s := slab.sectors[sI]
		requests[sI] = uploader.NewUploadRequest(ctx, s.ctx, s.data, sI, respChan, s.root, false)
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
loop:
	for slab.numInflight > 0 {
		select {
		case resp := <-respChan:
			// receive the response
			used, done := slab.receive(resp)
			if done {
				cancel()
				timer.Stop()
			}
			select {
			case <-ctx.Done():
				// upload is done, we don't need to launch more requests
				continue loop
			default:
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
				// relaunch buffered request or overdrive a sector
				if len(buffer) > 0 {
					if err := slab.launch(buffer[0]); err == nil {
						buffer = buffer[1:]
					}
				} else if slab.canOverdrive(overdriveTimeout) {
					_ = slab.launch(slab.nextRequest(respChan))
				}
			}
		case <-timer.C:
			// relaunch buffered request or overdrive a sector
			if len(buffer) > 0 {
				if err := slab.launch(buffer[0]); err == nil {
					buffer = buffer[1:]
				}
			} else if slab.canOverdrive(overdriveTimeout) {
				_ = slab.launch(slab.nextRequest(respChan))
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

	// collect the sectors
	for _, sector := range slab.sectors {
		if sector.isUploaded() {
			sectors = append(sectors, sector.uploaded)
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
		return ErrNoCandidateUploader
	}

	// enqueue the req
	ok := candidate.uploader.Enqueue(req)
	if !ok {
		return nil
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

	return uploader.NewUploadRequest(s.uploadCtx, nextSector.ctx, nextSector.data, nextSector.index, responseChan, nextSector.root, true)
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

	s.cancel(uploader.ErrSectorUploadFinished)
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

func (us uploadedSector) toObjectSector() object.Sector {
	return object.Sector{
		Contracts: map[types.PublicKey][]types.FileContractID{us.hk: {us.fcid}},
		Root:      us.root,
	}
}

func encryptPartialSlab(data []byte, key object.EncryptionKey, minShards, totalShards uint8) [][]byte {
	slab := object.Slab{
		EncryptionKey: key,
		MinShards:     minShards,
		Shards:        make([]object.Sector, totalShards),
	}
	encodedShards := make([][]byte, totalShards)
	slab.Encode(data, encodedShards)
	slab.Encrypt(encodedShards)
	return encodedShards
}
