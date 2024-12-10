package download

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/download/downloader"
	"go.sia.tech/renterd/internal/host"
	"go.sia.tech/renterd/internal/memory"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
)

type ObjectStore interface {
	DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) error
	FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error)
	Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error)
}

const (
	downloadMemoryLimitDenom = 6 // 1/6th of the available download memory can be used by a single download
)

var (
	ErrDownloadCancelled      = errors.New("download was cancelled")
	ErrDownloadNotEnoughHosts = errors.New("not enough hosts available to download the slab")
	ErrShuttingDown           = errors.New("download manager is shutting down")

	errHostNoLongerUsable = errors.New("host no longer usable")
)

type (
	Manager struct {
		hm        host.HostManager
		mm        memory.MemoryManager
		os        ObjectStore
		uploadKey *utils.UploadKey
		logger    *zap.SugaredLogger

		maxOverdrive     uint64
		overdriveTimeout time.Duration

		statsOverdrivePct                *utils.DataPoints
		statsSlabDownloadSpeedBytesPerMS *utils.DataPoints

		shutdownCtx context.Context

		mu          sync.Mutex
		downloaders map[types.PublicKey]*downloader.Downloader
	}

	slabDownload struct {
		mgr *Manager

		minShards int
		offset    uint64
		length    uint64

		created time.Time

		mu             sync.Mutex
		lastOverdrive  time.Time
		numCompleted   int
		numInflight    uint64
		numLaunched    uint64
		numOverdriving uint64

		sectors []*sectorInfo
		errs    utils.HostErrorSet
	}

	slabDownloadResponse struct {
		mem    memory.Memory
		shards [][]byte
		index  int
		err    error
	}

	sectorInfo struct {
		root     types.Hash256
		data     []byte
		hks      []types.PublicKey
		index    int
		selected int
	}

	Stats struct {
		AvgDownloadSpeedMBPS float64
		AvgOverdrivePct      float64
		HealthyDownloaders   uint64
		NumDownloaders       uint64
		DownloadSpeedsMBPS   map[types.PublicKey]float64
	}
)

func (s *sectorInfo) selectHost(h types.PublicKey) {
	for i, hk := range s.hks {
		if hk == h {
			s.hks = append(s.hks[:i], s.hks[i+1:]...) // remove the host
			s.selected++
			break
		}
	}
}

func NewManager(ctx context.Context, uploadKey *utils.UploadKey, hm host.HostManager, mm memory.MemoryManager, os ObjectStore, maxOverdrive uint64, overdriveTimeout time.Duration, logger *zap.Logger) *Manager {
	logger = logger.Named("downloadmanager")
	return &Manager{
		hm:        hm,
		mm:        mm,
		os:        os,
		uploadKey: uploadKey,
		logger:    logger.Sugar(),

		maxOverdrive:     maxOverdrive,
		overdriveTimeout: overdriveTimeout,

		statsOverdrivePct:                utils.NewDataPoints(0),
		statsSlabDownloadSpeedBytesPerMS: utils.NewDataPoints(0),

		shutdownCtx: ctx,

		downloaders: make(map[types.PublicKey]*downloader.Downloader),
	}
}

func (mgr *Manager) DownloadObject(ctx context.Context, w io.Writer, o object.Object, offset, length uint64, hosts []api.HostInfo) (err error) {
	// calculate what slabs we need
	var ss []slabSlice
	for _, s := range o.Slabs {
		ss = append(ss, slabSlice{
			SlabSlice:   s,
			PartialSlab: s.IsPartial(),
		})
	}
	slabs := slabsForDownload(ss, offset, length)
	if len(slabs) == 0 {
		return nil
	}

	// go through the slabs and fetch any partial slab data from the store.
	for i := range slabs {
		if !slabs[i].PartialSlab {
			continue
		}
		data, slab, err := mgr.fetchPartialSlab(ctx, slabs[i].SlabSlice.EncryptionKey, slabs[i].SlabSlice.Offset, slabs[i].SlabSlice.Length)
		if err != nil {
			return fmt.Errorf("failed to fetch partial slab data: %w", err)
		}
		if slab != nil {
			slabs[i].SlabSlice.Slab = *slab
			slabs[i].PartialSlab = slab.IsPartial()
		} else {
			slabs[i].Data = data
		}
	}

	// refresh the downloaders
	mgr.refreshDownloaders(hosts)

	// map available hosts
	available := make(map[types.PublicKey]struct{})
	for _, h := range hosts {
		available[h.PublicKey] = struct{}{}
	}

	// create the cipher writer
	cw, err := o.Key.Decrypt(w, object.EncryptionOptions{
		Offset: offset,
		Key:    mgr.uploadKey,
	})
	if err != nil {
		return fmt.Errorf("failed to create cipher writer: %w", err)
	}

	// buffer the writer we recover to making sure that we don't hammer the
	// response writer with tiny writes
	bw := bufio.NewWriter(cw)
	defer bw.Flush()

	// create response chan and ensure it's closed properly
	var wg sync.WaitGroup
	responseChan := make(chan *slabDownloadResponse)
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		wg.Wait()
		close(responseChan)
	}()

	// apply a per-download limit to the memory manager
	mm, err := mgr.mm.Limit(mgr.mm.Status().Total / downloadMemoryLimitDenom)
	if err != nil {
		return err
	}

	// launch a goroutine to launch consecutive slab downloads
	wg.Add(1)
	go func() {
		defer wg.Done()

		var slabIndex int
		for slabIndex = 0; slabIndex < len(slabs); slabIndex++ {
			next := slabs[slabIndex]

			// check if we need to abort
			select {
			case <-ctx.Done():
				return
			case <-mgr.shutdownCtx.Done():
				return
			default:
			}

			// check if the next slab is a partial slab.
			if next.PartialSlab {
				responseChan <- &slabDownloadResponse{index: slabIndex}
				continue // handle partial slab separately
			}

			// check if we have enough downloaders
			var numAvailable uint8
			for _, s := range next.Shards {
				if isSectorAvailable(s, available) {
					numAvailable++
				}
			}
			if numAvailable < next.MinShards {
				responseChan <- &slabDownloadResponse{err: fmt.Errorf("%w: %v/%v", ErrDownloadNotEnoughHosts, numAvailable, next.MinShards)}
				return
			}

			// acquire memory
			mem := mm.AcquireMemory(ctx, uint64(next.Length))
			if mem == nil {
				return // interrupted
			}

			// launch the download
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				shards, err := mgr.downloadSlab(ctx, next.SlabSlice)
				select {
				case responseChan <- &slabDownloadResponse{
					mem:    mem,
					shards: shards,
					index:  index,
					err:    err,
				}:
				case <-ctx.Done():
					mem.Release() // relase memory if we're interrupted
				}
			}(slabIndex)
		}
	}()

	// collect the response, responses might come in out of order so we keep
	// them in a map and return what we can when we can
	responses := make(map[int]*slabDownloadResponse)
	var respIndex int
outer:
	for {
		var resp *slabDownloadResponse
		select {
		case <-mgr.shutdownCtx.Done():
			return ErrShuttingDown
		case <-ctx.Done():
			return ErrDownloadCancelled
		case resp = <-responseChan:
		}

		// handle response
		err := func() error {
			if resp.mem != nil {
				defer resp.mem.Release()
			}

			if resp.err != nil {
				mgr.logger.Errorw("slab download failed",
					zap.Int("index", resp.index),
					zap.Error(err),
				)
				return resp.err
			}

			responses[resp.index] = resp
			for {
				if next, exists := responses[respIndex]; exists {
					s := slabs[respIndex]
					if s.PartialSlab {
						// Partial slab.
						_, err = bw.Write(s.Data)
						if err != nil {
							mgr.logger.Errorf("failed to send partial slab", respIndex, err)
							return err
						}
					} else {
						// Regular slab.
						slabs[respIndex].Decrypt(next.shards)
						err := slabs[respIndex].Recover(bw, next.shards)
						if err != nil {
							mgr.logger.Errorf("failed to recover slab %v: %v", respIndex, err)
							return err
						}
					}

					next = nil
					delete(responses, respIndex)
					respIndex++

					continue
				} else {
					break
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}

		// exit condition
		if respIndex == len(slabs) {
			break outer
		}
	}

	return nil
}

func (mgr *Manager) DownloadSlab(ctx context.Context, slab object.Slab, hosts []api.HostInfo) ([][]byte, error) {
	// refresh the downloaders
	mgr.refreshDownloaders(hosts)

	// map available hosts
	available := make(map[types.PublicKey]struct{})
	for _, h := range hosts {
		available[h.PublicKey] = struct{}{}
	}

	// count how many shards we can download (best-case)
	var availableShards uint8
	for _, shard := range slab.Shards {
		if isSectorAvailable(shard, available) {
			availableShards++
		}
	}

	// check if we have enough shards
	if availableShards < slab.MinShards {
		return nil, fmt.Errorf("not enough hosts available to download the slab: %v/%v", availableShards, slab.MinShards)
	}

	// NOTE: we don't acquire memory here since DownloadSlab is only used for
	// migrations which already have memory acquired

	// download the slab
	slice := object.SlabSlice{
		Slab:   slab,
		Offset: 0,
		Length: uint32(slab.MinShards) * rhpv2.SectorSize,
	}
	shards, err := mgr.downloadSlab(ctx, slice)
	if err != nil {
		return nil, err
	}

	// decrypt and recover
	slice.Decrypt(shards)
	err = slice.Reconstruct(shards)
	if err != nil {
		return nil, err
	}

	return shards, err
}

func (mgr *Manager) MemoryStatus() memory.Status {
	return mgr.mm.Status()
}

func (mgr *Manager) Stats() Stats {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// collect stats
	var numHealthy uint64
	speeds := make(map[types.PublicKey]float64)
	for _, d := range mgr.downloaders {
		speeds[d.PublicKey()] = d.AvgDownloadSpeedBytesPerMS()
		if d.Healthy() {
			numHealthy++
		}
	}

	return Stats{
		AvgDownloadSpeedMBPS: mgr.statsSlabDownloadSpeedBytesPerMS.Average() * 0.008, // convert bytes per ms to mbps,
		AvgOverdrivePct:      mgr.statsOverdrivePct.Average(),
		HealthyDownloaders:   numHealthy,
		NumDownloaders:       uint64(len(mgr.downloaders)),
		DownloadSpeedsMBPS:   speeds,
	}
}

func (mgr *Manager) Stop() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	for _, d := range mgr.downloaders {
		d.Stop(ErrShuttingDown)
	}
}

func (mgr *Manager) numDownloaders() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return len(mgr.downloaders)
}

// fetchPartialSlab fetches the data of a partial slab from the bus. It will
// fall back to ask the bus for the slab metadata in case the slab wasn't found
// in the partial slab buffer.
func (mgr *Manager) fetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, *object.Slab, error) {
	data, err := mgr.os.FetchPartialSlab(ctx, key, offset, length)
	if utils.IsErr(err, api.ErrObjectNotFound) {
		// Check if slab was already uploaded.
		slab, err := mgr.os.Slab(ctx, key)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to fetch uploaded partial slab: %v", err)
		}
		return nil, &slab, nil
	} else if err != nil {
		return nil, nil, err
	}
	return data, nil, nil
}

func (mgr *Manager) refreshDownloaders(hosts []api.HostInfo) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	// build map
	want := make(map[types.PublicKey]api.HostInfo)
	for _, h := range hosts {
		want[h.PublicKey] = h
	}

	// prune downloaders
	for hk := range mgr.downloaders {
		_, wanted := want[hk]
		if !wanted {
			mgr.downloaders[hk].Stop(errHostNoLongerUsable)
			delete(mgr.downloaders, hk)
			continue
		}

		// recompute the stats
		mgr.downloaders[hk].TryRecomputeStats()

		// remove from want so remainging ones are the missing ones
		delete(want, hk)
	}

	// update downloaders
	for hk, hi := range want {
		mgr.downloaders[hk] = downloader.New(mgr.shutdownCtx, mgr.hm.Downloader(hi))
		go mgr.downloaders[hk].Start()
	}
}

func (mgr *Manager) newSlabDownload(slice object.SlabSlice) *slabDownload {
	// calculate the offset and length
	offset, length := slice.SectorRegion()

	// build sectors
	var sectors []*sectorInfo
	for sI, s := range slice.Shards {
		hks := make([]types.PublicKey, 0, len(s.Contracts))
		for hk := range s.Contracts {
			hks = append(hks, hk)
		}
		sectors = append(sectors, &sectorInfo{
			root:  s.Root,
			index: sI,
			hks:   hks,
		})
	}

	// create slab download
	return &slabDownload{
		mgr: mgr,

		minShards: int(slice.MinShards),
		offset:    offset,
		length:    length,

		created: time.Now(),

		sectors: sectors,
		errs:    make(utils.HostErrorSet),
	}
}

func (mgr *Manager) downloadSlab(ctx context.Context, slice object.SlabSlice) ([][]byte, error) {
	// prepare new download
	slab := mgr.newSlabDownload(slice)

	// execute download
	return slab.download(ctx)
}

func (s *slabDownload) overdrive(ctx context.Context, resps *downloader.SectorResponses) (resetTimer func()) {
	// overdrive is disabled
	if s.mgr.overdriveTimeout == 0 {
		return func() {}
	}

	// create a helper function that increases the timeout for each overdrive
	timeout := func() time.Duration {
		s.mu.Lock()
		defer s.mu.Unlock()
		return time.Duration(s.numOverdriving+1) * s.mgr.overdriveTimeout
	}

	// create a timer to trigger overdrive
	timer := time.NewTimer(timeout())
	resetTimer = func() {
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
		timer.Reset(timeout())
	}

	// create a function to check whether overdrive is possible
	canOverdrive := func(timeout time.Duration) bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		// overdrive is not due yet
		if time.Since(s.lastOverdrive) < timeout {
			return false
		}

		// overdrive is maxed out
		remaining := s.minShards - s.numCompleted
		if s.numInflight >= s.mgr.maxOverdrive+uint64(remaining) {
			return false
		}

		s.lastOverdrive = time.Now()
		return true
	}

	// try overdriving every time the timer fires
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if canOverdrive(timeout()) {
					req := s.nextRequest(ctx, resps, true)
					if req != nil {
						s.launch(req)
					}
				}
				resetTimer()
			}
		}
	}()

	return
}

func (s *slabDownload) nextRequest(ctx context.Context, resps *downloader.SectorResponses, overdrive bool) *downloader.SectorDownloadReq {
	s.mu.Lock()
	defer s.mu.Unlock()

	// update pending sectors
	var pending []*sectorInfo
	for _, sector := range s.sectors {
		if len(sector.data) == 0 && len(sector.hks) > 0 {
			pending = append(pending, sector)
		}
	}

	// sort pending sectors
	sort.Slice(pending, func(i, j int) bool {
		// get fastest download for each sector
		iFastest := s.mgr.fastest(pending[i].hks)
		jFastest := s.mgr.fastest(pending[j].hks)

		// check edge case where a sector doesn't have a downloader
		if iFastest != nil && jFastest == nil {
			return true // prefer i
		} else if iFastest == nil && jFastest != nil {
			return false // prefer j
		} else if iFastest == nil && jFastest == nil {
			return false // doesn't matter
		}
		// both have a downloader, sort by number of selections next
		if pending[i].selected != pending[j].selected {
			return pending[i].selected < pending[j].selected
		}
		// both have been selected the same number of times, pick the faster one
		return iFastest.Estimate() < jFastest.Estimate()
	})

	for _, next := range pending {
		fastest := s.mgr.fastest(next.hks)
		if fastest == nil {
			// no host available for this sector, clean 'hks'
			next.hks = nil
			continue
		}
		next.selectHost(fastest.PublicKey())
		return &downloader.SectorDownloadReq{
			Ctx: ctx,

			Offset: s.offset,
			Length: s.length,
			Root:   next.root,
			Host:   fastest,

			Overdrive:   overdrive,
			SectorIndex: next.index,
			Resps:       resps,
		}
	}

	// we don't know if the download failed at this point so we register an
	// error that gets propagated in case it did
	s.errs[types.PublicKey{}] = fmt.Errorf("%w: no more hosts", ErrDownloadNotEnoughHosts)
	return nil
}

func (s *slabDownload) download(ctx context.Context) ([][]byte, error) {
	// cancel any sector downloads once the download is done
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create the responses queue
	resps := downloader.NewSectorResponses()
	defer resps.Close()

	// launch overdrive
	resetOverdrive := s.overdrive(ctx, resps)

	// launch 'MinShard' requests
	for i := 0; i < int(s.minShards); {
		req := s.nextRequest(ctx, resps, false)
		if req == nil {
			return nil, fmt.Errorf("no host available for shard %d", i)
		}
		s.launch(req)
		i++
	}

	// collect responses
	var done bool
	for s.inflight() > 0 && !done {
		select {
		case <-s.mgr.shutdownCtx.Done():
			return nil, errors.New("download stopped")
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-resps.Received():
			resetOverdrive()
		}

		for {
			resp := resps.Next()
			if resp == nil {
				break
			}

			// receive the response
			done = s.receive(*resp)
			if done {
				break
			}

			// handle errors
			if resp.Err != nil {
				// launch replacement request
				if req := s.nextRequest(ctx, resps, resp.Req.Overdrive); req != nil {
					s.launch(req)
				}

				// handle lost sectors
				if rhp3.IsSectorNotFound(resp.Err) {
					if err := s.mgr.os.DeleteHostSector(ctx, resp.Req.Host.PublicKey(), resp.Req.Root); err != nil {
						s.mgr.logger.Errorw("failed to mark sector as lost", "hk", resp.Req.Host.PublicKey(), "root", resp.Req.Root, zap.Error(err))
					}
				}
			}
		}
	}

	// track stats
	s.mgr.statsOverdrivePct.Track(s.overdrivePct())
	s.mgr.statsSlabDownloadSpeedBytesPerMS.Track(float64(s.downloadSpeed()))
	return s.finish()
}

func (s *slabDownload) overdrivePct() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	numOverdrive := int(s.numLaunched) - s.minShards
	if numOverdrive < 0 {
		numOverdrive = 0
	}

	return float64(numOverdrive) / float64(s.minShards)
}

func (s *slabDownload) downloadSpeed() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	bytes := s.numCompleted * rhpv2.SectorSize
	ms := time.Since(s.created).Milliseconds()
	if ms == 0 {
		ms = 1 // avoid division by zero
	}
	return int64(bytes) / ms
}

func (s *slabDownload) finish() ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.numCompleted < s.minShards {
		return nil, fmt.Errorf("failed to download slab: completed=%d inflight=%d launched=%d downloaders=%d errors=%d %v", s.numCompleted, s.numInflight, s.numLaunched, s.mgr.numDownloaders(), len(s.errs), s.errs)
	}

	data := make([][]byte, len(s.sectors))
	for i, sector := range s.sectors {
		data[i] = sector.data
	}
	return data, nil
}

func (s *slabDownload) inflight() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.numInflight
}

func (s *slabDownload) launch(req *downloader.SectorDownloadReq) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// queue the request
	req.Host.Enqueue(req)

	// update the state
	s.numInflight++
	if req.Overdrive {
		s.numOverdriving++
	}
	s.numLaunched++
}

func (s *slabDownload) receive(resp downloader.SectorDownloadResp) (finished bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// update num overdriving
	if resp.Req.Overdrive {
		s.numOverdriving--
	}

	// failed reqs can't complete the upload
	s.numInflight--
	if resp.Err != nil {
		s.errs[resp.Req.Host.PublicKey()] = resp.Err
		return false
	}

	// store the sector
	if len(s.sectors[resp.Req.SectorIndex].data) == 0 {
		s.sectors[resp.Req.SectorIndex].data = resp.Sector
		s.numCompleted++
	}

	return s.numCompleted >= s.minShards
}

func (mgr *Manager) fastest(hosts []types.PublicKey) (fastest *downloader.Downloader) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	lowest := math.MaxFloat64
	for _, h := range hosts {
		if d, ok := mgr.downloaders[h]; !ok {
			continue
		} else if estimate := d.Estimate(); estimate < lowest {
			lowest = estimate
			fastest = d
		}
	}
	return
}

type slabSlice struct {
	object.SlabSlice
	PartialSlab bool
	Data        []byte
}

func slabsForDownload(slabs []slabSlice, offset, length uint64) []slabSlice {
	// declare a helper to cast a uint64 to uint32 with overflow detection. This
	// could should never produce an overflow.
	cast32 := func(in uint64) uint32 {
		if in > math.MaxUint32 {
			panic("slabsForDownload: overflow detected")
		}
		return uint32(in)
	}

	// mutate a copy
	slabs = append([]slabSlice(nil), slabs...)

	firstOffset := offset
	for i, ss := range slabs {
		if firstOffset < uint64(ss.Length) {
			slabs = slabs[i:]
			break
		}
		firstOffset -= uint64(ss.Length)
	}
	slabs[0].Offset += cast32(firstOffset)
	slabs[0].Length -= cast32(firstOffset)

	lastLength := length
	for i, ss := range slabs {
		if lastLength <= uint64(ss.Length) {
			slabs = slabs[:i+1]
			break
		}
		lastLength -= uint64(ss.Length)
	}
	slabs[len(slabs)-1].Length = cast32(lastLength)
	return slabs
}

func isSectorAvailable(s object.Sector, hosts map[types.PublicKey]struct{}) bool {
	// if any of the other hosts that store the sector are
	// available, the sector is also considered available
	for hk := range s.Contracts {
		if _, available := hosts[hk]; available {
			return true
		}
	}
	return false
}
