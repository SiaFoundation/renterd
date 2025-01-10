package uploader

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/hosts"
	"go.sia.tech/renterd/internal/locking"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

const (
	lockingPriorityUpload     = 10
	revisionFetchTimeout      = 30 * time.Second
	sectorUploadTimeout       = 60 * time.Second
	statsRecomputeMinInterval = 3 * time.Second
)

var (
	errAcquireContractFailed = errors.New("failed to acquire contract lock")
	ErrStopped               = errors.New("uploader was stopped")
)

var (
	ErrSectorUploadFinished = errors.New("sector upload already finished")
)

type (
	ContractStore interface {
		RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error)
	}
)

type (
	SectorUploadReq struct {
		Ctx          context.Context
		Data         *[rhpv2.SectorSize]byte
		Idx          int
		ResponseChan chan SectorUploadResp
		Root         types.Hash256
		Overdrive    bool
	}

	SectorUploadResp struct {
		FCID types.FileContractID
		HK   types.PublicKey
		Req  *SectorUploadReq
		Err  error
	}
)

func NewUploadRequest(ctx context.Context, data *[rhpv2.SectorSize]byte, idx int, respChan chan SectorUploadResp, root types.Hash256, overdrive bool) *SectorUploadReq {
	return &SectorUploadReq{
		Ctx:          ctx,
		Data:         data,
		Idx:          idx,
		ResponseChan: respChan,
		Root:         root,
		Overdrive:    overdrive,
	}
}

type (
	Uploader struct {
		cs     ContractStore
		cl     locking.ContractLocker
		hm     hosts.Manager
		logger *zap.SugaredLogger

		hk              types.PublicKey
		signalNewUpload chan struct{}
		shutdownCtx     context.Context

		mu      sync.Mutex
		expiry  uint64
		fcid    types.FileContractID
		host    api.HostInfo
		queue   []*SectorUploadReq
		stopped bool

		// stats related field
		consecutiveFailures uint64
		lastRecompute       time.Time

		statsSectorUploadEstimateInMS    *utils.DataPoints
		statsSectorUploadSpeedBytesPerMS *utils.DataPoints
	}
)

func New(ctx context.Context, cl locking.ContractLocker, cs ContractStore, hm hosts.Manager, hi api.HostInfo, fcid types.FileContractID, endHeight uint64, l *zap.SugaredLogger) *Uploader {
	return &Uploader{
		cl:     cl,
		cs:     cs,
		hm:     hm,
		logger: l,

		// static
		hk:              hi.PublicKey,
		shutdownCtx:     ctx,
		signalNewUpload: make(chan struct{}, 1),

		// stats
		statsSectorUploadEstimateInMS:    utils.NewDataPoints(10 * time.Minute),
		statsSectorUploadSpeedBytesPerMS: utils.NewDataPoints(0),

		// covered by mutex
		expiry: endHeight,
		fcid:   fcid,
		host:   hi,
		queue:  make([]*SectorUploadReq, 0),
	}
}

func (u *Uploader) AvgUploadSpeedBytesPerMS() float64 {
	return u.statsSectorUploadSpeedBytesPerMS.Average()
}

func (u *Uploader) ContractID() types.FileContractID {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.fcid
}

func (u *Uploader) Expired(bh uint64) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return bh >= u.expiry
}

func (u *Uploader) Healthy() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.consecutiveFailures == 0
}

func (u *Uploader) PublicKey() types.PublicKey {
	return u.hk
}

func (u *Uploader) Refresh(hi *api.HostInfo, fcid types.FileContractID, endHeight uint64) {
	u.mu.Lock()
	defer u.mu.Unlock()

	// update state
	u.expiry = endHeight
	u.fcid = fcid
	if hi != nil {
		u.host = *hi
	}
}

func (u *Uploader) Start() {
outer:
	for {
		// wait for work
		select {
		case <-u.signalNewUpload:
		case <-u.shutdownCtx.Done():
			return
		}

		for {
			// check if we are stopped
			select {
			case <-u.shutdownCtx.Done():
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
			start := time.Now()
			duration, err := u.execute(req)
			elapsed := time.Since(start)
			if errors.Is(err, rhp3.ErrMaxRevisionReached) {
				if u.tryRefresh(req.Ctx) {
					u.Enqueue(req)
					continue outer
				}
			}

			// track stats
			success, failure, uploadEstimateMS, uploadSpeedBytesPerMS := handleSectorUpload(err, duration, elapsed, req.Overdrive)
			u.trackSectorUploadStats(uploadEstimateMS, uploadSpeedBytesPerMS)
			u.trackConsecutiveFailures(success, failure)

			// debug log
			if uploadEstimateMS > 0 && !success {
				u.logger.Debugw("sector upload failure was penalised", "uploadError", err, "uploadDuration", duration, "totalDuration", elapsed, "overdrive", req.Overdrive, "penalty", uploadEstimateMS, "hk", u.hk)
			} else if uploadEstimateMS == 0 && err != nil && !utils.IsErr(err, ErrSectorUploadFinished) {
				u.logger.Debugw("sector upload failure was ignored", "uploadError", err, "uploadDuration", duration, "totalDuration", elapsed, "overdrive", req.Overdrive, "hk", u.hk)
			}

			// send the response
			select {
			case <-req.Ctx.Done():
			case req.ResponseChan <- SectorUploadResp{
				FCID: u.fcid,
				HK:   u.hk,
				Err:  err,
				Req:  req,
			}:
			}
		}
	}
}

func handleSectorUpload(uploadErr error, uploadDuration, totalDuration time.Duration, overdrive bool) (success bool, failure bool, uploadEstimateMS float64, uploadSpeedBytesPerMS float64) {
	// no-op cases
	if utils.IsErr(uploadErr, rhp3.ErrMaxRevisionReached) {
		return false, false, 0, 0
	} else if utils.IsErr(uploadErr, context.Canceled) {
		return false, false, 0, 0
	}

	// happy case, upload was successful
	if uploadErr == nil {
		ms := uploadDuration.Milliseconds()
		if ms == 0 {
			ms = 1 // avoid division by zero
		}
		return true, false, float64(ms), float64(rhpv2.SectorSize / ms)
	}

	// upload failed because we weren't able to create a payment, in this case
	// we want to punish the host but only to ensure we stop using it, meaning
	// we don't increment consecutive failures
	if utils.IsErr(uploadErr, rhp3.ErrFailedToCreatePayment) {
		return false, false, float64(time.Hour.Milliseconds()), 0
	}

	// upload failed because the sector was already uploaded by another host, in
	// this case we want to punish the host for being too slow but only when we
	// weren't overdriving or when it took too long to dial
	if utils.IsErr(uploadErr, ErrSectorUploadFinished) {
		slowDial := utils.IsErr(uploadErr, rhp3.ErrDialTransport) && totalDuration > time.Second
		slowLock := utils.IsErr(uploadErr, errAcquireContractFailed) && totalDuration > time.Second
		slowFetchRev := utils.IsErr(uploadErr, rhp3.ErrFailedToFetchRevision) && totalDuration > time.Second
		if !overdrive || slowDial || slowLock || slowFetchRev {
			failure = overdrive
			uploadEstimateMS = float64(totalDuration.Milliseconds() * 10)
		}
		return false, failure, uploadEstimateMS, 0
	}

	// in all other cases we want to punish the host for failing the upload
	return false, true, float64(time.Hour.Milliseconds()), 0
}

func (u *Uploader) Stop(err error) {
	u.mu.Lock()
	u.stopped = true
	u.mu.Unlock()

	for {
		upload := u.pop()
		if upload == nil {
			break
		}
		if !upload.done() {
			upload.finish(err)
		}
	}
}

func (u *Uploader) Enqueue(req *SectorUploadReq) {
	u.mu.Lock()
	// check for stopped
	if u.stopped {
		u.mu.Unlock()
		go req.finish(ErrStopped) // don't block the caller
		return
	}

	// enqueue the request
	u.queue = append(u.queue, req)
	u.mu.Unlock()

	// signal there's work
	u.signalWork()
}

func (u *Uploader) Estimate() float64 {
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

// execute executes the sector upload request, if the upload was successful it
// returns the time it took to upload the sector to the host
func (u *Uploader) execute(req *SectorUploadReq) (_ time.Duration, err error) {
	// grab fields
	u.mu.Lock()
	host := u.host
	fcid := u.fcid
	u.mu.Unlock()

	// wrap cause
	defer func() {
		if cause := context.Cause(req.Ctx); cause != nil && !utils.IsErr(err, cause) {
			if err != nil {
				err = fmt.Errorf("%w; %w", cause, err)
			} else {
				err = cause
			}
		}
	}()

	// acquire contract lock
	lock, err := locking.NewContractLock(req.Ctx, fcid, lockingPriorityUpload, u.cl, u.logger)
	if err != nil {
		return 0, fmt.Errorf("%w; %w", errAcquireContractFailed, err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(u.shutdownCtx, 10*time.Second)
		lock.Release(ctx)
		cancel()
	}()

	// apply sane timeout
	ctx, cancel := context.WithTimeout(req.Ctx, sectorUploadTimeout)
	defer cancel()

	// upload the sector
	start := time.Now()
	err = u.hm.Uploader(host, fcid).UploadSector(ctx, req.Root, req.Data)
	if err != nil {
		return 0, fmt.Errorf("failed to upload sector to contract %v; %w", fcid, err)
	}

	return time.Since(start), nil
}

func (u *Uploader) pop() *SectorUploadReq {
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

func (u *Uploader) signalWork() {
	select {
	case u.signalNewUpload <- struct{}{}:
	default:
	}
}

func (u *Uploader) trackConsecutiveFailures(success, failure bool) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if success {
		u.consecutiveFailures = 0
	} else if failure {
		u.consecutiveFailures++
	}
}

func (u *Uploader) trackSectorUploadStats(uploadEstimateMS, uploadSpeedBytesPerMS float64) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if uploadEstimateMS > 0 {
		u.statsSectorUploadEstimateInMS.Track(uploadEstimateMS)
	}
	if uploadSpeedBytesPerMS > 0 {
		u.statsSectorUploadSpeedBytesPerMS.Track(uploadSpeedBytesPerMS)
	}
}

func (u *Uploader) TryRecomputeStats() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if time.Since(u.lastRecompute) < statsRecomputeMinInterval {
		return
	}

	u.lastRecompute = time.Now()
	u.statsSectorUploadEstimateInMS.Recompute()
	u.statsSectorUploadSpeedBytesPerMS.Recompute()
}

func (u *Uploader) tryRefresh(ctx context.Context) bool {
	// fetch the renewed contract
	renewed, err := u.cs.RenewedContract(ctx, u.ContractID())
	if utils.IsErr(err, api.ErrContractNotFound) || utils.IsErr(err, context.Canceled) {
		return false
	} else if err != nil {
		u.logger.Errorf("failed to fetch renewed contract %v, err: %v", u.ContractID(), err)
		return false
	}

	// renew the uploader with the renewed contract
	u.Refresh(nil, renewed.ID, renewed.WindowEnd)
	return true
}

func (req *SectorUploadReq) done() bool {
	select {
	case <-req.Ctx.Done():
		return true
	default:
		return false
	}
}

func (req *SectorUploadReq) finish(err error) {
	select {
	case <-req.Ctx.Done():
	case req.ResponseChan <- SectorUploadResp{
		Err: err,
		Req: req,
	}:
	}
}
