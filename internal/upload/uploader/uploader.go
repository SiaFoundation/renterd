package uploader

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/host"
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
	errFetchRevisionFailed   = errors.New("failed to fetch revision")
	errUploaderStopped       = errors.New("uploader was stopped")
)

var (
	ErrSectorUploadFinished = errors.New("sector upload already finished")
)

type (
	ContractStore interface {
		// TODO: REMOVE
		RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error)
	}
)

type (
	SectorUploadReq struct {
		ctx          context.Context
		responseChan chan SectorUploadResp
		root         types.Hash256
		data         *[rhpv2.SectorSize]byte
		overdrive    bool
	}

	SectorUploadResp struct {
		fcid types.FileContractID
		hk   types.PublicKey
		err  error
	}
)

type (
	Uploader struct {
		cs     ContractStore
		cl     locking.ContractLocker
		hm     host.HostManager
		logger *zap.SugaredLogger

		hk              types.PublicKey
		siamuxAddr      string
		signalNewUpload chan struct{}
		shutdownCtx     context.Context

		mu        sync.Mutex
		endHeight uint64
		fcid      types.FileContractID
		host      host.Host
		queue     []*SectorUploadReq
		stopped   bool

		// stats related field
		consecutiveFailures uint64
		lastRecompute       time.Time

		statsSectorUploadEstimateInMS    *utils.DataPoints
		statsSectorUploadSpeedBytesPerMS *utils.DataPoints
	}
)

func New(ctx context.Context, cl locking.ContractLocker, cs ContractStore, hm host.HostManager, c api.ContractMetadata, l *zap.SugaredLogger) *Uploader {
	return &Uploader{
		cl:     cl,
		cs:     cs,
		hm:     hm,
		logger: l,

		// static
		hk:              c.HostKey,
		siamuxAddr:      c.SiamuxAddr,
		shutdownCtx:     ctx,
		signalNewUpload: make(chan struct{}, 1),

		// stats
		statsSectorUploadEstimateInMS:    utils.NewDataPoints(10 * time.Minute),
		statsSectorUploadSpeedBytesPerMS: utils.NewDataPoints(0),

		// covered by mutex
		host:      hm.Host(c.HostKey, c.ID, c.SiamuxAddr),
		fcid:      c.ID,
		endHeight: c.WindowEnd,
		queue:     make([]*SectorUploadReq, 0),
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
	return bh >= u.endHeight
}

func (u *Uploader) Healthy() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.consecutiveFailures == 0
}

func (u *Uploader) PublicKey() types.PublicKey {
	return u.hk
}

func (u *Uploader) Refresh(c api.ContractMetadata) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.host = u.hm.Host(c.HostKey, c.ID, c.SiamuxAddr)
	u.fcid = c.ID
	u.siamuxAddr = c.SiamuxAddr
	u.endHeight = c.WindowEnd
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
				if u.tryRefresh(req.ctx) {
					u.Enqueue(req)
					continue outer
				}
			}

			// track stats
			success, failure, uploadEstimateMS, uploadSpeedBytesPerMS := handleSectorUpload(err, duration, elapsed, req.overdrive)
			u.trackSectorUploadStats(uploadEstimateMS, uploadSpeedBytesPerMS)
			u.trackConsecutiveFailures(success, failure)

			// debug log
			if uploadEstimateMS > 0 && !success {
				u.logger.Debugw("sector upload failure was penalised", "uploadError", err, "uploadDuration", duration, "totalDuration", elapsed, "overdrive", req.overdrive, "penalty", uploadEstimateMS, "hk", u.hk)
			} else if uploadEstimateMS == 0 && err != nil && !utils.IsErr(err, ErrSectorUploadFinished) {
				u.logger.Debugw("sector upload failure was ignored", "uploadError", err, "uploadDuration", duration, "totalDuration", elapsed, "overdrive", req.overdrive, "hk", u.hk)
			}

			// send the response
			select {
			case <-req.ctx.Done():
			case req.responseChan <- SectorUploadResp{
				fcid: u.fcid,
				hk:   u.hk,
				err:  err,
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
		slowFetchRev := utils.IsErr(uploadErr, errFetchRevisionFailed) && totalDuration > time.Second
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
		go req.finish(errUploaderStopped) // don't block the caller
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
		if cause := context.Cause(req.ctx); cause != nil && !utils.IsErr(err, cause) {
			if err != nil {
				err = fmt.Errorf("%w; %w", cause, err)
			} else {
				err = cause
			}
		}
	}()

	// acquire contract lock
	lock, err := locking.NewContractLock(req.ctx, fcid, lockingPriorityUpload, u.cl, u.logger)
	if err != nil {
		return 0, fmt.Errorf("%w; %w", errAcquireContractFailed, err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(u.shutdownCtx, 10*time.Second)
		lock.Release(ctx)
		cancel()
	}()

	// apply sane timeout
	ctx, cancel := context.WithTimeout(req.ctx, sectorUploadTimeout)
	defer cancel()

	// fetch the revision
	rev, err := host.FetchRevision(ctx, revisionFetchTimeout)
	if err != nil {
		return 0, fmt.Errorf("%w; %w", errFetchRevisionFailed, err)
	} else if rev.RevisionNumber == math.MaxUint64 {
		return 0, rhp3.ErrMaxRevisionReached
	}

	// upload the sector
	start := time.Now()
	err = host.UploadSector(ctx, req.root, req.data, rev)
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
	u.Refresh(renewed)
	return true
}

func (req *SectorUploadReq) done() bool {
	select {
	case <-req.ctx.Done():
		return true
	default:
		return false
	}
}

func (req *SectorUploadReq) finish(err error) {
	select {
	case <-req.ctx.Done():
	case req.responseChan <- SectorUploadResp{
		err: err,
	}:
	}
}
