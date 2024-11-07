package worker

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
	"go.sia.tech/renterd/internal/locking"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

const (
	sectorUploadTimeout = 60 * time.Second
)

var (
	errAcquireContractFailed = errors.New("failed to acquire contract lock")
	errFetchRevisionFailed   = errors.New("failed to fetch revision")
	errUploaderStopped       = errors.New("uploader was stopped")
)

type (
	uploader struct {
		os     ObjectStore
		cs     ContractStore
		cl     ContractLocker
		hm     HostManager
		logger *zap.SugaredLogger

		hk              types.PublicKey
		siamuxAddr      string
		signalNewUpload chan struct{}
		shutdownCtx     context.Context

		mu        sync.Mutex
		endHeight uint64
		fcid      types.FileContractID
		host      Host
		queue     []*sectorUploadReq
		stopped   bool

		// stats related field
		consecutiveFailures uint64
		lastRecompute       time.Time

		statsSectorUploadEstimateInMS    *utils.DataPoints
		statsSectorUploadSpeedBytesPerMS *utils.DataPoints
	}
)

func (u *uploader) ContractID() types.FileContractID {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.fcid
}

func (u *uploader) Expired(bh uint64) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return bh >= u.endHeight
}

func (u *uploader) Healthy() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.consecutiveFailures == 0
}

func (u *uploader) Refresh(c api.ContractMetadata) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.host = u.hm.Host(c.HostKey, c.ID, c.SiamuxAddr)
	u.fcid = c.ID
	u.siamuxAddr = c.SiamuxAddr
	u.endHeight = c.WindowEnd
}

func (u *uploader) Start() {
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

			// sanity check lock duration and priority are set
			if req.contractLockDuration == 0 || req.contractLockPriority == 0 {
				panic("lock duration and priority can't be 0") // developer error
			}

			// execute it
			start := time.Now()
			duration, err := u.execute(req)
			elapsed := time.Since(start)
			if errors.Is(err, rhp3.ErrMaxRevisionReached) {
				if u.tryRefresh(req.sector.ctx) {
					u.enqueue(req)
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
			} else if uploadEstimateMS == 0 && err != nil && !utils.IsErr(err, errSectorUploadFinished) {
				u.logger.Debugw("sector upload failure was ignored", "uploadError", err, "uploadDuration", duration, "totalDuration", elapsed, "overdrive", req.overdrive, "hk", u.hk)
			}

			// send the response
			select {
			case <-req.sector.ctx.Done():
			case req.responseChan <- sectorUploadResp{
				req: req,
				err: err,
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
	if utils.IsErr(uploadErr, errSectorUploadFinished) {
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

func (u *uploader) Stop(err error) {
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

func (u *uploader) enqueue(req *sectorUploadReq) {
	u.mu.Lock()
	// check for stopped
	if u.stopped {
		u.mu.Unlock()
		go req.finish(errUploaderStopped) // don't block the caller
		return
	}

	// decorate the request
	req.fcid = u.fcid
	req.hk = u.hk

	// enqueue the request
	u.queue = append(u.queue, req)
	u.mu.Unlock()

	// signal there's work
	u.signalWork()
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

// execute executes the sector upload request, if the upload was successful it
// returns the time it took to upload the sector to the host
func (u *uploader) execute(req *sectorUploadReq) (_ time.Duration, err error) {
	// grab fields
	u.mu.Lock()
	host := u.host
	fcid := u.fcid
	u.mu.Unlock()

	// wrap cause
	defer func() {
		if cause := context.Cause(req.sector.ctx); cause != nil && !utils.IsErr(err, cause) {
			if err != nil {
				err = fmt.Errorf("%w; %w", cause, err)
			} else {
				err = cause
			}
		}
	}()

	// acquire contract lock
	lockID, err := u.cl.AcquireContract(req.sector.ctx, fcid, req.contractLockPriority, req.contractLockDuration)
	if err != nil {
		return 0, fmt.Errorf("%w; %w", errAcquireContractFailed, err)
	}

	// defer the release
	lock := locking.NewContractLock(u.shutdownCtx, fcid, lockID, req.contractLockDuration, u.cl, u.logger)
	defer func() {
		ctx, cancel := context.WithTimeout(u.shutdownCtx, 10*time.Second)
		lock.Release(ctx)
		cancel()
	}()

	// apply sane timeout
	ctx, cancel := context.WithTimeout(req.sector.ctx, sectorUploadTimeout)
	defer cancel()

	// fetch the revision
	rev, err := host.FetchRevision(ctx, defaultRevisionFetchTimeout)
	if err != nil {
		return 0, fmt.Errorf("%w; %w", errFetchRevisionFailed, err)
	} else if rev.RevisionNumber == math.MaxUint64 {
		return 0, rhp3.ErrMaxRevisionReached
	}

	// update the bus
	if err := u.os.AddUploadingSector(ctx, req.uploadID, fcid, req.sector.root); err != nil {
		return 0, fmt.Errorf("failed to add uploading sector to contract %v; %w", fcid, err)
	}

	// upload the sector
	start := time.Now()
	err = host.UploadSector(ctx, req.sector.root, req.sector.sectorData(), rev)
	if err != nil {
		return 0, fmt.Errorf("failed to upload sector to contract %v; %w", fcid, err)
	}

	return time.Since(start), nil
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

func (u *uploader) signalWork() {
	select {
	case u.signalNewUpload <- struct{}{}:
	default:
	}
}

func (u *uploader) trackConsecutiveFailures(success, failure bool) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if success {
		u.consecutiveFailures = 0
	} else if failure {
		u.consecutiveFailures++
	}
}

func (u *uploader) trackSectorUploadStats(uploadEstimateMS, uploadSpeedBytesPerMS float64) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if uploadEstimateMS > 0 {
		u.statsSectorUploadEstimateInMS.Track(uploadEstimateMS)
	}
	if uploadSpeedBytesPerMS > 0 {
		u.statsSectorUploadSpeedBytesPerMS.Track(uploadSpeedBytesPerMS)
	}
}

func (u *uploader) tryRecomputeStats() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if time.Since(u.lastRecompute) < statsRecomputeMinInterval {
		return
	}

	u.lastRecompute = time.Now()
	u.statsSectorUploadEstimateInMS.Recompute()
	u.statsSectorUploadSpeedBytesPerMS.Recompute()
}

func (u *uploader) tryRefresh(ctx context.Context) bool {
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
