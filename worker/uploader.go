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
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/stats"
	"go.uber.org/zap"
)

const (
	sectorUploadTimeout = 60 * time.Second
)

var (
	errUploaderStopped = errors.New("uploader was stopped")
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

		statsSectorUploadEstimateInMS    *stats.DataPoints
		statsSectorUploadSpeedBytesPerMS *stats.DataPoints
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
			if err == nil {
				// only track the time it took to upload the sector in the happy case
				u.trackSectorUpload(true, duration)
				u.trackConsecutiveFailures(true)
			} else if errors.Is(err, errMaxRevisionReached) {
				// the uploader's contract got renewed, requeue the request
				if err := u.tryRefresh(); err == nil {
					u.enqueue(req)
					continue outer
				} else if !utils.IsErr(err, context.Canceled) {
					u.logger.Errorf("failed to refresh the uploader's contract %v, err: %v", u.ContractID(), err)
				}
				u.logger.Debugw("skip tracking sector upload", "total", time.Since(start), "duration", duration, "overdrive", req.overdrive, "err", err)
			} else if errors.Is(err, errSectorUploadFinished) && !req.overdrive {
				// punish the slow host by tracking a multiple of the total time
				// we lost on it, but we only do so if we weren't overdriving,
				// also note we are not tracking consecutive failures here
				// because we're not sure if we had a successful host
				// interaction
				u.trackSectorUpload(true, time.Since(start)*10)
			} else if !errors.Is(err, errSectorUploadFinished) {
				// punish the host for failing the upload
				u.trackSectorUpload(false, time.Hour)
				u.trackConsecutiveFailures(false)
				u.logger.Debugw("penalising host for failing to upload sector", "hk", u.hk, "overdrive", req.overdrive, "err", err)
			} else {
				u.logger.Debugw("skip tracking sector upload", "total", time.Since(start), "duration", duration, "overdrive", req.overdrive, "err", err)
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
func (u *uploader) execute(req *sectorUploadReq) (time.Duration, error) {
	// grab fields
	u.mu.Lock()
	host := u.host
	fcid := u.fcid
	u.mu.Unlock()

	// acquire contract lock
	lockID, err := u.cl.AcquireContract(req.sector.ctx, fcid, req.contractLockPriority, req.contractLockDuration)
	if err != nil {
		return 0, err
	}

	// defer the release
	lock := newContractLock(u.shutdownCtx, fcid, lockID, req.contractLockDuration, u.cl, u.logger)
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
		return 0, err
	} else if rev.RevisionNumber == math.MaxUint64 {
		return 0, errMaxRevisionReached
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

func (u *uploader) trackConsecutiveFailures(success bool) {
	u.mu.Lock()
	defer u.mu.Unlock()

	// update consecutive failures
	if success {
		u.consecutiveFailures = 0
	} else {
		u.consecutiveFailures++
	}
}

func (u *uploader) trackSectorUpload(success bool, d time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()

	// sanitize input
	ms := d.Milliseconds()
	if ms == 0 {
		ms = 1 // avoid division by zero
	}

	// update estimates
	if success {
		u.statsSectorUploadEstimateInMS.Track(float64(ms))
		u.statsSectorUploadSpeedBytesPerMS.Track(float64(rhpv2.SectorSize / ms)) // bytes per ms
	} else {
		u.statsSectorUploadEstimateInMS.Track(float64(ms))
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

func (u *uploader) tryRefresh() error {
	// use a sane timeout
	ctx, cancel := context.WithTimeout(u.shutdownCtx, 30*time.Second)
	defer cancel()

	// fetch the renewed contract
	renewed, err := u.cs.RenewedContract(ctx, u.ContractID())
	if err != nil {
		return err
	}

	// renew the uploader with the renewed contract
	u.Refresh(renewed)
	return nil
}
