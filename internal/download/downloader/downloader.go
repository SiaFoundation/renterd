package downloader

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/internal/host"
	rhp3 "go.sia.tech/renterd/v2/internal/rhp/v3"
	"go.sia.tech/renterd/v2/internal/utils"
)

const (
	downloadOverheadB           = 284
	maxConcurrentSectorsPerHost = 3
	statsRecomputeMinInterval   = 3 * time.Second
)

var (
	ErrStopped = errors.New("downloader was stopped")
)

type (
	SectorDownloadReq struct {
		Ctx context.Context

		Length uint64
		Offset uint64
		Root   types.Hash256
		Host   *Downloader

		Overdrive   bool
		SectorIndex int
		Resps       *SectorResponses
	}

	SectorDownloadResp struct {
		Req    *SectorDownloadReq
		Sector []byte
		Err    error
	}

	SectorResponses struct {
		c chan struct{} // signal that a new response is available

		mu        sync.Mutex
		closed    bool
		responses []*SectorDownloadResp
	}
)
type (
	Downloader struct {
		host host.Downloader

		statsDownloadSpeedBytesPerMS    *utils.DataPoints // keep track of this separately for stats (no decay is applied)
		statsSectorDownloadEstimateInMS *utils.DataPoints

		signalWorkChan chan struct{}
		shutdownCtx    context.Context

		mu                  sync.Mutex
		consecutiveFailures uint64
		lastRecompute       time.Time

		numDownloads uint64
		queue        []*SectorDownloadReq
		stopped      bool
	}
)

func New(ctx context.Context, h host.Downloader) *Downloader {
	return &Downloader{
		host: h,

		statsSectorDownloadEstimateInMS: utils.NewDataPoints(10 * time.Minute),
		statsDownloadSpeedBytesPerMS:    utils.NewDataPoints(0),

		signalWorkChan: make(chan struct{}, 1),
		shutdownCtx:    ctx,

		queue: make([]*SectorDownloadReq, 0),
	}
}

func NewSectorResponses() *SectorResponses {
	return &SectorResponses{
		c: make(chan struct{}, 1),
	}
}

func (d *Downloader) AvgDownloadSpeedBytesPerMS() float64 {
	return d.statsDownloadSpeedBytesPerMS.Average()
}

func (d *Downloader) Enqueue(download *SectorDownloadReq) {
	d.mu.Lock()
	// check for stopped
	if d.stopped {
		d.mu.Unlock()
		go download.fail(ErrStopped) // don't block the caller
		return
	}

	// enqueue the job
	d.queue = append(d.queue, download)
	d.mu.Unlock()

	// signal there's work
	select {
	case d.signalWorkChan <- struct{}{}:
	default:
	}
}

func (d *Downloader) Estimate() float64 {
	d.mu.Lock()
	defer d.mu.Unlock()

	// fetch estimated duration per sector
	estimateP90 := d.statsSectorDownloadEstimateInMS.P90()
	if estimateP90 == 0 {
		if avg := d.statsSectorDownloadEstimateInMS.Average(); avg > 0 {
			estimateP90 = avg
		} else {
			estimateP90 = 1
		}
	}

	numSectors := float64(len(d.queue) + 1)
	return numSectors * estimateP90
}

func (d *Downloader) Healthy() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.consecutiveFailures == 0
}

func (d *Downloader) PublicKey() types.PublicKey {
	return d.host.PublicKey()
}

func (d *Downloader) Stop(err error) {
	d.mu.Lock()
	d.stopped = true
	d.mu.Unlock()

	for {
		download := d.pop()
		if download == nil {
			break
		}
		if !download.done() {
			download.fail(err)
		}
	}
}

func (d *Downloader) TryRecomputeStats() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if time.Since(d.lastRecompute) < statsRecomputeMinInterval {
		return
	}

	d.lastRecompute = time.Now()
	d.statsDownloadSpeedBytesPerMS.Recompute()
	d.statsSectorDownloadEstimateInMS.Recompute()
}

func (d *Downloader) execute(req *SectorDownloadReq) (err error) {
	// download the sector
	buf := bytes.NewBuffer(make([]byte, 0, req.Length))
	err = d.host.DownloadSector(req.Ctx, buf, req.Root, req.Offset, req.Length)
	if err != nil {
		req.fail(err)
		return err
	}

	d.mu.Lock()
	d.numDownloads++
	d.mu.Unlock()

	req.succeed(buf.Bytes())
	return nil
}

func (d *Downloader) fillBatch() (batch []*SectorDownloadReq) {
	for len(batch) < maxConcurrentSectorsPerHost {
		if req := d.pop(); req == nil {
			break
		} else if req.done() {
			continue
		} else {
			batch = append(batch, req)
		}
	}
	return
}

func (d *Downloader) pop() *SectorDownloadReq {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.queue) > 0 {
		j := d.queue[0]
		d.queue[0] = nil
		d.queue = d.queue[1:]
		return j
	}
	return nil
}

func (d *Downloader) processBatch(batch []*SectorDownloadReq) chan struct{} {
	doneChan := make(chan struct{})

	// define some state to keep track of stats
	var mu sync.Mutex
	var start time.Time
	var concurrent int64
	var downloadedB int64
	trackStatsFn := func() {
		if start.IsZero() || time.Since(start).Milliseconds() == 0 || downloadedB == 0 {
			return
		}
		durationMS := time.Since(start).Milliseconds()
		d.statsDownloadSpeedBytesPerMS.Track(float64(downloadedB / durationMS))
		d.statsSectorDownloadEstimateInMS.Track(float64(durationMS))
		start = time.Time{}
		downloadedB = 0
	}

	// define a worker to process download requests
	inflight := uint64(len(batch))
	reqsChan := make(chan *SectorDownloadReq)
	workerFn := func() {
		for req := range reqsChan {
			// check if we need to abort
			select {
			case <-d.shutdownCtx.Done():
				return
			default:
			}

			// update state
			mu.Lock()
			if start.IsZero() {
				start = time.Now()
			}
			concurrent++
			mu.Unlock()

			// execute the request
			err := d.execute(req)
			d.trackFailure(err)

			// update state + potentially track stats
			mu.Lock()
			if err == nil {
				downloadedB += int64(req.Length) + downloadOverheadB
				if downloadedB >= maxConcurrentSectorsPerHost*rhpv2.SectorSize || concurrent == maxConcurrentSectorsPerHost {
					trackStatsFn()
				}
			}
			concurrent--
			if concurrent < 0 {
				panic("concurrent can never be less than zero") // developer error
			}
			mu.Unlock()
		}

		// last worker that's done closes the channel and flushes the stats
		if atomic.AddUint64(&inflight, ^uint64(0)) == 0 {
			close(doneChan)
			trackStatsFn()
		}
	}

	// launch workers
	for i := 0; i < len(batch); i++ {
		go workerFn()
	}
	for _, req := range batch {
		reqsChan <- req
	}

	// launch a goroutine to keep the request coming
	go func() {
		defer close(reqsChan)
		for {
			if req := d.pop(); req == nil {
				break
			} else if req.done() {
				continue
			} else {
				reqsChan <- req
			}
		}
	}()

	return doneChan
}

func (d *Downloader) Start() {
outer:
	for {
		// wait for work
		select {
		case <-d.signalWorkChan:
		case <-d.shutdownCtx.Done():
			return
		}

		for {
			// try fill a batch of requests
			batch := d.fillBatch()
			if len(batch) == 0 {
				continue outer
			}

			// process the batch
			doneChan := d.processBatch(batch)
			for {
				select {
				case <-d.shutdownCtx.Done():
					return
				case <-doneChan:
					continue outer
				}
			}
		}
	}
}

func (d *Downloader) trackFailure(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err == nil {
		d.consecutiveFailures = 0
		return
	}

	if utils.IsBalanceInsufficient(err) ||
		rhp3.IsPriceTableExpired(err) ||
		rhp3.IsPriceTableNotFound(err) ||
		rhp3.IsSectorNotFound(err) ||
		rhpv4.ErrorCode(err) == rhpv4.ErrorCodeBadRequest ||
		rhpv4.ErrorCode(err) == rhpv4.ErrorCodePayment ||
		utils.IsErr(err, rhpv4.ErrSectorNotFound) {
		return // host is not to blame for these errors
	}

	d.consecutiveFailures++
	d.statsSectorDownloadEstimateInMS.Track(float64(time.Hour.Milliseconds()))
}

func (sr *SectorResponses) Add(resp *SectorDownloadResp) {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if sr.closed {
		return
	}
	sr.responses = append(sr.responses, resp)
	select {
	case sr.c <- struct{}{}:
	default:
	}
}

func (sr *SectorResponses) Close() error {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	sr.closed = true
	sr.responses = nil // clear responses
	close(sr.c)
	return nil
}

func (sr *SectorResponses) Next() *SectorDownloadResp {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	if len(sr.responses) == 0 {
		return nil
	}
	resp := sr.responses[0]
	sr.responses = sr.responses[1:]
	return resp
}

func (sr *SectorResponses) Received() chan struct{} {
	return sr.c
}

func (req *SectorDownloadReq) done() bool {
	select {
	case <-req.Ctx.Done():
		return true
	default:
		return false
	}
}

func (req *SectorDownloadReq) fail(err error) {
	req.Resps.Add(&SectorDownloadResp{
		Req: req,
		Err: err,
	})
}

func (req *SectorDownloadReq) succeed(sector []byte) {
	req.Resps.Add(&SectorDownloadResp{
		Req:    req,
		Sector: sector,
	})
}
