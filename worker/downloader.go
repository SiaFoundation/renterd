package worker

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/stats"
)

const (
	downloadOverheadB           = 284
	maxConcurrentSectorsPerHost = 3
)

var (
	errDownloaderStopped = errors.New("downloader was stopped")
)

type (
	downloader struct {
		host Host

		statsDownloadSpeedBytesPerMS    *stats.DataPoints // keep track of this separately for stats (no decay is applied)
		statsSectorDownloadEstimateInMS *stats.DataPoints

		signalWorkChan chan struct{}
		shutdownCtx    context.Context

		mu                  sync.Mutex
		consecutiveFailures uint64
		numDownloads        uint64
		queue               []*sectorDownloadReq
		stopped             bool
	}
)

func newDownloader(ctx context.Context, host Host) *downloader {
	return &downloader{
		host: host,

		statsSectorDownloadEstimateInMS: stats.Default(),
		statsDownloadSpeedBytesPerMS:    stats.NoDecay(),

		signalWorkChan: make(chan struct{}, 1),
		shutdownCtx:    ctx,

		queue: make([]*sectorDownloadReq, 0),
	}
}

func (d *downloader) PublicKey() types.PublicKey {
	return d.host.PublicKey()
}

func (d *downloader) Stop() {
	d.mu.Lock()
	d.stopped = true
	d.mu.Unlock()

	for {
		download := d.pop()
		if download == nil {
			break
		}
		if !download.done() {
			download.fail(errDownloaderStopped)
		}
	}
}

func (d *downloader) fillBatch() (batch []*sectorDownloadReq) {
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

func (d *downloader) enqueue(download *sectorDownloadReq) {
	d.mu.Lock()
	// check for stopped
	if d.stopped {
		d.mu.Unlock()
		go download.fail(errDownloaderStopped) // don't block the caller
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

func (d *downloader) estimate() float64 {
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

func (d *downloader) execute(req *sectorDownloadReq) (err error) {
	// download the sector
	buf := bytes.NewBuffer(make([]byte, 0, req.length))
	err = d.host.DownloadSector(req.ctx, buf, req.root, req.offset, req.length, req.overpay)
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

func (d *downloader) pop() *sectorDownloadReq {
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

func (d *downloader) processBatch(batch []*sectorDownloadReq) chan struct{} {
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
	reqsChan := make(chan *sectorDownloadReq)
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
				downloadedB += int64(req.length) + downloadOverheadB
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

func (d *downloader) processQueue() {
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

func (d *downloader) stats() downloaderStats {
	d.mu.Lock()
	defer d.mu.Unlock()
	return downloaderStats{
		avgSpeedMBPS: d.statsDownloadSpeedBytesPerMS.Average() * 0.008,
		healthy:      d.consecutiveFailures == 0,
		numDownloads: d.numDownloads,
	}
}

func (d *downloader) trackFailure(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err == nil {
		d.consecutiveFailures = 0
		return
	}

	if isBalanceInsufficient(err) ||
		isPriceTableExpired(err) ||
		isPriceTableNotFound(err) ||
		isSectorNotFound(err) {
		return // host is not to blame for these errors
	}

	d.consecutiveFailures++
	d.statsSectorDownloadEstimateInMS.Track(float64(time.Hour.Milliseconds()))
}
