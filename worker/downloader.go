package worker

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/stats"
	"go.uber.org/zap"
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
		host   Host
		ms     MetricStore
		logger *zap.SugaredLogger

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

func (mgr *downloadManager) newDownloader(c api.ContractMetadata) *downloader {
	return &downloader{
		ms:     mgr.ms,
		logger: mgr.logger,

		// static
		shutdownCtx:    mgr.shutdownCtx,
		signalWorkChan: make(chan struct{}, 1),

		// stats
		statsSectorDownloadEstimateInMS: stats.Default(),
		statsDownloadSpeedBytesPerMS:    stats.NoDecay(),

		// covered by mutex
		host:  mgr.hm.Host(c.HostKey, c.ID, c.SiamuxAddr),
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

func (d *downloader) execute(req *sectorDownloadReq) ([]byte, time.Duration, error) {
	// prepare buffer
	buf := bytes.NewBuffer(make([]byte, 0, req.length))

	// download the sector
	start := time.Now()
	err := d.host.DownloadSector(req.ctx, buf, req.root, req.offset, req.length, req.overpay)
	if err != nil {
		return nil, 0, err
	}

	// calculate elapsed time
	elapsed := time.Since(start)
	return buf.Bytes(), elapsed, nil
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

			// execute the request
			sector, elapsed, err := d.execute(req)
			if err != nil {
				req.fail(err)
			} else {
				req.succeed(sector)
				if err := d.recordMetrics(req.host.PublicKey(), req.length, elapsed); err != nil {
					d.logger.Errorf("failed to record metrics, err %v", err)
				}
			}

			d.updateEstimate(err, elapsed)
		}

		// last worker that's done closes the channel and flushes the stats
		if atomic.AddUint64(&inflight, ^uint64(0)) == 0 {
			close(doneChan)
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

func (d *downloader) processQueue(hp HostManager) {
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

func (d *downloader) recordMetrics(hk types.PublicKey, downloaded uint32, elapsed time.Duration) error {
	// derive timeout ctx from the bg context to ensure we record metrics on shutdown
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// update stats (should be deprecated)
	ms := elapsed.Milliseconds()
	if ms == 0 {
		ms = 1 // avoid division by zero
	}
	d.statsDownloadSpeedBytesPerMS.Track(float64(downloaded) / float64(ms))

	// record performance metric
	return d.ms.RecordPerformanceMetric(ctx, api.PerformanceMetric{
		Timestamp: api.TimeNow(),
		Action:    "downloadsector",
		HostKey:   hk,
		Origin:    "worker",
		Duration:  elapsed,
	})
}

func (d *downloader) updateEstimate(err error, elapsed time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// track download
	d.numDownloads++

	// host is not to blame for these errors
	if isBalanceInsufficient(err) ||
		isPriceTableExpired(err) ||
		isPriceTableNotFound(err) ||
		isSectorNotFound(err) {
		return
	}

	// handle error
	if err != nil {
		d.consecutiveFailures++
		d.statsSectorDownloadEstimateInMS.Track(float64(time.Hour.Milliseconds()))
		return
	}

	ms := elapsed.Milliseconds()
	if ms == 0 {
		ms = 1 // avoid division by zero
	}

	d.consecutiveFailures = 0
	d.statsSectorDownloadEstimateInMS.Track(float64(ms))
}
