package autopilot

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

const (
	scannerTimeoutInterval   = 10 * time.Minute
	scannerTimeoutMinTimeout = 10 * time.Second

	trackerMinDataPoints     = 25
	trackerNumDataPoints     = 1000
	trackerTimeoutPercentile = 99
)

type (
	scanner struct {
		// TODO: use the actual bus and worker interfaces when they've consolidated
		// a bit, we currently use inline interfaces to avoid having to update the
		// scanner tests with every interface change
		bus interface {
			SearchHosts(ctx context.Context, opts api.SearchHostOptions) ([]api.Host, error)
			HostsForScanning(ctx context.Context, opts api.HostsForScanningOptions) ([]hostdb.HostAddress, error)
			RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (uint64, error)
		}

		tracker *tracker
		logger  *zap.SugaredLogger
		ap      *Autopilot
		wg      sync.WaitGroup

		scanBatchSize   uint64
		scanThreads     uint64
		scanMinInterval time.Duration

		timeoutMinInterval time.Duration
		timeoutMinTimeout  time.Duration

		mu                sync.Mutex
		scanning          bool
		scanningLastStart time.Time
		timeout           time.Duration
		timeoutLastUpdate time.Time
		interruptScanChan chan struct{}
	}
	scanWorker interface {
		RHPScan(ctx context.Context, hostKey types.PublicKey, hostIP string, timeout time.Duration) (api.RHPScanResponse, error)
	}

	scanReq struct {
		hostKey types.PublicKey
		hostIP  string
	}

	scanResp struct {
		hostKey  types.PublicKey
		settings rhpv2.HostSettings
		err      error
	}

	tracker struct {
		threshold  uint64
		percentile float64

		mu      sync.Mutex
		count   uint64
		timings []float64
	}
)

func newTracker(threshold, total uint64, percentile float64) *tracker {
	return &tracker{
		threshold:  threshold,
		percentile: percentile,
		timings:    make([]float64, total),
	}
}

func (t *tracker) addDataPoint(duration time.Duration) {
	if duration == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.timings[t.count%uint64(len(t.timings))] = float64(duration.Milliseconds())

	// NOTE: we silently overflow and disregard the threshold being reapplied
	// when we overflow entirely, since we only ever increment the count with 1
	// it will never happen
	t.count += 1
}

func (t *tracker) timeout() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.count < uint64(t.threshold) {
		return 0
	}

	percentile, err := percentile(t.timings, t.percentile)
	if err != nil {
		return 0
	}

	return time.Duration(percentile) * time.Millisecond
}

func newScanner(ap *Autopilot, scanBatchSize, scanThreads uint64, scanMinInterval, timeoutMinInterval, timeoutMinTimeout time.Duration) (*scanner, error) {
	if scanBatchSize == 0 {
		return nil, errors.New("scanner batch size has to be greater than zero")
	}
	if scanThreads == 0 {
		return nil, errors.New("scanner threads has to be greater than zero")
	}

	return &scanner{
		bus: ap.bus,
		tracker: newTracker(
			trackerMinDataPoints,
			trackerNumDataPoints,
			trackerTimeoutPercentile,
		),
		logger: ap.logger.Named("scanner"),
		ap:     ap,

		interruptScanChan: make(chan struct{}),

		scanBatchSize:   scanBatchSize,
		scanThreads:     scanThreads,
		scanMinInterval: scanMinInterval,

		timeoutMinInterval: timeoutMinInterval,
		timeoutMinTimeout:  timeoutMinTimeout,
	}, nil
}

func (s *scanner) Status() (bool, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanning, s.scanningLastStart
}

func (s *scanner) isInterrupted() bool {
	select {
	case <-s.interruptScanChan:
		return true
	default:
		return false
	}
}

func (s *scanner) tryPerformHostScan(ctx context.Context, w scanWorker, force bool) {
	if s.ap.isStopped() {
		return
	}

	scanType := "host scan"
	if force {
		scanType = "forced scan"
	}

	s.mu.Lock()
	if force {
		close(s.interruptScanChan)
		s.mu.Unlock()

		s.logger.Infof("waiting for ongoing scan to complete")
		s.wg.Wait()

		s.mu.Lock()
		s.interruptScanChan = make(chan struct{})
	} else if s.scanning || !s.isScanRequired() {
		s.mu.Unlock()
		return
	}
	s.scanningLastStart = time.Now()
	s.scanning = true
	s.mu.Unlock()

	s.logger.Infof("%s started", scanType)

	s.wg.Add(1)
	go func(st string) {
		defer s.wg.Done()

		var interrupted bool
		for resp := range s.launchScanWorkers(ctx, w, s.launchHostScans()) {
			if s.isInterrupted() || s.ap.isStopped() {
				interrupted = true
				break
			}
			if resp.err != nil && !strings.Contains(resp.err.Error(), "connection refused") {
				s.logger.Error(resp.err)
			}
		}

		// fetch the config right before removing offline hosts to get the most
		// recent settings in case they were updated while scanning.
		hostCfg := s.ap.State().cfg.Hosts
		maxDowntime := time.Duration(hostCfg.MaxDowntimeHours) * time.Hour
		minRecentScanFailures := hostCfg.MinRecentScanFailures

		if !interrupted && maxDowntime > 0 {
			s.logger.Infof("removing hosts that have been offline for more than %v and have failed at least %d scans", maxDowntime, minRecentScanFailures)
			removed, err := s.bus.RemoveOfflineHosts(ctx, minRecentScanFailures, maxDowntime)
			if err != nil {
				s.logger.Errorf("error occurred while removing offline hosts, err: %v", err)
			} else if removed > 0 {
				s.logger.Infof("removed %v offline hosts", removed)
			}
		}

		s.mu.Lock()
		s.scanning = false
		s.logger.Infof("%s finished after %v", st, time.Since(s.scanningLastStart))
		s.mu.Unlock()
	}(scanType)
	return
}

func (s *scanner) tryUpdateTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isTimeoutUpdateRequired() {
		return
	}

	updated := s.tracker.timeout()
	if updated < s.timeoutMinTimeout {
		s.logger.Infof("updated timeout is lower than min timeout, %v<%v", updated, s.timeoutMinTimeout)
		updated = s.timeoutMinTimeout
	}

	if s.timeout != updated {
		s.logger.Infof("updated timeout %v->%v", s.timeout, updated)
		s.timeout = updated
	}
	s.timeoutLastUpdate = time.Now()
}

func (s *scanner) launchHostScans() chan scanReq {
	reqChan := make(chan scanReq, s.scanBatchSize)

	s.ap.wg.Add(1)
	go func() {
		defer s.ap.wg.Done()
		defer close(reqChan)

		var offset int
		var exhausted bool
		cutoff := time.Now().Add(-s.scanMinInterval)
		for !s.ap.isStopped() && !exhausted {
			// fetch next batch
			hosts, err := s.bus.HostsForScanning(s.ap.shutdownCtx, api.HostsForScanningOptions{
				MaxLastScan: api.TimeRFC3339(cutoff),
				Offset:      offset,
				Limit:       int(s.scanBatchSize),
			})
			if err != nil {
				s.logger.Errorf("could not get hosts for scanning, err: %v", err)
				break
			}
			if len(hosts) == 0 {
				break
			}
			if len(hosts) < int(s.scanBatchSize) {
				exhausted = true
			}

			s.logger.Infof("scanning %d hosts in range %d-%d", len(hosts), offset, offset+int(s.scanBatchSize))
			offset += int(s.scanBatchSize)

			// add batch to scan queue
			for _, h := range hosts {
				select {
				case <-s.ap.shutdownCtx.Done():
					return
				case reqChan <- scanReq{
					hostKey: h.PublicKey,
					hostIP:  h.NetAddress,
				}:
				}
			}
		}
	}()

	return reqChan
}

func (s *scanner) launchScanWorkers(ctx context.Context, w scanWorker, reqs chan scanReq) chan scanResp {
	respChan := make(chan scanResp, s.scanThreads)
	liveThreads := s.scanThreads

	for i := uint64(0); i < s.scanThreads; i++ {
		go func() {
			for req := range reqs {
				if s.ap.isStopped() {
					break // shutdown
				}

				scan, err := w.RHPScan(ctx, req.hostKey, req.hostIP, s.currentTimeout())
				if err != nil {
					break // abort
				} else if !utils.IsErr(errors.New(scan.ScanError), errIOTimeout) && scan.Ping > 0 {
					s.tracker.addDataPoint(time.Duration(scan.Ping))
				}

				respChan <- scanResp{req.hostKey, scan.Settings, err}
			}

			if atomic.AddUint64(&liveThreads, ^uint64(0)) == 0 {
				close(respChan)
			}
		}()
	}

	return respChan
}

func (s *scanner) isScanRequired() bool {
	return s.scanningLastStart.IsZero() || time.Since(s.scanningLastStart) > s.scanMinInterval/20 // check 20 times per minInterval, so every 30 minutes
}

func (s *scanner) isTimeoutUpdateRequired() bool {
	return s.timeoutLastUpdate.IsZero() || time.Since(s.timeoutLastUpdate) > s.timeoutMinInterval
}

func (s *scanner) currentTimeout() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.timeout
}
