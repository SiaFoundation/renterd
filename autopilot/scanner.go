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
	"go.uber.org/zap"
)

const (
	// TODO: make these configurable
	scannerTimeoutInterval   = 10 * time.Minute
	scannerTimeoutMinTimeout = time.Second * 5

	// TODO: make these configurable
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
			Hosts(ctx context.Context, opts api.GetHostsOptions) ([]hostdb.Host, error)
			HostsForScanning(ctx context.Context, opts api.HostsForScanningOptions) ([]hostdb.HostAddress, error)
			RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (uint64, error)
		}

		tracker *tracker
		logger  *zap.SugaredLogger
		ap      *Autopilot

		scanBatchSize         uint64
		scanThreads           uint64
		scanMinInterval       time.Duration
		scanMinRecentFailures uint64

		timeoutMinInterval time.Duration
		timeoutMinTimeout  time.Duration

		mu                sync.Mutex
		scanning          bool
		scanningLastStart time.Time
		timeout           time.Duration
		timeoutLastUpdate time.Time
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

func newScanner(ap *Autopilot, scanBatchSize, scanMinRecentFailures, scanThreads uint64, scanMinInterval, timeoutMinInterval, timeoutMinTimeout time.Duration) (*scanner, error) {
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

		scanBatchSize:         scanBatchSize,
		scanThreads:           scanThreads,
		scanMinInterval:       scanMinInterval,
		scanMinRecentFailures: scanMinRecentFailures,

		timeoutMinInterval: timeoutMinInterval,
		timeoutMinTimeout:  timeoutMinTimeout,
	}, nil
}

func (s *scanner) Status() (bool, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanning, s.scanningLastStart
}

func (s *scanner) tryPerformHostScan(ctx context.Context, w scanWorker, force bool) bool {
	if s.ap.isStopped() {
		return false
	}

	s.mu.Lock()
	if !force && (s.scanning || !s.isScanRequired()) {
		s.mu.Unlock()
		return false
	}

	s.logger.Info("host scan started")
	s.scanningLastStart = time.Now()
	s.scanning = true
	s.mu.Unlock()

	maxDowntimeHours := s.ap.State().cfg.Hosts.MaxDowntimeHours

	go func() {
		for resp := range s.launchScanWorkers(ctx, w, s.launchHostScans()) {
			if s.ap.isStopped() {
				break
			}
			if resp.err != nil && !strings.Contains(resp.err.Error(), "connection refused") {
				s.logger.Error(resp.err)
			}
		}

		if !s.ap.isStopped() && maxDowntimeHours > 0 {
			s.logger.Debugf("removing hosts that have been offline for more than %v hours", maxDowntimeHours)
			maxDowntime := time.Hour * time.Duration(maxDowntimeHours)
			removed, err := s.bus.RemoveOfflineHosts(ctx, s.scanMinRecentFailures, maxDowntime)
			if removed > 0 {
				s.logger.Infof("removed %v offline hosts", removed)
			}
			if err != nil {
				s.logger.Errorf("error occurred while removing offline hosts, err: %v", err)
			}
		}

		s.mu.Lock()
		s.scanning = false
		s.logger.Debugf("host scan finished after %v", time.Since(s.scanningLastStart))
		s.mu.Unlock()
	}()
	return true
}

func (s *scanner) tryUpdateTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isTimeoutUpdateRequired() {
		return
	}

	updated := s.tracker.timeout()
	if updated < s.timeoutMinTimeout {
		s.logger.Debugf("updated timeout is lower than min timeout, %v<%v", updated, s.timeoutMinTimeout)
		updated = s.timeoutMinTimeout
	}

	if s.timeout != updated {
		s.logger.Debugf("updated timeout %v->%v", s.timeout, updated)
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
			hosts, err := s.bus.HostsForScanning(context.Background(), api.HostsForScanningOptions{
				MaxLastScan: cutoff,
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

			s.logger.Debugf("scanning %d hosts in range %d-%d", len(hosts), offset, offset+int(s.scanBatchSize))
			offset += int(s.scanBatchSize)

			// add batch to scan queue
			for _, h := range hosts {
				select {
				case <-s.ap.stopChan:
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
				}
				respChan <- scanResp{req.hostKey, scan.Settings, err}
				s.tracker.addDataPoint(time.Duration(scan.Ping))
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
