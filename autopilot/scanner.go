package autopilot

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
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
			Hosts(offset, limit int) ([]hostdb.Host, error)
			HostsForScanning(maxLastScan time.Time, offset, limit int) ([]hostdb.HostAddress, error)
		}
		worker interface {
			RHPScan(hostKey types.PublicKey, hostIP string, timeout time.Duration) (api.RHPScanResponse, error)
		}

		tracker *tracker
		logger  *zap.SugaredLogger

		scanBatchSize   uint64
		scanThreads     uint64
		scanMinInterval time.Duration

		timeoutMinInterval time.Duration
		timeoutMinTimeout  time.Duration

		stopChan chan struct{}

		mu                sync.Mutex
		scanning          bool
		scanningLastStart time.Time
		timeout           time.Duration
		timeoutLastUpdate time.Time
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
		bus:    ap.bus,
		worker: ap.worker,
		tracker: newTracker(
			trackerMinDataPoints,
			trackerNumDataPoints,
			trackerTimeoutPercentile,
		),
		logger: ap.logger.Named("scanner"),

		stopChan: ap.stopChan,

		scanBatchSize:   scanBatchSize,
		scanThreads:     scanThreads,
		scanMinInterval: scanMinInterval,

		timeoutMinInterval: timeoutMinInterval,
		timeoutMinTimeout:  timeoutMinTimeout,
	}, nil
}

func (s *scanner) tryPerformHostScan() {
	s.mu.Lock()
	if s.scanning || !s.isScanRequired() {
		s.mu.Unlock()
		return
	}

	s.logger.Info("host scan started")
	s.scanningLastStart = time.Now()
	s.scanning = true
	s.mu.Unlock()

	go func() {
		for resp := range s.launchScanWorkers(s.launchHostScans()) {
			if s.isStopped() {
				break
			}
			if resp.err != nil {
				s.logger.Error(resp.err)
			}
		}

		s.mu.Lock()
		s.scanning = false
		s.logger.Debugf("host scan finished after %v", time.Since(s.scanningLastStart))
		s.mu.Unlock()
	}()
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

	prev := s.timeout
	s.timeout = updated
	s.timeoutLastUpdate = time.Now()
	s.logger.Debugf("updated timeout %v->%v", prev, s.timeout)
}

func (s *scanner) launchHostScans() chan scanReq {
	reqChan := make(chan scanReq, s.scanBatchSize)

	go func() {
		var offset int
		var exhausted bool
		cutoff := time.Now().Add(-s.scanMinInterval)
		for !s.isStopped() && !exhausted {
			// fetch next batch
			hosts, err := s.bus.HostsForScanning(cutoff, offset, int(s.scanBatchSize))
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

			// add batch to scan queue
			for _, h := range hosts {
				reqChan <- scanReq{
					hostKey: h.PublicKey,
					hostIP:  h.NetAddress,
				}
			}

			offset += int(s.scanBatchSize)
		}
		close(reqChan)
	}()

	return reqChan
}

func (s *scanner) launchScanWorkers(reqs chan scanReq) chan scanResp {
	respChan := make(chan scanResp, s.scanThreads)
	liveThreads := s.scanThreads

	for i := uint64(0); i < s.scanThreads; i++ {
		go func() {
			for req := range reqs {
				if s.isStopped() {
					break
				}

				scan, err := s.worker.RHPScan(req.hostKey, req.hostIP, s.currentTimeout())
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

func (s *scanner) isStopped() bool {
	select {
	case <-s.stopChan:
		return true
	default:
	}
	return false
}

func (s *scanner) isTimeoutUpdateRequired() bool {
	return s.timeoutLastUpdate.IsZero() || time.Since(s.timeoutLastUpdate) > s.timeoutMinInterval
}

func (s *scanner) currentTimeout() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.timeout
}
