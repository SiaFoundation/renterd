package autopilot

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.uber.org/zap"
)

const (
	// TODO: we could make these configurable
	scannerBatchSize       = 100
	scannerNumThreads      = 5
	scannerTimeoutInterval = 10 * time.Minute

	trackerMinTimeout = time.Second * 30

	trackerMinDataPoints     = 25
	trackerNumDataPoints     = 1000
	trackerTimeoutPercentile = 99
)

type (
	// TODO: use the actual bus and worker interfaces when they've consolidated
	// a bit, we currently use inline interfaces to avoid having to update the
	// scanner tests with every interface change
	scanner struct {
		bus interface {
			Hosts(offset, limit int) ([]hostdb.Host, error)
		}
		worker interface {
			RHPScan(hostKey consensus.PublicKey, hostIP string, timeout time.Duration) (api.RHPScanResponse, error)
		}

		tracker *tracker
		logger  *zap.SugaredLogger

		scanBatchSize      uint64
		scanThreads        uint64
		scanMinInterval    time.Duration
		timeoutMinInterval time.Duration

		stopChan chan struct{}

		mu                sync.Mutex
		scanning          bool
		scanningLastStart time.Time
		timeout           time.Duration
		timeoutLastUpdate time.Time
	}

	scanReq struct {
		hostKey consensus.PublicKey
		hostIP  string
	}

	scanResp struct {
		hostKey  consensus.PublicKey
		settings rhpv2.HostSettings
		err      error
	}

	tracker struct {
		threshold  uint64
		percentile float64
		minTimeout time.Duration

		mu      sync.Mutex
		count   uint64
		timings []float64
	}
)

func newTracker(threshold, total uint64, percentile float64, minTimeout time.Duration) *tracker {
	return &tracker{
		threshold:  threshold,
		minTimeout: minTimeout,
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
		return t.minTimeout
	}

	percentile, err := percentile(t.timings, t.percentile)
	if err != nil {
		return t.minTimeout
	}

	timeout := time.Duration(percentile) * time.Millisecond
	if timeout < t.minTimeout {
		timeout = t.minTimeout
	}

	return timeout
}

func newScanner(ap *Autopilot, scanBatchSize, scanThreads uint64, scanMinInterval, timeoutMinInterval time.Duration) *scanner {
	return &scanner{
		bus:    ap.bus,
		worker: ap.worker,
		tracker: newTracker(
			trackerMinDataPoints,
			trackerNumDataPoints,
			trackerTimeoutPercentile,
			trackerMinTimeout,
		),
		logger: ap.logger.Named("scanner"),

		stopChan: ap.stopChan,

		scanBatchSize:      scanBatchSize,
		scanThreads:        scanThreads,
		scanMinInterval:    scanMinInterval,
		timeoutMinInterval: timeoutMinInterval,
	}
}

func (s *scanner) tryPerformHostScan(cfg api.AutopilotConfig) {
	s.mu.Lock()
	if s.scanning || !s.isScanRequired() {
		s.mu.Unlock()
		return
	}

	s.logger.Debug("host scan started")
	s.scanningLastStart = time.Now()
	s.scanning = true
	s.mu.Unlock()

	go func() {
		for res := range s.launchScanWorkers(s.launchHostScans(cfg)) {
			if res.err != nil {
				s.logger.Debugw(
					fmt.Sprintf("failed scan, err: %v", res.err),
					"hk", res.hostKey,
				)
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

	prev := s.timeout
	s.timeout = s.tracker.timeout()
	s.timeoutLastUpdate = time.Now()
	s.logger.Debugf("updated timeout %v->%v", prev, s.timeout)
}

func (s *scanner) launchHostScans(cfg api.AutopilotConfig) chan scanReq {
	reqChan := make(chan scanReq, s.scanBatchSize)
	offset := 0

	go func() {
		var exhausted bool
		for !s.isStopped() && !exhausted {
			s.logger.Debugf("fetching hosts batch %d-%d", offset, offset+int(s.scanBatchSize))

			// fetch next batch
			hosts, err := s.bus.Hosts(offset, int(s.scanBatchSize))
			if err != nil {
				s.logger.Errorf("could not get hosts, err: %v", err)
				break
			}
			if len(hosts) == 0 {
				break
			}
			if len(hosts) < int(s.scanBatchSize) {
				exhausted = true
			}

			// filter batch
			filtered := hosts[:0]
			for _, host := range hosts {
				if !isBlacklisted(cfg, Host{host}) && isWhitelisted(cfg, Host{host}) {
					filtered = append(filtered, host)
				}
			}

			// add batch to scan queue
			s.logger.Debugf("scanning %d hosts in batch %d-%d", len(filtered), offset, offset+int(s.scanBatchSize))
			for _, h := range filtered {
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
	return s.scanningLastStart.IsZero() || time.Since(s.scanningLastStart) > s.scanMinInterval
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
