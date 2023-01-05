package autopilot

import (
	"errors"
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
	scannerNumThreads      = 5
	scannerTimeoutInterval = 10 * time.Minute

	trackerMinTimeout = time.Second * 30

	trackerMinDataPoints     = 25
	trackerNumDataPoints     = 1000
	trackerTimeoutPercentile = 99
)

var (
	errScanInterrupted = errors.New("scan was interrupted")
)

type (
	// TODO: use the actual bus and worker interfaces when they've consolidated
	// a bit, we currently use inline interfaces to avoid having to update the
	// scanner tests with every interface change
	scanner struct {
		bus interface {
			AllHosts() ([]hostdb.Host, error)
			ConsensusState() (api.ConsensusState, error)
			RecordHostInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error
		}
		worker interface {
			RHPScan(hostKey consensus.PublicKey, hostIP string, timeout time.Duration) (api.RHPScanResponse, error)
		}

		tracker *tracker
		logger  *zap.SugaredLogger

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

func newScanner(ap *Autopilot, threads uint64, scanMinInterval, timeoutMinInterval time.Duration) *scanner {
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

		scanThreads:        threads,
		scanMinInterval:    scanMinInterval,
		timeoutMinInterval: timeoutMinInterval,
	}
}

func (s *scanner) tryPerformHostScan() <-chan error {
	s.mu.Lock()
	if s.scanning || !s.isScanRequired() {
		s.mu.Unlock()
		return nil
	}

	s.logger.Debug("host scan started")
	s.scanningLastStart = time.Now()
	s.scanning = true
	s.mu.Unlock()

	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)

		err := s.performHostScans()
		if err != nil {
			s.logger.Debug("host scan encountered error: err: %v", err)
		}

		s.mu.Lock()
		s.scanning = false
		s.logger.Debugf("host scan finished after %v", time.Since(s.scanningLastStart))
		s.mu.Unlock()

		errChan <- err
	}()
	return errChan
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

// performHostScans scans every host in our database
func (s *scanner) performHostScans() error {
	// TODO: running a scan on all hosts is not infinitely scalable, this will
	// need to be updated to be smarter and run on a subset of all hosts
	hosts, err := s.bus.AllHosts()
	if err != nil {
		return err
	}

	// nothing to do
	if len(hosts) == 0 {
		return nil
	}

	s.logger.Debugf("launching %v host scans", len(hosts))

	// add all hosts to a scan queue
	reqChan := make(chan scanReq, len(hosts))
	for _, h := range hosts {
		reqChan <- scanReq{
			hostKey: h.PublicKey,
			hostIP:  h.NetAddress,
		}
	}
	close(reqChan)

	// launch workers
	liveThreads := s.scanThreads
	respChan := make(chan scanResp, s.scanThreads)
	for i := uint64(0); i < s.scanThreads; i++ {
		go func() {
			for req := range reqChan {
				if s.isStopped() {
					break
				}

				scan, err := s.worker.RHPScan(req.hostKey, req.hostIP, s.currentTimeout())
				respChan <- scanResp{req.hostKey, scan.Settings, err}

				if err == nil {
					s.tracker.addDataPoint(time.Duration(scan.Ping))
				}
			}
			if atomic.AddUint64(&liveThreads, ^uint64(0)) == 0 {
				close(respChan)
			}
		}()
	}

	// handle responses
	inflight := len(hosts)
	for inflight > 0 {
		var res scanResp
		select {
		case <-s.stopChan:
			return errScanInterrupted
		case res = <-respChan:
			inflight--
		}

		if res.err != nil {
			s.logger.Debugw(
				fmt.Sprintf("failed scan, err: %v", res.err),
				"hk", res.hostKey,
			)
		}
	}
	return nil
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
