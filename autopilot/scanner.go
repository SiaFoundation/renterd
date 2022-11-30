package autopilot

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/worker"
)

const (
	// TODO: we could make these configurable
	scannerNumThreads      = 5
	scannerScanInterval    = 10 * time.Minute
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
			ConsensusState() (bus.ConsensusState, error)
			RecordHostInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error
		}
		worker interface {
			RHPScan(hostKey consensus.PublicKey, hostIP string, timeout time.Duration) (worker.RHPScanResponse, error)
		}

		pool    *scanPool
		tracker *tracker

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

	scanPool struct {
		s           *scanner
		numThreads  uint64
		liveThreads uint64
		scanQueue   chan scanReq
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

func newScanPool(s *scanner, threads uint64) *scanPool {
	return &scanPool{numThreads: threads, s: s}
}

func (p *scanPool) launchScans(hosts []hostdb.Host) {
	p.scanQueue = make(chan scanReq, len(hosts))
	for _, h := range hosts {
		p.scanQueue <- scanReq{
			hostKey: h.PublicKey,
			hostIP:  h.NetAddress(),
		}
	}
	close(p.scanQueue)
}

func (p *scanPool) launchThreads(workerFn func(chan scanResp)) chan scanResp {
	respChan := make(chan scanResp, p.numThreads)
	atomic.AddUint64(&p.liveThreads, p.numThreads)
	for i := uint64(0); i < p.numThreads; i++ {
		go func() {
			workerFn(respChan)
			if atomic.AddUint64(&p.liveThreads, ^uint64(0)) == 0 {
				close(respChan)
			}
		}()
	}
	return respChan
}

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

func (t *tracker) timeout() (time.Duration, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.count < uint64(t.threshold) {
		return t.minTimeout, nil
	}

	percentile, err := percentile(t.timings, t.percentile)
	if err != nil {
		return t.minTimeout, err
	}

	timeout := time.Duration(percentile) * time.Millisecond
	if timeout < t.minTimeout {
		timeout = t.minTimeout
	}

	return timeout, nil
}

func newScanner(ap *Autopilot, threads uint64, scanMinInterval, timeoutMinInterval time.Duration) *scanner {
	s := &scanner{
		bus:    ap.bus,
		worker: ap.worker,
		tracker: newTracker(
			trackerMinDataPoints,
			trackerNumDataPoints,
			trackerTimeoutPercentile,
			trackerMinTimeout,
		),

		stopChan: ap.stopChan,

		scanMinInterval:    scanMinInterval,
		timeoutMinInterval: timeoutMinInterval,
	}
	s.pool = newScanPool(s, threads)
	return s
}

func (s *scanner) tryPerformHostScan() <-chan error {
	s.mu.Lock()
	if s.scanning || !s.isScanRequired() {
		s.mu.Unlock()
		return nil
	}

	s.scanningLastStart = time.Now()
	s.scanning = true
	s.mu.Unlock()

	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		if err := s.performHostScans(); err != nil {
			errChan <- err
		}
		s.mu.Lock()
		s.scanning = false
		s.mu.Unlock()
	}()
	return errChan
}

func (s *scanner) tryUpdateTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isTimeoutUpdateRequired() {
		return
	}

	timeout, err := s.tracker.timeout()
	if err != nil {
		// TODO: log error
	}

	s.timeoutLastUpdate = time.Now()
	s.timeout = timeout
}

// performHostScans scans every host in our database
func (s *scanner) performHostScans() error {
	// TODO: running a scan on all hosts is not infinitely scalable, this will
	// need to be updated to be smarter and run on a subset of all hosts
	hosts, err := s.bus.AllHosts()
	if err != nil {
		return err
	}

	s.pool.launchScans(hosts)

	workerFn := func(resChan chan scanResp) {
		for req := range s.pool.scanQueue {
			if s.isStopped() {
				return
			}

			scan, err := s.worker.RHPScan(req.hostKey, req.hostIP, s.currentTimeout())
			resChan <- scanResp{req.hostKey, scan.Settings, err}

			if err == nil {
				s.tracker.addDataPoint(time.Duration(scan.Ping))
			}
		}
	}

	respChan := s.pool.launchThreads(workerFn)

	var errs error

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
			if err := s.bus.RecordHostInteraction(res.hostKey, hostdb.Interaction{
				Timestamp: time.Now(),
				Type:      "scan",
				Success:   false,
				Result:    json.RawMessage(`{"error": "` + res.err.Error() + `"}`),
			}); err != nil {
				if errs == nil {
					errs = err
				} else {
					errs = fmt.Errorf("%w; %s", errs, err.Error())
				}
			}
		} else {
			if err := s.bus.RecordHostInteraction(res.hostKey, hostdb.Interaction{
				Timestamp: time.Now(),
				Type:      "scan",
				Success:   true,
				Result:    json.RawMessage(jsonMarshal(res.settings)),
			}); err != nil {
				if errs == nil {
					errs = err
				} else {
					errs = fmt.Errorf("%w; %s", errs, err.Error())
				}
			}
		}
	}

	return errs
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

func jsonMarshal(v interface{}) []byte {
	js, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return js
}
