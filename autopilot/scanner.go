package autopilot

import (
	"encoding/json"
	"math"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/worker"
)

const (
	trackerNumDataPoints     = 100
	trackerMinDataPoints     = 25
	trackerTimeoutPercentile = 95
)

type (
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
		s          *scanner
		numThreads uint8
		scanQueue  chan scanReq
	}

	tracker struct {
		threshold  uint64
		percentile float64

		mu      sync.Mutex
		cnt     uint64
		timings []float64
	}
)

func newScanPool(s *scanner, threads uint8) *scanPool {
	return &scanPool{numThreads: threads, s: s}
}

func (p *scanPool) launchScans(hosts []hostdb.Host) {
	p.scanQueue = make(chan scanReq, p.numThreads)
	go func() {
		defer close(p.scanQueue)
		for _, h := range hosts {
			if p.s.isStopped() {
				return
			}
			p.scanQueue <- scanReq{
				hostKey: h.PublicKey,
				hostIP:  h.NetAddress(),
			}
		}
	}()
}

func (p *scanPool) launchWorkers(workerFn func(chan scanResp)) chan scanResp {
	respChan := make(chan scanResp, p.numThreads)
	for i := uint8(0); i < p.numThreads; i++ {
		go workerFn(respChan)
	}
	return respChan
}

func newTracker(min, total uint64, percentile float64) *tracker {
	return &tracker{
		threshold:  min,
		percentile: percentile,
		timings:    make([]float64, total),
	}
}

func (t *tracker) addTiming(timing float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.timings[t.cnt%uint64(len(t.timings))] = timing
	if t.cnt == math.MaxUint64 {
		t.cnt = 0
	} else {
		t.cnt += 1
	}
}

func (t *tracker) timeout() (time.Duration, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cnt < uint64(t.threshold) {
		return 0, nil
	}

	percentile, err := stats.Percentile(t.timings, t.percentile)
	if err != nil {
		return 0, err
	}

	return time.Duration(percentile) * time.Millisecond, nil
}

func newScanner(ap *Autopilot, threads uint8, scanMinInterval, timeoutMinInterval time.Duration) *scanner {
	s := &scanner{
		bus:    ap.bus,
		worker: ap.worker,

		tracker: newTracker(trackerMinDataPoints, trackerNumDataPoints, trackerTimeoutPercentile),

		stopChan: ap.stopChan,

		scanMinInterval:    scanMinInterval,
		timeoutMinInterval: timeoutMinInterval,
	}
	s.pool = newScanPool(s, threads)
	return s
}

func (s *scanner) tryPerformHostScan() bool {
	s.mu.Lock()
	if s.scanning || !s.isScanRequired() {
		s.mu.Unlock()
		return false
	}

	cs, _ := s.bus.ConsensusState()
	if !cs.Synced {
		s.mu.Unlock()
		return false
	}

	s.scanningLastStart = time.Now()
	s.scanning = true
	s.mu.Unlock()

	go func() {
		s.performHostScans()
		s.mu.Lock()
		s.scanning = false
		s.mu.Unlock()
	}()
	return true
}

func (s *scanner) tryUpdateTimeout() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isTimeoutUpdateRequired() {
		return s.timeout
	}

	timeout, _ := s.tracker.timeout() // TODO: handle error
	if timeout == 0 {
		return 0
	}

	s.timeoutLastUpdate = time.Now()
	s.timeout = timeout
	return s.timeout
}

// performHostScans scans every host in our database
func (s *scanner) performHostScans() {
	// TODO: running a scan on all hosts is not infinitely scalable, this will
	// need to be updated to be smarter and run on a subset of all hosts
	hosts, _ := s.bus.AllHosts()
	s.pool.launchScans(hosts)
	inflight := len(hosts)

	// launch workers
	respChan := s.pool.launchWorkers(func(resChan chan scanResp) {
		for req := range s.pool.scanQueue {
			if s.isStopped() {
				return
			}

			timeout := s.tryUpdateTimeout()
			scan, err := s.worker.RHPScan(req.hostKey, req.hostIP, timeout)
			if scan.Ping > 0 {
				s.tracker.addTiming(float64(time.Duration(scan.Ping).Milliseconds()))
			}

			resChan <- scanResp{req.hostKey, scan.Settings, err}
		}
	})

	// defer a close of the response channel
	defer func() {
		// NOTE: we do not close the channel if the scan was interrupted and
		// there are ongoing scans, this to avoid a race condition where a
		// response is being sent down the channel after it's closed
		if inflight == 0 {
			close(respChan)
		}
	}()

	// handle responses
	for inflight > 0 {
		var res scanResp
		select {
		case <-s.stopChan:
			return
		case res = <-respChan:
			inflight--
		}

		if res.err != nil {
			err := s.bus.RecordHostInteraction(res.hostKey, hostdb.Interaction{
				Timestamp: time.Now(),
				Type:      hostdb.InteractionScan,
				Success:   false,
				Result:    json.RawMessage(`{"error": "` + res.err.Error() + `"}`),
			})
			_ = err // TODO
		} else {
			js, _ := json.Marshal(res.settings)
			err := s.bus.RecordHostInteraction(res.hostKey, hostdb.Interaction{
				Timestamp: time.Now(),
				Type:      hostdb.InteractionScan,
				Success:   true,
				Result:    json.RawMessage(js),
			})
			_ = err // TODO
		}
	}
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
