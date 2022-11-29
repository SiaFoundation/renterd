package autopilot

import (
	"encoding/json"
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
	trackerNumDataPoints     = 1000
	trackerMinDataPoints     = 25
	trackerTimeoutPercentile = 95
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
	for i := uint64(0); i < p.numThreads; i++ {
		atomic.AddUint64(&p.liveThreads, 1)
		go func() {
			workerFn(respChan)
			if atomic.AddUint64(&p.liveThreads, ^uint64(0)) == 0 {
				close(respChan)
			}
		}()
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
		return 0, nil
	}

	percentile, err := percentile(t.timings, t.percentile)
	if err != nil {
		return 0, err
	}

	return time.Duration(percentile) * time.Millisecond, nil
}

func newScanner(ap *Autopilot, threads uint64, scanMinInterval, timeoutMinInterval time.Duration) *scanner {
	s := &scanner{
		bus:     ap.bus,
		worker:  ap.worker,
		tracker: newTracker(trackerMinDataPoints, trackerNumDataPoints, trackerTimeoutPercentile),

		stopChan: ap.stopChan,

		scanMinInterval:    scanMinInterval,
		timeoutMinInterval: timeoutMinInterval,
	}
	s.pool = newScanPool(s, threads)
	return s
}

func (s *scanner) tryPerformHostScan() <-chan struct{} {
	s.mu.Lock()
	if s.scanning || !s.isScanRequired() {
		s.mu.Unlock()
		return nil
	}

	s.scanningLastStart = time.Now()
	s.scanning = true
	s.mu.Unlock()

	doneChan := make(chan struct{})
	go func() {
		defer close(doneChan)
		s.performHostScans()
		s.mu.Lock()
		s.scanning = false
		s.mu.Unlock()
	}()
	return doneChan
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
	respChan := s.pool.launchThreads(func(resChan chan scanResp) {
		for req := range s.pool.scanQueue {
			if s.isStopped() {
				return
			}

			timeout := s.tryUpdateTimeout()
			scan, err := s.worker.RHPScan(req.hostKey, req.hostIP, timeout)
			if err == nil {
				s.tracker.addDataPoint(time.Duration(scan.Ping))
			}
			resChan <- scanResp{req.hostKey, scan.Settings, err}
		}
	})

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
				Type:      "scan",
				Success:   false,
				Result:    json.RawMessage(`{"error": "` + res.err.Error() + `"}`),
			})
			_ = err // TODO
		} else {
			err := s.bus.RecordHostInteraction(res.hostKey, hostdb.Interaction{
				Timestamp: time.Now(),
				Type:      "scan",
				Success:   true,
				Result:    json.RawMessage(jsonMarshal(res.settings)),
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

func jsonMarshal(v interface{}) []byte {
	js, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return js
}
