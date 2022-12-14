package autopilot

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type mockBus struct {
	hosts []hostdb.Host
	reqs  []string
}

func (b *mockBus) Hosts(offset, limit int) ([]hostdb.Host, error) {
	b.reqs = append(b.reqs, fmt.Sprintf("%d-%d", offset, offset+limit))

	start := offset
	if start > len(b.hosts) {
		return nil, nil
	}

	end := offset + limit
	if end > len(b.hosts) {
		end = len(b.hosts)
	}

	return b.hosts[start:end], nil
}

type mockWorker struct {
	blockChan chan struct{}

	mu        sync.Mutex
	scanCount int
}

func (w *mockWorker) RHPScan(hostKey consensus.PublicKey, hostIP string, _ time.Duration) (api.RHPScanResponse, error) {
	if w.blockChan != nil {
		<-w.blockChan
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.scanCount++

	return api.RHPScanResponse{}, nil
}

func (s *scanner) isScanning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanning
}

func TestScanner(t *testing.T) {
	// prepare 100 hosts
	hosts := newTestHosts(100)

	// init new scanner
	b := &mockBus{hosts: hosts}
	w := &mockWorker{blockChan: make(chan struct{})}
	s := newTestScanner(b, w)

	// assert it started a host scan
	s.tryPerformHostScan()
	if !s.isScanning() {
		t.Fatal("unexpected")
	}

	// unblock the worker and sleep
	close(w.blockChan)
	time.Sleep(time.Second)

	// assert the scan is done
	if s.isScanning() {
		t.Fatal("unexpected")
	}

	// assert the scanner made 3 batch reqs
	if len(b.reqs) != 3 {
		t.Fatalf("unexpected number of requests, %v != 3", len(b.reqs))
	}
	if b.reqs[0] != "0-40" || b.reqs[1] != "40-80" || b.reqs[2] != "80-120" {
		t.Fatalf("unexpected requests, %v", b.reqs)
	}

	// assert we scanned 100 hosts
	if w.scanCount != 100 {
		t.Fatalf("unexpected number of scans, %v != 100", w.scanCount)
	}

	// assert we prevent starting a host scan immediately after a scan was done
	s.tryPerformHostScan()
	if s.isScanning() {
		t.Fatal("unexpected")
	}

	// reset the scanner
	s.scanningLastStart = time.Time{}

	// assert it started a host scan
	s.tryPerformHostScan()
	if !s.isScanning() {
		t.Fatal("unexpected")
	}
}

func newTestScanner(b *mockBus, w *mockWorker) *scanner {
	return &scanner{
		bus:    b,
		worker: w,
		logger: zap.New(zapcore.NewNopCore()).Sugar(),
		tracker: newTracker(
			trackerMinDataPoints,
			trackerNumDataPoints,
			trackerTimeoutPercentile,
		),
		stopChan:        make(chan struct{}),
		scanBatchSize:   40,
		scanThreads:     3,
		scanMinInterval: time.Minute,
	}
}
