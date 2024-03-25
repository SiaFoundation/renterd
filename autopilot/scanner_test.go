package autopilot

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type mockBus struct {
	hosts []hostdb.Host
	reqs  []string
}

func (b *mockBus) SearchHosts(ctx context.Context, opts api.SearchHostOptions) ([]api.Host, error) {
	b.reqs = append(b.reqs, fmt.Sprintf("%d-%d", opts.Offset, opts.Offset+opts.Limit))

	start := opts.Offset
	if start > len(b.hosts) {
		return nil, nil
	}

	end := opts.Offset + opts.Limit
	if end > len(b.hosts) {
		end = len(b.hosts)
	}

	hosts := make([]api.Host, len(b.hosts[start:end]))
	for i, h := range b.hosts[start:end] {
		hosts[i] = api.Host{Host: h}
	}
	return hosts, nil
}

func (b *mockBus) HostsForScanning(ctx context.Context, opts api.HostsForScanningOptions) ([]hostdb.HostAddress, error) {
	hosts, err := b.SearchHosts(ctx, api.SearchHostOptions{
		Offset: opts.Offset,
		Limit:  opts.Limit,
	})
	if err != nil {
		return nil, err
	}
	var hostAddresses []hostdb.HostAddress
	for _, h := range hosts {
		hostAddresses = append(hostAddresses, hostdb.HostAddress{
			NetAddress: h.NetAddress,
			PublicKey:  h.PublicKey,
		})
	}
	return hostAddresses, nil
}

func (b *mockBus) RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (uint64, error) {
	return 0, nil
}

type mockWorker struct {
	blockChan chan struct{}

	mu        sync.Mutex
	scanCount int
}

func (w *mockWorker) RHPScan(ctx context.Context, hostKey types.PublicKey, hostIP string, _ time.Duration) (api.RHPScanResponse, error) {
	if w.blockChan != nil {
		<-w.blockChan
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.scanCount++

	return api.RHPScanResponse{}, nil
}

func (w *mockWorker) RHPPriceTable(ctx context.Context, hostKey types.PublicKey, siamuxAddr string) (hostdb.HostPriceTable, error) {
	return hostdb.HostPriceTable{}, nil
}

func TestScanner(t *testing.T) {
	// prepare 100 hosts
	hosts := newTestHosts(100)

	// init new scanner
	b := &mockBus{hosts: hosts}
	w := &mockWorker{blockChan: make(chan struct{})}
	s := newTestScanner(b)

	// assert it started a host scan
	s.tryPerformHostScan(context.Background(), w, false)
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
	s.tryPerformHostScan(context.Background(), w, false)
	if s.isScanning() {
		t.Fatal("unexpected")
	}

	// reset the scanner
	s.scanningLastStart = time.Time{}

	// assert it started a host scan
	s.tryPerformHostScan(context.Background(), w, false)
	if !s.isScanning() {
		t.Fatal("unexpected")
	}
}

func (s *scanner) isScanning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanning
}

func newTestScanner(b *mockBus) *scanner {
	ap := &Autopilot{}
	ap.shutdownCtx, ap.shutdownCtxCancel = context.WithCancel(context.Background())
	return &scanner{
		ap:     ap,
		bus:    b,
		logger: zap.New(zapcore.NewNopCore()).Sugar(),
		tracker: newTracker(
			trackerMinDataPoints,
			trackerNumDataPoints,
			trackerTimeoutPercentile,
		),

		interruptScanChan: make(chan struct{}),

		scanBatchSize:   40,
		scanThreads:     3,
		scanMinInterval: time.Minute,
	}
}
