package autopilot

import (
	"errors"
	"testing"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"lukechampine.com/frand"
)

var (
	testHost1 consensus.PublicKey
	testHost2 consensus.PublicKey
	testHost3 consensus.PublicKey
)

type mockBus struct {
	hosts []hostdb.Host
}

func (b *mockBus) AllHosts() ([]hostdb.Host, error) { return b.hosts, nil }
func (b *mockBus) ConsensusState() (api.ConsensusState, error) {
	return api.ConsensusState{BlockHeight: 0, Synced: true}, nil
}
func (b *mockBus) RecordHostInteraction(hostKey consensus.PublicKey, itx hostdb.Interaction) error {
	panic("never called")
}

type mockWorker struct {
	blockChan chan struct{}
}

func (w *mockWorker) RHPScan(hostKey consensus.PublicKey, hostIP string, _ time.Duration) (r api.RHPScanResponse, e error) {
	if w.blockChan != nil {
		<-w.blockChan
	}
	if hostKey == testHost1 || hostKey == testHost2 {
		e = errors.New("fail")
	}
	return
}

func (s *scanner) isScanning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanning
}

func TestScanner(t *testing.T) {
	// init host keys
	frand.Read(testHost1[:])
	frand.Read(testHost2[:])
	frand.Read(testHost3[:])

	// prepare some hosts
	settings := newTestHostSettings()
	var hosts []hostdb.Host
	for i := 0; i < 8; i++ {
		h := newTestHost(randomHostKey(), settings)
		hosts = append(hosts, h)
	}
	hosts = append(hosts, newTestHost(testHost1, settings))
	hosts = append(hosts, newTestHost(testHost2, settings))

	// init new scanner
	b := &mockBus{hosts: hosts}
	w := &mockWorker{}
	s := newTestScanner(b, w)

	// assert it started a host scan
	errChan := s.tryPerformHostScan()
	if errChan == nil {
		t.Fatal("unexpected")
	}

	// wait until the scan is done
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatal("unexpected error", err)
		}
	case <-time.After(time.Second): // avoid test deadlock
		t.Fatal("scan took longer than expected")
	}

	// assert we prevent starting a host scan immediately after a scan was done
	if s.tryPerformHostScan() != nil {
		t.Fatal("unexpected")
	}

	// reset the scanner
	w.blockChan = make(chan struct{})
	s.scanningLastStart = time.Time{}

	// start another scan
	if errChan = s.tryPerformHostScan(); errChan == nil {
		t.Fatal("unexpected")
	}
	if !s.isScanning() {
		t.Fatal("unexpected")
	}
	close(w.blockChan) // we have to block on a channel to avoid an NDF on the isScanning check

	// immediately interrupt the scanner
	close(s.stopChan)

	// wait until the scan is done
	select {
	case err := <-errChan:
		if err != errScanInterrupted {
			t.Fatal("unexpected error", err)
		}
	case <-time.After(time.Second): // avoid test deadlock
		t.Fatal("scan took longer than expected")
	}

	// assert scanner is no longer scanning
	if s.isScanning() {
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
			trackerMinTimeout,
		),
		stopChan:        make(chan struct{}),
		scanThreads:     3,
		scanMinInterval: time.Second,
	}
}
