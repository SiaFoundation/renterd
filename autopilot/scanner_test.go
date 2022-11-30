package autopilot

import (
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/worker"
	"lukechampine.com/frand"
)

type mockBus struct {
	hosts []hostdb.Host

	mu   sync.Mutex
	itxs []hostdb.Interaction
}

func (b *mockBus) AllHosts() ([]hostdb.Host, error) { return b.hosts, nil }
func (b *mockBus) ConsensusState() (bus.ConsensusState, error) {
	return bus.ConsensusState{BlockHeight: 0, Synced: true}, nil
}
func (b *mockBus) RecordHostInteraction(_ consensus.PublicKey, itx hostdb.Interaction) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.itxs = append(b.itxs, itx)
	return nil
}

func (b *mockBus) counts() (success int, failed int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, itx := range b.itxs {
		if itx.Success {
			success++
		} else {
			failed++
		}
	}
	return
}

type mockWorker struct{}

func (w *mockWorker) RHPScan(_ consensus.PublicKey, hostIP string, _ time.Duration) (r worker.RHPScanResponse, e error) {
	if strings.HasSuffix(hostIP, "fail") {
		e = errors.New("fail")
	}
	if strings.HasSuffix(hostIP, "lock") {
		<-make(chan struct{}) // scan never completes
	}
	return
}

func TestScanner(t *testing.T) {
	h := testHosts(100)
	b := &mockBus{hosts: h}
	w := &mockWorker{}
	s := &scanner{
		bus:    b,
		worker: w,
		tracker: newTracker(
			trackerMinDataPoints,
			trackerNumDataPoints,
			trackerTimeoutPercentile,
			trackerMinTimeout,
		),
		stopChan:        make(chan struct{}),
		scanMinInterval: time.Second,
	}
	s.pool = &scanPool{numThreads: 3, s: s}

	h[0].Announcements[0].NetAddress += "fail"
	h[1].Announcements[0].NetAddress += "fail"
	h[2].Announcements[0].NetAddress += "fail"

	// assert it started a host scan
	doneChan := s.tryPerformHostScan()
	if doneChan == nil {
		t.Fatal("unexpected")
	}

	// wait until the scan is done
	select {
	case <-doneChan:
	case <-time.After(time.Second): // avoid test deadlock
		t.Fatal("scan took longer than expected")
	}

	// assert interactions were recorded
	success, fail := b.counts()
	if success != 97 || fail != 3 {
		t.Fatal("unexpected", success, fail)
	}

	// ensure one scan hangs
	h[4].Announcements[0].NetAddress += "lock"

	// start scan
	b.itxs = b.itxs[:0]
	s.scanningLastStart = time.Time{}
	doneChan = s.tryPerformHostScan()
	if doneChan == nil {
		t.Fatal("unexpected")
	}

	// interrupt the scan
	time.Sleep(time.Second)
	close(s.stopChan)

	// wait until the scan is done
	select {
	case <-doneChan:
	case <-time.After(time.Second): // avoid test deadlock
		t.Fatal("scan took longer than expected")
	}

	// assert scanner is not scanning
	if s.isScanning() {
		t.Fatal("unexpected")
	}

	// assert scan was interrupted, one scan did not complete
	success, fail = b.counts()
	if success != 96 || fail != 3 {
		t.Fatal("unexpected", len(b.itxs))
	}
}

func testHosts(n int) []hostdb.Host {
	randIP := func() string {
		rawIP := make([]byte, 16)
		frand.Read(rawIP)
		return net.IP(rawIP).String()
	}

	hosts := make([]hostdb.Host, n)
	for i := 0; i < n; i++ {
		var h hostdb.Host
		frand.Read(h.PublicKey[:])
		hosts[i] = hostdb.Host{Announcements: []hostdb.Announcement{{NetAddress: randIP()}}}
	}
	return hosts
}

func (s *scanner) isScanning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanning
}
