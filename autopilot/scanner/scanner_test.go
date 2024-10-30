package scanner

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
	"go.uber.org/zap"
)

const (
	testBatchSize  = 40
	testNumThreads = 3
)

type mockHostStore struct {
	hosts []api.Host

	mu       sync.Mutex
	scans    []string
	removals []string
}

func (hs *mockHostStore) Hosts(ctx context.Context, opts api.HostOptions) ([]api.Host, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.scans = append(hs.scans, fmt.Sprintf("%d-%d", opts.Offset, opts.Offset+opts.Limit))

	var hosts []api.Host
	for _, host := range hs.hosts {
		if !opts.MaxLastScan.IsZero() && opts.MaxLastScan.Std().Before(host.Interactions.LastScan) {
			continue
		}
		hosts = append(hosts, host)
	}

	start := opts.Offset
	if start > len(hosts) {
		return nil, nil
	}

	end := opts.Offset + opts.Limit
	if end > len(hosts) {
		end = len(hosts)
	}

	return hosts[start:end], nil
}

func (hs *mockHostStore) RemoveOfflineHosts(ctx context.Context, maxConsecutiveScanFailures uint64, maxDowntime time.Duration) (uint64, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	hs.removals = append(hs.removals, fmt.Sprintf("%d-%d", maxConsecutiveScanFailures, maxDowntime))
	return 0, nil
}

func (hs *mockHostStore) recordScan(hk types.PublicKey) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	for i, host := range hs.hosts {
		if host.PublicKey == hk {
			hs.hosts[i].Interactions.LastScan = time.Now().UTC()
			return
		}
	}
	panic("unknown host")
}

func (hs *mockHostStore) state() ([]string, []string) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	return hs.scans, hs.removals
}

type mockBus struct {
	blockChan chan struct{}
	hs        *mockHostStore

	mu        sync.Mutex
	scanCount int
}

func (w *mockBus) ScanHost(ctx context.Context, hostKey types.PublicKey, hostIP string, _ time.Duration) (api.HostScanResponse, error) {
	if w.blockChan != nil {
		<-w.blockChan
	}

	w.hs.recordScan(hostKey)

	w.mu.Lock()
	defer w.mu.Unlock()
	w.scanCount++

	return api.HostScanResponse{}, nil
}

func TestScanner(t *testing.T) {
	// create mock store
	hs := &mockHostStore{hosts: test.NewHosts(100)}

	// create test scanner
	s, err := New(hs, testBatchSize, testNumThreads, time.Minute, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())

	// assert it's not scanning
	scanning, _ := s.Status()
	if scanning {
		t.Fatal("unexpected")
	}

	// initiate a host scan using a worker that blocks
	b := &mockBus{
		blockChan: make(chan struct{}),
		hs:        hs,
	}
	s.Scan(context.Background(), b, false)

	// assert it's scanning
	scanning, _ = s.Status()
	if !scanning {
		t.Fatal("unexpected")
	}

	// unblock the worker and sleep
	close(b.blockChan)
	time.Sleep(time.Second)

	// assert the scan is done
	scanning, _ = s.Status()
	if scanning {
		t.Fatal("unexpected")
	}

	// assert we did not remove offline hosts
	if _, removals := hs.state(); len(removals) != 0 {
		t.Fatalf("unexpected removals, %v != 0", len(removals))
	}

	// assert the scanner made 3 batch reqs
	if scans, _ := hs.state(); len(scans) != 3 {
		t.Fatalf("unexpected number of requests, %v != 3", len(scans))
	} else if scans[0] != "0-40" || scans[1] != "0-40" || scans[2] != "0-40" {
		t.Fatalf("unexpected requests, %v", scans)
	}

	// assert we scanned 100 hosts
	if b.scanCount != 100 {
		t.Fatalf("unexpected number of scans, %v != 100", b.scanCount)
	}

	// assert we prevent starting a host scan immediately after a scan was done
	s.Scan(context.Background(), b, false)
	scanning, _ = s.Status()
	if scanning {
		t.Fatal("unexpected")
	}

	// update the hosts config
	s.UpdateHostsConfig(api.HostsConfig{
		MaxConsecutiveScanFailures: 10,
		MaxDowntimeHours:           1,
	})

	s.Scan(context.Background(), b, true)
	time.Sleep(time.Second)

	// assert we removed offline hosts
	if _, removals := hs.state(); len(removals) != 1 {
		t.Fatalf("unexpected removals, %v != 1", len(removals))
	} else if removals[0] != "10-3600000000000" {
		t.Fatalf("unexpected removals, %v", removals)
	}
}
