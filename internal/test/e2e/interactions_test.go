package e2e

import (
	"context"
	"testing"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
)

func TestInteractions(t *testing.T) {
	// create a new test cluster
	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()
	b := cluster.Bus
	tt := cluster.tt

	// add a host
	hosts := cluster.AddHosts(1)
	h1 := hosts[0]

	// wait for contracts
	cluster.WaitForContracts()

	// shut down the autopilot to prevent it from interfering with the test
	cluster.ShutdownAutopilot(context.Background())
	time.Sleep(time.Second)

	// fetch the host
	h, err := b.Host(context.Background(), h1.PublicKey())
	tt.OK(err)

	// assert the host got scanned at least once
	ts := h.Interactions.TotalScans
	if ts == 0 {
		t.Fatal("expected at least one scan")
	}

	// assert that either prices or price table were set
	prices := h.V2Settings.Prices
	if prices == (rhpv4.HostPrices{}) {
		t.Fatal("expected prices to be set")
	}

	// scan the host manually
	tt.OKAll(b.ScanHost(context.Background(), h1.PublicKey(), 0))
	time.Sleep(3 * testBusFlushInterval)

	// fetch the host
	h, err = b.Host(context.Background(), h1.PublicKey())
	tt.OK(err)

	// assert the scan was registered
	if ts+1 != h.Interactions.TotalScans {
		t.Fatal("expected one new scan")
	}
}
