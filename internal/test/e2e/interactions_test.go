package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
)

func TestInteractions(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a new test cluster
	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()
	b := cluster.Bus
	w := cluster.Worker
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

	// assert the price table was set
	ptUID := h.PriceTable.UID
	if ptUID == (rhpv3.SettingsID{}) {
		t.Fatal("expected pt UID to be set")
	}

	// assert price table gets updated
	var ptUpdates int
	tt.Retry(100, 100*time.Millisecond, func() error {
		// fetch contracts (this registers host interactions)
		tt.OKAll(w.Contracts(context.Background(), time.Minute))

		// fetch the host
		h, err := b.Host(context.Background(), h1.PublicKey())
		tt.OK(err)

		// make sure it did not get scanned again
		if h.Interactions.TotalScans != ts {
			t.Fatal("expected no new scans", h.Interactions.TotalScans, ts)
		}

		// keep track of pt updates
		if h.PriceTable.UID != ptUID {
			ptUID = h.PriceTable.UID
			ptUpdates++
		}

		// assert the price table was updated
		if ptUpdates < 2 {
			return errors.New("price table should be updated from time to time")
		}
		return nil
	})

	// scan the host manually
	tt.OKAll(w.RHPScan(context.Background(), h1.PublicKey(), h.NetAddress, 0))
	time.Sleep(3 * testBusFlushInterval)

	// fetch the host
	h, err = b.Host(context.Background(), h1.PublicKey())
	tt.OK(err)

	// assert the scan was registered
	if ts+1 != h.Interactions.TotalScans {
		t.Fatal("expected one new scan")
	}
}
