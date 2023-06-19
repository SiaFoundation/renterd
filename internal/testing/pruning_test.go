package testing

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
)

func TestHostPruning(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	// update the min scan interval to ensure the scanner scans all hosts on
	// every iteration of the autopilot loop, this ensures we try and remove
	// offline hosts in every autopilot loop
	apCfg := testApCfg()
	apCfg.ScannerInterval = 0

	// create a new test cluster
	cluster, err := newTestClusterCustom(t.TempDir(), t.Name(), true, types.GeneratePrivateKey(), testBusCfg(), testWorkerCfg(), apCfg, newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	b := cluster.Bus
	w := cluster.Worker
	a := cluster.Autopilot

	// create a helper function that records n failed interactions
	now := time.Now()
	recordFailedInteractions := func(n int, hk types.PublicKey) {
		his := make([]hostdb.Interaction, n)
		for i := 0; i < n; i++ {
			now = now.Add(time.Hour).Add(time.Minute) // 1m leeway
			his[i] = hostdb.Interaction{
				Host:      hk,
				Timestamp: now,
				Success:   false,
				Type:      hostdb.InteractionTypeScan,
			}
		}
		if err = b.RecordInteractions(context.Background(), his); err != nil {
			t.Fatal(err)
		}
	}

	// create a helper function that waits for an autopilot loop to finish
	waitForAutopilotLoop := func() {
		var nTriggered int
		if err := Retry(10, 500*time.Millisecond, func() error {
			if triggered, err := a.Trigger(true); err != nil {
				t.Fatal(err)
			} else if triggered {
				nTriggered++
				if nTriggered > 1 {
					return nil
				}
			}
			return errors.New("autopilot loop has not finished")
		}); err != nil {
			t.Fatal(err)
		}
	}

	// add a host
	hosts, err := cluster.AddHosts(1)
	if err != nil {
		t.Fatal(err)
	}
	h1 := hosts[0]

	// fetch the host
	h, err := b.Host(context.Background(), h1.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	// scan the host (lastScan needs to be > 0 for downtime to start counting)
	_, err = w.RHPScan(context.Background(), h1.PublicKey(), h.NetAddress, 0)
	if err != nil {
		t.Fatal(err)
	}

	// block the host
	err = b.UpdateHostBlocklist(ctx, []string{h1.PublicKey().String()}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// remove it from the cluster manually
	cluster.hosts = cluster.hosts[1:]
	err = hosts[0].Close()
	if err != nil {
		t.Fatal(err)
	}

	// shut down the worker manually, this will flush any interactions
	err = cluster.workerShutdownFns[1](context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cluster.workerShutdownFns = cluster.workerShutdownFns[:1]

	// record 9 failed interactions, right before the pruning threshold, and
	// wait for the autopilot loop to finish at least once
	recordFailedInteractions(9, h1.PublicKey())
	waitForAutopilotLoop()

	// assert the host was not pruned
	hostss, err := b.Hosts(context.Background(), 0, -1)
	if err != nil {
		t.Fatal(err)
	} else if len(hostss) != 1 {
		t.Fatal("host was pruned")
	}

	// record one more failed interaction, this should push the host over the
	// pruning threshold
	recordFailedInteractions(1, h1.PublicKey())
	waitForAutopilotLoop()

	// assert the host was not pruned
	hostss, err = b.Hosts(context.Background(), 0, -1)
	if err != nil {
		t.Fatal(err)
	} else if len(hostss) != 0 {
		t.Fatalf("host was not pruned, %+v", hostss[0].Interactions)
	}

	// assert validation on MaxDowntimeHours
	cfg := api.DefaultAutopilotConfig()
	cfg.Hosts.MaxDowntimeHours = 99*365*24 + 1 // exceed by one
	if err = b.UpdateAutopilot(context.Background(), api.Autopilot{ID: t.Name(), Config: cfg}); !strings.Contains(err.Error(), api.ErrMaxDowntimeHoursTooHigh.Error()) {
		t.Fatal(err)
	}
	cfg.Hosts.MaxDowntimeHours = 99 * 365 * 24 // allowed max
	if err = b.UpdateAutopilot(context.Background(), api.Autopilot{ID: t.Name(), Config: cfg}); err != nil {
		t.Fatal(err)
	}
}
