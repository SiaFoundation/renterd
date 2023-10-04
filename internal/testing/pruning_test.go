package testing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
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
		t.Helper()
		his := make([]hostdb.HostScan, n)
		for i := 0; i < n; i++ {
			now = now.Add(time.Hour).Add(time.Minute) // 1m leeway
			his[i] = hostdb.HostScan{
				HostKey:   hk,
				Timestamp: now,
				Success:   false,
			}
		}
		if err = b.RecordHostScans(context.Background(), his); err != nil {
			t.Fatal(err)
		}
	}

	// create a helper function that waits for an autopilot loop to finish
	waitForAutopilotLoop := func() {
		t.Helper()
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
	hostss, err := b.Hosts(context.Background(), api.GetHostsOptions{})
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
	hostss, err = b.Hosts(context.Background(), api.GetHostsOptions{})
	if err != nil {
		t.Fatal(err)
	} else if len(hostss) != 0 {
		t.Fatalf("host was not pruned, %+v", hostss[0].Interactions)
	}

	// assert validation on MaxDowntimeHours
	ap, err := b.Autopilot(context.Background(), api.DefaultAutopilotID)
	if err != nil {
		t.Fatal(err)
	}

	ap.Config.Hosts.MaxDowntimeHours = 99*365*24 + 1 // exceed by one
	if err = b.UpdateAutopilot(context.Background(), api.Autopilot{ID: t.Name(), Config: ap.Config}); !strings.Contains(err.Error(), api.ErrMaxDowntimeHoursTooHigh.Error()) {
		t.Fatal(err)
	}
	ap.Config.Hosts.MaxDowntimeHours = 99 * 365 * 24 // allowed max
	if err = b.UpdateAutopilot(context.Background(), api.Autopilot{ID: t.Name(), Config: ap.Config}); err != nil {
		t.Fatal(err)
	}
}

func TestSectorPruning(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a cluster
	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// add a helper to check whether a root is in a given slice
	hasRoot := func(roots []types.Hash256, root types.Hash256) bool {
		for _, r := range roots {
			if r == root {
				return true
			}
		}
		return false
	}

	// convenience variables
	cfg := testAutopilotConfig
	rs := testRedundancySettings
	w := cluster.Worker
	b := cluster.Bus

	numObjects := 10

	// add hosts
	_, err = cluster.AddHostsBlocking(int(cfg.Contracts.Amount))
	if err != nil {
		t.Fatal(err)
	}

	// wait until we have accounts
	if _, err := cluster.WaitForAccounts(); err != nil {
		t.Fatal(err)
	}

	// create a contracts dict
	contracts, err := b.Contracts(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// add several objects
	for i := 0; i < numObjects; i++ {
		filename := fmt.Sprintf("obj_%d", i)
		if _, err := w.UploadObject(context.Background(), bytes.NewReader([]byte(filename)), filename); err != nil {
			t.Fatal(err)
		}
	}

	// compare database against roots returned by the host
	var n int
	for _, c := range contracts {
		dbRoots, _, err := b.ContractRoots(context.Background(), c.ID)
		if err != nil {
			t.Fatal(err)
		}
		cRoots, err := w.RHPContractRoots(context.Background(), c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(dbRoots) != len(cRoots) {
			t.Fatal("unexpected number of roots", dbRoots, cRoots)
		}
		for _, root := range dbRoots {
			if !hasRoot(cRoots, root) {
				t.Fatal("missing root", dbRoots, cRoots)
			}
		}
		n += len(cRoots)
	}
	if n != rs.TotalShards*numObjects {
		t.Fatal("unexpected number of roots", n)
	}

	// reboot the cluster to ensure spending records get flushed
	// Restart cluster to have worker fetch the account from the bus again.
	cluster2, err := cluster.Reboot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster2.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
	b = cluster2.Bus
	w = cluster2.Worker

	// assert prunable data is 0
	if res, err := b.PrunableData(context.Background()); err != nil {
		t.Fatal(err)
	} else if res.TotalPrunable != 0 {
		t.Fatal("expected 0 prunable data", n)
	}

	// delete every other object
	for i := 0; i < numObjects; i += 2 {
		filename := fmt.Sprintf("obj_%d", i)
		if err := b.DeleteObject(context.Background(), api.DefaultBucketName, filename, api.DeleteObjectOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	// assert amount of prunable data
	if res, err := b.PrunableData(context.Background()); err != nil {
		t.Fatal(err)
	} else if res.TotalPrunable != uint64(math.Ceil(float64(numObjects)/2))*uint64(rs.TotalShards)*rhpv2.SectorSize {
		t.Fatal("unexpected prunable data", n)
	}

	// prune all contracts
	for _, c := range contracts {
		if _, _, err := w.RHPPruneContract(context.Background(), c.ID, 0); err != nil {
			t.Fatal(err)
		}
	}

	// assert spending records were updated and prunable data is 0
	if err = Retry(10, testBusFlushInterval, func() error {
		if res, err := b.PrunableData(context.Background()); err != nil {
			t.Fatal(err)
		} else if res.TotalPrunable != 0 {
			return fmt.Errorf("unexpected prunable data: %d", n)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// assert spending was updated
	for _, c := range contracts {
		c, err := b.Contract(context.Background(), c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if c.Spending.SectorRoots.IsZero() {
			t.Fatal("spending record not updated")
		}
		if c.Spending.Deletions.IsZero() {
			t.Fatal("spending record not updated")
		}
	}
}
