package testing

import (
	"bytes"
	"context"
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

	// create a new test cluster
	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()
	b := cluster.Bus
	w := cluster.Worker
	a := cluster.Autopilot
	tt := cluster.tt

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
		tt.OK(b.RecordHostScans(context.Background(), his))
	}

	// add a host
	hosts := cluster.AddHosts(1)
	h1 := hosts[0]

	// fetch the host
	h, err := b.Host(context.Background(), h1.PublicKey())
	tt.OK(err)

	// scan the host (lastScan needs to be > 0 for downtime to start counting)
	tt.OKAll(w.RHPScan(context.Background(), h1.PublicKey(), h.NetAddress, 0))

	// block the host
	tt.OK(b.UpdateHostBlocklist(context.Background(), []string{h1.PublicKey().String()}, nil, false))

	// remove it from the cluster manually
	cluster.RemoveHost(h1)

	// shut down the worker manually, this will flush any interactions
	cluster.ShutdownWorker(context.Background())

	// record 9 failed interactions, right before the pruning threshold, and
	// wait for the autopilot loop to finish at least once
	recordFailedInteractions(9, h1.PublicKey())

	// trigger the autopilot loop twice, failing to trigger it twice shouldn't
	// fail the test, this avoids an NDF on windows
	remaining := 2
	for i := 1; i < 100; i++ {
		triggered, err := a.Trigger(false)
		tt.OK(err)
		if triggered {
			remaining--
			if remaining == 0 {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	// assert the host was not pruned
	hostss, err := b.Hosts(context.Background(), api.GetHostsOptions{})
	tt.OK(err)
	if len(hostss) != 1 {
		t.Fatal("host was pruned")
	}

	// record one more failed interaction, this should push the host over the
	// pruning threshold
	recordFailedInteractions(1, h1.PublicKey())

	// assert the host was pruned
	tt.Retry(10, time.Second, func() error {
		hostss, err = b.Hosts(context.Background(), api.GetHostsOptions{})
		tt.OK(err)
		if len(hostss) != 0 {
			return fmt.Errorf("host was not pruned, %+v", hostss[0].Interactions)
		}
		return nil
	})

	// assert validation on MaxDowntimeHours
	ap, err := b.Autopilot(context.Background(), api.DefaultAutopilotID)
	tt.OK(err)

	ap.Config.Hosts.MaxDowntimeHours = 99*365*24 + 1 // exceed by one
	if err = b.UpdateAutopilot(context.Background(), api.Autopilot{ID: t.Name(), Config: ap.Config}); !strings.Contains(err.Error(), api.ErrMaxDowntimeHoursTooHigh.Error()) {
		t.Fatal(err)
	}
	ap.Config.Hosts.MaxDowntimeHours = 99 * 365 * 24 // allowed max
	tt.OK(b.UpdateAutopilot(context.Background(), api.Autopilot{ID: t.Name(), Config: ap.Config}))
}

func TestSectorPruning(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a cluster
	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()

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
	tt := cluster.tt

	numObjects := 10

	// add hosts
	cluster.AddHostsBlocking(int(cfg.Contracts.Amount))

	// wait until we have accounts
	cluster.WaitForAccounts()

	// create a contracts dict
	contracts, err := b.Contracts(context.Background())
	tt.OK(err)

	// add several objects
	for i := 0; i < numObjects; i++ {
		filename := fmt.Sprintf("obj_%d", i)
		tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader([]byte(filename)), api.DefaultBucketName, filename, api.UploadObjectOptions{}))
	}

	// compare database against roots returned by the host
	var n int
	for _, c := range contracts {
		dbRoots, _, err := b.ContractRoots(context.Background(), c.ID)
		tt.OK(err)
		cRoots, err := w.RHPContractRoots(context.Background(), c.ID)
		tt.OK(err)
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
	cluster2 := cluster.Reboot(context.Background())
	defer cluster2.Shutdown()
	b = cluster2.Bus
	w = cluster2.Worker

	// assert prunable data is 0
	res, err := b.PrunableData(context.Background())
	tt.OK(err)
	if res.TotalPrunable != 0 {
		t.Fatal("expected 0 prunable data", n)
	}

	// delete every other object
	for i := 0; i < numObjects; i += 2 {
		filename := fmt.Sprintf("obj_%d", i)
		tt.OK(b.DeleteObject(context.Background(), api.DefaultBucketName, filename, api.DeleteObjectOptions{}))
	}

	// assert amount of prunable data
	res, err = b.PrunableData(context.Background())
	tt.OK(err)
	if res.TotalPrunable != uint64(math.Ceil(float64(numObjects)/2))*uint64(rs.TotalShards)*rhpv2.SectorSize {
		t.Fatal("unexpected prunable data", n)
	}

	// prune all contracts
	for _, c := range contracts {
		tt.OKAll(w.RHPPruneContract(context.Background(), c.ID, 0))
	}

	// assert spending records were updated and prunable data is 0
	tt.Retry(10, testBusFlushInterval, func() error {
		res, err := b.PrunableData(context.Background())
		tt.OK(err)
		if res.TotalPrunable != 0 {
			return fmt.Errorf("unexpected prunable data: %d", n)
		}
		return nil
	})

	// assert spending was updated
	for _, c := range contracts {
		c, err := b.Contract(context.Background(), c.ID)
		tt.OK(err)
		if c.Spending.SectorRoots.IsZero() {
			t.Fatal("spending record not updated")
		}
		if c.Spending.Deletions.IsZero() {
			t.Fatal("spending record not updated")
		}
	}
}
