package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus/client"
	"go.sia.tech/renterd/internal/test"
)

func TestHostPruning(t *testing.T) {
	// create a new test cluster
	cluster := newTestCluster(t, testClusterOptions{hosts: 1})
	defer cluster.Shutdown()

	// convenience variables
	b := cluster.Bus
	a := cluster.Autopilot
	tt := cluster.tt

	// create a helper function that records n failed interactions
	now := time.Now()
	recordFailedInteractions := func(n int, hk types.PublicKey) {
		t.Helper()
		his := make([]api.HostScan, n)
		for i := 0; i < n; i++ {
			now = now.Add(time.Hour).Add(time.Minute) // 1m leeway
			his[i] = api.HostScan{
				HostKey:   hk,
				Timestamp: now,
				Success:   false,
			}
		}
		tt.OK(cluster.bs.RecordHostScans(context.Background(), his))
	}

	// shut down the worker manually, this will flush any interactions
	cluster.ShutdownWorker(context.Background())

	// remove it from the cluster manually
	h1 := cluster.hosts[0]
	cluster.RemoveHost(h1)

	// record 9 failed interactions, right before the pruning threshold, and
	// wait for the autopilot loop to finish at least once
	recordFailedInteractions(9, h1.PublicKey())

	// trigger the autopilot
	tt.OKAll(a.Trigger(true))

	// assert the host was not pruned
	hostss, err := b.Hosts(context.Background(), api.HostOptions{})
	tt.OK(err)
	if len(hostss) != 1 {
		t.Fatal("host was pruned")
	}

	// record one more failed interaction, this should push the host over the
	// pruning threshold
	recordFailedInteractions(1, h1.PublicKey())

	// assert the host was pruned
	tt.Retry(10, time.Second, func() error {
		hostss, err = b.Hosts(context.Background(), api.HostOptions{})
		tt.OK(err)
		if len(hostss) != 0 {
			a.Trigger(true) // trigger autopilot
			return fmt.Errorf("host was not pruned, %+v", hostss[0].Interactions)
		}
		return nil
	})
}

func TestContractPruning(t *testing.T) {
	// create a cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	})
	defer cluster.Shutdown()

	// convenience variables
	w := cluster.Worker
	b := cluster.Bus
	tt := cluster.tt

	// helper to assert there is data to prune yes or no, which essentially
	// checks whether the pruner is running or not
	assertPrunableData := func(prunable bool) {
		tt.Retry(100, 100*time.Millisecond, func() error {
			res, err := b.PrunableData(context.Background())
			tt.OK(err)
			if prunable && res.TotalPrunable == 0 {
				return errors.New("expected prunable data")
			} else if !prunable && res.TotalPrunable != 0 {
				return errors.New("expected no prunable data")
			}
			return nil
		})
	}

	// create prunable data by adding and immediately removing an object
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader([]byte(t.Name())), testBucket, t.Name(), api.UploadObjectOptions{}))
	tt.OK(b.DeleteObject(context.Background(), testBucket, t.Name()))

	// assert there's data to prune and there's nothing pruning it
	assertPrunableData(true)
	time.Sleep(time.Second)
	assertPrunableData(true)

	// enable contract pruning
	ap, err := b.AutopilotConfig(context.Background())
	tt.OK(err)
	cfg := ap.Contracts
	cfg.Prune = true
	tt.OK(b.UpdateAutopilotConfig(context.Background(), client.WithContractsConfig(cfg)))

	// assert data got pruned
	assertPrunableData(false)
}

func TestSectorPruning(t *testing.T) {
	// create a cluster
	opts := clusterOptsDefault
	cluster := newTestCluster(t, opts)
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
	rs := test.RedundancySettings
	w := cluster.Worker
	b := cluster.Bus
	tt := cluster.tt

	numObjects := 10

	// add hosts
	hosts := cluster.AddHostsBlocking(rs.TotalShards)

	// wait until we have accounts
	cluster.WaitForAccounts()

	// wait until we have contracts
	cluster.WaitForContracts()

	// add several objects
	for i := 0; i < numObjects; i++ {
		filename := fmt.Sprintf("obj_%d", i)
		tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader([]byte(filename)), testBucket, filename, api.UploadObjectOptions{}))
	}

	// shut down the autopilot to prevent it from interfering
	cluster.ShutdownAutopilot(context.Background())

	// create a contracts dict
	contracts, err := b.Contracts(context.Background(), api.ContractsOpts{})
	tt.OK(err)

	// compare database against roots returned by the host
	var n int
	for _, c := range contracts {
		dbRoots, err := b.ContractRoots(context.Background(), c.ID)
		tt.OK(err)

		cRoots, err := cluster.ContractRoots(context.Background(), c.ID)
		tt.OK(err)
		if len(dbRoots) != len(cRoots) {
			t.Fatal("unexpected number of roots", len(dbRoots), len(cRoots))
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

	// sleep to ensure spending records get flushed
	time.Sleep(3 * testBusFlushInterval)

	// assert prunable data is 0
	res, err := b.PrunableData(context.Background())
	tt.OK(err)
	if res.TotalPrunable != 0 {
		t.Fatal("expected 0 prunable data", n)
	}

	// delete every other object
	for i := 0; i < numObjects; i += 2 {
		filename := fmt.Sprintf("obj_%d", i)
		tt.OK(b.DeleteObject(context.Background(), testBucket, filename))
	}

	// assert amount of prunable data
	tt.Retry(300, 100*time.Millisecond, func() error {
		res, err = b.PrunableData(context.Background())
		tt.OK(err)
		if res.TotalPrunable != uint64(math.Ceil(float64(numObjects)/2))*rs.SlabSize() {
			return fmt.Errorf("unexpected prunable data %v", n)
		}
		return nil
	})

	// prune all contracts
	for _, c := range contracts {
		res, err := b.PruneContract(context.Background(), c.ID, 0)
		tt.OK(err)
		if res.Pruned == 0 {
			t.Fatal("expected pruned to be non-zero")
		} else if res.Remaining != 0 {
			t.Fatal("expected remaining to be zero")
		}
	}

	// assert prunable data is 0
	res, err = b.PrunableData(context.Background())
	tt.OK(err)
	if res.TotalPrunable != 0 {
		t.Fatalf("unexpected no prunable data: %d", n)
	}

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

	// delete other object
	for i := 1; i < numObjects; i += 2 {
		filename := fmt.Sprintf("obj_%d", i)
		tt.OK(b.DeleteObject(context.Background(), testBucket, filename))
	}

	// assert amount of prunable data
	tt.Retry(300, 100*time.Millisecond, func() error {
		res, err = b.PrunableData(context.Background())
		tt.OK(err)

		if len(res.Contracts) != len(contracts) {
			return fmt.Errorf("expected %d contracts, got %d", len(contracts), len(res.Contracts))
		} else if res.TotalPrunable == 0 {
			var sizes []string
			for _, c := range res.Contracts {
				res, _ := b.ContractSize(context.Background(), c.ID)
				sizes = append(sizes, fmt.Sprintf("c: %v size: %v prunable: %v", c.ID, res.Size, res.Prunable))
			}
			return errors.New("expected prunable data, contract sizes:\n" + strings.Join(sizes, "\n"))
		}
		return nil
	})

	// update the host settings so it's gouging
	host := hosts[0]
	settings := host.settings.Settings()
	settings.IngressPrice = types.Siacoins(1)
	settings.EgressPrice = types.Siacoins(1)
	settings.BaseRPCPrice = types.Siacoins(1)
	tt.OK(host.UpdateSettings(settings))

	// find the corresponding contract
	var c api.ContractMetadata
	for _, c = range contracts {
		if c.HostKey == host.PublicKey() {
			break
		}
	}

	// prune the contract and assert it threw a gouging error
	_, err = b.PruneContract(context.Background(), c.ID, 0)
	if err == nil || !strings.Contains(err.Error(), "gouging") {
		t.Fatal("expected gouging error", err)
	}
}
