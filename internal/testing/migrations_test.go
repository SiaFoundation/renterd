package testing

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"lukechampine.com/frand"
)

func TestMigrations(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a new test cluster
	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// create a helper to fetch used hosts
	usedHosts := func(path string) map[types.PublicKey]struct{} {
		// fetch used hosts
		obj, _, err := cluster.Bus.Object(context.Background(), path, "", 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		used := make(map[types.PublicKey]struct{})
		for _, slab := range obj.Slabs {
			for _, sector := range slab.Shards {
				used[sector.Host] = struct{}{}
			}
		}
		return used
	}

	// convenience variables
	cfg := testAutopilotConfig
	w := cluster.Worker

	// configure the cluster to use 1 more host than the total shards in the
	// redundancy settings.
	cfg.Contracts.Amount = uint64(testRedundancySettings.TotalShards) + 1
	if err := cluster.Autopilot.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}

	// add hosts
	hosts, err := cluster.AddHostsBlocking(int(cfg.Contracts.Amount))
	if err != nil {
		t.Fatal(err)
	}

	// wait until we have accounts
	if _, err := cluster.WaitForAccounts(); err != nil {
		t.Fatal(err)
	}

	// add an object
	data := make([]byte, rhpv2.SectorSize)
	frand.Read(data)
	if err := w.UploadObject(context.Background(), bytes.NewReader(data), "foo"); err != nil {
		t.Fatal(err)
	}

	// assert amount of hosts used
	used := usedHosts("foo")
	if len(used) != testRedundancySettings.TotalShards {
		t.Fatal("unexpected amount of hosts used", len(used), testRedundancySettings.TotalShards)
	}

	// select one host to remove
	var removed types.PublicKey
	for _, h := range hosts {
		if _, ok := used[h.PublicKey()]; ok {
			if err := cluster.RemoveHost(h); err != nil {
				t.Fatal(err)
			}
			removed = h.PublicKey()

			// find the contract
			contracts, err := cluster.Bus.Contracts(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var contract *api.ContractMetadata
			for _, c := range contracts {
				if c.HostKey == removed {
					contract = &c
					break
				}
			}
			if contract == nil {
				t.Fatal("contract not found")
			}

			// mine until we archive the contract
			endHeight := contract.WindowEnd
			cs, err := cluster.Bus.ConsensusState(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := cluster.MineBlocks(int(endHeight - cs.BlockHeight + 1)); err != nil {
				t.Fatal(err)
			}
			break
		}
	}

	// assert we migrated away from the bad host
	if err := Retry(300, 100*time.Millisecond, func() error {
		if _, used := usedHosts("foo")[removed]; used {
			if err := cluster.MineBlocks(1); err != nil {
				t.Fatal(err)
			}
			return errors.New("host is still used")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
