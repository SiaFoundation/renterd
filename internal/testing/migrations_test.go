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
	cfg := testAutopilotConfig
	cfg.Contracts.Amount = uint64(testRedundancySettings.TotalShards) + 1
	cluster := newTestCluster(t, testClusterOptions{
		// configure the cluster to use 1 more host than the total shards in the
		// redundancy settings.
		autopilotSettings: &cfg,
		hosts:             int(testRedundancySettings.TotalShards) + 1,
	})
	defer cluster.Shutdown()

	// create a helper to fetch used hosts
	usedHosts := func(path string) map[types.PublicKey]struct{} {
		// fetch used hosts
		res, err := cluster.Bus.Object(context.Background(), api.DefaultBucketName, path, api.GetObjectOptions{})
		if err != nil {
			t.Fatal(err)
		} else if res.Object == nil {
			t.Fatal("object not found")
		}

		used := make(map[types.PublicKey]struct{})
		for _, slab := range res.Object.Slabs {
			for _, sector := range slab.Shards {
				used[sector.Host] = struct{}{}
			}
		}
		return used
	}

	// convenience variables
	w := cluster.Worker
	tt := cluster.tt

	// add an object
	data := make([]byte, rhpv2.SectorSize)
	frand.Read(data)
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, "foo", api.UploadObjectOptions{}))

	// assert amount of hosts used
	used := usedHosts("foo")
	if len(used) != testRedundancySettings.TotalShards {
		t.Fatal("unexpected amount of hosts used", len(used), testRedundancySettings.TotalShards)
	}

	// select one host to remove
	var removed types.PublicKey
	for _, h := range cluster.hosts {
		if _, ok := used[h.PublicKey()]; ok {
			cluster.RemoveHost(h)
			removed = h.PublicKey()

			// find the contract
			contracts, err := cluster.Bus.Contracts(context.Background())
			tt.OK(err)
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
			tt.OK(err)
			cluster.MineBlocks(int(endHeight - cs.BlockHeight + 1))
			break
		}
	}

	// assert we migrated away from the bad host
	tt.Retry(300, 100*time.Millisecond, func() error {
		if _, used := usedHosts("foo")[removed]; used {
			cluster.MineBlocks(1)
			return errors.New("host is still used")
		}
		return nil
	})
}
