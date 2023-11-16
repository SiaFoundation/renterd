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
				used[sector.LatestHost] = struct{}{}
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
	path := "foo"
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, path, api.UploadObjectOptions{}))

	// assert amount of hosts used
	used := usedHosts(path)
	if len(used) != testRedundancySettings.TotalShards {
		t.Fatal("unexpected amount of hosts used", len(used), testRedundancySettings.TotalShards)
	}

	// select one host to remove
	var removed types.PublicKey
	for _, h := range cluster.hosts {
		if _, ok := used[h.PublicKey()]; ok {
			cluster.RemoveHost(h)
			removed = h.PublicKey()
			break
		}
	}

	// assert we migrated away from the bad host
	tt.Retry(300, 100*time.Millisecond, func() error {
		if _, used := usedHosts(path)[removed]; used {
			return errors.New("host is still used")
		}
		return nil
	})

	res, err := cluster.Bus.Object(context.Background(), api.DefaultBucketName, path, api.GetObjectOptions{})
	tt.OK(err)

	// check slabs
	shardHosts := 0
	for _, slab := range res.Object.Slabs {
		hosts := make(map[types.PublicKey]struct{})
		roots := make(map[types.Hash256]struct{})
		for _, shard := range slab.Shards {
			if shard.LatestHost == (types.PublicKey{}) {
				t.Fatal("latest host should be set")
			} else if len(shard.Contracts) == 0 {
				t.Fatal("each shard should have > 0 hosts")
			}
			for hpk, contracts := range shard.Contracts {
				if len(contracts) != 1 {
					t.Fatal("each host should have one contract")
				} else if _, found := hosts[hpk]; found {
					t.Fatal("each host should only be used once per slab")
				}
				hosts[hpk] = struct{}{}
			}
			roots[shard.Root] = struct{}{}
			shardHosts += len(shard.Contracts)
		}
	}
	// all shards should have 1 host except for 1. So we end up with 4 in total.
	if shardHosts != 4 {
		t.Fatalf("expected 4 shard hosts, got %v", shardHosts)
	}
}
