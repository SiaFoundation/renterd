package testing

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"go.sia.tech/core/types"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
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

	// convenience variables
	cfg := defaultAutopilotConfig
	w := cluster.Worker
	b := cluster.Bus

	// add hosts
	hosts, err := cluster.AddHostsBlocking(int(cfg.Contracts.Amount))
	if err != nil {
		t.Fatal(err)
	}

	// add an object
	data := make([]byte, rhpv2.SectorSize*4)
	frand.Read(data)
	if err := w.UploadObject(bytes.NewReader(data), "foo"); err != nil {
		t.Fatal(err)
	}

	usedHosts := func() []types.PublicKey {
		t.Helper()
		obj, _, err := b.Object("foo")
		if err != nil {
			t.Fatal(err)
		}

		hostsMap := make(map[types.PublicKey]struct{})
		for _, slab := range obj.Slabs {
			for _, sector := range slab.Shards {
				hostsMap[sector.Host] = struct{}{}
			}
		}

		hks := make([]types.PublicKey, 0, len(hostsMap))
		for hk := range hostsMap {
			hks = append(hks, hk)
		}

		return hks
	}

	isUsed := func(hk types.PublicKey, usedHosts []types.PublicKey) bool {
		t.Helper()
		for _, h := range usedHosts {
			if h == hk {
				return true
			}
		}
		return false
	}

	removeUsedHost := func() (hk types.PublicKey) {
		t.Helper()
		hks := usedHosts()
		for _, h := range hosts {
			if hk = h.HostKey(); isUsed(hk, hks) {
				if err := cluster.RemoveHost(h); err != nil {
					t.Fatal(err)
				}
				break
			}
		}
		return
	}

	// remove one (random) host from the cluster
	hk := removeUsedHost()

	// assert we migrated away from the bad host
	if err := Retry(30, time.Second, func() error {
		if isUsed(hk, usedHosts()) {
			return errors.New("host is still used")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
