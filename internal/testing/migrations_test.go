package testing

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"lukechampine.com/frand"
)

func TestMigrations(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a new test cluster
	cluster, err := newTestCluster(t.TempDir(), zap.New(zapcore.NewNopCore()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// convenience variables
	cfg := defaultAutopilotConfig.Contracts
	w := cluster.Worker
	b := cluster.Bus

	// add hosts
	hosts, err := cluster.AddHosts(int(cfg.Hosts))
	if err != nil {
		t.Fatal(err)
	}

	// wait until we have contracts
	if _, err := cluster.WaitForContracts(); err != nil {
		t.Fatal(err)
	}

	// add an object
	data := make([]byte, rhpv2.SectorSize*4)
	frand.Read(data)
	if err := w.UploadObject(bytes.NewReader(data), "foo"); err != nil {
		t.Fatal(err)
	}

	usedHosts := func() []consensus.PublicKey {
		t.Helper()
		obj, _, err := b.Object("/foo")
		if err != nil {
			t.Fatal(err)
		}

		hostmap := make(map[consensus.PublicKey]struct{})
		for _, slab := range obj.Slabs {
			for _, sector := range slab.Shards {
				hostmap[sector.Host] = struct{}{}
			}
		}

		hks := make([]consensus.PublicKey, 0, len(hostmap))
		for hk := range hostmap {
			hks = append(hks, hk)
		}

		return hks
	}

	isUsed := func(hk consensus.PublicKey, usedHosts []consensus.PublicKey) bool {
		t.Helper()
		for _, h := range usedHosts {
			if h == hk {
				return true
			}
		}
		return false
	}

	// remove one (random) host from the cluster
	hks := usedHosts()
	var hk consensus.PublicKey
	for _, h := range hosts {
		if hk = h.HostKey(); isUsed(hk, hks) {
			if err := cluster.RemoveHost(h); err != nil {
				t.Fatal(err)
			}
			break
		}
	}

	// assert we migrated away from the bad host
	if err := Retry(30, time.Second, func() error {
		hks := usedHosts()
		if isUsed(hk, hks) {
			return errors.New("host is still used")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
