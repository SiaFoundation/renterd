package testing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/siad/build"
)

func TestHostPruning(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	// create a new test cluster
	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	b := cluster.Bus

	// add 3 hosts
	hosts, err := cluster.AddHostsBlocking(3)
	if err != nil {
		t.Fatal(err)
	}

	// remove the first host manually and close it
	cluster.hosts = cluster.hosts[1:]
	err = hosts[0].Close()
	if err != nil {
		t.Fatal(err)
	}

	// add h1 to the blocklist
	hk1 := hosts[0].PublicKey()
	err = b.UpdateHostBlocklist(ctx, []string{hk1.String()}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// record a couple of failed interactions for h1 that push his downtime well
	// over the default 'MaxDowntimeHours' setting of 10 hours, and his recent
	// scan failures over the default 'MinRecentScanFailures'
	//
	// NOTE: we do this manually to avoid bypassing the condition by adding
	// flags or extending the config with properties only used in testing
	his := make([]hostdb.Interaction, 10)
	for i := 0; i < 10; i++ {
		his[i] = hostdb.Interaction{
			Host:      hk1,
			Timestamp: time.Now().Add(time.Hour * 20),
			Type:      hostdb.InteractionTypeScan,
		}
	}
	if err = b.RecordInteractions(context.Background(), his); err != nil {
		t.Fatal(err)
	}

	// wait until we have 2 hosts in the set - we expect one host to be removed
	// by the host pruning - this takes ~10 failed scans so we retry for 30s
	if err := build.Retry(30, time.Second, func() error {
		hosts, err := b.Hosts(context.Background(), 0, -1)
		if err != nil {
			t.Fatal(err)
		} else if len(hosts) != 2 {
			return fmt.Errorf("unexpected number of hosts, %v != 2", len(hosts))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
