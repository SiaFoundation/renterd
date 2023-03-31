package testing

import (
	"context"
	"fmt"
	"testing"
	"time"

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

	// add h1 to the blocklist
	hk1 := hosts[0].PublicKey()
	err = b.UpdateHostBlocklist(ctx, []string{hk1.String()}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// remove the first host manually and close it
	cluster.hosts = cluster.hosts[1:]
	err = hosts[0].Close()
	if err != nil {
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
