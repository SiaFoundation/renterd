package testing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.uber.org/zap/zapcore"
)

func TestHostPruning(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	// create a new test cluster
	cluster, err := newTestCluster(t.TempDir(), newTestLoggerCustom(zapcore.ErrorLevel))
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

	// assert the host was scanned
	h, err := b.Host(context.Background(), hk1)
	if err != nil {
		t.Fatal(err)
	}
	if h.Interactions.LastScan.IsZero() {
		t.Fatal("expected last scan to be set")
	}

	// remove the first host manually and close it
	cluster.hosts = cluster.hosts[1:]
	err = hosts[0].Close()
	if err != nil {
		t.Fatal(err)
	}

	// create a helper function to record a number of failed interactions
	now := time.Now()
	recordFailedInteractions := func() {
		his := make([]hostdb.Interaction, 10)
		for i := 0; i < 10; i++ {
			now = now.Add(time.Hour)
			his[i] = hostdb.Interaction{
				Host:      hk1,
				Timestamp: now,
				Success:   false,
				Type:      hostdb.InteractionTypeScan,
			}
		}
		if err = b.RecordInteractions(context.Background(), his); err != nil {
			t.Fatal(err)
		}
	}

	// record a number of failed interaction for h1 that push his downtime well
	// over the default 'MaxDowntimeHours' setting of 10 hours and ensures we
	// push it over the 'MinRecentScanFailures' limit
	//
	// NOTE: we do this manually to avoid bypassing the condition by adding
	// flags or extending the config with properties only used in testing
	recordFailedInteractions()

	// wait until we have 2 hosts in the set - we expect one host to be removed
	// by the host pruning
	if err := Retry(30, 100*time.Millisecond, func() error {
		// check if the host got pruned
		hosts, err := b.Hosts(context.Background(), 0, -1)
		if err != nil {
			t.Fatal(err)
		} else if len(hosts) != 2 {
			recordFailedInteractions()
			return fmt.Errorf("unexpected number of hosts, %v != 2", len(hosts))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
