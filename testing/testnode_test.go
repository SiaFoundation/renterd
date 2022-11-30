package testing

import (
	"context"
	"reflect"
	"testing"

	"go.sia.tech/renterd/bus"
)

// TestNewTestCluster is a smoke test for creating a cluster of Nodes for
// testing and shutting them down.
func TestNewTestCluster(t *testing.T) {
	cluster, err := newTestCluster(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// Add a host.
	if err := cluster.AddHosts(1); err != nil {
		t.Fatal(err)
	}

	// Try talking to the bus API.
	b := cluster.Bus()
	busHealth, err := b.Health()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(busHealth, bus.HealthResponse{
		Syncer:          true,
		ChainManager:    true,
		TransactionPool: true,
		Wallet:          true,
		HostDB:          true,
		ContractStore:   true,
		HostSetStore:    true,
		ObjectStore:     true,
	}) {
		t.Fatal("bus isn't healthy")
	}

	// Try talking to the worker. Use an endpoint that forces the worker to
	// reach out to the bus as well.
	w := cluster.Workers()[0]
	wHealth, err := w.Health()
	if err != nil {
		t.Fatal(err)
	}
	if !wHealth.Bus {
		t.Fatal("worker isn't connected to bus")
	}

	// TODO: Once there is an autopilot client we test the autopilot as
	// well.

	// Shutdown
	if err := cluster.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
