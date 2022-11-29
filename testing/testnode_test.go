package testing

import (
	"bytes"
	"context"
	"testing"

	"lukechampine.com/frand"
)

// TestNewTestCluster is a smoke test for creating a cluster of Nodes for
// testing and shutting them down.
func TestNewTestCluster(t *testing.T) {
	cluster, err := newTestCluster(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	// Try talking to the bus API.
	bus := cluster.Bus()
	hosts, err := bus.AllHosts()
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 0 {
		t.Fatalf("expected 0 hosts but got %v", len(hosts))
	}

	// Try talking to the worker. Use an endpoint that forces the worker to
	// reach out to the bus as well.
	worker := cluster.Workers()[0]
	r := bytes.NewReader(frand.Bytes(100))
	if err := worker.UploadObject(r, "foo"); err != nil {
		t.Fatal(err)
	}

	// TODO: Once there is an autopilot client we test the autopilot as
	// well.

	// Shutdown
	if err := cluster.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
