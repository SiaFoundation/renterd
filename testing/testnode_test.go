package testing

import (
	"context"
	"testing"
)

// TestNewTestCluster is a smoke test for creating a cluster of Nodes for
// testing and shutting them down.
func TestNewTestCluster(t *testing.T) {
	cluster, err := newTestCluster(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
