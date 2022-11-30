package testing

import (
	"context"
	"testing"
)

// TestNewCluster is a smoke test for creating a cluster of Nodes for
// testing and shutting them down.
func TestNewCluster(t *testing.T) {
	cluster, err := newCluster(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
