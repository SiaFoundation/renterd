package testing

import (
	"context"
	"testing"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	"go.sia.tech/siad/types"
)

// TestNewTestCluster is a smoke test for creating a cluster of Nodes for
// testing and shutting them down.
func TestNewTestCluster(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster, err := newTestCluster(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// Add a host.
	b := cluster.Bus
	if err := cluster.AddHosts(1); err != nil {
		t.Fatal(err)
	}

	// Try talking to the bus API by adding an object.
	err = b.AddObject("/foo", object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards:    []object.Sector{}, // slab without sectors
				},
				Offset: 0,
				Length: 0,
			},
		},
	}, map[consensus.PublicKey]types.FileContractID{})
	if err != nil {
		t.Fatal(err)
	}

	// Try talking to the worker and request the object.
	w := cluster.Worker
	err = w.DeleteObject("/foo")
	if err != nil {
		t.Fatal(err)
	}

	//	// Wait for the contract to form.
	//	err = Retry(20, time.Second, func() error {
	//		contracts, err := b.Contracts()
	//		if err != nil {
	//			t.Fatal(err)
	//		}
	//		if len(contracts) != 1 {
	//			return errors.New("no contract")
	//		}
	//		return nil
	//	})
	//	if err != nil {
	//		t.Fatal(err)
	//	}

	// TODO: Once there is an autopilot client we test the autopilot as
	// well.
}
