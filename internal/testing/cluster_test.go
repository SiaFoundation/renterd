package testing

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
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

	// See if autopilot is running by fetching the config.
	_, err = cluster.Autopilot.Config()
	if err != nil {
		t.Fatal(err)
	}

	cs, err := b.ConsensusState()
	if err != nil {
		t.Fatal(err)
	}
	height := cs.BlockHeight

	rk := consensus.GeneratePrivateKey()
	locations, err := cluster.Locations(rk)
	if err != nil {
		t.Fatal(err)
	}
	_, err = w.UploadSlab(bytes.NewReader(frand.Bytes(10)), 1, 1, height, locations)
	if err != nil {
		t.Fatal(err)
	}

	//  Wait for the contract to form.
	err = Retry(20, time.Second, func() error {
		contracts, err := b.Contracts()
		if err != nil {
			t.Fatal(err)
		}
		if len(contracts) != 1 {
			return errors.New("no contract")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
