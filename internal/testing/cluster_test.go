package testing

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TestNewTestCluster is a test for creating a cluster of Nodes for testing,
// making sure that it forms contracts, renews contracts and shuts down.
func TestNewTestCluster(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster, err := newTestCluster(t.TempDir(), zap.New(zapcore.NewNopCore()))
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

	//  Wait for the contract to form.
	var contract worker.Contract
	err = Retry(20, time.Second, func() error {
		contracts, err := w.Contracts()
		if err != nil {
			return err
		}
		if len(contracts) != 1 {
			return errors.New("no contract")
		}
		contract = contracts[0]
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify startHeight and endHeight of the contract.
	currentPeriod, err := cluster.Autopilot.Status()
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := cluster.Autopilot.Config()
	if err != nil {
		t.Fatal(err)
	}
	if contract.EndHeight() != currentPeriod+cfg.Contracts.Period+cfg.Contracts.RenewWindow {
		t.Fatal("wrong endHeight")
	}

	// Mine blocks until contracts start renewing.
	if err := cluster.MineToRenewWindow(); err != nil {
		t.Fatal(err)
	}

	// Wait for the contract to be renewed.
	err = Retry(100, 100*time.Millisecond, func() error {
		contracts, err := w.Contracts()
		if err != nil {
			return err
		}
		if len(contracts) != 1 {
			return errors.New("no renewed contract")
		}
		if contracts[0].RenewedFrom != contract.ID {
			return fmt.Errorf("contract wasn't renewed %v != %v", contracts[0].RenewedFrom, contract.ID)
		}
		if contracts[0].EndHeight() != contract.EndHeight()+cfg.Contracts.Period {
			t.Fatal("wrong endHeight")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
