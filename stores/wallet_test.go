package stores

import (
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"lukechampine.com/frand"
)

func TestWalletBroadcastedSets(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	set := wallet.BroadcastedSet{
		Basis:         types.ChainIndex{Height: 1, ID: types.BlockID{1}},
		BroadcastedAt: time.Now(),
		Transactions: []types.V2Transaction{
			{
				ArbitraryData: frand.Bytes(10),
			},
			{
				ArbitraryData: frand.Bytes(10),
			},
			{
				ArbitraryData: frand.Bytes(10),
			},
		},
	}

	if err := ss.AddBroadcastedSet(set); err != nil {
		t.Fatal(err)
	}

	sets, err := ss.BroadcastedSets()
	if err != nil {
		t.Fatal(err)
	} else if len(sets) != 1 {
		t.Fatalf("expected 1 broadcasted set, got %d", len(sets))
	} else if sets[0].Basis != set.Basis {
		t.Fatalf("expected basis %v, got %v", set.Basis, sets[0].Basis)
	} else if !sets[0].BroadcastedAt.Truncate(time.Second).UTC().Equal(set.BroadcastedAt.Truncate(time.Second).UTC()) {
		t.Fatalf("expected broadcasted at %v, got %v", set.BroadcastedAt, sets[0].BroadcastedAt)
	} else if sets[0].ID() != set.ID() {
		t.Fatalf("expected ID %v, got %v", set.ID(), sets[0].ID())
	}

	if err := ss.RemoveBroadcastedSet(set); err != nil {
		t.Fatal(err)
	}

	sets, err = ss.BroadcastedSets()
	if err != nil {
		t.Fatal(err)
	} else if len(sets) != 0 {
		t.Fatalf("expected 0 broadcasted sets, got %d", len(sets))
	}
}
