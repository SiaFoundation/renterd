package stores

import (
	"context"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/chain"
)

// TestChainUpdateTx tests the chain update transaction.
func TestChainUpdateTx(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)

	// add test host and contract
	hks, err := ss.addTestHosts(1)
	if err != nil {
		t.Fatal(err)
	}
	fcids, _, err := ss.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	} else if len(fcids) != 1 {
		t.Fatal("expected one contract", len(fcids))
	}
	fcid := fcids[0]

	// assert commit with no changes is successful
	tx, err := ss.BeginChainUpdateTx()
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal("unexpected error", err)
	}

	// assert rollback with no changes is successful
	tx, err = ss.BeginChainUpdateTx()
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal("unexpected error", err)
	}

	// assert contract state returns the correct state
	tx, err = ss.BeginChainUpdateTx()
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	state, err := tx.ContractState(fcid)
	if err != nil {
		t.Fatal("unexpected error", err)
	} else if state != api.ContractStatePending {
		t.Fatal("expected pending state", state)
	}

	// assert update chain index is successful
	if curr, err := ss.ChainIndex(); err != nil {
		t.Fatal("unexpected error", err)
	} else if curr.Height != 0 {
		t.Fatal("unexpected height", curr.Height)
	}
	index := types.ChainIndex{Height: 1}
	if err := tx.UpdateChainIndex(index); err != nil {
		t.Fatal("unexpected error", err)
	} else if err := tx.Commit(); err != nil {
		t.Fatal("unexpected error", err)
	}
	if got, err := ss.ChainIndex(); err != nil {
		t.Fatal("unexpected error", err)
	} else if got.Height != index.Height {
		t.Fatal("unexpected height", got.Height)
	}

	// assert update contract is successful
	var we uint64
	tx, err = ss.BeginChainUpdateTx()
	if err != nil {
		t.Fatal("unexpected error", err)
	} else if err := tx.UpdateContract(fcid, 1, 2, 3); err != nil {
		t.Fatal("unexpected error", err)
	} else if err := tx.UpdateContractState(fcid, api.ContractStateActive); err != nil {
		t.Fatal("unexpected error", err)
	} else if err := tx.UpdateContractProofHeight(fcid, 4); err != nil {
		t.Fatal("unexpected error", err)
	} else if err := tx.Commit(); err != nil {
		t.Fatal("unexpected error", err)
	} else if c, err := ss.contract(context.Background(), fileContractID(fcid)); err != nil {
		t.Fatal("unexpected error", err)
	} else if c.RevisionHeight != 1 {
		t.Fatal("unexpected revision height", c.RevisionHeight)
	} else if c.RevisionNumber != "2" {
		t.Fatal("unexpected revision number", c.RevisionNumber)
	} else if c.Size != 3 {
		t.Fatal("unexpected size", c.Size)
	} else if c.State.String() != api.ContractStateActive {
		t.Fatal("unexpected state", c.State)
	} else {
		we = c.WindowEnd
	}

	// assert update failed contracts is successful
	tx, err = ss.BeginChainUpdateTx()
	if err != nil {
		t.Fatal("unexpected error", err)
	} else if err := tx.UpdateFailedContracts(we + 1); err != nil {
		t.Fatal("unexpected error", err)
	} else if err := tx.Commit(); err != nil {
		t.Fatal("unexpected error", err)
	} else if c, err := ss.contract(context.Background(), fileContractID(fcid)); err != nil {
		t.Fatal("unexpected error", err)
	} else if c.State.String() != api.ContractStateFailed {
		t.Fatal("unexpected state", c.State)
	}

	// assert update host is successful
	tx, err = ss.BeginChainUpdateTx()
	if err != nil {
		t.Fatal("unexpected error", err)
	} else if err := tx.UpdateHost(hks[0], chain.HostAnnouncement{NetAddress: "foo"}, 1, types.BlockID{}, types.CurrentTimestamp()); err != nil {
		t.Fatal("unexpected error", err)
	} else if err := tx.Commit(); err != nil {
		t.Fatal("unexpected error", err)
	} else if h, err := ss.Host(context.Background(), hks[0]); err != nil {
		t.Fatal("unexpected error", err)
	} else if h.NetAddress != "foo" {
		t.Fatal("unexpected net address", h.NetAddress)
	}
}
