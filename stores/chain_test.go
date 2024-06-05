package stores

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/chain"
)

// TestProcessChainUpdate tests the ProcessChainUpdate method on the SQL store.
func TestProcessChainUpdate(t *testing.T) {
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

	// assert contract state returns the correct state
	var state api.ContractState
	if err := ss.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) (err error) {
		state, err = tx.ContractState(fcid)
		return
	}); err != nil {
		t.Fatal("unexpected error", err)
	} else if state != api.ContractStatePending {
		t.Fatalf("unexpected state '%v'", state)
	}

	// check current index
	if curr, err := ss.ChainIndex(context.Background()); err != nil {
		t.Fatal(err)
	} else if curr.Height != 0 {
		t.Fatalf("unexpected height %v", curr.Height)
	}

	// assert update chain index is successful
	if err := ss.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) error {
		return tx.UpdateChainIndex(types.ChainIndex{Height: 1})
	}); err != nil {
		t.Fatal("unexpected error", err)
	}

	// check updated index
	if curr, err := ss.ChainIndex(context.Background()); err != nil {
		t.Fatal(err)
	} else if curr.Height != 1 {
		t.Fatalf("unexpected height %v", curr.Height)
	}

	// assert update contract is successful
	if err := ss.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) error {
		if err := tx.UpdateContract(fcid, 1, 2, 3); err != nil {
			return err
		} else if err := tx.UpdateContractState(fcid, api.ContractStateActive); err != nil {
			return err
		} else if err := tx.UpdateContractProofHeight(fcid, 4); err != nil {
			return err
		} else {
			return nil
		}
	}); err != nil {
		t.Fatal("unexpected error", err)
	}

	// assert contract was updated successfully
	var we uint64
	if c, err := ss.contract(context.Background(), fileContractID(fcid)); err != nil {
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

	// assert we only update revision height if the rev number doesn't increase
	if err := ss.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) error {
		return tx.UpdateContract(fcid, 2, 2, 4)
	}); err != nil {
		t.Fatal("unexpected error", err)
	}
	if c, err := ss.contract(context.Background(), fileContractID(fcid)); err != nil {
		t.Fatal("unexpected error", err)
	} else if c.RevisionHeight != 2 {
		t.Fatal("unexpected revision height", c.RevisionHeight)
	} else if c.RevisionNumber != "2" {
		t.Fatal("unexpected revision number", c.RevisionNumber)
	} else if c.Size != 3 {
		t.Fatal("unexpected size", c.Size)
	}

	// assert update failed contracts is successful
	if err := ss.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) error {
		return tx.UpdateFailedContracts(we + 1)
	}); err != nil {
		t.Fatal("unexpected error", err)
	}
	if c, err := ss.contract(context.Background(), fileContractID(fcid)); err != nil {
		t.Fatal("unexpected error", err)
	} else if c.State.String() != api.ContractStateFailed {
		t.Fatal("unexpected state", c.State)
	}

	// renew the contract
	_, err = ss.addTestRenewedContract(types.FileContractID{2}, fcid, hks[0], 1)
	if err != nil {
		t.Fatal(err)
	}

	// assert we can fetch the state of the archived contract
	if err := ss.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) (err error) {
		state, err = tx.ContractState(fcid)
		return
	}); err != nil {
		t.Fatal("unexpected error", err)
	} else if state != api.ContractStateFailed {
		t.Fatalf("unexpected state '%v'", state)
	}

	// assert update host is successful
	if err := ss.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) error {
		return tx.UpdateHost(hks[0], chain.HostAnnouncement{NetAddress: "foo"}, 1, types.BlockID{}, types.CurrentTimestamp())
	}); err != nil {
		t.Fatal("unexpected error", err)
	}
	if h, err := ss.Host(context.Background(), hks[0]); err != nil {
		t.Fatal("unexpected error", err)
	} else if h.NetAddress != "foo" {
		t.Fatal("unexpected net address", h.NetAddress)
	}

	// assert passing empty function is successful
	if err := ss.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) error { return nil }); err != nil {
		t.Fatal("unexpected error", err)
	}

	// assert we rollback on error
	if err := ss.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) error {
		if err := tx.UpdateChainIndex(types.ChainIndex{Height: 2}); err != nil {
			return err
		}
		return errors.New("some error")
	}); err == nil || !strings.Contains(err.Error(), "some error") {
		t.Fatal("unexpected error", err)
	}

	// check chain index was rolled back
	if curr, err := ss.ChainIndex(context.Background()); err != nil {
		t.Fatal(err)
	} else if curr.Height != 1 {
		t.Fatalf("unexpected height %v", curr.Height)
	}

	// assert we recover from panic
	if err := ss.ProcessChainUpdate(context.Background(), func(tx chain.ChainUpdateTx) error { return nil }); err != nil {
		panic("oh no")
	}
}
