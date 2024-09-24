package stores

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/stores/sql"
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

	// check current contract state
	var state api.ContractState
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) (err error) {
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

	// run chain update
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
		// update chain index
		if err := tx.UpdateChainIndex(types.ChainIndex{Height: 1}); err != nil {
			return err
		}

		// update contract
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

	// assert updated index
	if curr, err := ss.ChainIndex(context.Background()); err != nil {
		t.Fatal(err)
	} else if curr.Height != 1 {
		t.Fatalf("unexpected height %v", curr.Height)
	}

	// assert updated contract
	var we uint64
	if c, err := ss.Contract(context.Background(), fcid); err != nil {
		t.Fatal("unexpected error", err)
	} else if c.RevisionHeight != 1 {
		t.Fatal("unexpected revision height", c.RevisionHeight)
	} else if c.RevisionNumber != 2 {
		t.Fatal("unexpected revision number", c.RevisionNumber)
	} else if c.Size != 3 {
		t.Fatal("unexpected size", c.Size)
	} else if c.State != api.ContractStateActive {
		t.Fatal("unexpected state", c.State)
	} else {
		we = c.WindowEnd
	}

	// assert we only update revision height if the rev number doesn't increase
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
		return tx.UpdateContract(fcid, 2, 2, 4)
	}); err != nil {
		t.Fatal("unexpected error", err)
	}
	if c, err := ss.Contract(context.Background(), fcid); err != nil {
		t.Fatal("unexpected error", err)
	} else if c.RevisionHeight != 2 {
		t.Fatal("unexpected revision height", c.RevisionHeight)
	} else if c.RevisionNumber != 2 {
		t.Fatal("unexpected revision number", c.RevisionNumber)
	} else if c.Size != 3 {
		t.Fatal("unexpected size", c.Size)
	}

	// assert update failed contracts is successful
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
		return tx.UpdateFailedContracts(we + 1)
	}); err != nil {
		t.Fatal("unexpected error", err)
	}
	if c, err := ss.Contract(context.Background(), fcid); err != nil {
		t.Fatal("unexpected error", err)
	} else if c.State != api.ContractStateFailed {
		t.Fatal("unexpected state", c.State)
	}

	// renew the contract
	_, err = ss.addTestRenewedContract(types.FileContractID{2}, fcid, hks[0], 1)
	if err != nil {
		t.Fatal(err)
	}

	// assert we can fetch the state of the archived contract
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) (err error) {
		state, err = tx.ContractState(fcid)
		return
	}); err != nil {
		t.Fatal("unexpected error", err)
	} else if state != api.ContractStateFailed {
		t.Fatalf("unexpected state '%v'", state)
	}

	// assert update host is successful
	ts := time.Now().Truncate(time.Second).Add(-time.Minute).UTC()
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
		return tx.UpdateHost(hks[0], chain.HostAnnouncement{NetAddress: "foo"}, 1, types.BlockID{}, ts)
	}); err != nil {
		t.Fatal("unexpected error", err)
	}
	if h, err := ss.Host(context.Background(), hks[0]); err != nil {
		t.Fatal("unexpected error", err)
	} else if h.NetAddress != "foo" {
		t.Fatal("unexpected net address", h.NetAddress)
	} else if !h.LastAnnouncement.Truncate(time.Second).Equal(ts) {
		t.Fatalf("unexpected last announcement %v != %v", h.LastAnnouncement, ts)
	}

	// record 2 scans for the host to give it some uptime
	err = ss.RecordHostScans(context.Background(), []api.HostScan{
		{HostKey: hks[0], Success: true, Timestamp: time.Now()},
		{HostKey: hks[0], Success: true, Timestamp: time.Now().Add(time.Minute)},
	})
	if err != nil {
		t.Fatal(err)
	} else if h, err := ss.Host(context.Background(), hks[0]); err != nil {
		t.Fatal(err)
	} else if h.Interactions.Uptime < time.Minute || h.Interactions.Uptime > time.Minute+time.Second {
		t.Fatalf("unexpected uptime %v", h.Interactions.Uptime)
	}

	// reannounce the host and make sure the uptime is the same
	ts = ts.Add(time.Minute)
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
		return tx.UpdateHost(hks[0], chain.HostAnnouncement{NetAddress: "fooNew"}, 1, types.BlockID{}, ts)
	}); err != nil {
		t.Fatal("unexpected error", err)
	}
	if h, err := ss.Host(context.Background(), hks[0]); err != nil {
		t.Fatal("unexpected error", err)
	} else if h.Interactions.Uptime < time.Minute || h.Interactions.Uptime > time.Minute+time.Second {
		t.Fatalf("unexpected uptime %v", h.Interactions.Uptime)
	} else if h.NetAddress != "fooNew" {
		t.Fatal("unexpected net address", h.NetAddress)
	} else if !h.LastAnnouncement.Equal(ts) {
		t.Fatalf("unexpected last announcement %v != %v", h.LastAnnouncement, ts)
	}

	// assert passing empty function is successful
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error { return nil }); err != nil {
		t.Fatal("unexpected error", err)
	}

	// assert we rollback on error
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
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
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error { return nil }); err != nil {
		panic("oh no")
	}

	// assert we can revert spent outputs
	now := time.Now().Round(time.Millisecond)
	var ses []types.StateElement
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
		index3 := types.ChainIndex{Height: 3}
		index4 := types.ChainIndex{Height: 4}
		created := []types.SiacoinElement{
			{
				StateElement: types.StateElement{
					ID:          types.Hash256{1},
					LeafIndex:   1,
					MerkleProof: []types.Hash256{{1}, {2}},
				},
				SiacoinOutput: types.SiacoinOutput{
					Address: types.Address{1},
					Value:   types.NewCurrency64(1),
				},
				MaturityHeight: 100,
			},
		}

		// try spending non-existent output
		err = tx.WalletApplyIndex(index4, nil, created, nil, time.Now())
		if !errors.Is(err, sql.ErrOutputNotFound) {
			return fmt.Errorf("expected ErrOutputNotFound, instead got: %w", err)
		}

		// create the elements
		err = tx.WalletApplyIndex(index3, created, nil, nil, time.Now())
		if err != nil {
			return err
		}

		// spend them
		err = tx.WalletApplyIndex(index4, nil, created, nil, time.Now())
		if err != nil {
			return err
		}

		// revert the spend
		err = tx.WalletRevertIndex(index4, nil, created, time.Now())
		if err != nil {
			return err
		}

		// revert the creation
		err = tx.WalletRevertIndex(index3, nil, created, time.Now())
		if err != nil {
			return err
		}

		// prepare event
		events := []wallet.Event{
			{
				ID:        types.Hash256{1},
				Index:     types.ChainIndex{Height: 5},
				Type:      wallet.EventTypeV2Transaction,
				Data:      wallet.EventV2Transaction{},
				Timestamp: now,
			},
		}

		// add them
		err = tx.WalletApplyIndex(types.ChainIndex{Height: 5}, nil, nil, events, time.Now())
		if err != nil {
			return err
		}

		// fetch elements
		ses, err = tx.WalletStateElements()
		return err
	}); err != nil {
		t.Fatal("unexpected error", err)
	}

	// assert wallet state elements
	if len(ses) != 1 {
		t.Fatal("unexpected number of state elements", len(ses))
	} else if se := ses[0]; se.ID != (types.Hash256{1}) {
		t.Fatal("unexpected state element id", se.ID)
	} else if se.LeafIndex != 1 {
		t.Fatal("unexpected state element leaf index", se.LeafIndex)
	} else if len(se.MerkleProof) != 2 {
		t.Fatal("unexpected state element merkle proof", len(se.MerkleProof))
	} else if se.MerkleProof[0] != (types.Hash256{1}) {
		t.Fatal("unexpected state element merkle proof[0]", se.MerkleProof[0])
	} else if se.MerkleProof[1] != (types.Hash256{2}) {
		t.Fatal("unexpected state element merkle proof[1]", se.MerkleProof[1])
	}

	// update state elements
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
		ses[0].LeafIndex = 2
		ses[0].MerkleProof = []types.Hash256{{3}, {4}}
		err := tx.UpdateWalletStateElements(ses)
		if err != nil {
			return err
		}

		ses, err = tx.WalletStateElements()
		return err
	}); err != nil {
		t.Fatal("unexpected error", err)
	}

	// assert wallet state elements
	if len(ses) != 1 {
		t.Fatal("unexpected number of state elements", len(ses))
	} else if se := ses[0]; se.LeafIndex != 2 {
		t.Fatal("unexpected state element leaf index", se.LeafIndex)
	} else if len(se.MerkleProof) != 2 {
		t.Fatal("unexpected state element merkle proof length", len(se.MerkleProof))
	} else if se.MerkleProof[0] != (types.Hash256{3}) {
		t.Fatal("unexpected state element merkle proof[0]", se.MerkleProof[0])
	} else if se.MerkleProof[1] != (types.Hash256{4}) {
		t.Fatal("unexpected state element merkle proof[1]", se.MerkleProof[1])
	}

	// assert events
	events, err := ss.WalletEvents(0, -1)
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatal("unexpected number of events", len(events))
	} else if events[0].Index.Height != 5 {
		t.Fatal("unexpected event index height", events[0].Index.Height, events[0])
	} else if events[0].Timestamp != now {
		t.Fatal("unexpected event timestamp", events[0].Timestamp, now)
	}

	// revert the index and assert the event got removed
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
		return tx.WalletRevertIndex(types.ChainIndex{Height: 5}, nil, nil, time.Now())
	}); err != nil {
		t.Fatal("expected error")
	}
	events, err = ss.WalletEvents(0, -1)
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 0 {
		t.Fatal("unexpected number of events", len(events))
	}

	// assert we can't delete non-existing outputs when reverting
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
		return tx.WalletRevertIndex(types.ChainIndex{Height: 5}, []types.SiacoinElement{
			{
				StateElement:   types.StateElement{ID: types.Hash256{2}},
				SiacoinOutput:  types.SiacoinOutput{},
				MaturityHeight: 100,
			},
		}, nil, time.Now())
	}); !errors.Is(err, sql.ErrOutputNotFound) {
		t.Fatal("expected ErrOutputNotFound", err)
	}

	// assert we can't apply an index and pass events with mismatching index
	if err := ss.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
		return tx.WalletApplyIndex(types.ChainIndex{Height: 5}, nil, nil, []wallet.Event{
			{
				ID:        types.Hash256{1},
				Index:     types.ChainIndex{Height: 6},
				Type:      wallet.EventTypeV2Transaction,
				Data:      wallet.EventV2Transaction{},
				Timestamp: now,
			},
		}, now)
	}); !errors.Is(err, sql.ErrIndexMissmatch) {
		t.Fatal("expected ErrIndexMissmatch", err)
	}
}
