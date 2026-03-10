package bus

import (
	"fmt"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/contracts"
	"go.uber.org/zap"
)

// mockChainUpdateTx implements sql.ChainUpdateTx for testing contract state
// transitions. Only the methods used by applyV2ContractUpdate and
// revertV2ContractUpdate are implemented.
type mockChainUpdateTx struct {
	known       map[types.FileContractID]bool
	states      map[types.FileContractID]api.ContractState
	proofHeight map[types.FileContractID]uint64
	renewals    map[types.FileContractID]types.FileContractID
	revisions   map[types.FileContractID]struct {
		height, revisionNumber, size uint64
	}
}

func newMockChainUpdateTx() *mockChainUpdateTx {
	return &mockChainUpdateTx{
		known:       make(map[types.FileContractID]bool),
		states:      make(map[types.FileContractID]api.ContractState),
		proofHeight: make(map[types.FileContractID]uint64),
		renewals:    make(map[types.FileContractID]types.FileContractID),
		revisions: make(map[types.FileContractID]struct {
			height, revisionNumber, size uint64
		}),
	}
}

func (tx *mockChainUpdateTx) addContract(fcid types.FileContractID) {
	tx.known[fcid] = true
	tx.states[fcid] = api.ContractStatePending
}

func (tx *mockChainUpdateTx) ContractState(fcid types.FileContractID) (api.ContractState, error) {
	state, ok := tx.states[fcid]
	if !ok {
		return "", fmt.Errorf("contract not found")
	}
	return state, nil
}

func (tx *mockChainUpdateTx) IsKnownContract(fcid types.FileContractID) (bool, error) {
	return tx.known[fcid], nil
}

func (tx *mockChainUpdateTx) UpdateContractRevision(fcid types.FileContractID, revisionHeight, revisionNumber, size uint64) error {
	tx.revisions[fcid] = struct {
		height, revisionNumber, size uint64
	}{revisionHeight, revisionNumber, size}
	return nil
}

func (tx *mockChainUpdateTx) UpdateContractProofHeight(fcid types.FileContractID, proofHeight uint64) error {
	tx.proofHeight[fcid] = proofHeight
	return nil
}

func (tx *mockChainUpdateTx) UpdateContractState(fcid types.FileContractID, state api.ContractState) error {
	tx.states[fcid] = state
	return nil
}

func (tx *mockChainUpdateTx) RecordContractRenewal(old, renewed types.FileContractID) error {
	tx.renewals[old] = renewed
	return nil
}

func (tx *mockChainUpdateTx) DeleteFileContractElement(fcid types.FileContractID) error {
	delete(tx.known, fcid)
	return nil
}

// stubs for remaining sql.ChainUpdateTx methods (unused by the functions under test)
func (tx *mockChainUpdateTx) ExpiredFileContractElements(uint64) ([]contracts.V2BroadcastElement, error) {
	panic("unexpected call")
}
func (tx *mockChainUpdateTx) FileContractElement(types.FileContractID) (contracts.V2BroadcastElement, error) {
	panic("unexpected call")
}
func (tx *mockChainUpdateTx) PruneFileContractElements(uint64) error { panic("unexpected call") }
func (tx *mockChainUpdateTx) UpdateFileContractElements([]types.V2FileContractElement) error {
	panic("unexpected call")
}
func (tx *mockChainUpdateTx) UpdateChainIndex(types.ChainIndex) error { panic("unexpected call") }
func (tx *mockChainUpdateTx) UpdateFileContractElementProofs(wallet.ProofUpdater) error {
	panic("unexpected call")
}
func (tx *mockChainUpdateTx) UpdateFailedContracts(uint64) error { panic("unexpected call") }
func (tx *mockChainUpdateTx) UpdateHost(types.PublicKey, chain.V2HostAnnouncement, uint64, types.BlockID, time.Time) error {
	panic("unexpected call")
}
func (tx *mockChainUpdateTx) UpdateWalletSiacoinElementProofs(wallet.ProofUpdater) error {
	panic("unexpected call")
}
func (tx *mockChainUpdateTx) WalletApplyIndex(types.ChainIndex, []types.SiacoinElement, []types.SiacoinElement, []wallet.Event, time.Time) error {
	panic("unexpected call")
}
func (tx *mockChainUpdateTx) WalletRevertIndex(types.ChainIndex, []types.SiacoinElement, []types.SiacoinElement, time.Time) error {
	panic("unexpected call")
}

func newTestSubscriber() *chainSubscriber {
	return &chainSubscriber{
		logger: zap.NewNop().Sugar(),
	}
}

func TestApplyAndRevertV2ContractUpdate(t *testing.T) {
	fcid := types.FileContractID{1}

	newFCE := func() types.V2FileContractElement {
		return types.V2FileContractElement{
			ID: fcid,
			V2FileContract: types.V2FileContract{
				RevisionNumber: 0,
				Filesize:       0,
			},
		}
	}

	t.Run("pending to active to complete via storage proof", func(t *testing.T) {
		tx := newMockChainUpdateTx()
		tx.addContract(fcid)
		s := newTestSubscriber()

		// apply creation at height 10 -> active
		fce := newFCE()
		if err := s.applyV2ContractUpdate(tx, types.ChainIndex{Height: 10}, fce, true, nil, nil); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateActive {
			t.Fatalf("expected active, got %v", tx.states[fcid])
		}

		// apply revision at height 15 (no state change)
		rev := types.V2FileContract{RevisionNumber: 1, Filesize: 100}
		if err := s.applyV2ContractUpdate(tx, types.ChainIndex{Height: 15}, fce, false, &rev, nil); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateActive {
			t.Fatalf("expected active, got %v", tx.states[fcid])
		}
		if r := tx.revisions[fcid]; r.revisionNumber != 1 || r.size != 100 {
			t.Fatalf("unexpected revision: %+v", r)
		}

		// apply storage proof at height 20 -> complete
		if err := s.applyV2ContractUpdate(tx, types.ChainIndex{Height: 20}, fce, false, nil, &types.V2StorageProof{}); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateComplete {
			t.Fatalf("expected complete, got %v", tx.states[fcid])
		}
		if tx.proofHeight[fcid] != 20 {
			t.Fatalf("expected proof height 20, got %v", tx.proofHeight[fcid])
		}

		// revert storage proof -> back to active
		if err := s.revertV2ContractUpdate(tx, fce, false, &types.V2StorageProof{}); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateActive {
			t.Fatalf("expected active, got %v", tx.states[fcid])
		}
		if tx.proofHeight[fcid] != 0 {
			t.Fatalf("expected proof height 0, got %v", tx.proofHeight[fcid])
		}

		// revert creation -> back to pending
		if err := s.revertV2ContractUpdate(tx, fce, true, nil); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStatePending {
			t.Fatalf("expected pending, got %v", tx.states[fcid])
		}
	})

	t.Run("pending to active to failed via expiration", func(t *testing.T) {
		tx := newMockChainUpdateTx()
		tx.addContract(fcid)
		s := newTestSubscriber()

		// apply creation at height 10 -> active
		fce := newFCE()
		if err := s.applyV2ContractUpdate(tx, types.ChainIndex{Height: 10}, fce, true, nil, nil); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateActive {
			t.Fatalf("expected active, got %v", tx.states[fcid])
		}

		// apply expiration at height 30 -> failed
		if err := s.applyV2ContractUpdate(tx, types.ChainIndex{Height: 30}, fce, false, nil, &types.V2FileContractExpiration{}); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateFailed {
			t.Fatalf("expected failed, got %v", tx.states[fcid])
		}
		if tx.proofHeight[fcid] != 30 {
			t.Fatalf("expected proof height 30, got %v", tx.proofHeight[fcid])
		}

		// revert expiration -> back to active
		if err := s.revertV2ContractUpdate(tx, fce, false, &types.V2FileContractExpiration{}); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateActive {
			t.Fatalf("expected active, got %v", tx.states[fcid])
		}
		if tx.proofHeight[fcid] != 0 {
			t.Fatalf("expected proof height 0, got %v", tx.proofHeight[fcid])
		}

		// revert creation -> back to pending
		if err := s.revertV2ContractUpdate(tx, fce, true, nil); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStatePending {
			t.Fatalf("expected pending, got %v", tx.states[fcid])
		}
	})

	t.Run("pending to active to complete via renewal", func(t *testing.T) {
		tx := newMockChainUpdateTx()
		tx.addContract(fcid)
		s := newTestSubscriber()

		// apply creation at height 10 -> active
		fce := newFCE()
		if err := s.applyV2ContractUpdate(tx, types.ChainIndex{Height: 10}, fce, true, nil, nil); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateActive {
			t.Fatalf("expected active, got %v", tx.states[fcid])
		}

		// apply renewal at height 25 -> complete
		if err := s.applyV2ContractUpdate(tx, types.ChainIndex{Height: 25}, fce, false, nil, &types.V2FileContractRenewal{}); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateComplete {
			t.Fatalf("expected complete, got %v", tx.states[fcid])
		}
		if tx.proofHeight[fcid] != 25 {
			t.Fatalf("expected proof height 25, got %v", tx.proofHeight[fcid])
		}
		// verify renewal was recorded
		expectedRenewalID := fcid.V2RenewalID()
		if tx.renewals[fcid] != expectedRenewalID {
			t.Fatalf("expected renewal to %v, got %v", expectedRenewalID, tx.renewals[fcid])
		}

		// revert renewal -> back to active
		if err := s.revertV2ContractUpdate(tx, fce, false, &types.V2FileContractRenewal{}); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateActive {
			t.Fatalf("expected active, got %v", tx.states[fcid])
		}
		if tx.proofHeight[fcid] != 0 {
			t.Fatalf("expected proof height 0, got %v", tx.proofHeight[fcid])
		}

		// revert creation -> back to pending
		if err := s.revertV2ContractUpdate(tx, fce, true, nil); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStatePending {
			t.Fatalf("expected pending, got %v", tx.states[fcid])
		}
	})

	t.Run("revision and storage proof in same block", func(t *testing.T) {
		tx := newMockChainUpdateTx()
		tx.addContract(fcid)
		s := newTestSubscriber()

		// apply creation at height 10 -> active
		fce := newFCE()
		if err := s.applyV2ContractUpdate(tx, types.ChainIndex{Height: 10}, fce, true, nil, nil); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateActive {
			t.Fatalf("expected active, got %v", tx.states[fcid])
		}

		// apply revision and storage proof at the same height (20)
		// first the revision
		rev := types.V2FileContract{RevisionNumber: 2, Filesize: 200}
		if err := s.applyV2ContractUpdate(tx, types.ChainIndex{Height: 20}, fce, false, &rev, &types.V2StorageProof{}); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateComplete {
			t.Fatalf("expected complete, got %v", tx.states[fcid])
		}
		if tx.proofHeight[fcid] != 20 {
			t.Fatalf("expected proof height 20, got %v", tx.proofHeight[fcid])
		}

		// revert storage proof -> back to active
		if err := s.revertV2ContractUpdate(tx, fce, false, &types.V2StorageProof{}); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateActive {
			t.Fatalf("expected active, got %v", tx.states[fcid])
		}
		if tx.proofHeight[fcid] != 0 {
			t.Fatalf("expected proof height 0, got %v", tx.proofHeight[fcid])
		}

		// revert creation -> back to pending
		if err := s.revertV2ContractUpdate(tx, fce, true, nil); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStatePending {
			t.Fatalf("expected pending, got %v", tx.states[fcid])
		}
	})

	t.Run("revision and renewal in same block", func(t *testing.T) {
		tx := newMockChainUpdateTx()
		tx.addContract(fcid)
		s := newTestSubscriber()

		// apply creation at height 10 -> active
		fce := newFCE()
		if err := s.applyV2ContractUpdate(tx, types.ChainIndex{Height: 10}, fce, true, nil, nil); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateActive {
			t.Fatalf("expected active, got %v", tx.states[fcid])
		}

		// apply revision and renewal at the same height (20)
		rev := types.V2FileContract{RevisionNumber: 3, Filesize: 300}
		if err := s.applyV2ContractUpdate(tx, types.ChainIndex{Height: 20}, fce, false, &rev, &types.V2FileContractRenewal{}); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateComplete {
			t.Fatalf("expected complete, got %v", tx.states[fcid])
		}
		if tx.proofHeight[fcid] != 20 {
			t.Fatalf("expected proof height 20, got %v", tx.proofHeight[fcid])
		}
		expectedRenewalID := fcid.V2RenewalID()
		if tx.renewals[fcid] != expectedRenewalID {
			t.Fatalf("expected renewal to %v, got %v", expectedRenewalID, tx.renewals[fcid])
		}

		// revert renewal -> back to active
		if err := s.revertV2ContractUpdate(tx, fce, false, &types.V2FileContractRenewal{}); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStateActive {
			t.Fatalf("expected active, got %v", tx.states[fcid])
		}
		if tx.proofHeight[fcid] != 0 {
			t.Fatalf("expected proof height 0, got %v", tx.proofHeight[fcid])
		}

		// revert creation -> back to pending
		if err := s.revertV2ContractUpdate(tx, fce, true, nil); err != nil {
			t.Fatal(err)
		}
		if tx.states[fcid] != api.ContractStatePending {
			t.Fatalf("expected pending, got %v", tx.states[fcid])
		}
	})
}
