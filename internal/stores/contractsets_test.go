package stores

import (
	"errors"
	"testing"

	bus "go.sia.tech/renterd/api/bus"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
)

func testContractRevision(fcid types.FileContractID, hk consensus.PublicKey) rhpv2.ContractRevision {
	uc, _ := types.GenerateDeterministicMultisig(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837
	return rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:          fcid,
			UnlockConditions:  uc,
			NewRevisionNumber: 200,
			NewFileSize:       4096,
			NewFileMerkleRoot: crypto.Hash{222},
			NewWindowStart:    400,
			NewWindowEnd:      500,
			NewValidProofOutputs: []types.SiacoinOutput{
				{
					Value:      types.NewCurrency64(121),
					UnlockHash: types.UnlockHash{2, 1, 2},
				},
			},
			NewMissedProofOutputs: []types.SiacoinOutput{
				{
					Value:      types.NewCurrency64(323),
					UnlockHash: types.UnlockHash{2, 3, 2},
				},
			},
			NewUnlockHash: types.UnlockHash{6, 6, 6},
		},
		Signatures: [2]types.TransactionSignature{
			{
				ParentID:       crypto.Hash(fcid),
				PublicKeyIndex: 0,
				Timelock:       100000,
				CoveredFields:  types.FullCoveredFields,
				Signature:      []byte("signature1"),
			},
			{
				ParentID:       crypto.Hash(fcid),
				PublicKeyIndex: 1,
				Timelock:       200000,
				CoveredFields:  types.FullCoveredFields,
				Signature:      []byte("signature2"),
			},
		},
	}
}

func (s *SQLStore) addTestContract(fcid types.FileContractID, hk consensus.PublicKey) (bus.Contract, error) {
	rev := testContractRevision(fcid, hk)
	return s.AddContract(rev, types.ZeroCurrency, 0)
}

func (s *SQLStore) addTestRenewedContract(fcid, renewedFrom types.FileContractID, hk consensus.PublicKey, startHeight uint64) (bus.Contract, error) {
	rev := testContractRevision(fcid, hk)
	return s.AddRenewedContract(rev, types.ZeroCurrency, startHeight, renewedFrom)
}

// TestSQLContractSetStore tests the ContractSetStore methods on the SQLContractStore.
func TestSQLContractSetStore(t *testing.T) {
	cs, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Add a host.
	hk1 := consensus.GeneratePrivateKey().PublicKey()
	err = cs.addTestHost(hk1)
	if err != nil {
		t.Fatal(err)
	}

	// Add 2 contracts with that host.
	c1, err := cs.addTestContract(types.FileContractID{1, 2, 3}, hk1)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := cs.addTestContract(types.FileContractID{3, 2, 1}, hk1)
	if err != nil {
		t.Fatal(err)
	}

	// Check db before adding a set.
	setName := "foo"
	sets, err := cs.ContractSets()
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 0 {
		t.Fatalf("expected 0 sets got %v", len(sets))
	}
	_, err = cs.ContractSet(setName)
	if !errors.Is(err, ErrContractSetNotFound) {
		t.Fatal("should fail", err)
	}

	// Add a set.
	contracts := []types.FileContractID{c1.ID, c2.ID}
	if err := cs.SetContractSet(setName, contracts); err != nil {
		t.Fatal(err)
	}

	// Check again.
	sets, err = cs.ContractSets()
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 1 {
		t.Fatalf("expected 0 sets got %v", len(sets))
	}
	set, err := cs.ContractSet(setName)
	if err != nil {
		t.Fatal("should fail", err)
	}
	if len(set) != 2 {
		t.Fatal("wrong set len", len(set))
	}
	if set[0].ID != c1.ID {
		t.Fatal("wrong contract")
	}
	if set[1].ID != c2.ID {
		t.Fatal("wrong contract")
	}

	// Remove a contract.
	contracts = []types.FileContractID{c1.ID}
	if err := cs.SetContractSet(setName, contracts); err != nil {
		t.Fatal(err)
	}

	// Check again.
	sets, err = cs.ContractSets()
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 1 {
		t.Fatalf("expected 0 sets got %v", len(sets))
	}
	set, err = cs.ContractSet(setName)
	if err != nil {
		t.Fatal("should fail", err)
	}
	if len(set) != 1 {
		t.Fatal("wrong set len", len(set))
	}
	if set[0].ID != c1.ID {
		t.Fatal("wrong contract")
	}
}
