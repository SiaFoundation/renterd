package stores

import (
	"errors"
	"reflect"
	"testing"

	"go.sia.tech/siad/types"
)

// TestSQLContractSetStore tests the bus.ContractSetStore methods on the SQLContractStore.
func TestSQLContractSetStore(t *testing.T) {
	cs, _, _, err := newTestSQLStore()
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
	contracts := []types.FileContractID{{1, 2, 3}, {3, 2, 1}}
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
	if !reflect.DeepEqual(set, contracts) {
		t.Fatal("set mismatch")
	}

	// Remove a contract.
	contracts = []types.FileContractID{{1, 2, 3}}
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
	if !reflect.DeepEqual(set, contracts) {
		t.Fatal("set mismatch", set, contracts)
	}
}
