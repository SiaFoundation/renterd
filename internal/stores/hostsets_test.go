package stores

import (
	"errors"
	"reflect"
	"testing"

	"go.sia.tech/renterd/internal/consensus"
)

// TestSQLHostSetStore tests the bus.HostSetStore methods on the SQLContractStore.
func TestSQLHostSetStore(t *testing.T) {
	cs, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Check db before adding a set.
	setName := "foo"
	sets, err := cs.HostSets()
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 0 {
		t.Fatalf("expected 0 sets got %v", len(sets))
	}
	_, err = cs.HostSet(setName)
	if !errors.Is(err, ErrHostSetNotFound) {
		t.Fatal("should fail", err)
	}

	// Add another one.
	pks := []consensus.PublicKey{{1, 2, 3}, {3, 2, 1}}
	if err := cs.SetHostSet(setName, pks); err != nil {
		t.Fatal(err)
	}

	// Check again.
	sets, err = cs.HostSets()
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 1 {
		t.Fatalf("expected 0 sets got %v", len(sets))
	}
	set, err := cs.HostSet(setName)
	if err != nil {
		t.Fatal("should fail", err)
	}
	if !reflect.DeepEqual(set, pks) {
		t.Fatal("set mismatch")
	}
}
