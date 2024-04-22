package node

import (
	"reflect"
	"testing"

	"go.sia.tech/core/types"
)

func TestUnconfirmedParents(t *testing.T) {
	grandparent := types.Transaction{
		SiacoinOutputs: []types.SiacoinOutput{{}},
	}
	parent := types.Transaction{
		SiacoinInputs: []types.SiacoinInput{
			{
				ParentID: grandparent.SiacoinOutputID(0),
			},
		},
		SiacoinOutputs: []types.SiacoinOutput{{}},
	}
	txn := types.Transaction{
		SiacoinInputs: []types.SiacoinInput{
			{
				ParentID: parent.SiacoinOutputID(0),
			},
		},
		SiacoinOutputs: []types.SiacoinOutput{{}},
	}
	pool := []types.Transaction{grandparent, parent}

	parents := unconfirmedParents(txn, pool)
	if len(parents) != 2 {
		t.Fatalf("expected 2 parents, got %v", len(parents))
	} else if !reflect.DeepEqual(parents[0], grandparent) {
		t.Fatalf("expected grandparent")
	} else if !reflect.DeepEqual(parents[1], parent) {
		t.Fatalf("expected parent")
	}
}
