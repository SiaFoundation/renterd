package stores

import (
	"strings"
	"testing"

	"go.sia.tech/core/types"
)

func TestTypeMerkleProof(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	var proofs []merkleProof
	if err := ss.db.
		Raw(`WITH input(merkle_proof) as (values (?)) SELECT merkle_proof FROM input`, merkleProof([]types.Hash256{})).
		Scan(&proofs).
		Error; err == nil || !strings.Contains(err.Error(), "no bytes found") {
		t.Fatalf("expected error 'no bytes found', got '%v'", err)
	}

	if err := ss.db.
		Raw(`WITH input(merkle_proof) as (values (?)) SELECT merkle_proof FROM input`, merkleProof([]types.Hash256{{2}, {1}, {3}})).
		Scan(&proofs).
		Error; err != nil {
		t.Fatal("unexpected err", err)
	} else if len(proofs) != 1 {
		t.Fatal("expected 1 proof")
	} else if len(proofs[0]) != 3 {
		t.Fatalf("expected 3 hashes, got %v", len(proofs[0]))
	} else if proofs[0][0] != (types.Hash256{2}) || proofs[0][1] != (types.Hash256{1}) || proofs[0][2] != (types.Hash256{3}) {
		t.Fatalf("unexpected proof %+v", proofs[0])
	}
}
