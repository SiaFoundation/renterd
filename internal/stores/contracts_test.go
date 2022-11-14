package stores

import (
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"testing"

	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

// TestSQLContractStore tests SQLContractStore functionality.
func TestSQLContractStore(t *testing.T) {
	dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db

	conn := NewEphemeralSQLiteConnection(dbName)
	cs, err := NewSQLContractStore(conn, true)
	if err != nil {
		t.Fatal(err)
	}

	// Create random unlock conditions.
	uc, _ := types.GenerateDeterministicMultisig(1, 2, "salt")
	uc.Timelock = 192837

	// Create a contract and set all fields.
	fcid := types.FileContractID{1, 1, 1, 1, 1}
	c := rhpv2.Contract{
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

	// Look it up. Should fail.
	_, err = cs.Contract(c.ID())
	if !errors.Is(err, ErrContractNotFound) {
		t.Fatal(err)
	}

	// Insert it.
	if err := cs.AddContract(c); err != nil {
		t.Fatal(err)
	}

	// Look it up again.
	fetched, err := cs.Contract(c.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fetched, c) {
		fmt.Println(fetched.Revision)
		fmt.Println(c.Revision)
		t.Fatal("contract mismatch")
	}

	// TODO: delete it
}
