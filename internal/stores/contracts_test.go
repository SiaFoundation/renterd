package stores

import (
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
	"gorm.io/gorm/schema"
	"lukechampine.com/frand"
)

// newTestSQLStore creates a new SQLContractStore for testing.
func newTestSQLStore() (*SQLContractStore, error) {
	dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db
	conn := NewEphemeralSQLiteConnection(dbName)
	return NewSQLContractStore(conn, true)
}

// TestSQLContractStore tests SQLContractStore functionality.
func TestSQLContractStore(t *testing.T) {
	cs, err := newTestSQLStore()
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
	contracts, err := cs.Contracts()
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 0 {
		t.Fatalf("should have 0 contracts but got %v", len(contracts))
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
		t.Fatal("contract mismatch")
	}
	contracts, err = cs.Contracts()
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 1 {
		t.Fatalf("should have 1 contracts but got %v", len(contracts))
	}
	if !reflect.DeepEqual(contracts[0], c) {
		t.Fatal("contract mismatch")
	}

	// Delete the contract.
	if err := cs.RemoveContract(c.ID()); err != nil {
		t.Fatal(err)
	}

	// Look it up. Should fail.
	_, err = cs.Contract(c.ID())
	if !errors.Is(err, ErrContractNotFound) {
		t.Fatal(err)
	}
	contracts, err = cs.Contracts()
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 0 {
		t.Fatalf("should have 0 contracts but got %v", len(contracts))
	}

	// Make sure the db was cleaned up properly through the CASCADE delete.
	tableCountCheck := func(table interface{}, tblCount int64) error {
		var count int64
		if err := cs.db.Model(table).Count(&count).Error; err != nil {
			return err
		}
		if count != tblCount {
			return fmt.Errorf("expected %v objects in table %v but got %v", tblCount, table.(schema.Tabler).TableName(), count)
		}
		return nil
	}
	if err := tableCountCheck(&dbContractRHPv2{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tableCountCheck(&dbFileContractRevision{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tableCountCheck(&dbValidSiacoinOutput{}, 0); err != nil {
		t.Fatal(err)
	}
}

// TestSQLHostSetStore tests the bus.HostSetStore methods on the SQLContractStore.
func TestSQLHostSetStore(t *testing.T) {
	cs, err := newTestSQLStore()
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
