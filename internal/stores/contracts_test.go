package stores

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/types"
	"go.sia.tech/siad/crypto"
	siatypes "go.sia.tech/siad/types"
	"gorm.io/gorm/schema"
)

// TestSQLContractStore tests SQLContractStore functionality.
func TestSQLContractStore(t *testing.T) {
	cs, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Create a host for the contract.
	hk := consensus.GeneratePrivateKey().PublicKey()
	err = cs.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// Create random unlock conditions for the host.
	uc, _ := siatypes.GenerateDeterministicMultisig(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837

	// Create a contract and set all fields.
	fcid := siatypes.FileContractID{1, 1, 1, 1, 1}
	c := rhpv2.Contract{
		Revision: siatypes.FileContractRevision{
			ParentID:          fcid,
			UnlockConditions:  uc,
			NewRevisionNumber: 200,
			NewFileSize:       4096,
			NewFileMerkleRoot: crypto.Hash{222},
			NewWindowStart:    400,
			NewWindowEnd:      500,
			NewValidProofOutputs: []siatypes.SiacoinOutput{
				{
					Value:      siatypes.NewCurrency64(121),
					UnlockHash: siatypes.UnlockHash{2, 1, 2},
				},
			},
			NewMissedProofOutputs: []siatypes.SiacoinOutput{
				{
					Value:      siatypes.NewCurrency64(323),
					UnlockHash: siatypes.UnlockHash{2, 3, 2},
				},
			},
			NewUnlockHash: siatypes.UnlockHash{6, 6, 6},
		},
		Signatures: [2]siatypes.TransactionSignature{
			{
				ParentID:       crypto.Hash(fcid),
				PublicKeyIndex: 0,
				Timelock:       100000,
				CoveredFields:  siatypes.FullCoveredFields,
				Signature:      []byte("signature1"),
			},
			{
				ParentID:       crypto.Hash(fcid),
				PublicKeyIndex: 1,
				Timelock:       200000,
				CoveredFields:  siatypes.FullCoveredFields,
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
	totalCost := siatypes.NewCurrency64(456)
	if err := cs.AddContract(c, totalCost); err != nil {
		t.Fatal(err)
	}

	// Look it up again.
	fetched, err := cs.Contract(c.ID())
	if err != nil {
		t.Fatal(err)
	}
	expected := types.Contract{
		HostIP:      "",
		StartHeight: 0,
		Revision:    c.Revision,
		Signatures:  c.Signatures,
		ContractMetadata: types.ContractMetadata{
			RenewedFrom: siatypes.FileContractID{},
			Spending:    types.ContractSpending{},
			TotalCost:   totalCost,
		},
	}
	if !reflect.DeepEqual(fetched, expected) {
		t.Fatal("contract mismatch")
	}
	contracts, err = cs.Contracts()
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 1 {
		t.Fatalf("should have 1 contracts but got %v", len(contracts))
	}
	if !reflect.DeepEqual(contracts[0], expected) {
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
	if err := tableCountCheck(&dbContract{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tableCountCheck(&dbFileContractRevision{}, 0); err != nil {
		t.Fatal(err)
	}
	if err := tableCountCheck(&dbValidSiacoinOutput{}, 0); err != nil {
		t.Fatal(err)
	}

	// Check join table count as well.
	var count int64
	if err := cs.db.Table("contract_sectors").Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected %v objects in contract_sectors but got %v", 0, count)
	}
}

// TestContractLocking is a test for verifying AcquireContract and
// ReleaseContract.
func TestContractLocking(t *testing.T) {
	cs, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Create a host for the contract.
	hk := consensus.GeneratePrivateKey().PublicKey()
	err = cs.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// Create random unlock conditions for the host.
	uc, _ := siatypes.GenerateDeterministicMultisig(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837

	// Insert a contract.
	fcid := siatypes.FileContractID{1, 1, 1, 1, 1}
	c := rhpv2.Contract{
		Revision: siatypes.FileContractRevision{
			ParentID:         fcid,
			UnlockConditions: uc,
		},
	}
	totalCost := siatypes.NewCurrency64(654)
	if err := cs.AddContract(c, totalCost); err != nil {
		t.Fatal(err)
	}

	// Lock it.
	rev, acquired, err := cs.AcquireContract(fcid, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("contract wasn't locked")
	}
	if rev.ParentID != fcid {
		t.Fatalf("wrong parent id %v != %v", rev.ParentID, fcid)
	}

	// Lock again. Shouldn't work.
	_, acquired, err = cs.AcquireContract(fcid, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if acquired {
		t.Fatal("shouldn't be able to acquire")
	}

	// Release.
	if err := cs.ReleaseContract(fcid); err != nil {
		t.Fatal(err)
	}

	// Acquire again.
	_, acquired, err = cs.AcquireContract(fcid, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("contract wasn't locked")
	}
}

// TestRenewContract is a test for AddRenewedContract.
func TestRenewedContract(t *testing.T) {
	cs, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Create a host for the contract.
	hk := consensus.GeneratePrivateKey().PublicKey()
	err = cs.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// Create random unlock conditions for the host.
	uc, _ := siatypes.GenerateDeterministicMultisig(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837

	// Insert a contract.
	fcid := siatypes.FileContractID{1, 1, 1, 1, 1}
	c := rhpv2.Contract{
		Revision: siatypes.FileContractRevision{
			NewFileSize:       1,
			NewWindowStart:    2,
			NewWindowEnd:      3,
			NewRevisionNumber: 4,
			ParentID:          fcid,
			UnlockConditions:  uc,
		},
	}
	oldContractTotal := siatypes.NewCurrency64(111)
	if err := cs.AddContract(c, oldContractTotal); err != nil {
		t.Fatal(err)
	}

	// Renew it.
	fcid2 := siatypes.FileContractID{2, 2, 2, 2, 2}
	renewed := rhpv2.Contract{
		Revision: siatypes.FileContractRevision{
			ParentID:              fcid2,
			UnlockConditions:      uc,
			NewMissedProofOutputs: []siatypes.SiacoinOutput{},
			NewValidProofOutputs:  []siatypes.SiacoinOutput{},
		},
	}
	newContractTotal := siatypes.NewCurrency64(222)
	if err := cs.AddRenewedContract(renewed, newContractTotal, fcid); err != nil {
		t.Fatal(err)
	}

	// Contract should be gone from active contracts.
	_, err = cs.Contract(fcid)
	if !errors.Is(err, ErrContractNotFound) {
		t.Fatal(err)
	}

	// New contract should exist.
	newContract, err := cs.Contract(fcid2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(newContract, renewed) {
		t.Fatal("mismatch")
	}

	// Archived contract should exist.
	var ac dbArchivedContract
	err = cs.db.Model(&dbArchivedContract{}).
		Where("fcid", fileContractID(fcid).gob()).
		Take(&ac).
		Error
	if err != nil {
		t.Fatal(err)
	}

	ac.Model = Model{}
	expectedContract := dbArchivedContract{
		FCID:           fcid,
		FileSize:       c.Revision.NewFileSize,
		Host:           c.HostKey(),
		RenewedTo:      fcid2,
		Reason:         archivalReasonRenewed,
		RevisionNumber: c.Revision.NewRevisionNumber,
		WindowStart:    c.Revision.NewWindowStart,
		WindowEnd:      c.Revision.NewWindowEnd,
	}
	if !reflect.DeepEqual(ac, expectedContract) {
		fmt.Println(ac)
		fmt.Println(expectedContract)
		t.Fatal("mismatch")
	}
}
