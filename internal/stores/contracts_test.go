package stores

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
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

	// Add an announcement.
	err = insertAnnouncement(cs.db, hk, hostdb.Announcement{NetAddress: "address"})
	if err != nil {
		t.Fatal(err)
	}

	// Create random unlock conditions for the host.
	uc, _ := types.GenerateDeterministicMultisig(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837

	// Create a contract and set all fields.
	fcid := types.FileContractID{1, 1, 1, 1, 1}
	c := rhpv2.ContractRevision{
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
	totalCost := types.NewCurrency64(456)
	startHeight := uint64(100)
	if _, err := cs.AddContract(c, totalCost, startHeight); err != nil {
		t.Fatal(err)
	}

	// Look it up again.
	fetched, err := cs.Contract(c.ID())
	if err != nil {
		t.Fatal(err)
	}
	expected := bus.Contract{
		ID:          fcid,
		HostIP:      "address",
		HostKey:     hk,
		StartHeight: 100,
		ContractMetadata: bus.ContractMetadata{
			RenewedFrom: types.FileContractID{},
			Spending:    bus.ContractSpending{},
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
	uc, _ := types.GenerateDeterministicMultisig(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837

	// Insert a contract.
	fcid := types.FileContractID{1, 1, 1, 1, 1}
	c := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid,
			UnlockConditions: uc,
		},
	}
	totalCost := types.NewCurrency64(654)
	startHeight := uint64(100)
	if _, err := cs.AddContract(c, totalCost, startHeight); err != nil {
		t.Fatal(err)
	}

	// Lock it.
	acquired, err := cs.AcquireContract(fcid, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("contract wasn't locked")
	}

	// Lock again. Shouldn't work.
	acquired, err = cs.AcquireContract(fcid, time.Minute)
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
	acquired, err = cs.AcquireContract(fcid, time.Minute)
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

	// Add an announcement.
	err = insertAnnouncement(cs.db, hk, hostdb.Announcement{NetAddress: "address"})
	if err != nil {
		t.Fatal(err)
	}

	// Create random unlock conditions for the host.
	uc, _ := types.GenerateDeterministicMultisig(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837

	// Insert a contract.
	fcid := types.FileContractID{1, 1, 1, 1, 1}
	c := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			NewFileSize:       1,
			NewWindowStart:    2,
			NewWindowEnd:      3,
			NewRevisionNumber: 4,
			ParentID:          fcid,
			UnlockConditions:  uc,
		},
	}
	oldContractTotal := types.NewCurrency64(111)
	oldContractStartHeight := uint64(100)
	added, err := cs.AddContract(c, oldContractTotal, oldContractStartHeight)
	if err != nil {
		t.Fatal(err)
	}

	// Assert the contract is returned.
	if added.RenewedFrom != (types.FileContractID{}) {
		t.Fatal("unexpected")
	}

	// Renew it.
	fcid2 := types.FileContractID{2, 2, 2, 2, 2}
	renewed := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:              fcid2,
			UnlockConditions:      uc,
			NewMissedProofOutputs: []types.SiacoinOutput{},
			NewValidProofOutputs:  []types.SiacoinOutput{},
		},
	}
	newContractTotal := types.NewCurrency64(222)
	newContractStartHeight := uint64(200)
	if _, err := cs.AddRenewedContract(renewed, newContractTotal, newContractStartHeight, fcid); err != nil {
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
	expected := bus.Contract{
		ID:          fcid2,
		HostIP:      "address",
		HostKey:     hk,
		StartHeight: newContractStartHeight,
		ContractMetadata: bus.ContractMetadata{
			RenewedFrom: fcid,
			Spending:    bus.ContractSpending{},
			TotalCost:   newContractTotal,
		},
	}
	if !reflect.DeepEqual(newContract, expected) {
		t.Fatal("mismatch")
	}

	// Archived contract should exist.
	var ac dbArchivedContract
	err = cs.db.Model(&dbArchivedContract{}).
		Where("fcid", gobEncode(fcid)).
		Take(&ac).
		Error
	if err != nil {
		t.Fatal(err)
	}

	ac.Model = Model{}
	expectedContract := dbArchivedContract{
		FCID:      fcid,
		Host:      c.HostKey(),
		RenewedTo: fcid2,
		Reason:    archivalReasonRenewed,
	}
	if !reflect.DeepEqual(ac, expectedContract) {
		fmt.Println(ac)
		fmt.Println(expectedContract)
		t.Fatal("mismatch")
	}

	// Renew it once more.
	fcid3 := types.FileContractID{3, 3, 3, 3, 3}
	renewed = rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:              fcid3,
			UnlockConditions:      uc,
			NewMissedProofOutputs: []types.SiacoinOutput{},
			NewValidProofOutputs:  []types.SiacoinOutput{},
		},
	}
	newContractTotal = types.NewCurrency64(333)
	newContractStartHeight = uint64(300)

	// Assert the renewed contract is returned
	renewedContract, err := cs.AddRenewedContract(renewed, newContractTotal, newContractStartHeight, fcid2)
	if err != nil {
		t.Fatal(err)
	}
	if renewedContract.RenewedFrom != fcid2 {
		t.Fatal("unexpected")
	}
}

func gobEncode(fcid types.FileContractID) []byte {
	buf := bytes.NewBuffer(nil)
	if err := gob.NewEncoder(buf).Encode(fcid); err != nil {
		panic(err)
	}
	return buf.Bytes()
}
