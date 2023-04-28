package stores

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/object"
	"gorm.io/gorm/schema"
	"lukechampine.com/frand"
)

func generateMultisigUC(m, n uint64, salt string) types.UnlockConditions {
	uc := types.UnlockConditions{
		PublicKeys:         make([]types.UnlockKey, n),
		SignaturesRequired: uint64(m),
	}
	for i := range uc.PublicKeys {
		uc.PublicKeys[i].Algorithm = types.SpecifierEd25519
		uc.PublicKeys[i].Key = frand.Bytes(32)
	}
	return uc
}

// TestSQLContractStore tests SQLContractStore functionality.
func TestSQLContractStore(t *testing.T) {
	cs, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Create a host for the contract.
	hk := types.GeneratePrivateKey().PublicKey()
	err = cs.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// Add an announcement.
	err = cs.insertTestAnnouncement(hk, hostdb.Announcement{NetAddress: "address"})
	if err != nil {
		t.Fatal(err)
	}

	// Create random unlock conditions for the host.
	uc := generateMultisigUC(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837

	// Create a contract and set all fields.
	fcid := types.FileContractID{1, 1, 1, 1, 1}
	c := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				RevisionNumber: 200,
				Filesize:       4096,
				FileMerkleRoot: types.Hash256{222},
				WindowStart:    400,
				WindowEnd:      500,
				ValidProofOutputs: []types.SiacoinOutput{
					{
						Value:   types.NewCurrency64(121),
						Address: types.Address{2, 1, 2},
					},
				},
				MissedProofOutputs: []types.SiacoinOutput{
					{
						Value:   types.NewCurrency64(323),
						Address: types.Address{2, 3, 2},
					},
				},
				UnlockHash: types.Hash256{6, 6, 6},
			},
		},
		Signatures: [2]types.TransactionSignature{
			{
				ParentID:       types.Hash256(fcid),
				PublicKeyIndex: 0,
				Timelock:       100000,
				CoveredFields:  types.CoveredFields{WholeTransaction: true},
				Signature:      []byte("signature1"),
			},
			{
				ParentID:       types.Hash256(fcid),
				PublicKeyIndex: 1,
				Timelock:       200000,
				CoveredFields:  types.CoveredFields{WholeTransaction: true},
				Signature:      []byte("signature2"),
			},
		},
	}

	// Look it up. Should fail.
	ctx := context.Background()
	_, err = cs.Contract(ctx, c.ID())
	if !errors.Is(err, ErrContractNotFound) {
		t.Fatal(err)
	}
	contracts, err := cs.ActiveContracts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 0 {
		t.Fatalf("should have 0 contracts but got %v", len(contracts))
	}

	// Insert it.
	totalCost := types.NewCurrency64(456)
	startHeight := uint64(100)
	returned, err := cs.AddContract(ctx, c, totalCost, startHeight)
	if err != nil {
		t.Fatal(err)
	}
	expected := api.ContractMetadata{
		ID:          fcid,
		HostIP:      "address",
		HostKey:     hk,
		StartHeight: 100,
		WindowStart: 400,
		WindowEnd:   500,
		RenewedFrom: types.FileContractID{},
		Spending: api.ContractSpending{
			Uploads:     types.ZeroCurrency,
			Downloads:   types.ZeroCurrency,
			FundAccount: types.ZeroCurrency,
		},
		TotalCost: totalCost,
	}
	if !reflect.DeepEqual(returned, expected) {
		t.Fatal("contract mismatch")
	}

	// Look it up again.
	fetched, err := cs.Contract(ctx, c.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fetched, expected) {
		t.Fatal("contract mismatch")
	}
	contracts, err = cs.ActiveContracts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 1 {
		t.Fatalf("should have 1 contracts but got %v", len(contracts))
	}
	if !reflect.DeepEqual(contracts[0], expected) {
		t.Fatal("contract mismatch")
	}

	// Add a contract set with our contract and assert we can fetch it using the set name
	if err := cs.SetContractSet(ctx, "foo", []types.FileContractID{contracts[0].ID}); err != nil {
		t.Fatal(err)
	}
	if contracts, err := cs.Contracts(ctx, "foo"); err != nil {
		t.Fatal(err)
	} else if len(contracts) != 1 {
		t.Fatalf("should have 1 contracts but got %v", len(contracts))
	}
	if _, err := cs.Contracts(ctx, "bar"); !errors.Is(err, api.ErrContractSetNotFound) {
		t.Fatal(err)
	}

	// Add another contract set.
	if err := cs.SetContractSet(ctx, "foo2", []types.FileContractID{contracts[0].ID}); err != nil {
		t.Fatal(err)
	}

	// Fetch contract sets.
	sets, err := cs.ContractSets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 2 {
		t.Fatal("wrong number of sets")
	}
	if sets[0] != "foo" || sets[1] != "foo2" {
		t.Fatal("wrong sets returned", sets)
	}

	// Delete the contract.
	if err := cs.ArchiveContract(ctx, c.ID(), api.ContractArchivalReasonRemoved); err != nil {
		t.Fatal(err)
	}

	// Look it up. Should fail.
	_, err = cs.Contract(ctx, c.ID())
	if !errors.Is(err, ErrContractNotFound) {
		t.Fatal(err)
	}
	contracts, err = cs.ActiveContracts(ctx)
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

func TestContractsForHost(t *testing.T) {
	// create a SQL store
	cs, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// add 2 hosts
	hks, err := cs.addTestHosts(2)
	if err != nil {
		t.Fatal(err)
	}

	// add 2 contracts
	_, _, err = cs.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}

	// fetch raw hosts
	var hosts []dbHost
	if err := cs.db.
		Model(&dbHost{}).
		Find(&hosts).
		Error; err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 2 {
		t.Fatal("unexpected number of hosts")
	}

	contracts, _ := contractsForHost(cs.db, hosts[0])
	if len(contracts) != 1 || contracts[0].Host.convert().PublicKey.String() != hosts[0].convert().PublicKey.String() {
		t.Fatal("unexpected", len(contracts), contracts)
	}

	contracts, _ = contractsForHost(cs.db, hosts[1])
	if len(contracts) != 1 || contracts[0].Host.convert().PublicKey.String() != hosts[1].convert().PublicKey.String() {
		t.Fatalf("unexpected contracts, %+v", contracts)
	}
}

// TestRenewContract is a test for AddRenewedContract.
func TestRenewedContract(t *testing.T) {
	cs, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Create a host for the contract and another one for redundancy.
	hks, err := cs.addTestHosts(2)
	if err != nil {
		t.Fatal(err)
	}
	hk, hk2 := hks[0], hks[1]

	// Add announcements.
	err = cs.insertTestAnnouncement(hk, hostdb.Announcement{NetAddress: "address"})
	if err != nil {
		t.Fatal(err)
	}
	err = cs.insertTestAnnouncement(hk2, hostdb.Announcement{NetAddress: "address2"})
	if err != nil {
		t.Fatal(err)
	}

	// Create random unlock conditions for the hosts.
	uc := generateMultisigUC(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837

	uc2 := generateMultisigUC(1, 2, "salt")
	uc2.PublicKeys[1].Key = hk2[:]
	uc2.Timelock = 192837

	// Insert the contracts.
	fcid1 := types.FileContractID{1, 1, 1, 1, 1}
	c := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid1,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				Filesize:       1,
				WindowStart:    2,
				WindowEnd:      3,
				RevisionNumber: 4,
			},
		},
	}
	oldContractTotal := types.NewCurrency64(111)
	oldContractStartHeight := uint64(100)
	ctx := context.Background()
	added, err := cs.AddContract(ctx, c, oldContractTotal, oldContractStartHeight)
	if err != nil {
		t.Fatal(err)
	}

	// Assert the contract is returned.
	if added.RenewedFrom != (types.FileContractID{}) {
		t.Fatal("unexpected")
	}

	fcid2 := types.FileContractID{9, 9, 9, 9, 9}
	c2 := c
	c2.Revision.ParentID = fcid2
	c2.Revision.UnlockConditions = uc2
	_, err = cs.AddContract(ctx, c2, oldContractTotal, oldContractStartHeight)
	if err != nil {
		t.Fatal(err)
	}

	// add an object for that contract.
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			// good slab
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: hk,
							Root: types.Hash256{1},
						},
						{
							Host: hk2,
							Root: types.Hash256{2},
						},
					},
				},
			},
		},
	}

	// create a contract set with both contracts.
	if err := cs.SetContractSet(context.Background(), "test", []types.FileContractID{fcid1, fcid2}); err != nil {
		t.Fatal(err)
	}

	// add the object.
	if err := cs.UpdateObject(context.Background(), "foo", obj, map[types.PublicKey]types.FileContractID{
		hk:  fcid1,
		hk2: fcid2,
	}); err != nil {
		t.Fatal(err)
	}

	// no slabs should be unhealthy.
	slabs, err := cs.UnhealthySlabs(context.Background(), 0.99, "test", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) > 0 {
		t.Fatal("shouldn't return any slabs", len(slabs))
	}

	// Renew it.
	fcid1Renewed := types.FileContractID{2, 2, 2, 2, 2}
	renewed := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid1Renewed,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				MissedProofOutputs: []types.SiacoinOutput{},
				ValidProofOutputs:  []types.SiacoinOutput{},
			},
		},
	}
	newContractTotal := types.NewCurrency64(222)
	newContractStartHeight := uint64(200)
	if _, err := cs.AddRenewedContract(ctx, renewed, newContractTotal, newContractStartHeight, fcid1); err != nil {
		t.Fatal(err)
	}

	// make sure the contract set was updated.
	setContracts, err := cs.Contracts(context.Background(), "test")
	if err != nil {
		t.Fatal(err)
	}
	if len(setContracts) != 2 || (setContracts[0].ID != fcid1Renewed && setContracts[1].ID != fcid1Renewed) {
		t.Fatal("contract set wasn't updated", setContracts)
	}

	// slab should still be in good shape.
	slabs, err = cs.UnhealthySlabs(context.Background(), 0.99, "test", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) > 0 {
		t.Fatal("shouldn't return any slabs", len(slabs))
	}

	// Contract should be gone from active contracts.
	_, err = cs.Contract(ctx, fcid1)
	if !errors.Is(err, ErrContractNotFound) {
		t.Fatal(err)
	}

	// New contract should exist.
	newContract, err := cs.Contract(ctx, fcid1Renewed)
	if err != nil {
		t.Fatal(err)
	}
	expected := api.ContractMetadata{
		ID:          fcid1Renewed,
		HostIP:      "address",
		HostKey:     hk,
		StartHeight: newContractStartHeight,
		RenewedFrom: fcid1,
		Spending: api.ContractSpending{
			Uploads:     types.ZeroCurrency,
			Downloads:   types.ZeroCurrency,
			FundAccount: types.ZeroCurrency,
		},
		TotalCost: newContractTotal,
	}
	if !reflect.DeepEqual(newContract, expected) {
		t.Fatal("mismatch")
	}

	// Archived contract should exist.
	var ac dbArchivedContract
	err = cs.db.Model(&dbArchivedContract{}).
		Where("fcid", fileContractID(fcid1)).
		Take(&ac).
		Error
	if err != nil {
		t.Fatal(err)
	}

	ac.Model = Model{}
	expectedContract := dbArchivedContract{
		Host:      publicKey(c.HostKey()),
		RenewedTo: fileContractID(fcid1Renewed),
		Reason:    api.ContractArchivalReasonRenewed,

		ContractCommon: ContractCommon{
			FCID: fileContractID(fcid1),

			TotalCost:      currency(oldContractTotal),
			ProofHeight:    0,
			RevisionHeight: 0,
			RevisionNumber: "0",
			StartHeight:    100,
			WindowStart:    2,
			WindowEnd:      3,

			UploadSpending:      zeroCurrency,
			DownloadSpending:    zeroCurrency,
			FundAccountSpending: zeroCurrency,
		},
	}
	if !reflect.DeepEqual(ac, expectedContract) {
		t.Fatal("mismatch", cmp.Diff(ac, expectedContract))
	}

	// Renew it once more.
	fcid3 := types.FileContractID{3, 3, 3, 3, 3}
	renewed = rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid3,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				MissedProofOutputs: []types.SiacoinOutput{},
				ValidProofOutputs:  []types.SiacoinOutput{},
			},
		},
	}
	newContractTotal = types.NewCurrency64(333)
	newContractStartHeight = uint64(300)

	// Assert the renewed contract is returned
	renewedContract, err := cs.AddRenewedContract(ctx, renewed, newContractTotal, newContractStartHeight, fcid1Renewed)
	if err != nil {
		t.Fatal(err)
	}
	if renewedContract.RenewedFrom != fcid1Renewed {
		t.Fatal("unexpected")
	}
}

// TestAncestorsContracts verifies that AncestorContracts returns the right
// ancestors in the correct order.
func TestAncestorsContracts(t *testing.T) {
	cs, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	hk := types.PublicKey{1, 2, 3}
	if err := cs.addTestHost(hk); err != nil {
		t.Fatal(err)
	}

	// Create a chain of 4 contracts.
	// Their start heights are 0, 1, 2, 3.
	fcids := []types.FileContractID{{1}, {2}, {3}, {4}}
	if _, err := cs.addTestContract(fcids[0], hk); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(fcids); i++ {
		if _, err := cs.addTestRenewedContract(fcids[i], fcids[i-1], hk, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}

	// Fetch the ancestors but only the ones with a startHeight >= 1. That
	// should return 2 contracts. The active one with height 3 isn't
	// returned and the one with height 0 is also not returned.
	contracts, err := cs.AncestorContracts(context.Background(), fcids[len(fcids)-1], 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != len(fcids)-2 {
		t.Fatal("wrong number of contracts returned", len(contracts))
	}
	for i := 0; i < len(contracts)-1; i++ {
		if !reflect.DeepEqual(contracts[i], api.ArchivedContract{
			ID:          fcids[len(fcids)-2-i],
			HostKey:     hk,
			RenewedTo:   fcids[len(fcids)-1-i],
			StartHeight: 2,
			WindowStart: 400,
			WindowEnd:   500,
		}) {
			t.Fatal("wrong contract", i)
		}
	}
}

func TestArchiveContracts(t *testing.T) {
	cs, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// add 3 hosts
	hks, err := cs.addTestHosts(3)
	if err != nil {
		t.Fatal(err)
	}

	// add 3 contracts
	fcids, _, err := cs.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}

	// archive 2 of them
	toArchive := map[types.FileContractID]string{
		fcids[1]: "foo",
		fcids[2]: "bar",
	}
	if err := cs.ArchiveContracts(context.Background(), toArchive); err != nil {
		t.Fatal(err)
	}

	// assert the first one is still active
	active, err := cs.ActiveContracts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 || active[0].ID != fcids[0] {
		t.Fatal("wrong active contracts", active)
	}

	// assert the two others were archived
	ffcids := make([]fileContractID, 2)
	ffcids[0] = fileContractID(fcids[1])
	ffcids[1] = fileContractID(fcids[2])
	var acs []dbArchivedContract
	err = cs.db.Model(&dbArchivedContract{}).
		Where("fcid IN (?)", ffcids).
		Find(&acs).
		Error
	if err != nil {
		t.Fatal(err)
	}
	if len(acs) != 2 {
		t.Fatal("wrong number of archived contracts", len(acs))
	}
	if acs[0].Reason != "foo" || acs[1].Reason != "bar" {
		t.Fatal("unexpected reason", acs[0].Reason, acs[1].Reason)
	}
}

func (s *SQLStore) addTestContracts(keys []types.PublicKey) (fcids []types.FileContractID, contracts []api.ContractMetadata, err error) {
	cnt, err := s.contractsCount()
	if err != nil {
		return nil, nil, err
	}
	for i, key := range keys {
		fcids = append(fcids, types.FileContractID{byte(int(cnt) + i + 1)})
		contract, err := s.addTestContract(fcids[len(fcids)-1], key)
		if err != nil {
			return nil, nil, err
		}
		contracts = append(contracts, contract)
	}
	return
}

func (s *SQLStore) addTestContract(fcid types.FileContractID, hk types.PublicKey) (api.ContractMetadata, error) {
	rev := testContractRevision(fcid, hk)
	return s.AddContract(context.Background(), rev, types.ZeroCurrency, 0)
}

func (s *SQLStore) addTestRenewedContract(fcid, renewedFrom types.FileContractID, hk types.PublicKey, startHeight uint64) (api.ContractMetadata, error) {
	rev := testContractRevision(fcid, hk)
	return s.AddRenewedContract(context.Background(), rev, types.ZeroCurrency, startHeight, renewedFrom)
}

func (s *SQLStore) contractsCount() (cnt int64, err error) {
	err = s.db.
		Model(&dbContract{}).
		Count(&cnt).
		Error
	return
}

func testContractRevision(fcid types.FileContractID, hk types.PublicKey) rhpv2.ContractRevision {
	uc := generateMultisigUC(1, 2, "salt")
	uc.PublicKeys[1].Key = hk[:]
	uc.Timelock = 192837
	return rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				RevisionNumber: 200,
				Filesize:       4096,
				FileMerkleRoot: types.Hash256{222},
				WindowStart:    400,
				WindowEnd:      500,
				ValidProofOutputs: []types.SiacoinOutput{
					{
						Value:   types.NewCurrency64(121),
						Address: types.Address{2, 1, 2},
					},
				},
				MissedProofOutputs: []types.SiacoinOutput{
					{
						Value:   types.NewCurrency64(323),
						Address: types.Address{2, 3, 2},
					},
				},
				UnlockHash: types.Hash256{6, 6, 6},
			},
		},
		Signatures: [2]types.TransactionSignature{
			{
				ParentID:       types.Hash256(fcid),
				PublicKeyIndex: 0,
				Timelock:       100000,
				CoveredFields:  types.CoveredFields{WholeTransaction: true},
				Signature:      []byte("signature1"),
			},
			{
				ParentID:       types.Hash256(fcid),
				PublicKeyIndex: 1,
				Timelock:       200000,
				CoveredFields:  types.CoveredFields{WholeTransaction: true},
				Signature:      []byte("signature2"),
			},
		},
	}
}

// TestSQLMetadataStore tests basic MetadataStore functionality.
func TestSQLMetadataStore(t *testing.T) {
	db, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Create 2 hosts
	hks, err := db.addTestHosts(2)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2 := hks[0], hks[1]

	// Create 2 contracts
	fcids, contracts, err := db.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1, fcid2 := fcids[0], fcids[1]

	// Extract start height and total cost
	startHeight1, totalCost1 := contracts[0].StartHeight, contracts[0].TotalCost
	startHeight2, totalCost2 := contracts[1].StartHeight, contracts[1].TotalCost

	// Define usedHosts.
	usedHosts := map[types.PublicKey]types.FileContractID{
		hk1: fcid1,
		hk2: fcid2,
	}

	// Create an object with 2 slabs pointing to 2 different sectors.
	obj1 := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: hk1,
							Root: types.Hash256{1},
						},
					},
				},
				Offset: 10,
				Length: 100,
			},
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 2,
					Shards: []object.Sector{
						{
							Host: hk2,
							Root: types.Hash256{2},
						},
					},
				},
				Offset: 20,
				Length: 200,
			},
		},
	}

	// Store it.
	ctx := context.Background()
	objID := "key1"
	if err := db.UpdateObject(ctx, objID, obj1, usedHosts); err != nil {
		t.Fatal(err)
	}

	// Try to store it again. Should work.
	if err := db.UpdateObject(ctx, objID, obj1, usedHosts); err != nil {
		t.Fatal(err)
	}

	// Fetch it using get and verify every field.
	obj, err := db.object(ctx, objID)
	if err != nil {
		t.Fatal(err)
	}

	obj1Key, err := obj1.Key.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	obj1Slab0Key, err := obj1.Slabs[0].Key.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	obj1Slab1Key, err := obj1.Slabs[1].Key.MarshalText()
	if err != nil {
		t.Fatal(err)
	}

	// Set the Model fields to zero before comparing. These are set by gorm
	// itself and contain a few timestamps which would make the following
	// code a lot more verbose.
	obj.Model = Model{}
	for i := range obj.Slabs {
		obj.Slabs[i].Model = Model{}
		obj.Slabs[i].Slab.Model = Model{}
		obj.Slabs[i].Slab.Shards[0].ID = 0
		obj.Slabs[i].Slab.Shards[0].DBSector.Model = Model{}
		obj.Slabs[i].Slab.Shards[0].DBSector.Contracts[0].Model = Model{}
		obj.Slabs[i].Slab.Shards[0].DBSector.Contracts[0].Host.Model = Model{}
	}

	expectedObj := dbObject{
		ObjectID: objID,
		Key:      obj1Key,
		Size:     obj1.Size(),
		Slabs: []dbSlice{
			{
				DBObjectID: 1,
				Slab: dbSlab{
					DBSliceID:   1,
					Key:         obj1Slab0Key,
					MinShards:   1,
					TotalShards: 1,
					Shards: []dbShard{
						{
							DBSlabID:   1,
							DBSectorID: 1,
							DBSector: dbSector{
								Root:       obj1.Slabs[0].Shards[0].Root[:],
								LatestHost: publicKey(obj1.Slabs[0].Shards[0].Host),
								Contracts: []dbContract{
									{
										HostID: 1,
										Host: dbHost{
											PublicKey: publicKey(hk1),
										},

										ContractCommon: ContractCommon{
											FCID: fileContractID(fcid1),

											TotalCost:      currency(totalCost1),
											RevisionNumber: "0",
											StartHeight:    startHeight1,
											WindowStart:    400,
											WindowEnd:      500,

											UploadSpending:      zeroCurrency,
											DownloadSpending:    zeroCurrency,
											FundAccountSpending: zeroCurrency,
										},
									},
								},
							},
						},
					},
				},
				Offset: 10,
				Length: 100,
			},
			{
				DBObjectID: 1,
				Slab: dbSlab{
					DBSliceID:   2,
					Key:         obj1Slab1Key,
					MinShards:   2,
					TotalShards: 1,
					Shards: []dbShard{
						{
							DBSlabID:   2,
							DBSectorID: 2,
							DBSector: dbSector{
								Root:       obj1.Slabs[1].Shards[0].Root[:],
								LatestHost: publicKey(obj1.Slabs[1].Shards[0].Host),
								Contracts: []dbContract{
									{
										HostID: 2,
										Host: dbHost{
											PublicKey: publicKey(hk2),
										},
										ContractCommon: ContractCommon{
											FCID: fileContractID(fcid2),

											TotalCost:      currency(totalCost2),
											RevisionNumber: "0",
											StartHeight:    startHeight2,
											WindowStart:    400,
											WindowEnd:      500,

											UploadSpending:      zeroCurrency,
											DownloadSpending:    zeroCurrency,
											FundAccountSpending: zeroCurrency,
										},
									},
								},
							},
						},
					},
				},
				Offset: 20,
				Length: 200,
			},
		},
	}
	if !reflect.DeepEqual(obj, expectedObj) {
		t.Fatal("object mismatch", cmp.Diff(obj, expectedObj))
	}

	// Fetch it and verify again.
	fullObj, err := db.Object(ctx, objID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fullObj, obj1) {
		t.Fatal("object mismatch")
	}

	// Remove the first slab of the object and change the min shards of the
	// second one.
	obj1.Slabs = obj1.Slabs[1:]
	obj1.Slabs[0].Slab.MinShards = 123
	if err := db.UpdateObject(ctx, objID, obj1, usedHosts); err != nil {
		t.Fatal(err)
	}
	fullObj, err = db.Object(ctx, objID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fullObj, obj1) {
		t.Fatal("object mismatch")
	}

	// Sanity check the db at the end of the test. We expect:
	// - 1 element in the object table since we only stored and overwrote a single object
	// - 1 element in the slabs table since we updated the object to only have 1 slab
	// - 1 element in the slices table for the same reason
	// - 2 elements in the sectors table because we don't delete sectors from the sectors table
	// - 1 element in the sector_slabs table since we got 1 slab linked to a sector
	countCheck := func(objCount, sliceCount, slabCount, shardCount, sectorCount, sectorSlabCount int64) error {
		tableCountCheck := func(table interface{}, tblCount int64) error {
			var count int64
			if err := db.db.Model(table).Count(&count).Error; err != nil {
				return err
			}
			if count != tblCount {
				return fmt.Errorf("expected %v objects in table %v but got %v", tblCount, table.(schema.Tabler).TableName(), count)
			}
			return nil
		}
		// Check all tables.
		if err := tableCountCheck(&dbObject{}, objCount); err != nil {
			return err
		}
		if err := tableCountCheck(&dbSlice{}, sliceCount); err != nil {
			return err
		}
		if err := tableCountCheck(&dbSlab{}, slabCount); err != nil {
			return err
		}
		if err := tableCountCheck(&dbSector{}, sectorCount); err != nil {
			return err
		}
		var ssc int64
		if err := db.db.Table("shards").Count(&ssc).Error; err != nil {
			return err
		}
		return nil
	}
	if err := countCheck(1, 1, 1, 1, 2, 1); err != nil {
		t.Error(err)
	}

	// Delete the object. Due to the cascade this should delete everything
	// but the sectors.
	if err := db.RemoveObject(ctx, objID); err != nil {
		t.Fatal(err)
	}
	if err := countCheck(0, 0, 0, 0, 2, 0); err != nil {
		t.Fatal(err)
	}
}

// TestObjectEntries is a test for the ObjectEntries method.
func TestObjectEntries(t *testing.T) {
	os, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}
	objects := []struct {
		path string
		size int64
	}{
		{"/foo/bar", 1},
		{"/foo/bat", 2},
		{"/foo/baz/quux", 3},
		{"/foo/baz/quuz", 4},
		{"/gab/guub", 5},
		{"/fileś/śpecial", 6}, // utf8
	}
	ctx := context.Background()
	for _, o := range objects {
		obj, ucs := newTestObject(frand.Intn(9) + 1)
		obj.Slabs = obj.Slabs[:1]
		obj.Slabs[0].Length = uint32(o.size)
		os.UpdateObject(ctx, o.path, obj, ucs)
	}
	tests := []struct {
		path   string
		prefix string
		want   []api.ObjectMetadata
	}{
		{"/", "", []api.ObjectMetadata{{Name: "/fileś/", Size: 6}, {Name: "/foo/", Size: 10}, {Name: "/gab/", Size: 5}}},
		{"/foo/", "", []api.ObjectMetadata{{Name: "/foo/bar", Size: 1}, {Name: "/foo/bat", Size: 2}, {Name: "/foo/baz/", Size: 7}}},
		{"/foo/baz/", "", []api.ObjectMetadata{{Name: "/foo/baz/quux", Size: 3}, {Name: "/foo/baz/quuz", Size: 4}}},
		{"/gab/", "", []api.ObjectMetadata{{Name: "/gab/guub", Size: 5}}},
		{"/fileś/", "", []api.ObjectMetadata{{Name: "/fileś/śpecial", Size: 6}}},

		{"/", "f", []api.ObjectMetadata{{Name: "/fileś/", Size: 6}, {Name: "/foo/", Size: 10}}},
		{"/foo/", "fo", []api.ObjectMetadata{}},
		{"/foo/baz/", "quux", []api.ObjectMetadata{{Name: "/foo/baz/quux", Size: 3}}},
		{"/gab/", "/guub", []api.ObjectMetadata{}},
	}
	for _, test := range tests {
		got, err := os.ObjectEntries(ctx, test.path, test.prefix, 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		if !(len(got) == 0 && len(test.want) == 0) && !reflect.DeepEqual(got, test.want) {
			t.Errorf("\nlist: %v\nprefix: %v\ngot: %v\nwant: %v", test.path, test.prefix, got, test.want)
		}
		for offset := 0; offset < len(test.want); offset++ {
			got, err := os.ObjectEntries(ctx, test.path, test.prefix, offset, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0] != test.want[offset] {
				t.Errorf("\nlist: %v\nprefix: %v\ngot: %v\nwant: %v", test.path, test.prefix, got, test.want[offset])
			}
		}
	}
}

// TestSearchObjects is a test for the SearchObjects method.
func TestSearchObjects(t *testing.T) {
	os, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}
	objects := []struct {
		path string
		size int64
	}{
		{"/foo/bar", 1},
		{"/foo/bat", 2},
		{"/foo/baz/quux", 3},
		{"/foo/baz/quuz", 4},
		{"/gab/guub", 5},
	}
	ctx := context.Background()
	for _, o := range objects {
		obj, ucs := newTestObject(frand.Intn(9) + 1)
		obj.Slabs = obj.Slabs[:1]
		obj.Slabs[0].Length = uint32(o.size)
		os.UpdateObject(ctx, o.path, obj, ucs)
	}
	tests := []struct {
		path string
		want []api.ObjectMetadata
	}{
		{"/", []api.ObjectMetadata{{Name: "/foo/bar", Size: 1}, {Name: "/foo/bat", Size: 2}, {Name: "/foo/baz/quux", Size: 3}, {Name: "/foo/baz/quuz", Size: 4}, {Name: "/gab/guub", Size: 5}}},
		{"/foo/b", []api.ObjectMetadata{{Name: "/foo/bar", Size: 1}, {Name: "/foo/bat", Size: 2}, {Name: "/foo/baz/quux", Size: 3}, {Name: "/foo/baz/quuz", Size: 4}}},
		{"o/baz/quu", []api.ObjectMetadata{{Name: "/foo/baz/quux", Size: 3}, {Name: "/foo/baz/quuz", Size: 4}}},
		{"uu", []api.ObjectMetadata{{Name: "/foo/baz/quux", Size: 3}, {Name: "/foo/baz/quuz", Size: 4}, {Name: "/gab/guub", Size: 5}}},
	}
	for _, test := range tests {
		got, err := os.SearchObjects(ctx, test.path, 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		if !(len(got) == 0 && len(test.want) == 0) && !reflect.DeepEqual(got, test.want) {
			t.Errorf("\nkey: %v\ngot: %v\nwant: %v", test.path, got, test.want)
		}
		for offset := 0; offset < len(test.want); offset++ {
			got, err := os.SearchObjects(ctx, test.path, offset, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0] != test.want[offset] {
				t.Errorf("\nkey: %v\ngot: %v\nwant: %v", test.path, got, test.want[offset])
			}
		}
	}
}

// TestUnhealthySlabs tests the functionality of UnhealthySlabs.
func TestUnhealthySlabs(t *testing.T) {
	db, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// add 4 hosts
	hks, err := db.addTestHosts(4)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2, hk3, hk4 := hks[0], hks[1], hks[2], hks[3]

	// add 4 contracts
	fcids, _, err := db.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1, fcid2, fcid3, fcid4 := fcids[0], fcids[1], fcids[2], fcids[3]

	// select the first three contracts as good contracts
	goodContracts := []types.FileContractID{fcid1, fcid2, fcid3}
	if err := db.SetContractSet(context.Background(), "autopilot", goodContracts); err != nil {
		t.Fatal(err)
	}

	// add an object
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			// good slab
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: hk1,
							Root: types.Hash256{1},
						},
						{
							Host: hk2,
							Root: types.Hash256{2},
						},
						{
							Host: hk3,
							Root: types.Hash256{3},
						},
					},
				},
			},
			// unhealthy slab - hk4 is bad (1/3)
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: hk1,
							Root: types.Hash256{4},
						},
						{
							Host: hk2,
							Root: types.Hash256{5},
						},
						{
							Host: hk4,
							Root: types.Hash256{6},
						},
					},
				},
			},
			// unhealthy slab - hk4 is bad (2/3)
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: hk1,
							Root: types.Hash256{7},
						},
						{
							Host: hk4,
							Root: types.Hash256{8},
						},
						{
							Host: hk4,
							Root: types.Hash256{9},
						},
					},
				},
			},
			// unhealthy slab - hk5 is deleted (1/3)
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: hk1,
							Root: types.Hash256{10},
						},
						{
							Host: hk2,
							Root: types.Hash256{11},
						},
						{
							Host: types.PublicKey{5},
							Root: types.Hash256{12},
						},
					},
				},
			},
			// unhealthy slab - h1 is reused
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: hk1,
							Root: types.Hash256{13},
						},
						{
							Host: hk1,
							Root: types.Hash256{14},
						},
						{
							Host: hk1,
							Root: types.Hash256{15},
						},
					},
				},
			},
			// lost slab - no good pieces (0/3)
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: types.PublicKey{1},
							Root: types.Hash256{16},
						},
						{
							Host: types.PublicKey{2},
							Root: types.Hash256{17},
						},
						{
							Host: types.PublicKey{3},
							Root: types.Hash256{18},
						},
					},
				},
			},
		},
	}

	ctx := context.Background()
	if err := db.UpdateObject(ctx, "foo", obj, map[types.PublicKey]types.FileContractID{
		hk1: fcid1,
		hk2: fcid2,
		hk3: fcid3,
		hk4: fcid4,
		{5}: {5}, // deleted host and contract
	}); err != nil {
		t.Fatal(err)
	}

	slabs, err := db.UnhealthySlabs(ctx, 0.99, "autopilot", -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 4 {
		t.Fatalf("unexpected amount of slabs to migrate, %v!=4", len(slabs))
	}

	expected := []object.SlabSlice{
		obj.Slabs[4],
		obj.Slabs[2],
		obj.Slabs[1],
		obj.Slabs[3],
	}
	if reflect.DeepEqual(slabs, expected) {
		t.Fatal("slabs are not returned in the correct order")
	}

	slabs, err = db.UnhealthySlabs(ctx, 0.49, "autopilot", -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 2 {
		t.Fatalf("unexpected amount of slabs to migrate, %v!=2", len(slabs))
	}

	expected = []object.SlabSlice{
		obj.Slabs[4],
		obj.Slabs[2],
	}
	if reflect.DeepEqual(slabs, expected) {
		t.Fatal("slabs are not returned in the correct order")
	}
}

// TestUnhealthySlabs tests the functionality of UnhealthySlabs on slabs that
// don't have any redundancy.
func TestUnhealthySlabsNoRedundancy(t *testing.T) {
	db, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// add 3 hosts
	hks, err := db.addTestHosts(4)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2, hk3 := hks[0], hks[1], hks[2]

	// add 4 contracts
	fcids, _, err := db.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1, fcid2, fcid3 := fcids[0], fcids[1], fcids[2]

	// select the first two contracts as good contracts
	goodContracts := []types.FileContractID{fcid1, fcid2}
	if err := db.SetContractSet(context.Background(), "autopilot", goodContracts); err != nil {
		t.Fatal(err)
	}

	// add an object
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			// hk1 is good so this slab should have full health.
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: hk1,
							Root: types.Hash256{1},
						},
					},
				},
			},
			// hk4 is bad so this slab should have no health.
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 2,
					Shards: []object.Sector{
						{
							Host: hk2,
							Root: types.Hash256{2},
						},
						{
							Host: hk3,
							Root: types.Hash256{4},
						},
					},
				},
			},
		},
	}

	ctx := context.Background()
	if err := db.UpdateObject(ctx, "foo", obj, map[types.PublicKey]types.FileContractID{
		hk1: fcid1,
		hk2: fcid2,
		hk3: fcid3,
	}); err != nil {
		t.Fatal(err)
	}

	slabs, err := db.UnhealthySlabs(ctx, 0.99, "autopilot", -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 0 {
		t.Fatalf("unexpected amount of slabs to migrate, %v!=0", len(slabs))
	}

	expected := []object.SlabSlice{
		obj.Slabs[0],
	}
	if reflect.DeepEqual(slabs, expected) {
		t.Fatal("slabs are not returned in the correct order")
	}
}

// TestContractSectors is a test for the contract_sectors join table. It
// verifies that deleting contracts or sectors also cleans up the join table.
func TestContractSectors(t *testing.T) {
	db, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Create a host, contract and sector to upload to that host into the
	// given contract.
	hk1 := types.PublicKey{1}
	fcid1 := types.FileContractID{1}
	err = db.addTestHost(hk1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.addTestContract(fcid1, hk1)
	if err != nil {
		t.Fatal(err)
	}
	sectorGood := object.Sector{
		Host: hk1,
		Root: types.Hash256{1},
	}

	// Prepare used contracts.
	usedContracts := map[types.PublicKey]types.FileContractID{
		hk1: fcid1,
	}

	// Create object.
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						sectorGood,
					},
				},
			},
		},
	}
	ctx := context.Background()
	if err := db.UpdateObject(ctx, "foo", obj, usedContracts); err != nil {
		t.Fatal(err)
	}

	// Delete the contract.
	err = db.ArchiveContract(ctx, fcid1, api.ContractArchivalReasonRemoved)
	if err != nil {
		t.Fatal(err)
	}

	// Check the join table. Should be empty.
	var css []dbContractSector
	if err := db.db.Find(&css).Error; err != nil {
		t.Fatal(err)
	}
	if len(css) != 0 {
		t.Fatal("table should be empty", len(css))
	}

	// Add the contract back.
	_, err = db.addTestContract(fcid1, hk1)
	if err != nil {
		t.Fatal(err)
	}

	// Add the object again.
	if err := db.UpdateObject(ctx, "foo", obj, usedContracts); err != nil {
		t.Fatal(err)
	}

	// Delete the object.
	if err := db.RemoveObject(ctx, "foo"); err != nil {
		t.Fatal(err)
	}

	// Delete the sector.
	if err := db.db.Delete(&dbSector{Model: Model{ID: 1}}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.db.Find(&css).Error; err != nil {
		t.Fatal(err)
	}
	if len(css) != 0 {
		t.Fatal("table should be empty")
	}
}

// TestPutSlab verifies the functionality of PutSlab.
func TestPutSlab(t *testing.T) {
	db, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// add 3 hosts
	hks, err := db.addTestHosts(3)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2, hk3 := hks[0], hks[1], hks[2]

	// add 3 contracts
	fcids, _, err := db.addTestContracts(hks)
	if err != nil {
		t.Fatal(err)
	}
	fcid1, fcid2, fcid3 := fcids[0], fcids[1], fcids[2]

	// add an object
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: hk1,
							Root: types.Hash256{1},
						},
						{
							Host: hk2,
							Root: types.Hash256{2},
						},
					},
				},
			},
		},
	}
	ctx := context.Background()
	if err := db.UpdateObject(ctx, "foo", obj, map[types.PublicKey]types.FileContractID{
		hk1: fcid1,
		hk2: fcid2,
	}); err != nil {
		t.Fatal(err)
	}

	// extract the slab key
	key, err := obj.Slabs[0].Key.MarshalText()
	if err != nil {
		t.Fatal(err)
	}

	// helper to fetch a slab from the database
	fetchSlab := func() (slab dbSlab) {
		t.Helper()
		if err = db.db.
			Where(&dbSlab{Key: key}).
			Preload("Shards.DBSector").
			Preload("Shards.DBSector.Contracts").
			Preload("Shards.DBSector.Hosts").
			Take(&slab).
			Error; err != nil {
			t.Fatal(err)
		}
		return
	}

	// helper to extract the FCID from a list of contracts
	contractIds := func(contracts []dbContract) (ids []fileContractID) {
		for _, c := range contracts {
			ids = append(ids, fileContractID(c.FCID))
		}
		return
	}

	// helper to extract the hostkey from a list of hosts
	hostKeys := func(hosts []dbHost) (ids []types.PublicKey) {
		for _, h := range hosts {
			ids = append(ids, types.PublicKey(h.PublicKey))
		}
		return
	}

	// fetch inserted slab
	inserted := fetchSlab()

	// assert both sectors were upload to one contract/host
	for i := 0; i < 2; i++ {
		if cids := contractIds(inserted.Shards[i].DBSector.Contracts); len(cids) != 1 {
			t.Fatalf("sector %d was uploaded to unexpected amount of contracts, %v!=1", i+1, len(cids))
		} else if hks := hostKeys(inserted.Shards[i].DBSector.Hosts); len(hks) != 1 {
			t.Fatalf("sector %d was uploaded to unexpected amount of hosts, %v!=1", i+1, len(hks))
		}
	}

	// select contracts h1 and h3 as good contracts (h2 is bad)
	goodContracts := []types.FileContractID{fcid1, fcid3}
	if err := db.SetContractSet(ctx, "autopilot", goodContracts); err != nil {
		t.Fatal(err)
	}

	// fetch slabs for migration and assert there is only one
	toMigrate, err := db.UnhealthySlabs(ctx, 0.99, "autopilot", -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(toMigrate) != 1 {
		t.Fatal("unexpected number of slabs to migrate", len(toMigrate))
	}

	// migrate the sector from h2 to h3
	slab := toMigrate[0]
	slab.Shards[1] = object.Sector{
		Host: hk3,
		Root: types.Hash256{2},
	}

	// update the slab to reflect the migration
	err = db.UpdateSlab(ctx, slab, map[types.PublicKey]types.FileContractID{
		hk1: fcid1,
		hk3: fcid3,
	})
	if err != nil {
		t.Fatal(err)
	}

	// fetch updated slab
	updated := fetchSlab()

	// assert the first sector is still only on one host, also assert it's h1
	if cids := contractIds(updated.Shards[0].DBSector.Contracts); len(cids) != 1 {
		t.Fatalf("sector 1 was uploaded to unexpected amount of contracts, %v!=1", len(cids))
	} else if types.FileContractID(cids[0]) != fcid1 {
		t.Fatal("sector 1 was uploaded to unexpected contract", cids[0])
	} else if hks := hostKeys(updated.Shards[0].DBSector.Hosts); len(hks) != 1 {
		t.Fatalf("sector 1 was uploaded to unexpected amount of hosts, %v!=1", len(hks))
	} else if hks[0] != hk1 {
		t.Fatal("sector 1 was uploaded to unexpected host", hks[0])
	}

	// assert the second sector however is uploaded to two hosts, assert it's h2 and h3
	if cids := contractIds(updated.Shards[1].DBSector.Contracts); len(cids) != 2 {
		t.Fatalf("sector 1 was uploaded to unexpected amount of contracts, %v!=2", len(cids))
	} else if types.FileContractID(cids[0]) != fcid2 || types.FileContractID(cids[1]) != fcid3 {
		t.Fatal("sector 1 was uploaded to unexpected contracts", cids[0], cids[1])
	} else if hks := hostKeys(updated.Shards[1].DBSector.Hosts); len(hks) != 2 {
		t.Fatalf("sector 1 was uploaded to unexpected amount of hosts, %v!=2", len(hks))
	} else if hks[0] != hk2 || hks[1] != hk3 {
		t.Fatal("sector 1 was uploaded to unexpected hosts", hks[0], hks[1])
	}

	// assert there's still only one entry in the dbslab table
	var cnt int64
	if err := db.db.Model(&dbSlab{}).Count(&cnt).Error; err != nil {
		t.Fatal(err)
	} else if cnt != 1 {
		t.Fatalf("unexpected number of entries in dbslab, %v != 1", cnt)
	}

	// fetch slabs for migration and assert there are none left
	toMigrate, err = db.UnhealthySlabs(ctx, 0.99, "autopilot", -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(toMigrate) != 0 {
		t.Fatal("unexpected number of slabs to migrate", len(toMigrate))
	}

	if obj, err := db.object(ctx, "foo"); err != nil {
		t.Fatal(err)
	} else if len(obj.Slabs) != 1 {
		t.Fatalf("unexpected number of slabs, %v != 1", len(obj.Slabs))
	} else if obj.Slabs[0].ID != updated.ID {
		t.Fatalf("unexpected slab, %v != %v", obj.Slabs[0].ID, updated.ID)
	}
}

func newTestObject(slabs int) (object.Object, map[types.PublicKey]types.FileContractID) {
	obj := object.Object{}
	usedContracts := make(map[types.PublicKey]types.FileContractID)

	obj.Slabs = make([]object.SlabSlice, slabs)
	obj.Key = object.GenerateEncryptionKey()
	for i := range obj.Slabs {
		n := uint8(frand.Uint64n(10) + 1)
		offset := uint32(frand.Uint64n(1 << 22))
		length := offset + uint32(frand.Uint64n(1<<22))
		obj.Slabs[i] = object.SlabSlice{
			Slab: object.Slab{
				Key:       object.GenerateEncryptionKey(),
				MinShards: n,
				Shards:    make([]object.Sector, n*2),
			},
			Offset: offset,
			Length: length,
		}
		for j := range obj.Slabs[i].Shards {
			var fcid types.FileContractID
			frand.Read(fcid[:])
			obj.Slabs[i].Shards[j].Root = frand.Entropy256()
			obj.Slabs[i].Shards[j].Host = frand.Entropy256()
			usedContracts[obj.Slabs[i].Shards[j].Host] = fcid
		}
	}
	return obj, usedContracts
}

// TestRecordContractSpending tests RecordContractSpending.
func TestRecordContractSpending(t *testing.T) {
	cs, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Create a host for the contract.
	hk := types.GeneratePrivateKey().PublicKey()
	err = cs.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// Add an announcement.
	err = cs.insertTestAnnouncement(hk, hostdb.Announcement{NetAddress: "address"})
	if err != nil {
		t.Fatal(err)
	}

	fcid := types.FileContractID{1, 1, 1, 1, 1}
	cm, err := cs.addTestContract(fcid, hk)
	if err != nil {
		t.Fatal(err)
	}
	if cm.Spending != (api.ContractSpending{}) {
		t.Fatal("spending should be all 0")
	}

	// Record some spending.
	expectedSpending := api.ContractSpending{
		Uploads:     types.Siacoins(1),
		Downloads:   types.Siacoins(2),
		FundAccount: types.Siacoins(3),
	}
	err = cs.RecordContractSpending(context.Background(), []api.ContractSpendingRecord{
		// non-existent contract
		{
			ContractID: types.FileContractID{1, 2, 3},
		},
		// valid spending
		{
			ContractID:       fcid,
			ContractSpending: expectedSpending,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cm2, err := cs.Contract(context.Background(), fcid)
	if err != nil {
		t.Fatal(err)
	}
	if cm2.Spending != expectedSpending {
		t.Fatal("invalid spending")
	}

	// Record the same spending again.
	err = cs.RecordContractSpending(context.Background(), []api.ContractSpendingRecord{
		{
			ContractID:       fcid,
			ContractSpending: expectedSpending,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectedSpending = expectedSpending.Add(expectedSpending)
	cm3, err := cs.Contract(context.Background(), fcid)
	if err != nil {
		t.Fatal(err)
	}
	if cm3.Spending != expectedSpending {
		t.Fatal("invalid spending")
	}
}

// TestObjectsStats is a unit test for ObjectsStats.
func TestObjectsStats(t *testing.T) {
	cs, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Create a few objects of different size.
	var objectsSize uint64
	var sectorsSize uint64
	for i := 0; i < 2; i++ {
		obj, contracts := newTestObject(1)
		objectsSize += uint64(obj.Size())
		for _, slab := range obj.Slabs {
			sectorsSize += uint64(len(slab.Shards) * rhpv2.SectorSize)
		}

		for hpk, fcid := range contracts {
			if err := cs.addTestHost(hpk); err != nil {
				t.Fatal(err)
			}
			_, err := cs.addTestContract(fcid, hpk)
			if err != nil {
				t.Fatal(err)
			}
		}

		key := hex.EncodeToString(frand.Bytes(32))
		err := cs.UpdateObject(context.Background(), key, obj, contracts)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Get all entries in contract_sectors and store them again with a different
	// contract id. This should cause the uploaded size to double.
	var contractSectors []dbContractSector
	err = cs.db.Find(&contractSectors).Error
	if err != nil {
		t.Fatal(err)
	}
	var newContractID types.FileContractID
	frand.Read(newContractID[:])
	_, err = cs.addTestContract(newContractID, types.PublicKey{})
	if err != nil {
		t.Fatal(err)
	}
	newContract, err := cs.contract(context.Background(), fileContractID(newContractID))
	if err != nil {
		t.Fatal(err)
	}
	for _, contractSector := range contractSectors {
		contractSector.DBContractID = newContract.ID
		err = cs.db.Create(&contractSector).Error
		if err != nil {
			t.Fatal(err)
		}
	}

	// Check sizes.
	info, err := cs.ObjectsStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.TotalObjectsSize != objectsSize {
		t.Fatal("wrong size", info.TotalObjectsSize, objectsSize)
	}
	if info.TotalSectorsSize != sectorsSize {
		t.Fatal("wrong size", info.TotalSectorsSize, sectorsSize)
	}
	if info.TotalUploadedSize != sectorsSize*2 {
		t.Fatal("wrong size", info.TotalUploadedSize, sectorsSize*2)
	}
	if info.NumObjects != 2 {
		t.Fatal("wrong number of objects", info.NumObjects, 2)
	}
}
