package stores

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

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
	if _, err := cs.AddContract(ctx, c, totalCost, startHeight); err != nil {
		t.Fatal(err)
	}

	// Look it up again.
	fetched, err := cs.Contract(ctx, c.ID())
	if err != nil {
		t.Fatal(err)
	}
	expected := api.ContractMetadata{
		ID:          fcid,
		HostIP:      "address",
		HostKey:     hk,
		StartHeight: 100,
		RenewedFrom: types.FileContractID{},
		Spending: api.ContractSpending{
			Uploads:     types.ZeroCurrency,
			Downloads:   types.ZeroCurrency,
			FundAccount: types.ZeroCurrency,
		},
		TotalCost: totalCost,
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
	if _, err := cs.Contracts(ctx, "bar"); err != ErrContractSetNotFound {
		t.Fatal(err)
	}

	// Delete the contract.
	if err := cs.RemoveContract(ctx, c.ID()); err != nil {
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

// TestRenewContract is a test for AddRenewedContract.
func TestRenewedContract(t *testing.T) {
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

	// Insert a contract.
	fcid := types.FileContractID{1, 1, 1, 1, 1}
	c := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid,
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

	// Renew it.
	fcid2 := types.FileContractID{2, 2, 2, 2, 2}
	renewed := rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         fcid2,
			UnlockConditions: uc,
			FileContract: types.FileContract{
				MissedProofOutputs: []types.SiacoinOutput{},
				ValidProofOutputs:  []types.SiacoinOutput{},
			},
		},
	}
	newContractTotal := types.NewCurrency64(222)
	newContractStartHeight := uint64(200)
	if _, err := cs.AddRenewedContract(ctx, renewed, newContractTotal, newContractStartHeight, fcid); err != nil {
		t.Fatal(err)
	}

	// Contract should be gone from active contracts.
	_, err = cs.Contract(ctx, fcid)
	if !errors.Is(err, ErrContractNotFound) {
		t.Fatal(err)
	}

	// New contract should exist.
	newContract, err := cs.Contract(ctx, fcid2)
	if err != nil {
		t.Fatal(err)
	}
	expected := api.ContractMetadata{
		ID:          fcid2,
		HostIP:      "address",
		HostKey:     hk,
		StartHeight: newContractStartHeight,
		RenewedFrom: fcid,
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
		Where("fcid", fileContractID(fcid)).
		Take(&ac).
		Error
	if err != nil {
		t.Fatal(err)
	}

	ac.Model = Model{}
	expectedContract := dbArchivedContract{
		FCID:                fileContractID(fcid),
		Host:                publicKey(c.HostKey()),
		RenewedTo:           fileContractID(fcid2),
		Reason:              archivalReasonRenewed,
		StartHeight:         100,
		UploadSpending:      zeroCurrency,
		DownloadSpending:    zeroCurrency,
		FundAccountSpending: zeroCurrency,
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
	renewedContract, err := cs.AddRenewedContract(ctx, renewed, newContractTotal, newContractStartHeight, fcid2)
	if err != nil {
		t.Fatal(err)
	}
	if renewedContract.RenewedFrom != fcid2 {
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
			ID:        fcids[len(fcids)-2-i],
			HostKey:   hk,
			RenewedTo: fcids[len(fcids)-1-i],
		}) {
			t.Fatal("wrong contract", i)
		}
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
										FCID:                fileContractID(fcid1),
										StartHeight:         startHeight1,
										TotalCost:           currency(totalCost1),
										UploadSpending:      zeroCurrency,
										DownloadSpending:    zeroCurrency,
										FundAccountSpending: zeroCurrency,
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
										FCID:                fileContractID(fcid2),
										StartHeight:         startHeight2,
										TotalCost:           currency(totalCost2),
										UploadSpending:      zeroCurrency,
										DownloadSpending:    zeroCurrency,
										FundAccountSpending: zeroCurrency,
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
		js1, _ := json.MarshalIndent(obj, "", "  ")
		js2, _ := json.MarshalIndent(expectedObj, "", "  ")
		t.Fatal("object mismatch", string(js1), string(js2))
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

// TestObjects is a test for the Objects method.
func TestObjects(t *testing.T) {
	os, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{
		"/foo/bar",
		"/foo/bat",
		"/foo/baz/quux",
		"/foo/baz/quuz",
		"/gab/guub",
	}
	ctx := context.Background()
	for _, path := range paths {
		obj, ucs := newTestObject(frand.Intn(10))
		os.UpdateObject(ctx, path, obj, ucs)
	}
	tests := []struct {
		prefix string
		want   []string
	}{
		{"/", []string{"/foo/", "/gab/"}},
		{"/foo/", []string{"/foo/bar", "/foo/bat", "/foo/baz/"}},
		{"/foo/baz/", []string{"/foo/baz/quux", "/foo/baz/quuz"}},
		{"/gab/", []string{"/gab/guub"}},
	}
	for _, test := range tests {
		got, err := os.Objects(ctx, test.prefix)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("\nlist: %v\ngot:  %v\nwant: %v", test.prefix, got, test.want)
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
	if len(slabs) != 1 {
		t.Fatalf("unexpected amount of slabs to migrate, %v!=1", len(slabs))
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
	err = db.RemoveContract(ctx, fcid1)
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
			obj.Slabs[i].Shards[j].Root = frand.Entropy256()
			obj.Slabs[i].Shards[j].Host = frand.Entropy256()
			usedContracts[obj.Slabs[i].Shards[j].Host] = types.FileContractID{}
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
