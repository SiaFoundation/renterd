package stores

import (
	"fmt"
	"math/big"
	"reflect"
	"testing"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
	"gorm.io/gorm/schema"
	"lukechampine.com/frand"
)

// TestSQLPutSlab verifies the functionality of PutSlab.
func TestSQLPutSlab(t *testing.T) {
	db, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// create 3 hosts
	hk1 := consensus.GeneratePrivateKey().PublicKey()
	if err := db.addTestHost(hk1); err != nil {
		t.Fatal(err)
	}
	hk2 := consensus.GeneratePrivateKey().PublicKey()
	if err := db.addTestHost(hk2); err != nil {
		t.Fatal(err)
	}
	hk3 := consensus.GeneratePrivateKey().PublicKey()
	if err := db.addTestHost(hk3); err != nil {
		t.Fatal(err)
	}

	// create 3 contracts
	fcid1 := types.FileContractID{1}
	if _, err := db.addTestContract(fcid1, hk1); err != nil {
		t.Fatal(err)
	}
	fcid2 := types.FileContractID{2}
	if _, err := db.addTestContract(fcid2, hk2); err != nil {
		t.Fatal(err)
	}
	fcid3 := types.FileContractID{3}
	if _, err := db.addTestContract(fcid3, hk3); err != nil {
		t.Fatal(err)
	}

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
							Root: consensus.Hash256{1},
						},
						{
							Host: hk2,
							Root: consensus.Hash256{2},
						},
					},
				},
			},
		},
	}
	if err := db.Put("foo", obj, map[consensus.PublicKey]types.FileContractID{
		hk1: fcid1,
		hk2: fcid2,
	}); err != nil {
		t.Fatal(err)
	}

	// migrate the slab and move the second sector to a new host
	obj.Slabs[0].Slab.Shards[1] = object.Sector{
		Host: hk3,
		Root: consensus.Hash256{3},
	}
	if err := db.PutSlab(obj.Slabs[0].Slab, map[consensus.PublicKey]types.FileContractID{
		hk1: fcid1,
		hk3: fcid3,
	}, []consensus.PublicKey{hk2}); err != nil {
		t.Fatal(err)
	}
}

// TestSQLObjectStore tests basic SQLObjectStore functionality.
func TestSQLObjectStore(t *testing.T) {
	os, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Create hosts for the contracts to avoid the foreign key constraint
	// failing.
	hk1 := consensus.GeneratePrivateKey().PublicKey()
	hk2 := consensus.GeneratePrivateKey().PublicKey()
	err = os.addTestHost(hk1)
	if err != nil {
		t.Fatal(err)
	}
	err = os.addTestHost(hk2)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file contract for the object to avoid the foreign key
	// constraint failing.
	fcid1, fcid2 := types.FileContractID{1}, types.FileContractID{2}
	c1, totalCost1, startHeight1 := newTestContract(fcid1, hk1)
	c2, totalCost2, startHeight2 := newTestContract(fcid2, hk2)
	_, err = os.AddContract(c1, totalCost1, startHeight1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = os.AddContract(c2, totalCost2, startHeight2)
	if err != nil {
		t.Fatal(err)
	}

	// Define usedHosts.
	usedHosts := map[consensus.PublicKey]types.FileContractID{
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
							Root: consensus.Hash256{1},
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
							Root: consensus.Hash256{2},
						},
					},
				},
				Offset: 20,
				Length: 200,
			},
		},
	}

	// Store it.
	objID := "key1"
	if err := os.Put(objID, obj1, usedHosts); err != nil {
		t.Fatal(err)
	}

	// Try to store it again. Should work.
	if err := os.Put(objID, obj1, usedHosts); err != nil {
		t.Fatal(err)
	}

	// Fetch it using get and verify every field.
	obj, err := os.get(objID)
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
					DBSliceID: 1,
					Key:       obj1Slab0Key,
					MinShards: 1,
					Shards: []dbShard{
						{
							DBSlabID:   1,
							DBSectorID: 1,
							DBSector: dbSector{
								Root: obj1.Slabs[0].Shards[0].Root,
								Contracts: []dbContract{
									{
										HostID: 1,
										Host: dbHost{
											PublicKey: hk1,
										},
										FCID:                fcid1,
										StartHeight:         startHeight1,
										TotalCost:           totalCost1.Big(),
										UploadSpending:      big.NewInt(0),
										DownloadSpending:    big.NewInt(0),
										FundAccountSpending: big.NewInt(0),
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
					DBSliceID: 2,
					Key:       obj1Slab1Key,
					MinShards: 2,
					Shards: []dbShard{
						{
							DBSlabID:   2,
							DBSectorID: 2,
							DBSector: dbSector{
								Root: obj1.Slabs[1].Shards[0].Root,
								Contracts: []dbContract{
									{
										HostID: 2,
										Host: dbHost{
											PublicKey: hk2,
										},
										FCID:                fcid2,
										StartHeight:         startHeight2,
										TotalCost:           totalCost2.Big(),
										UploadSpending:      big.NewInt(0),
										DownloadSpending:    big.NewInt(0),
										FundAccountSpending: big.NewInt(0),
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
		t.Fatal("object mismatch")
	}

	// Fetch it using Get and verify again.
	fullObj, err := os.Get(objID)
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
	if err := os.Put(objID, obj1, usedHosts); err != nil {
		t.Fatal(err)
	}
	fullObj, err = os.Get(objID)
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
			if err := os.db.Model(table).Count(&count).Error; err != nil {
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
		if err := os.db.Table("shards").Count(&ssc).Error; err != nil {
			return err
		}
		return nil
	}
	if err := countCheck(1, 1, 1, 1, 2, 1); err != nil {
		t.Error(err)
	}

	// Delete the object. Due to the cascade this should delete everything
	// but the sectors.
	if err := os.Delete(objID); err != nil {
		t.Fatal(err)
	}
	if err := countCheck(0, 0, 0, 0, 2, 0); err != nil {
		t.Fatal(err)
	}
}

// TestSQLList is a test for (*SQLObjectStore).List.
func TestSQLList(t *testing.T) {
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
	for _, path := range paths {
		obj, ucs := newTestObject()
		os.Put(path, obj, ucs)
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
		got, err := os.List(test.prefix)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("\nlist: %v\ngot:  %v\nwant: %v", test.prefix, got, test.want)
		}
	}
}

// TestSlabsForMigration tests the functionality of SlabsForMigration.
func TestSlabsForMigration(t *testing.T) {
	db, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// add 3 hosts
	hk1 := consensus.PublicKey{1}
	err = db.addTestHost(hk1)
	if err != nil {
		t.Fatal(err)
	}

	hk2 := consensus.PublicKey{2}
	err = db.addTestHost(hk2)
	if err != nil {
		t.Fatal(err)
	}

	hk3 := consensus.PublicKey{3}
	err = db.addTestHost(hk3)
	if err != nil {
		t.Fatal(err)
	}

	// add 3 contracts
	fc1 := types.FileContractID{1}
	_, err = db.AddContract(newTestContract(fc1, hk1))
	if err != nil {
		t.Fatal(err)
	}

	fc2 := types.FileContractID{2}
	_, err = db.AddContract(newTestContract(fc2, hk2))
	if err != nil {
		t.Fatal(err)
	}

	fc3 := types.FileContractID{3}
	_, err = db.AddContract(newTestContract(fc3, hk3))
	if err != nil {
		t.Fatal(err)
	}

	// add the first two contracts to the autopilot
	if err = db.SetContractSet("autopilot", []types.FileContractID{fc1, fc2}); err != nil {
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
							Root: consensus.Hash256{1},
						},
						{
							Host: hk2,
							Root: consensus.Hash256{2},
						},
						{
							Host: hk2,
							Root: consensus.Hash256{3},
						},
					},
				},
			},
			// unhealthy slab - hk3 is bad (1/3)
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: hk1,
							Root: consensus.Hash256{4},
						},
						{
							Host: hk2,
							Root: consensus.Hash256{5},
						},
						{
							Host: hk3,
							Root: consensus.Hash256{6},
						},
					},
				},
			},
			// unhealthy slab - hk3 is bad (2/3)
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: hk1,
							Root: consensus.Hash256{7},
						},
						{
							Host: hk3,
							Root: consensus.Hash256{8},
						},
						{
							Host: hk3,
							Root: consensus.Hash256{9},
						},
					},
				},
			},
			// unhealthy slab - hk4 is deleted (1/3)
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						{
							Host: hk1,
							Root: consensus.Hash256{10},
						},
						{
							Host: hk2,
							Root: consensus.Hash256{11},
						},
						{
							Host: consensus.PublicKey{4},
							Root: consensus.Hash256{12},
						},
					},
				},
			},
		},
	}

	if err := db.Put("foo", obj, map[consensus.PublicKey]types.FileContractID{
		hk1: fc1,
		hk2: fc2,
		hk3: fc3,
		{4}: {4}, // deleted host and contract
	}); err != nil {
		t.Fatal(err)
	}

	slabs, err := db.SlabsForMigration("autopilot", -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(slabs) != 3 {
		t.Fatalf("unexpected amount of slabs to migrate, %v!=3", len(slabs))
	}

	expected := []object.SlabSlice{
		obj.Slabs[2],
		obj.Slabs[1],
		obj.Slabs[3],
	}
	if reflect.DeepEqual(slabs, expected) {
		t.Fatal("slabs are not returned in the correct order")
	}
}

// TestContractSectors is a test for the contract_sectors join table. It
// verifies that deleting contracts or sectors also cleans up the join table.
func TestContractSectors(t *testing.T) {
	os, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Create a host, contract and sector to upload to that host into the
	// given contract.
	hk1 := consensus.PublicKey{1}
	fcid1 := types.FileContractID{1}
	err = os.addTestHost(hk1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = os.AddContract(newTestContract(fcid1, hk1))
	if err != nil {
		t.Fatal(err)
	}
	sectorGood := object.Sector{
		Host: hk1,
		Root: consensus.Hash256{1},
	}

	// Prepare used contracts.
	usedContracts := map[consensus.PublicKey]types.FileContractID{
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
	if err := os.Put("foo", obj, usedContracts); err != nil {
		t.Fatal(err)
	}

	// Delete the contract.
	err = os.RemoveContract(fcid1)
	if err != nil {
		t.Fatal(err)
	}

	// Check the join table. Should be empty.
	var css []dbContractSector
	if err := os.db.Find(&css).Error; err != nil {
		t.Fatal(err)
	}
	if len(css) != 0 {
		t.Fatal("table should be empty", len(css))
	}

	// Add the contract back.
	_, err = os.AddContract(newTestContract(fcid1, hk1))
	if err != nil {
		t.Fatal(err)
	}

	// Add the object again.
	if err := os.Put("foo", obj, usedContracts); err != nil {
		t.Fatal(err)
	}

	// Delete the object.
	if err := os.Delete("foo"); err != nil {
		t.Fatal(err)
	}

	// Delete the sector.
	if err := os.db.Delete(&dbSector{Model: Model{ID: 1}}).Error; err != nil {
		t.Fatal(err)
	}
	if err := os.db.Find(&css).Error; err != nil {
		t.Fatal(err)
	}
	if len(css) != 0 {
		t.Fatal("table should be empty")
	}
}

func newTestContract(id types.FileContractID, hk consensus.PublicKey) (rhp.ContractRevision, types.Currency, uint64) {
	uc := types.UnlockConditions{
		PublicKeys:         make([]types.SiaPublicKey, 2),
		SignaturesRequired: 2,
	}
	uc.PublicKeys[1].Algorithm = types.SignatureEd25519
	uc.PublicKeys[1].Key = hk[:]

	totalCost := types.NewCurrency64(frand.Uint64n(1000))
	return rhp.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID:         id,
			UnlockConditions: uc,
		},
	}, totalCost, frand.Uint64n(100)
}

func newTestObject() (object.Object, map[consensus.PublicKey]types.FileContractID) {
	obj := object.Object{}
	usedContracts := make(map[consensus.PublicKey]types.FileContractID)

	n := frand.Intn(10)
	obj.Slabs = make([]object.SlabSlice, n)
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
