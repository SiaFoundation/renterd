package stores

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/object"
	"gorm.io/gorm/schema"
	"lukechampine.com/frand"
)

// TestSQLObjectStore tests basic SQLObjectStore functionality.
func TestSQLObjectStore(t *testing.T) {
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
	objID := "key1"
	if err := db.Put(objID, obj1, usedHosts); err != nil {
		t.Fatal(err)
	}

	// Try to store it again. Should work.
	if err := db.Put(objID, obj1, usedHosts); err != nil {
		t.Fatal(err)
	}

	// Fetch it using get and verify every field.
	obj, err := db.get(objID)
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
								Root:       obj1.Slabs[0].Shards[0].Root,
								LatestHost: obj1.Slabs[0].Shards[0].Host,
								Contracts: []dbContract{
									{
										HostID: 1,
										Host: dbHost{
											PublicKey: hk1,
										},
										FCID:                fcid1,
										StartHeight:         startHeight1,
										TotalCost:           totalCost1,
										UploadSpending:      types.ZeroCurrency,
										DownloadSpending:    types.ZeroCurrency,
										FundAccountSpending: types.ZeroCurrency,
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
								Root:       obj1.Slabs[1].Shards[0].Root,
								LatestHost: obj1.Slabs[1].Shards[0].Host,
								Contracts: []dbContract{
									{
										HostID: 2,
										Host: dbHost{
											PublicKey: hk2,
										},
										FCID:                fcid2,
										StartHeight:         startHeight2,
										TotalCost:           totalCost2,
										UploadSpending:      types.ZeroCurrency,
										DownloadSpending:    types.ZeroCurrency,
										FundAccountSpending: types.ZeroCurrency,
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

	// Fetch it using Get and verify again.
	fullObj, err := db.Get(objID)
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
	if err := db.Put(objID, obj1, usedHosts); err != nil {
		t.Fatal(err)
	}
	fullObj, err = db.Get(objID)
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
	if err := db.Delete(objID); err != nil {
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
		obj, ucs := newTestObject(frand.Intn(10))
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

	if err := db.Put("foo", obj, map[types.PublicKey]types.FileContractID{
		hk1: fcid1,
		hk2: fcid2,
		hk3: fcid3,
		hk4: fcid4,
		{5}: {5}, // deleted host and contract
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.PrepareSlabsForMigration(goodContracts); err != nil {
		t.Fatal(err)
	}
	slabs, err := db.SlabsForMigration(0, -1)
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
	if err := db.Put("foo", obj, usedContracts); err != nil {
		t.Fatal(err)
	}

	// Delete the contract.
	err = db.RemoveContract(fcid1)
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
	if err := db.Put("foo", obj, usedContracts); err != nil {
		t.Fatal(err)
	}

	// Delete the object.
	if err := db.Delete("foo"); err != nil {
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
	if err := db.Put("foo", obj, map[types.PublicKey]types.FileContractID{
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
	contractIds := func(contracts []dbContract) (ids []types.FileContractID) {
		for _, c := range contracts {
			ids = append(ids, c.FCID)
		}
		return
	}

	// helper to extract the hostkey from a list of hosts
	hostKeys := func(hosts []dbHost) (ids []types.PublicKey) {
		for _, h := range hosts {
			ids = append(ids, h.PublicKey)
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

	// fetch slabs for migration and assert there is only one
	if err := db.PrepareSlabsForMigration(goodContracts); err != nil {
		t.Fatal(err)
	}
	toMigrate, err := db.SlabsForMigration(0, -1)
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
	err = db.PutSlab(slab, map[types.PublicKey]types.FileContractID{
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
	} else if cids[0] != fcid1 {
		t.Fatal("sector 1 was uploaded to unexpected contract", cids[0])
	} else if hks := hostKeys(updated.Shards[0].DBSector.Hosts); len(hks) != 1 {
		t.Fatalf("sector 1 was uploaded to unexpected amount of hosts, %v!=1", len(hks))
	} else if hks[0] != hk1 {
		t.Fatal("sector 1 was uploaded to unexpected host", hks[0])
	}

	// assert the second sector however is uploaded to two hosts, assert it's h2 and h3
	if cids := contractIds(updated.Shards[1].DBSector.Contracts); len(cids) != 2 {
		t.Fatalf("sector 1 was uploaded to unexpected amount of contracts, %v!=2", len(cids))
	} else if cids[0] != fcid2 || cids[1] != fcid3 {
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
	if err := db.PrepareSlabsForMigration(goodContracts); err != nil {
		t.Fatal(err)
	}
	toMigrate, err = db.SlabsForMigration(0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(toMigrate) != 0 {
		t.Fatal("unexpected number of slabs to migrate", len(toMigrate))
	}

	if obj, err := db.get("foo"); err != nil {
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
