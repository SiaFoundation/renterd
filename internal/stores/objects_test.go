package stores

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
	"gorm.io/gorm/schema"
	"lukechampine.com/frand"
)

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

func TestList(t *testing.T) {
	es := NewEphemeralObjectStore()
	paths := []string{
		"/foo/bar",
		"/foo/bat",
		"/foo/baz/quux",
		"/foo/baz/quuz",
		"/gab/guub",
	}
	for _, path := range paths {
		es.Put(path, object.Object{})
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
		got := es.List(test.prefix)
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("\nlist: %v\ngot:  %v\nwant: %v", test.prefix, got, test.want)
		}
	}
}

func randomObject() (o object.Object) {
	n := frand.Intn(10)
	o.Slabs = make([]object.SlabSlice, n)
	o.Key = object.GenerateEncryptionKey()
	for i := range o.Slabs {
		n := uint8(frand.Uint64n(10) + 1)
		offset := uint32(frand.Uint64n(1 << 22))
		length := offset + uint32(frand.Uint64n(1<<22))
		o.Slabs[i] = object.SlabSlice{
			Slab: object.Slab{
				Key:       object.GenerateEncryptionKey(),
				MinShards: n,
				Shards:    make([]object.Sector, n*2),
			},
			Offset: offset,
			Length: length,
		}
		for j := range o.Slabs[i].Shards {
			o.Slabs[i].Shards[j].Root = frand.Entropy256()
			o.Slabs[i].Shards[j].Host = frand.Entropy256()
		}
	}
	return
}

func TestJSONObjectStore(t *testing.T) {
	dir := t.TempDir()
	os, err := NewJSONObjectStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// put an object
	obj := randomObject()
	if err := os.Put("foo", obj); err != nil {
		t.Fatal(err)
	}

	// get the object
	got, err := os.Get("foo")
	if err != nil {
		t.Fatal("object not found")
	} else if !reflect.DeepEqual(got, obj) {
		t.Fatal("objects are not equal")
	}

	// reload the store
	os, err = NewJSONObjectStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// get the object
	got, err = os.Get("foo")
	if err != nil {
		t.Fatal("object not found")
	} else if !reflect.DeepEqual(got, obj) {
		t.Fatal("objects are not equal")
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
	err = os.AddContract(c1, totalCost1, startHeight1)
	if err != nil {
		t.Fatal(err)
	}
	err = os.AddContract(c2, totalCost2, startHeight2)
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
										FCID:        fcid1,
										StartHeight: startHeight1,
										TotalCost:   totalCost1.Big(),
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
										FCID:        fcid2,
										StartHeight: startHeight2,
										TotalCost:   totalCost2.Big(),
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
		os.Put(path, object.Object{
			Key: object.GenerateEncryptionKey(),
		}, map[consensus.PublicKey]types.FileContractID{})
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

// TestSlabsForRepair tests the functionality of slabsForRepair.
func TestSlabsForRepair(t *testing.T) {
	os, _, _, err := newTestSQLStore()
	if err != nil {
		t.Fatal(err)
	}

	// Prepare public keys and contract ids for a host with a good contract
	// and a host with a bad contract.
	hkGood := consensus.PublicKey{1}
	hkBad := consensus.PublicKey{2}
	fcidGood := types.FileContractID{1}
	fcidBad := types.FileContractID{2}
	hkDeleted := consensus.PublicKey{3}
	fcidDeleted := types.FileContractID{3}

	// Create hosts.
	err = os.addTestHost(hkGood)
	if err != nil {
		t.Fatal(err)
	}
	err = os.addTestHost(hkBad)
	if err != nil {
		t.Fatal(err)
	}
	err = os.addTestHost(hkDeleted)
	if err != nil {
		t.Fatal(err)
	}

	// Prepare 3 sectors:
	// - one that is part of a good contract
	// - one that is part of a bad contract
	// - one that is part of no contract (because it failed to renew etc.)
	err = os.AddContract(newTestContract(fcidGood, hkGood))
	if err != nil {
		t.Fatal(err)
	}
	err = os.AddContract(newTestContract(fcidBad, hkBad))
	if err != nil {
		t.Fatal(err)
	}
	err = os.AddContract(newTestContract(fcidDeleted, hkDeleted))
	if err != nil {
		t.Fatal(err)
	}
	sectorGood := object.Sector{
		Host: hkGood,
		Root: consensus.Hash256{1},
	}
	sectorBad := object.Sector{
		Host: hkBad,
		Root: consensus.Hash256{2},
	}
	sectorDeleted := object.Sector{
		Host: hkDeleted,
		Root: consensus.Hash256{3},
	}

	// Prepare used contracts.
	usedContracts := map[consensus.PublicKey]types.FileContractID{
		hkGood:    fcidGood,
		hkBad:     fcidBad,
		hkDeleted: fcidDeleted,
	}

	// Create object.
	obj := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			// 2/3 sectors bad
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						sectorGood,
						sectorBad,
						sectorDeleted,
					},
				},
			},
			// 1/3 sectors bad
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						sectorGood,
						sectorGood,
						sectorDeleted,
					},
				},
			},
			// 2/3 sectors bad
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						sectorBad,
						sectorBad,
						sectorGood,
					},
				},
			},
			// 2/3 sectors bad
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						sectorBad,
						sectorDeleted,
						sectorGood,
					},
				},
			},
			// 2/3 sectors bad
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						sectorDeleted,
						sectorDeleted,
						sectorGood,
					},
				},
			},
			// 3/3 sectors bad
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []object.Sector{
						sectorBad,
						sectorBad,
						sectorDeleted,
					},
				},
			},
		},
	}
	if err := os.Put("foo", obj, usedContracts); err != nil {
		t.Fatal(err)
	}

	// Only consider fcidGood and fcidDeleted good contracts.
	goodContracts := []types.FileContractID{fcidGood, fcidDeleted}

	// Delete the contract.
	err = os.RemoveContract(fcidDeleted)
	if err != nil {
		t.Fatal(err)
	}

	// Count the shards. Every slab should have 3 shards associated with it
	// which makes 18 in total.
	var sslabs []dbShard
	if err := os.db.Find(&sslabs).Error; err != nil {
		t.Fatal(err)
	}
	if len(sslabs) != len(obj.Slabs)*3 {
		t.Fatalf("wrong length %v != %v", len(sslabs), len(obj.Slabs)*3)
	}

	// Make sure the slab IDs are returned in the right order.
	// 5 first since it doesn't have any good sectors
	// 1 last since it only got 1 bad sector
	// 0, 2, 3, 4 in the middle since they all have 2 bad sectors.
	expectedSlabs := []object.Slab{
		obj.Slabs[5].Slab,
		obj.Slabs[0].Slab,
		obj.Slabs[2].Slab,
		obj.Slabs[3].Slab,
		obj.Slabs[4].Slab,
		obj.Slabs[1].Slab,
	}
	for i := range expectedSlabs {
		// Check the i worst slabs.
		slabs, err := os.SlabsForMigration(i+1, time.Now(), goodContracts)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := json.MarshalIndent(slabs, "", "  ")
		exp, _ := json.MarshalIndent(expectedSlabs[:i+1], "", "  ")
		if string(got) != string(exp) {
			t.Fatalf("wrong slabs returned: %v != %v", string(got), string(exp))
		}
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
	err = os.AddContract(newTestContract(fcid1, hk1))
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
	err = os.AddContract(newTestContract(fcid1, hk1))
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
