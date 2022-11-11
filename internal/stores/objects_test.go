package stores

import (
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"testing"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/slab"
	"gorm.io/gorm/schema"
	"lukechampine.com/frand"
)

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
	o.Slabs = make([]slab.Slice, n)
	o.Key = object.GenerateEncryptionKey()
	for i := range o.Slabs {
		n := uint8(frand.Uint64n(10) + 1)
		offset := uint32(frand.Uint64n(1 << 22))
		length := offset + uint32(frand.Uint64n(1<<22))
		o.Slabs[i] = slab.Slice{
			Slab: slab.Slab{
				Key:       slab.GenerateEncryptionKey(),
				MinShards: n,
				Shards:    make([]slab.Sector, n*2),
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
	//dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db
	dbName := "test.db"

	//conn := NewEphemeralSQLiteConnection(dbName)
	os.RemoveAll(dbName)
	conn := NewSQLiteConnection(dbName)
	os, err := NewSQLObjectStore(conn, true)
	if err != nil {
		t.Fatal(err)
	}

	// Create some sectors.
	sector1 := dbSector{
		Root: []byte{1, 2},
	}
	sector2 := dbSector{
		Root: []byte{2, 1},
	}

	err = os.db.Create(&dbSlab{
		Key:       "foo1",
		MinShards: 10,
		Shards: []dbShard{
			{
				Sector: sector1,
			},
			{
				Sector: sector2,
			},
		},
	}).Error
	if err != nil {
		t.Fatal(err)
	}

	err = os.db.Create(&dbSlab{
		Key:       "foo2",
		MinShards: 10,
		Shards: []dbShard{
			{
				Sector: sector1,
			},
			{
				Sector: sector2,
			},
		},
	}).Error
	if err != nil {
		t.Fatal(err)
	}

}

// TestSQLPut verifies the functionality of (*SQLObjectStore).Put and Get.
func TestSQLObjectStorePut(t *testing.T) {
	dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db

	conn := NewEphemeralSQLiteConnection(dbName)
	os, err := NewSQLObjectStore(conn, true)
	if err != nil {
		t.Fatal(err)
	}

	// Create an object with 2 slabs pointing to 2 different sectors.
	obj1 := object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []slab.Slice{
			{
				Slab: slab.Slab{
					Key:       slab.GenerateEncryptionKey(),
					MinShards: 1,
					Shards: []slab.Sector{
						{
							Host: consensus.GeneratePrivateKey().PublicKey(),
							Root: consensus.Hash256{1},
						},
					},
				},
				Offset: 10,
				Length: 100,
			},
			{
				Slab: slab.Slab{
					Key:       slab.GenerateEncryptionKey(),
					MinShards: 2,
					Shards: []slab.Sector{
						{
							Host: consensus.GeneratePrivateKey().PublicKey(),
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
	if err := os.Put(objID, obj1); err != nil {
		t.Fatal(err)
	}

	// Try to store it again. Should work.
	if err := os.Put(objID, obj1); err != nil {
		t.Fatal(err)
	}

	// Fetch it using get and verify every field.
	obj, err := os.get(objID)
	if err != nil {
		t.Fatal(err)
	}

	expectedObj := dbObject{
		ID:  objID,
		Key: obj1.Key.String(),
		Slabs: []dbSlice{
			{
				ID:       1,
				ObjectID: objID,
				Slab: dbSlab{
					ID:        1,
					Key:       obj1.Slabs[0].Key.String(),
					MinShards: 1,
					Shards: []dbShard{
						{
							ID:       1,
							SlabID:   1,
							SectorID: obj1.Slabs[0].Shards[0].Root[:],
							Sector: dbSector{
								Root: obj1.Slabs[0].Shards[0].Root[:],
								Host: obj1.Slabs[0].Shards[0].Host[:],
							},
						},
					},
				},
				Offset: 10,
				Length: 100,
			},
			{
				ID:       2,
				ObjectID: objID,
				Slab: dbSlab{
					ID:        2,
					Key:       obj1.Slabs[1].Key.String(),
					MinShards: 2,
					Shards: []dbShard{
						{
							ID:       2,
							SlabID:   2,
							SectorID: obj1.Slabs[1].Shards[0].Root[:],
							Sector: dbSector{
								Root: obj1.Slabs[1].Shards[0].Root[:],
								Host: obj1.Slabs[1].Shards[0].Host[:],
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
	if err := os.Put(objID, obj1); err != nil {
		t.Fatal(err)
	}
	fullObj, err = os.Get(objID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(fullObj, obj1) {
		fmt.Println(fullObj)
		fmt.Println(obj1)
		t.Fatal("object mismatch")
	}

	// Sanity check the db at the end of the test. We expect:
	// - 1 element in the object table since we only stored and overwrote a single object
	// - 1 element in the slabs table since we updated the object to only have 1 slab
	// - 1 element in the slices table for the same reason
	// - 2 elements in the sectors table because we don't delete sectors
	countCheck := func(objCount, sliceCount, slabCount, shardCount, sectorCount int64) error {
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
		if err := tableCountCheck(&dbShard{}, shardCount); err != nil {
			return err
		}
		if err := tableCountCheck(&dbSector{}, sectorCount); err != nil {
			return err
		}
		return nil
	}
	if err := countCheck(1, 1, 1, 1, 2); err != nil {
		t.Fatal(err)
	}

	// Delete the object. Due to the cascade this should delete everything
	// but the sectors.
	if err := os.Delete(objID); err != nil {
		t.Fatal(err)
	}
	if err := countCheck(0, 0, 0, 0, 2); err != nil {
		t.Fatal(err)
	}
}

// TestSQLList is a test for (*SQLObjectStore).List.
func TestSQLList(t *testing.T) {
	//dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db
	dbName := "/Users/cschinnerl/Desktop/test.sqlite"
	//conn := NewEphemeralSQLiteConnection(dbName)
	conn := NewSQLiteConnection(dbName)
	os, err := NewSQLObjectStore(conn, true)
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
		})
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
