package stores

import (
	"os"
	"reflect"
	"testing"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/slab"
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

// TestSQLPut verifies the functionality of (*SQLObjectStore).Put.
func TestSQLObjectStorePut(t *testing.T) {
	//dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db
	dbName := "/Users/cschinnerl/Desktop/db2.sqlite"

	//conn := NewEphemeralSQLiteConnection(dbName)
	os.RemoveAll(dbName)
	conn := NewSQLiteConnection(dbName)
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
	if err := os.Put("key1", obj1, false); err != nil {
		t.Fatal(err)
	}

	// Try to store it again. Should work.
	if err := os.Put("key1", obj1, true); err != nil {
		t.Fatal(err)
	}
}
