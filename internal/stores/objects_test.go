package stores

import (
	"encoding/hex"
	"reflect"
	"testing"

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
	dbName := hex.EncodeToString(frand.Bytes(32)) // random name for db

	conn := NewEphemeralSQLiteConnection(dbName)
	_, err := NewSQLObjectStore(conn, true)
	if err != nil {
		t.Fatal(err)
	}
}
