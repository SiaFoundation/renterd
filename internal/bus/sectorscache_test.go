package bus

import (
	"errors"
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"lukechampine.com/frand"
)

func TestUploadingSectorsCache(t *testing.T) {
	sc := NewSectorsCache()

	uID1 := newTestUploadID()
	uID2 := newTestUploadID()

	sc.StartUpload(uID1)
	sc.StartUpload(uID2)

	_ = sc.AddSectors(uID1, types.Hash256{1})
	_ = sc.AddSectors(uID1, types.Hash256{2})
	_ = sc.AddSectors(uID2, types.Hash256{3})

	assertSectors := func(uID api.UploadID, expected []types.Hash256) {
		t.Helper()
		if ou, exists := sc.uploads[uID]; !exists {
			t.Fatal("upload doesn't exist")
		} else if len(ou.sectors) != len(expected) {
			t.Fatalf("unexpected num of sectors: %v %v", len(ou.sectors), len(expected))
		} else if !reflect.DeepEqual(ou.sectors, expected) {
			t.Fatal("wrong sectors")
		}
	}

	assertSectors(uID1, []types.Hash256{{1}, {2}})
	assertSectors(uID2, []types.Hash256{{3}})
	if !reflect.DeepEqual(sc.Sectors(), []types.Hash256{{1}, {2}, {3}}) {
		t.Fatal("wrong sectors")
	}
	if o1, exists := sc.uploads[uID1]; !exists || o1.started.IsZero() {
		t.Fatal("unexpected")
	}
	if o2, exists := sc.uploads[uID2]; !exists || o2.started.IsZero() {
		t.Fatal("unexpected")
	}

	sc.FinishUpload(uID1)
	if _, exists := sc.uploads[uID1]; exists {
		t.Fatal("unexpected")
	}
	sc.FinishUpload(uID2)
	if _, exists := sc.uploads[uID2]; exists {
		t.Fatal("unexpected")
	}

	if err := sc.AddSectors(uID1, types.Hash256{1}); !errors.Is(err, api.ErrUnknownUpload) {
		t.Fatal("unexpected error", err)
	}
	if err := sc.StartUpload(uID1); err != nil {
		t.Fatal("unexpected error", err)
	}
	if err := sc.StartUpload(uID1); !errors.Is(err, api.ErrUploadAlreadyExists) {
		t.Fatal("unexpected error", err)
	}
	if len(sc.Sectors()) != 0 {
		t.Fatal("shouldn't have any sectors")
	}
}

func newTestUploadID() api.UploadID {
	var uID api.UploadID
	frand.Read(uID[:])
	return uID
}
