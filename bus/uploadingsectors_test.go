package bus

import (
	"errors"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"lukechampine.com/frand"
)

func TestUploadingSectorsCache(t *testing.T) {
	c := newUploadingSectorsCache()

	uID1 := newTestUploadID()
	uID2 := newTestUploadID()

	c.trackUpload(uID1)
	c.trackUpload(uID2)

	_ = c.addUploadingSector(uID1, types.FileContractID{1}, types.Hash256{1})
	_ = c.addUploadingSector(uID1, types.FileContractID{2}, types.Hash256{2})
	_ = c.addUploadingSector(uID2, types.FileContractID{2}, types.Hash256{3})

	if roots1 := c.sectors(types.FileContractID{1}); len(roots1) != 1 || roots1[0] != (types.Hash256{1}) {
		t.Fatal("unexpected cached sectors")
	}
	if roots2 := c.sectors(types.FileContractID{2}); len(roots2) != 2 {
		t.Fatal("unexpected cached sectors", roots2)
	}
	if roots3 := c.sectors(types.FileContractID{3}); len(roots3) != 0 {
		t.Fatal("unexpected cached sectors")
	}

	if o1, exists := c.uploads[uID1]; !exists || o1.started.IsZero() {
		t.Fatal("unexpected")
	}
	if o2, exists := c.uploads[uID2]; !exists || o2.started.IsZero() {
		t.Fatal("unexpected")
	}

	c.finishUpload(uID1)
	if roots1 := c.sectors(types.FileContractID{1}); len(roots1) != 0 {
		t.Fatal("unexpected cached sectors")
	}
	if roots2 := c.sectors(types.FileContractID{2}); len(roots2) != 1 || roots2[0] != (types.Hash256{3}) {
		t.Fatal("unexpected cached sectors")
	}

	c.finishUpload(uID2)
	if roots2 := c.sectors(types.FileContractID{1}); len(roots2) != 0 {
		t.Fatal("unexpected cached sectors")
	}

	if err := c.addUploadingSector(uID1, types.FileContractID{1}, types.Hash256{1}); !errors.Is(err, api.ErrUnknownUpload) {
		t.Fatal("unexpected error", err)
	}
	if err := c.trackUpload(uID1); err != nil {
		t.Fatal("unexpected error", err)
	}
	if err := c.trackUpload(uID1); !errors.Is(err, api.ErrUploadAlreadyExists) {
		t.Fatal("unexpected error", err)
	}
}

func newTestUploadID() api.UploadID {
	var uID api.UploadID
	frand.Read(uID[:])
	return uID
}
