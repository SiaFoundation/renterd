package bus

import (
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"lukechampine.com/frand"
)

func TestUploadedSectorsCache(t *testing.T) {
	c := newUploadedSectorsCache()

	uID1 := newTestUploadID()
	uID2 := newTestUploadID()

	c.addUploadedSector(uID1, types.FileContractID{1}, types.Hash256{1})
	c.addUploadedSector(uID1, types.FileContractID{2}, types.Hash256{2})
	c.addUploadedSector(uID2, types.FileContractID{2}, types.Hash256{3})

	if roots1 := c.cachedSectors(types.FileContractID{1}); len(roots1) != 1 || roots1[0] != (types.Hash256{1}) {
		t.Fatal("unexpected cached sectors")
	}
	if roots2 := c.cachedSectors(types.FileContractID{2}); len(roots2) != 2 || roots2[0] != (types.Hash256{2}) || roots2[1] != (types.Hash256{3}) {
		t.Fatal("unexpected cached sectors", roots2)
	}
	if roots3 := c.cachedSectors(types.FileContractID{3}); len(roots3) != 0 {
		t.Fatal("unexpected cached sectors")
	}

	if t1, exists := c.uploads[uID1]; !exists || t1.IsZero() {
		t.Fatal("unexpected")
	}
	if t2, exists := c.uploads[uID2]; !exists || t2.IsZero() {
		t.Fatal("unexpected")
	}

	c.finishUpload(uID1)
	if roots1 := c.cachedSectors(types.FileContractID{1}); len(roots1) != 0 {
		t.Fatal("unexpected cached sectors")
	}
	if roots2 := c.cachedSectors(types.FileContractID{2}); len(roots2) != 1 || roots2[0] != (types.Hash256{3}) {
		t.Fatal("unexpected cached sectors")
	}

	c.finishUpload(uID2)
	if roots2 := c.cachedSectors(types.FileContractID{1}); len(roots2) != 0 {
		t.Fatal("unexpected cached sectors")
	}
}

func newTestUploadID() api.UploadID {
	var uID api.UploadID
	frand.Read(uID[:])
	return uID
}
