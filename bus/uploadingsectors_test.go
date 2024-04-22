package bus

import (
	"errors"
	"testing"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"lukechampine.com/frand"
)

func TestUploadingSectorsCache(t *testing.T) {
	c := newUploadingSectorsCache()

	uID1 := newTestUploadID()
	uID2 := newTestUploadID()

	fcid1 := types.FileContractID{1}
	fcid2 := types.FileContractID{2}
	fcid3 := types.FileContractID{3}

	c.StartUpload(uID1)
	c.StartUpload(uID2)

	_ = c.AddSector(uID1, fcid1, types.Hash256{1})
	_ = c.AddSector(uID1, fcid2, types.Hash256{2})
	_ = c.AddSector(uID2, fcid2, types.Hash256{3})

	if roots1 := c.Sectors(fcid1); len(roots1) != 1 || roots1[0] != (types.Hash256{1}) {
		t.Fatal("unexpected cached sectors")
	}
	if roots2 := c.Sectors(fcid2); len(roots2) != 2 {
		t.Fatal("unexpected cached sectors", roots2)
	}
	if roots3 := c.Sectors(fcid3); len(roots3) != 0 {
		t.Fatal("unexpected cached sectors")
	}

	if o1, exists := c.uploads[uID1]; !exists || o1.started.IsZero() {
		t.Fatal("unexpected")
	}
	if o2, exists := c.uploads[uID2]; !exists || o2.started.IsZero() {
		t.Fatal("unexpected")
	}

	c.FinishUpload(uID1)
	if roots1 := c.Sectors(fcid1); len(roots1) != 0 {
		t.Fatal("unexpected cached sectors")
	}
	if roots2 := c.Sectors(fcid2); len(roots2) != 1 || roots2[0] != (types.Hash256{3}) {
		t.Fatal("unexpected cached sectors")
	}

	c.FinishUpload(uID2)
	if roots2 := c.Sectors(fcid1); len(roots2) != 0 {
		t.Fatal("unexpected cached sectors")
	}

	if err := c.AddSector(uID1, fcid1, types.Hash256{1}); !errors.Is(err, api.ErrUnknownUpload) {
		t.Fatal("unexpected error", err)
	}
	if err := c.StartUpload(uID1); err != nil {
		t.Fatal("unexpected error", err)
	}
	if err := c.StartUpload(uID1); !errors.Is(err, api.ErrUploadAlreadyExists) {
		t.Fatal("unexpected error", err)
	}

	// reset cache
	c = newUploadingSectorsCache()

	// track upload that uploads across two contracts
	c.StartUpload(uID1)
	c.AddSector(uID1, fcid1, types.Hash256{1})
	c.AddSector(uID1, fcid1, types.Hash256{2})
	c.HandleRenewal(fcid2, fcid1)
	c.AddSector(uID1, fcid2, types.Hash256{3})
	c.AddSector(uID1, fcid2, types.Hash256{4})

	// assert pending sizes for both contracts should be 4 sectors
	p1 := c.Pending(fcid1)
	p2 := c.Pending(fcid2)
	if p1 != p2 || p1 != 4*rhpv2.SectorSize {
		t.Fatal("unexpected pending size", p1/rhpv2.SectorSize, p2/rhpv2.SectorSize)
	}

	// assert sectors for both contracts contain 4 sectors
	s1 := c.Sectors(fcid1)
	s2 := c.Sectors(fcid2)
	if len(s1) != 4 || len(s2) != 4 {
		t.Fatal("unexpected sectors", len(s1), len(s2))
	}

	// finish upload
	c.FinishUpload(uID1)
	s1 = c.Sectors(fcid1)
	s2 = c.Sectors(fcid2)
	if len(s1) != 0 || len(s2) != 0 {
		t.Fatal("unexpected sectors", len(s1), len(s2))
	}

	// renew the contract
	c.HandleRenewal(fcid3, fcid2)

	// trigger pruning
	c.StartUpload(uID2)
	c.FinishUpload(uID2)

	// assert renewedTo gets pruned
	if len(c.renewedTo) != 1 {
		t.Fatal("unexpected", len(c.renewedTo))
	}
}

func newTestUploadID() api.UploadID {
	var uID api.UploadID
	frand.Read(uID[:])
	return uID
}
