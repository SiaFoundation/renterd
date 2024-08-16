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
	sc := NewSectorsCache()

	uID1 := newTestUploadID()
	uID2 := newTestUploadID()

	fcid1 := types.FileContractID{1}
	fcid2 := types.FileContractID{2}
	fcid3 := types.FileContractID{3}

	sc.StartUpload(uID1)
	sc.StartUpload(uID2)

	_ = sc.AddSector(uID1, fcid1, types.Hash256{1})
	_ = sc.AddSector(uID1, fcid2, types.Hash256{2})
	_ = sc.AddSector(uID2, fcid2, types.Hash256{3})

	if roots1 := sc.Sectors(fcid1); len(roots1) != 1 || roots1[0] != (types.Hash256{1}) {
		t.Fatal("unexpected cached sectors")
	}
	if roots2 := sc.Sectors(fcid2); len(roots2) != 2 {
		t.Fatal("unexpected cached sectors", roots2)
	}
	if roots3 := sc.Sectors(fcid3); len(roots3) != 0 {
		t.Fatal("unexpected cached sectors")
	}

	if o1, exists := sc.uploads[uID1]; !exists || o1.started.IsZero() {
		t.Fatal("unexpected")
	}
	if o2, exists := sc.uploads[uID2]; !exists || o2.started.IsZero() {
		t.Fatal("unexpected")
	}

	sc.FinishUpload(uID1)
	if roots1 := sc.Sectors(fcid1); len(roots1) != 0 {
		t.Fatal("unexpected cached sectors")
	}
	if roots2 := sc.Sectors(fcid2); len(roots2) != 1 || roots2[0] != (types.Hash256{3}) {
		t.Fatal("unexpected cached sectors")
	}

	sc.FinishUpload(uID2)
	if roots2 := sc.Sectors(fcid1); len(roots2) != 0 {
		t.Fatal("unexpected cached sectors")
	}

	if err := sc.AddSector(uID1, fcid1, types.Hash256{1}); !errors.Is(err, api.ErrUnknownUpload) {
		t.Fatal("unexpected error", err)
	}
	if err := sc.StartUpload(uID1); err != nil {
		t.Fatal("unexpected error", err)
	}
	if err := sc.StartUpload(uID1); !errors.Is(err, api.ErrUploadAlreadyExists) {
		t.Fatal("unexpected error", err)
	}

	// reset cache
	sc = NewSectorsCache()

	// track upload that uploads across two contracts
	sc.StartUpload(uID1)
	sc.AddSector(uID1, fcid1, types.Hash256{1})
	sc.AddSector(uID1, fcid1, types.Hash256{2})
	sc.HandleRenewal(fcid2, fcid1)
	sc.AddSector(uID1, fcid2, types.Hash256{3})
	sc.AddSector(uID1, fcid2, types.Hash256{4})

	// assert pending sizes for both contracts should be 4 sectors
	p1 := sc.Pending(fcid1)
	p2 := sc.Pending(fcid2)
	if p1 != p2 || p1 != 4*rhpv2.SectorSize {
		t.Fatal("unexpected pending size", p1/rhpv2.SectorSize, p2/rhpv2.SectorSize)
	}

	// assert sectors for both contracts contain 4 sectors
	s1 := sc.Sectors(fcid1)
	s2 := sc.Sectors(fcid2)
	if len(s1) != 4 || len(s2) != 4 {
		t.Fatal("unexpected sectors", len(s1), len(s2))
	}

	// finish upload
	sc.FinishUpload(uID1)
	s1 = sc.Sectors(fcid1)
	s2 = sc.Sectors(fcid2)
	if len(s1) != 0 || len(s2) != 0 {
		t.Fatal("unexpected sectors", len(s1), len(s2))
	}

	// renew the contract
	sc.HandleRenewal(fcid3, fcid2)

	// trigger pruning
	sc.StartUpload(uID2)
	sc.FinishUpload(uID2)

	// assert renewedTo gets pruned
	if len(sc.renewedTo) != 1 {
		t.Fatal("unexpected", len(sc.renewedTo))
	}
}

func newTestUploadID() api.UploadID {
	var uID api.UploadID
	frand.Read(uID[:])
	return uID
}
