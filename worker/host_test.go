package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"lukechampine.com/frand"
)

type (
	mockHost struct {
		hk      types.PublicKey
		fcid    types.FileContractID
		sectors map[types.Hash256]*[rhpv2.SectorSize]byte
	}
)

var (
	_ Host = (*mockHost)(nil)
)

var (
	errSectorOutOfBounds = errors.New("sector out of bounds")
)

func (h *mockHost) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) error {
	sector, exist := h.sectors[root]
	if !exist {
		return errSectorNotFound
	}
	if offset+length > rhpv2.SectorSize {
		return errSectorOutOfBounds
	}
	_, err := w.Write(sector[offset : offset+length])
	return err
}

func (h *mockHost) UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) (types.Hash256, error) {
	root := rhpv2.SectorRoot(sector)
	h.sectors[root] = sector
	return root, nil
}

func (h *mockHost) FetchRevision(ctx context.Context, fetchTimeout time.Duration, blockHeight uint64) (rev types.FileContractRevision, _ error) {
	return
}

func (h *mockHost) FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error) {
	return
}

func (h *mockHost) FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error {
	return nil
}

func (h *mockHost) RenewContract(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, _ types.Currency, err error) {
	return
}

func (h *mockHost) SyncAccount(ctx context.Context, rev *types.FileContractRevision) error {
	return nil
}

func TestHost(t *testing.T) {
	h := newMockHost()
	sector, root := newMockSector()

	// upload the sector
	uploaded, err := h.UploadSector(context.Background(), sector, types.FileContractRevision{})
	if err != nil {
		t.Fatal(err)
	} else if uploaded != root {
		t.Fatal("root mismatch")
	}

	// download entire sector
	var buf bytes.Buffer
	err = h.DownloadSector(context.Background(), &buf, root, 0, rhpv2.SectorSize, false)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), sector[:]) {
		t.Fatal("sector mismatch")
	}

	// download part of the sector
	buf.Reset()
	err = h.DownloadSector(context.Background(), &buf, root, 64, 64, false)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), sector[64:128]) {
		t.Fatal("sector mismatch")
	}

	// try downloading out of bounds
	err = h.DownloadSector(context.Background(), &buf, root, rhpv2.SectorSize, 64, false)
	if !errors.Is(err, errSectorOutOfBounds) {
		t.Fatal("expected out of bounds error", err)
	}
}

func newMockHost() *mockHost {
	return &mockHost{
		hk:      types.PublicKey{1},
		fcid:    types.FileContractID{1},
		sectors: make(map[types.Hash256]*[rhpv2.SectorSize]byte),
	}
}

func newMockSector() (*[rhpv2.SectorSize]byte, types.Hash256) {
	var sector [rhpv2.SectorSize]byte
	frand.Read(sector[:])
	return &sector, rhpv2.SectorRoot(&sector)
}
