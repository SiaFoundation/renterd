package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
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

func (h *mockHost) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) error {
	sector, exist := h.sectors[root]
	if !exist {
		return errors.New("sector not found")
	}
	if offset+length > rhpv2.SectorSize {
		return errors.New("sector out of bounds")
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
	h := newTestHost()
	s := newTestSector()

	root, err := h.UploadSector(context.Background(), s, types.FileContractRevision{})
	if err != nil {
		t.Fatal(err)
	}

	// download entire sector
	var buf bytes.Buffer
	err = h.DownloadSector(context.Background(), &buf, root, 0, rhpv2.SectorSize, false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), s[:]) {
		t.Fatal("sector mismatch")
	}

	// download part of the sector
	buf.Reset()
	err = h.DownloadSector(context.Background(), &buf, root, 64, 64, false)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), s[64:128]) {
		t.Fatal("sector mismatch")
	}

	// try downloading out of bounds
	err = h.DownloadSector(context.Background(), &buf, root, rhpv2.SectorSize, 64, false)
	if err == nil || !strings.Contains(err.Error(), "out of bounds") {
		t.Fatal("expected out of bounds error", err)
	}
}

func newTestHost() Host {
	return &mockHost{
		hk:      types.PublicKey{1},
		fcid:    types.FileContractID{1},
		sectors: make(map[types.Hash256]*[rhpv2.SectorSize]byte),
	}
}

func newTestSector() *[rhpv2.SectorSize]byte {
	var sector [rhpv2.SectorSize]byte
	frand.Read(sector[:])
	return &sector
}
