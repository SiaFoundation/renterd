package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
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
		hk types.PublicKey

		mu sync.Mutex
		c  *mockContract
	}

	mockHostManager struct {
		hosts map[types.PublicKey]Host
	}

	mockContract struct {
		rev types.FileContractRevision

		mu      sync.Mutex
		sectors map[types.Hash256]*[rhpv2.SectorSize]byte
	}

	mockContractLocker struct {
		contracts map[types.FileContractID]*mockContract
	}
)

var (
	_ Host           = (*mockHost)(nil)
	_ HostManager    = (*mockHostManager)(nil)
	_ ContractLocker = (*mockContractLocker)(nil)
)

var (
	errContractNotFound  = errors.New("contract not found")
	errSectorOutOfBounds = errors.New("sector out of bounds")
)

func (h *mockHost) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) error {
	sector, exist := h.c.sectors[root]
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
	h.c.sectors[root] = sector
	return root, nil
}

func (h *mockHost) FetchRevision(ctx context.Context, fetchTimeout time.Duration, blockHeight uint64) (rev types.FileContractRevision, _ error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rev = h.c.rev
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

func (hp *mockHostManager) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) Host {
	if _, ok := hp.hosts[hk]; !ok {
		panic("host not found")
	}
	return hp.hosts[hk]
}

func (cl *mockContractLocker) AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error) {
	if lock, ok := cl.contracts[fcid]; !ok {
		return 0, errContractNotFound
	} else {
		lock.mu.Lock()
	}

	return 0, nil
}
func (cl *mockContractLocker) ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error) {
	if lock, ok := cl.contracts[fcid]; !ok {
		return errContractNotFound
	} else {
		lock.mu.Unlock()
	}
	return nil
}

func (cl *mockContractLocker) KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error) {
	return nil
}

func TestHost(t *testing.T) {
	c := newMockContract(types.FileContractID{1})
	h := newMockHost(types.PublicKey{1}, c)
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

func newMockHost(hk types.PublicKey, c *mockContract) *mockHost {
	return &mockHost{
		hk: hk,
		c:  c,
	}
}

func newMockContract(fcid types.FileContractID) *mockContract {
	return &mockContract{
		rev:     types.FileContractRevision{ParentID: fcid},
		sectors: make(map[types.Hash256]*[rhpv2.SectorSize]byte),
	}
}

func newMockSector() (*[rhpv2.SectorSize]byte, types.Hash256) {
	var sector [rhpv2.SectorSize]byte
	frand.Read(sector[:])
	return &sector, rhpv2.SectorRoot(&sector)
}
