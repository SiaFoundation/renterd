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
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
	"lukechampine.com/frand"
)

type (
	testHost struct {
		*hostMock
		*contractMock
		hptFn       func() api.HostPriceTable
		uploadDelay time.Duration
	}

	testHostManager struct {
		tt test.TT

		mu    sync.Mutex
		hosts map[types.PublicKey]*testHost
	}
)

func newTestHostManager(t test.TestingCommon) *testHostManager {
	return &testHostManager{tt: test.NewTT(t), hosts: make(map[types.PublicKey]*testHost)}
}

func (hm *testHostManager) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) Host {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if _, ok := hm.hosts[hk]; !ok {
		hm.tt.Fatal("host not found")
	}
	return hm.hosts[hk]
}

func (hm *testHostManager) addHost(h *testHost) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.hosts[h.hk] = h
}

func newTestHost(h *hostMock, c *contractMock) *testHost {
	return newTestHostCustom(h, c, newTestHostPriceTable)
}

func newTestHostCustom(h *hostMock, c *contractMock, hptFn func() api.HostPriceTable) *testHost {
	return &testHost{
		hostMock:     h,
		contractMock: c,
		hptFn:        hptFn,
	}
}

func newTestHostPriceTable() api.HostPriceTable {
	var uid rhpv3.SettingsID
	frand.Read(uid[:])

	return api.HostPriceTable{
		HostPriceTable: rhpv3.HostPriceTable{UID: uid, HostBlockHeight: 100, Validity: time.Minute},
		Expiry:         time.Now().Add(time.Minute),
	}
}

func (h *testHost) PublicKey() types.PublicKey {
	return h.hk
}

func (h *testHost) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) error {
	sector, exist := h.Sector(root)
	if !exist {
		return errSectorNotFound
	}
	if offset+length > rhpv2.SectorSize {
		return errSectorOutOfBounds
	}
	_, err := w.Write(sector[offset : offset+length])
	return err
}

func (h *testHost) UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) error {
	h.AddSector(sectorRoot, sector)
	if h.uploadDelay > 0 {
		select {
		case <-time.After(h.uploadDelay):
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return nil
}

func (h *testHost) FetchRevision(ctx context.Context, fetchTimeout time.Duration) (rev types.FileContractRevision, _ error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rev = h.rev
	return rev, nil
}

func (h *testHost) FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (api.HostPriceTable, error) {
	return h.hptFn(), nil
}

func (h *testHost) FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error {
	return nil
}

func (h *testHost) RenewContract(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, _ types.Currency, err error) {
	return rhpv2.ContractRevision{}, nil, types.ZeroCurrency, nil
}

func (h *testHost) SyncAccount(ctx context.Context, rev *types.FileContractRevision) error {
	return nil
}

func TestHost(t *testing.T) {
	// create test host
	h := newTestHost(
		newHostMock(types.PublicKey{1}),
		newContractMock(types.PublicKey{1}, types.FileContractID{1}),
	)

	// upload the sector
	sector, root := newTestSector()
	err := h.UploadSector(context.Background(), root, sector, types.FileContractRevision{})
	if err != nil {
		t.Fatal(err)
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
