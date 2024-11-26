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
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/host"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/internal/test/mocks"
	"lukechampine.com/frand"
)

type (
	testHost struct {
		*mocks.Host
		*mocks.Contract
		hptFn       func() api.HostPriceTable
		pFn         func() rhpv4.HostPrices
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

func (hm *testHostManager) Downloader(hk types.PublicKey, siamuxAddr string) host.Downloader {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if _, ok := hm.hosts[hk]; !ok {
		hm.tt.Fatal("host not found")
	}
	return hm.hosts[hk]
}

func (hm *testHostManager) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) host.Host {
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
	hm.hosts[h.PublicKey()] = h
}

func newTestHost(h *mocks.Host, c *mocks.Contract) *testHost {
	return newTestHostCustom(h, c, newTestHostPriceTable, newTestHostPrices)
}

func newTestHostCustom(h *mocks.Host, c *mocks.Contract, hptFn func() api.HostPriceTable, pFn func() rhpv4.HostPrices) *testHost {
	return &testHost{
		Host:     h,
		Contract: c,
		pFn:      pFn,
		hptFn:    hptFn,
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

func newTestHostPrices() rhpv4.HostPrices {
	var sig types.Signature
	frand.Read(sig[:])

	return rhpv4.HostPrices{
		TipHeight:  100,
		ValidUntil: time.Now().Add(time.Minute),
		Signature:  sig,
	}
}

func (h *testHost) PublicKey() types.PublicKey {
	return h.Host.PublicKey()
}

func (h *testHost) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) error {
	sector, exist := h.Sector(root)
	if !exist {
		return rhp3.ErrSectorNotFound
	}
	if offset+length > rhpv2.SectorSize {
		return mocks.ErrSectorOutOfBounds
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
	return h.Contract.Revision(), nil
}

func (h *testHost) PriceTable(ctx context.Context, rev *types.FileContractRevision) (api.HostPriceTable, types.Currency, error) {
	return h.hptFn(), types.ZeroCurrency, nil
}

func (h *testHost) Prices(ctx context.Context) (rhpv4.HostPrices, error) {
	return h.pFn(), nil
}

func (h *testHost) PriceTableUnpaid(ctx context.Context) (api.HostPriceTable, error) {
	return h.hptFn(), nil
}

func (h *testHost) FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error {
	return nil
}

func (h *testHost) SyncAccount(ctx context.Context, rev *types.FileContractRevision) error {
	return nil
}

func TestHost(t *testing.T) {
	// create test host
	h := newTestHost(
		mocks.NewHost(types.PublicKey{1}),
		mocks.NewContract(types.PublicKey{1}, types.FileContractID{1}),
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
	if !errors.Is(err, mocks.ErrSectorOutOfBounds) {
		t.Fatal("expected out of bounds error", err)
	}
}
