package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/host"
	"go.sia.tech/renterd/v2/internal/test"
	"go.sia.tech/renterd/v2/internal/test/mocks"
	"lukechampine.com/frand"
)

type (
	testHost struct {
		*mocks.Host
		*mocks.Contract
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

func (hm *testHostManager) Downloader(hi api.HostInfo) host.Downloader {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if _, ok := hm.hosts[hi.PublicKey]; !ok {
		hm.tt.Fatal("host not found")
	}
	return hm.hosts[hi.PublicKey]
}

func (hm *testHostManager) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) host.Host {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if _, ok := hm.hosts[hk]; !ok {
		hm.tt.Fatal("host not found")
	}
	return hm.hosts[hk]
}

func (hm *testHostManager) Uploader(hi api.HostInfo, _ types.FileContractID) host.Uploader {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	if _, ok := hm.hosts[hi.PublicKey]; !ok {
		hm.tt.Fatal("host not found")
	}
	return hm.hosts[hi.PublicKey]
}

func (hm *testHostManager) addHost(h *testHost) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.hosts[h.PublicKey()] = h
}

func newTestHost(h *mocks.Host, c *mocks.Contract) *testHost {
	return newTestHostCustom(h, c, newTestHostPrices)
}

func newTestHostCustom(h *mocks.Host, c *mocks.Contract, pFn func() rhpv4.HostPrices) *testHost {
	return &testHost{
		Host:     h,
		Contract: c,
		pFn:      pFn,
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

func (h *testHost) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) error {
	sector, exist := h.Contract.Sector(root)
	if !exist {
		return rhpv4.ErrSectorNotFound
	}
	if offset+length > rhpv4.SectorSize {
		return mocks.ErrSectorOutOfBounds
	}
	_, err := w.Write(sector[offset : offset+length])
	return err
}

func (h *testHost) UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv4.SectorSize]byte) error {
	h.Contract.AddSector(sectorRoot, sector)
	if h.uploadDelay > 0 {
		select {
		case <-time.After(h.uploadDelay):
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return nil
}

func (h *testHost) FetchRevision(ctx context.Context, fcid types.FileContractID) (rev types.FileContractRevision, _ error) {
	return h.Contract.Revision(), nil
}

func (h *testHost) Prices(ctx context.Context) (rhpv4.HostPrices, error) {
	return h.pFn(), nil
}

func (h *testHost) FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error {
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
	err := h.UploadSector(context.Background(), root, sector)
	if err != nil {
		t.Fatal(err)
	}

	// download entire sector
	var buf bytes.Buffer
	err = h.DownloadSector(context.Background(), &buf, root, 0, rhpv4.SectorSize)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), sector[:]) {
		t.Fatal("sector mismatch")
	}

	// download part of the sector
	buf.Reset()
	err = h.DownloadSector(context.Background(), &buf, root, 64, 64)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), sector[64:128]) {
		t.Fatal("sector mismatch")
	}

	// try downloading out of bounds
	err = h.DownloadSector(context.Background(), &buf, root, rhpv4.SectorSize, 64)
	if !errors.Is(err, mocks.ErrSectorOutOfBounds) {
		t.Fatal("expected out of bounds error", err)
	}
}
