package mocks

import (
	"context"
	"errors"
	"io"
	"sync"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/host"
)

type HostStore struct {
	mu     sync.Mutex
	hosts  map[types.PublicKey]*Host
	hkCntr uint
}

func NewHostStore() *HostStore {
	return &HostStore{hosts: make(map[types.PublicKey]*Host)}
}

func (hs *HostStore) Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	h, ok := hs.hosts[hostKey]
	if !ok {
		return api.Host{}, api.ErrHostNotFound
	}
	return h.hi, nil
}

func (hs *HostStore) RecordHostScans(ctx context.Context, scans []api.HostScan) error {
	return nil
}

func (hs *HostStore) RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error {
	return nil
}

func (hs *HostStore) UsableHosts(ctx context.Context) (hosts []api.HostInfo, _ error) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	for _, h := range hs.hosts {
		hosts = append(hosts, h.hi.Info())
	}
	return
}

func (hs *HostStore) AddHost() *Host {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.hkCntr++
	hk := types.PublicKey{byte(hs.hkCntr)}
	hs.hosts[hk] = NewHost(hk)
	return hs.hosts[hk]
}

type HostManager struct {
	HostStore
}

func NewHostManager() *HostManager {
	return &HostManager{
		HostStore: *NewHostStore(),
	}
}

func (hm *HostManager) Downloader(hi api.HostInfo) host.Downloader {
	return NewHost(hi.PublicKey)
}

func (hm *HostManager) Uploader(hi api.HostInfo, _ types.FileContractID) host.Uploader {
	return NewHost(hi.PublicKey)
}

func (hm *HostManager) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) host.Host {
	return NewHost(hk)
}

func (h *Host) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) error {
	return errors.New("implement when needed")
}

func (h *Host) UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv4.SectorSize]byte) error {
	return errors.New("implement when needed")
}

func (h *Host) Prices(ctx context.Context) (rhpv4.HostPrices, error) {
	return h.hi.V2Settings.Prices, nil
}

func (h *Host) FetchRevision(ctx context.Context, fcid types.FileContractID) (types.FileContractRevision, error) {
	return types.FileContractRevision{}, errors.New("implement when needed")
}

func (h *Host) FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error {
	return errors.New("implement when needed")
}
