package mocks

import (
	"context"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
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

func (hs *HostStore) RecordPriceTables(ctx context.Context, priceTableUpdate []api.HostPriceTableUpdate) error {
	return nil
}

func (hs *HostStore) RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error {
	return nil
}

func (hs *HostStore) AddHost() *Host {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.hkCntr++
	hk := types.PublicKey{byte(hs.hkCntr)}
	hs.hosts[hk] = NewHost(hk)
	return hs.hosts[hk]
}
