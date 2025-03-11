package mocks

import (
	"context"
	"sync"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

type ContractStore struct {
	mu         sync.Mutex
	contracts  map[types.FileContractID]*Contract
	hosts2fcid map[types.PublicKey]types.FileContractID
	fcidCntr   uint
}

func NewContractStore() *ContractStore {
	return &ContractStore{
		contracts:  make(map[types.FileContractID]*Contract),
		hosts2fcid: make(map[types.PublicKey]types.FileContractID),
	}
}

func (cs *ContractStore) RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, c := range cs.contracts {
		if c.metadata.RenewedFrom == renewedFrom {
			return c.metadata, nil
		}
	}
	return api.ContractMetadata{}, api.ErrContractNotFound
}

func (cs *ContractStore) Contract(_ context.Context, fcid types.FileContractID) (api.ContractMetadata, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	contract, ok := cs.contracts[fcid]
	if !ok {
		return api.ContractMetadata{}, api.ErrContractNotFound
	}
	return contract.metadata, nil
}

func (*ContractStore) ContractSize(context.Context, types.FileContractID) (api.ContractSize, error) {
	return api.ContractSize{}, nil
}

func (*ContractStore) ContractRoots(context.Context, types.FileContractID) ([]types.Hash256, error) {
	return nil, nil
}

func (cs *ContractStore) Contracts(context.Context, api.ContractsOpts) (metadatas []api.ContractMetadata, _ error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	for _, c := range cs.contracts {
		metadatas = append(metadatas, c.metadata)
	}
	return
}

func (cs *ContractStore) AddContract(hk types.PublicKey) *Contract {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	fcid := cs.newFileContractID()
	cs.contracts[fcid] = NewContract(hk, fcid)
	cs.hosts2fcid[hk] = fcid
	return cs.contracts[fcid]
}

func (cs *ContractStore) DeleteContracdt(fcid types.FileContractID) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	delete(cs.contracts, fcid)
}

func (cs *ContractStore) RenewContract(hk types.PublicKey) *Contract {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	curr, ok := cs.hosts2fcid[hk]
	if !ok {
		panic("host not found") // developer error
	}
	c := cs.contracts[curr]
	if c == nil {
		panic("contract not found") // developer error
	}
	delete(cs.contracts, curr)

	renewal := NewContract(hk, cs.newFileContractID())
	renewal.metadata.RenewedFrom = c.metadata.ID
	renewal.metadata.WindowStart = c.metadata.WindowEnd
	renewal.metadata.WindowEnd = renewal.metadata.WindowStart + (c.metadata.WindowEnd - c.metadata.WindowStart)
	cs.contracts[renewal.metadata.ID] = renewal
	cs.hosts2fcid[hk] = renewal.metadata.ID
	return renewal
}

func (cs *ContractStore) newFileContractID() types.FileContractID {
	cs.fcidCntr++
	return types.FileContractID{byte(cs.fcidCntr)}
}

type Contract struct {
	rev      types.FileContractRevision
	metadata api.ContractMetadata

	mu      sync.Mutex
	sectors map[types.Hash256]*[rhpv2.SectorSize]byte
}

func NewContract(hk types.PublicKey, fcid types.FileContractID) *Contract {
	return &Contract{
		metadata: api.ContractMetadata{
			ID:          fcid,
			HostKey:     hk,
			WindowStart: 0,
			WindowEnd:   10,
		},
		rev:     types.FileContractRevision{ParentID: fcid},
		sectors: make(map[types.Hash256]*[rhpv2.SectorSize]byte),
	}
}

func (c *Contract) AddSector(root types.Hash256, sector *[rhpv2.SectorSize]byte) {
	c.mu.Lock()
	c.sectors[root] = sector
	c.mu.Unlock()
}

func (c *Contract) ID() types.FileContractID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.metadata.ID
}

func (c *Contract) Metadata() api.ContractMetadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.metadata
}

func (c *Contract) Revision() types.FileContractRevision {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rev
}

func (c *Contract) Sector(root types.Hash256) (sector *[rhpv2.SectorSize]byte, found bool) {
	c.mu.Lock()
	sector, found = c.sectors[root]
	c.mu.Unlock()
	return
}
