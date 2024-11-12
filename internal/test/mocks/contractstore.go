package mocks

import (
	"context"
	"errors"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
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

func (cs *ContractStore) RenewedContract(ctx context.Context, fcid types.FileContractID) (api.ContractMetadata, error) {
	return cs.Contract(ctx, fcid)
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

func (*ContractStore) ContractRoots(context.Context, types.FileContractID) ([]types.Hash256, []types.Hash256, error) {
	return nil, nil, nil
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

func (cs *ContractStore) RenewContract(hk types.PublicKey) (*Contract, error) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	curr, ok := cs.hosts2fcid[hk]
	if !ok {
		return nil, errors.New("host not found")
	}
	c := cs.contracts[curr]
	if c == nil {
		return nil, errors.New("host does not have a contract to renew")
	}
	delete(cs.contracts, curr)

	renewal := NewContract(hk, cs.newFileContractID())
	renewal.metadata.RenewedFrom = c.metadata.ID
	renewal.metadata.WindowStart = c.metadata.WindowEnd
	renewal.metadata.WindowEnd = renewal.metadata.WindowStart + (c.metadata.WindowEnd - c.metadata.WindowStart)
	cs.contracts[renewal.metadata.ID] = renewal
	cs.hosts2fcid[hk] = renewal.metadata.ID
	return renewal, nil
}

func (cs *ContractStore) newFileContractID() types.FileContractID {
	cs.fcidCntr++
	return types.FileContractID{byte(cs.fcidCntr)}
}
