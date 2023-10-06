package client

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

// AddContract adds the provided contract to the metadata store.
func (c *Client) AddContract(ctx context.Context, contract rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (added api.ContractMetadata, err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/contract/%s", contract.ID()), api.ContractsIDAddRequest{
		Contract:    contract,
		StartHeight: startHeight,
		TotalCost:   totalCost,
	}, &added)
	return
}

// AddRenewedContract adds the provided contract to the metadata store.
func (c *Client) AddRenewedContract(ctx context.Context, contract rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (renewed api.ContractMetadata, err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/contract/%s/renewed", contract.ID()), api.ContractsIDRenewedRequest{
		Contract:    contract,
		RenewedFrom: renewedFrom,
		StartHeight: startHeight,
		TotalCost:   totalCost,
	}, &renewed)
	return
}

// AncestorContracts returns any ancestors of a given contract.
func (c *Client) AncestorContracts(ctx context.Context, fcid types.FileContractID, minStartHeight uint64) (contracts []api.ArchivedContract, err error) {
	values := url.Values{}
	values.Set("minStartHeight", fmt.Sprint(minStartHeight))
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contract/%s/ancestors?"+values.Encode(), fcid), &contracts)
	return
}

// AcquireContract acquires a contract for a given amount of time unless
// released manually before that time.
func (c *Client) AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error) {
	var resp api.ContractAcquireResponse
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/contract/%s/acquire", fcid), api.ContractAcquireRequest{
		Duration: api.DurationMS(d),
		Priority: priority,
	}, &resp)
	lockID = resp.LockID
	return
}

// ArchiveContracts archives the contracts with the given IDs and archival reason.
func (c *Client) ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) (err error) {
	err = c.c.WithContext(ctx).POST("/contracts/archive", toArchive, nil)
	return
}

// Contract returns the contract with the given ID.
func (c *Client) Contract(ctx context.Context, id types.FileContractID) (contract api.ContractMetadata, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contract/%s", id), &contract)
	return
}

// ContractRoots returns the sector roots, as well as the ones that are still
// uploading, for the contract with given id.
func (c *Client) ContractRoots(ctx context.Context, fcid types.FileContractID) (roots, uploading []types.Hash256, err error) {
	var resp api.ContractRootsResponse
	if err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contract/%s/roots", fcid), &resp); err != nil {
		return
	}
	return resp.Roots, resp.Uploading, nil
}

// ContractSets returns the contract sets of the bus.
func (c *Client) ContractSets(ctx context.Context) (sets []string, err error) {
	err = c.c.WithContext(ctx).GET("/contracts/sets", &sets)
	return
}

// ContractSize returns the contract's size.
func (c *Client) ContractSize(ctx context.Context, fcid types.FileContractID) (size api.ContractSize, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contract/%s/size", fcid), &size)
	return
}

// ContractSetContracts returns the contracts for the given set from the
// metadata store.
func (c *Client) ContractSetContracts(ctx context.Context, set string) (contracts []api.ContractMetadata, err error) {
	if set == "" {
		return nil, errors.New("set cannot be empty")
	}
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contracts/set/%s", set), &contracts)
	return
}

// Contracts returns all contracts in the metadata store.
func (c *Client) Contracts(ctx context.Context) (contracts []api.ContractMetadata, err error) {
	err = c.c.WithContext(ctx).GET("/contracts", &contracts)
	return
}

// DeleteContract deletes the contract with the given ID.
func (c *Client) DeleteContract(ctx context.Context, id types.FileContractID) (err error) {
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/contract/%s", id))
	return
}

// DeleteContracts deletes the contracts with the given IDs.
func (c *Client) DeleteContracts(ctx context.Context, ids []types.FileContractID) error {
	// TODO: batch delete
	for _, id := range ids {
		if err := c.DeleteContract(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// DeleteAllContracts deletes all contracts from the bus.
func (c *Client) DeleteAllContracts(ctx context.Context) (err error) {
	err = c.c.WithContext(ctx).DELETE("/contracts/all")
	return
}

// DeleteContractSet removes the contract set from the bus.
func (c *Client) DeleteContractSet(ctx context.Context, set string) (err error) {
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/contracts/set/%s", set))
	return
}

// KeepaliveContract extends the duration on an already acquired lock on a
// contract.
func (c *Client) KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/contract/%s/keepalive", fcid), api.ContractKeepaliveRequest{
		Duration: api.DurationMS(d),
		LockID:   lockID,
	}, nil)
	return
}

// PrunableData returns an overview of all contract sizes, the total size and
// the amount of data that can be pruned.
func (c *Client) PrunableData(ctx context.Context) (prunableData api.ContractsPrunableDataResponse, err error) {
	err = c.c.WithContext(ctx).GET("/contracts/prunable", &prunableData)
	return
}

// RenewedContract returns the renewed contract for the given ID.
func (c *Client) RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (contract api.ContractMetadata, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/contracts/renewed/%s", renewedFrom), &contract)
	return
}

// RecordContractSpending records contract spending metrics for contracts.
func (c *Client) RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) (err error) {
	err = c.c.WithContext(ctx).POST("/contracts/spending", records, nil)
	return
}

// ReleaseContract releases a contract that was previously acquired using AcquireContract.
func (c *Client) ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/contract/%s/release", fcid), api.ContractReleaseRequest{
		LockID: lockID,
	}, nil)
	return
}

// SetContractSet adds the given contracts to the given set.
func (c *Client) SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) (err error) {
	err = c.c.WithContext(ctx).PUT(fmt.Sprintf("/contracts/set/%s", set), contracts)
	return
}
