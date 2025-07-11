package client

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

// AddContract adds the provided contract to the metadata store, if the contract
// already exists it will be replaced.
func (c *Client) AddContract(ctx context.Context, contract api.ContractMetadata) error {
	return c.c.PUT(ctx, "/contracts", contract)
}

// AncestorContracts returns any ancestors of a given contract.
func (c *Client) AncestorContracts(ctx context.Context, contractID types.FileContractID, minStartHeight uint64) (contracts []api.ContractMetadata, err error) {
	values := url.Values{}
	values.Set("minstartheight", fmt.Sprint(minStartHeight))
	err = c.c.GET(ctx, fmt.Sprintf("/contract/%s/ancestors?"+values.Encode(), contractID), &contracts)
	return
}

// AcquireContract acquires a contract for a given amount of time unless
// released manually before that time.
func (c *Client) AcquireContract(ctx context.Context, contractID types.FileContractID, priority int, d time.Duration) (lockID uint64, err error) {
	var resp api.ContractAcquireResponse
	err = c.c.POST(ctx, fmt.Sprintf("/contract/%s/acquire", contractID), api.ContractAcquireRequest{
		Duration: api.DurationMS(d),
		Priority: priority,
	}, &resp)
	lockID = resp.LockID
	return
}

// ArchiveContracts archives the contracts with the given IDs and archival reason.
func (c *Client) ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) (err error) {
	err = c.c.POST(ctx, "/contracts/archive", toArchive, nil)
	return
}

// BroadcastContract broadcasts the latest revision for a contract.
func (c *Client) BroadcastContract(ctx context.Context, contractID types.FileContractID) (txnID types.TransactionID, err error) {
	err = c.c.POST(ctx, fmt.Sprintf("/contract/%s/broadcast", contractID), nil, &txnID)
	return
}

// Contract returns the contract with the given ID.
func (c *Client) Contract(ctx context.Context, id types.FileContractID) (contract api.ContractMetadata, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/contract/%s", id), &contract)
	return
}

// ContractRoots returns the sector roots, as well as the ones that are still
// uploading, for the contract with given id.
func (c *Client) ContractRoots(ctx context.Context, contractID types.FileContractID) (roots []types.Hash256, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/contract/%s/roots", contractID), &roots)
	return
}

// ContractSize returns the contract's size.
func (c *Client) ContractSize(ctx context.Context, contractID types.FileContractID) (size api.ContractSize, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/contract/%s/size", contractID), &size)
	return
}

// Contracts retrieves contracts from the metadata store. If no filter is set,
// all contracts are returned.
func (c *Client) Contracts(ctx context.Context, opts api.ContractsOpts) (contracts []api.ContractMetadata, err error) {
	values := url.Values{}
	if opts.FilterMode != "" {
		values.Set("filtermode", opts.FilterMode)
	}
	err = c.c.GET(ctx, "/contracts?"+values.Encode(), &contracts)
	return
}

// DeleteContract deletes the contract with the given ID.
func (c *Client) DeleteContract(ctx context.Context, id types.FileContractID) (err error) {
	err = c.c.DELETE(ctx, fmt.Sprintf("/contract/%s", id))
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
	err = c.c.DELETE(ctx, "/contracts/all")
	return
}

// FormContract forms a contract with a host and adds it to the bus.
func (c *Client) FormContract(ctx context.Context, renterAddress types.Address, renterFunds types.Currency, hostKey types.PublicKey, hostCollateral types.Currency, endHeight uint64) (contract api.ContractMetadata, err error) {
	err = c.c.POST(ctx, "/contracts/form", api.ContractFormRequest{
		EndHeight:      endHeight,
		HostCollateral: hostCollateral,
		HostKey:        hostKey,
		RenterFunds:    renterFunds,
		RenterAddress:  renterAddress,
	}, &contract)
	return
}

// KeepaliveContract extends the duration on an already acquired lock on a
// contract.
func (c *Client) KeepaliveContract(ctx context.Context, contractID types.FileContractID, lockID uint64, d time.Duration) (err error) {
	err = c.c.POST(ctx, fmt.Sprintf("/contract/%s/keepalive", contractID), api.ContractKeepaliveRequest{
		Duration: api.DurationMS(d),
		LockID:   lockID,
	}, nil)
	return
}

// ContractRevision fetches the latest revision of a contract directly from the
// host.
func (c *Client) ContractRevision(ctx context.Context, contractID types.FileContractID) (resp api.Revision, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/contract/%s/revision", contractID), &resp)
	return
}

// PrunableData returns an overview of all contract sizes, the total size and
// the amount of data that can be pruned.
func (c *Client) PrunableData(ctx context.Context) (prunableData api.ContractsPrunableDataResponse, err error) {
	err = c.c.GET(ctx, "/contracts/prunable", &prunableData)
	return
}

// PruneContract prunes the given contract.
func (c *Client) PruneContract(ctx context.Context, contractID types.FileContractID, timeout time.Duration) (res api.ContractPruneResponse, err error) {
	err = c.c.POST(ctx, fmt.Sprintf("/contract/%s/prune", contractID), api.ContractPruneRequest{Timeout: api.DurationMS(timeout)}, &res)
	return
}

// RenewContract renews an existing contract with a host and adds it to the bus.
func (c *Client) RenewContract(ctx context.Context, contractID types.FileContractID, endHeight uint64, renterFunds, minNewCollateral types.Currency) (renewal api.ContractMetadata, err error) {
	req := api.ContractRenewRequest{
		EndHeight:        endHeight,
		MinNewCollateral: minNewCollateral,
		RenterFunds:      renterFunds,
	}
	err = c.c.POST(ctx, fmt.Sprintf("/contract/%s/renew", contractID), req, &renewal)
	return
}

// RenewedContract returns the renewed contract for the given ID.
func (c *Client) RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (contract api.ContractMetadata, err error) {
	err = c.c.GET(ctx, fmt.Sprintf("/contracts/renewed/%s", renewedFrom), &contract)
	return
}

// RecordContractSpending records contract spending metrics for contracts.
func (c *Client) RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) (err error) {
	err = c.c.POST(ctx, "/contracts/spending", records, nil)
	return
}

// ReleaseContract releases a contract that was previously acquired using AcquireContract.
func (c *Client) ReleaseContract(ctx context.Context, contractID types.FileContractID, lockID uint64) (err error) {
	err = c.c.POST(ctx, fmt.Sprintf("/contract/%s/release", contractID), api.ContractReleaseRequest{
		LockID: lockID,
	}, nil)
	return
}

// UpdateContractUsability updates the usability of the given contract.
func (c *Client) UpdateContractUsability(ctx context.Context, contractID types.FileContractID, usability string) (err error) {
	err = c.c.PUT(ctx, fmt.Sprintf("/contract/%s/usability", contractID), usability)
	return
}
