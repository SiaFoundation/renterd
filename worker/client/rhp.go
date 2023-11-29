package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
)

// RHPBroadcast broadcasts the latest revision for a contract.
func (c *Client) RHPBroadcast(ctx context.Context, contractID types.FileContractID) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/rhp/contract/%s/broadcast", contractID), nil, nil)
	return
}

// RHPContractRoots fetches the roots of the contract with given id.
func (c *Client) RHPContractRoots(ctx context.Context, contractID types.FileContractID) (roots []types.Hash256, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/rhp/contract/%s/roots", contractID), &roots)
	return
}

// RHPForm forms a contract with a host.
func (c *Client) RHPForm(ctx context.Context, endHeight uint64, hostKey types.PublicKey, hostIP string, renterAddress types.Address, renterFunds types.Currency, hostCollateral types.Currency) (rhpv2.ContractRevision, []types.Transaction, error) {
	req := api.RHPFormRequest{
		EndHeight:      endHeight,
		HostCollateral: hostCollateral,
		HostKey:        hostKey,
		HostIP:         hostIP,
		RenterFunds:    renterFunds,
		RenterAddress:  renterAddress,
	}
	var resp api.RHPFormResponse
	err := c.c.WithContext(ctx).POST("/rhp/form", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

// RHPFund funds an ephemeral account using the supplied contract.
func (c *Client) RHPFund(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, hostIP, siamuxAddr string, balance types.Currency) (err error) {
	req := api.RHPFundRequest{
		ContractID: contractID,
		HostKey:    hostKey,
		SiamuxAddr: siamuxAddr,
		Balance:    balance,
	}
	err = c.c.WithContext(ctx).POST("/rhp/fund", req, nil)
	return
}

// RHPPriceTable fetches a price table for a host.
func (c *Client) RHPPriceTable(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, timeout time.Duration) (pt hostdb.HostPriceTable, err error) {
	req := api.RHPPriceTableRequest{
		HostKey:    hostKey,
		SiamuxAddr: siamuxAddr,
		Timeout:    api.DurationMS(timeout),
	}
	err = c.c.WithContext(ctx).POST("/rhp/pricetable", req, &pt)
	return
}

// RHPPruneContract prunes deleted sectors from the contract with given id.
func (c *Client) RHPPruneContract(ctx context.Context, contractID types.FileContractID, timeout time.Duration) (prunable, pruned, remaining uint64, err error) {
	var res api.RHPPruneContractResponse
	if err = c.c.WithContext(ctx).POST(fmt.Sprintf("/rhp/contract/%s/prune", contractID), api.RHPPruneContractRequest{
		Timeout: api.DurationMS(timeout),
	}, &res); err != nil {
		return
	} else if res.Error != "" {
		err = errors.New(res.Error)
	}

	prunable = res.Prunable
	pruned = res.Pruned
	remaining = res.Remaining
	return
}

// RHPReadRegistry reads a registry value.
func (c *Client) RHPReadRegistry(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, key rhpv3.RegistryKey, payment rhpv3.PayByEphemeralAccountRequest) (resp rhpv3.RegistryValue, err error) {
	req := api.RHPRegistryReadRequest{
		HostKey:     hostKey,
		SiamuxAddr:  siamuxAddr,
		RegistryKey: key,
		Payment:     payment,
	}
	err = c.c.WithContext(ctx).POST("/rhp/registry/read", req, &resp)
	return
}

// RHPRenew renews an existing contract with a host.
func (c *Client) RHPRenew(ctx context.Context, contractID types.FileContractID, endHeight uint64, hostKey types.PublicKey, siamuxAddr string, hostAddress, renterAddress types.Address, renterFunds, newCollateral types.Currency, windowSize uint64) (rhpv2.ContractRevision, []types.Transaction, error) {
	req := api.RHPRenewRequest{
		ContractID:    contractID,
		EndHeight:     endHeight,
		HostAddress:   hostAddress,
		HostKey:       hostKey,
		NewCollateral: newCollateral,
		RenterAddress: renterAddress,
		RenterFunds:   renterFunds,
		SiamuxAddr:    siamuxAddr,
		WindowSize:    windowSize,
	}

	var resp api.RHPRenewResponse
	err := c.c.WithContext(ctx).POST("/rhp/renew", req, &resp)
	return resp.Contract, resp.TransactionSet, err
}

// RHPScan scans a host, returning its current settings.
func (c *Client) RHPScan(ctx context.Context, hostKey types.PublicKey, hostIP string, timeout time.Duration) (resp api.RHPScanResponse, err error) {
	err = c.c.WithContext(ctx).POST("/rhp/scan", api.RHPScanRequest{
		HostKey: hostKey,
		HostIP:  hostIP,
		Timeout: api.DurationMS(timeout),
	}, &resp)
	return
}

// RHPSync funds an ephemeral account using the supplied contract.
func (c *Client) RHPSync(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, hostIP, siamuxAddr string) (err error) {
	req := api.RHPSyncRequest{
		ContractID: contractID,
		HostKey:    hostKey,
		SiamuxAddr: siamuxAddr,
	}
	err = c.c.WithContext(ctx).POST("/rhp/sync", req, nil)
	return
}

// RHPUpdateRegistry updates a registry value.
func (c *Client) RHPUpdateRegistry(ctx context.Context, hostKey types.PublicKey, key rhpv3.RegistryKey, value rhpv3.RegistryValue) (err error) {
	req := api.RHPRegistryUpdateRequest{
		HostKey:       hostKey,
		RegistryKey:   key,
		RegistryValue: value,
	}
	err = c.c.WithContext(ctx).POST("/rhp/registry/update", req, nil)
	return
}
