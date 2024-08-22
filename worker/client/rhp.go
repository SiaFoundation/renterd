package client

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
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
func (c *Client) RHPPriceTable(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, timeout time.Duration) (pt api.HostPriceTable, err error) {
	req := api.RHPPriceTableRequest{
		HostKey:    hostKey,
		SiamuxAddr: siamuxAddr,
		Timeout:    api.DurationMS(timeout),
	}
	err = c.c.WithContext(ctx).POST("/rhp/pricetable", req, &pt)
	return
}

// RHPRenew renews an existing contract with a host.
func (c *Client) RHPRenew(ctx context.Context, contractID types.FileContractID, endHeight uint64, hostKey types.PublicKey, siamuxAddr string, hostAddress, renterAddress types.Address, renterFunds, minNewCollateral, maxFundAmount types.Currency, expectedStorage, windowSize uint64) (resp api.RHPRenewResponse, err error) {
	req := api.RHPRenewRequest{
		ContractID:         contractID,
		EndHeight:          endHeight,
		ExpectedNewStorage: expectedStorage,
		HostAddress:        hostAddress,
		HostKey:            hostKey,
		MaxFundAmount:      maxFundAmount,
		MinNewCollateral:   minNewCollateral,
		RenterAddress:      renterAddress,
		RenterFunds:        renterFunds,
		SiamuxAddr:         siamuxAddr,
		WindowSize:         windowSize,
	}
	err = c.c.WithContext(ctx).POST("/rhp/renew", req, &resp)
	return
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
