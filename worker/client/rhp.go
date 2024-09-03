package client

import (
	"context"
	"errors"
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

// RHPPruneContract prunes deleted sectors from the contract with given id.
func (c *Client) RHPPruneContract(ctx context.Context, contractID types.FileContractID, timeout time.Duration) (pruned, remaining uint64, err error) {
	var res api.RHPPruneContractResponse
	if err = c.c.WithContext(ctx).POST(fmt.Sprintf("/rhp/contract/%s/prune", contractID), api.RHPPruneContractRequest{
		Timeout: api.DurationMS(timeout),
	}, &res); err != nil {
		return
	} else if res.Error != "" {
		err = errors.New(res.Error)
	}

	pruned = res.Pruned
	remaining = res.Remaining
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
