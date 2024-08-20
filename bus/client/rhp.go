package client

import (
	"context"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

// RHPForm forms a contract with a host and adds it to the bus.
func (c *Client) RHPForm(ctx context.Context, endHeight uint64, hostKey types.PublicKey, hostIP string, renterAddress types.Address, renterFunds types.Currency, hostCollateral types.Currency) (contractID types.FileContractID, err error) {
	err = c.c.WithContext(ctx).POST("/rhp/form", api.RHPFormRequest{
		EndHeight:      endHeight,
		HostCollateral: hostCollateral,
		HostKey:        hostKey,
		HostIP:         hostIP,
		RenterFunds:    renterFunds,
		RenterAddress:  renterAddress,
	}, &contractID)
	return
}
