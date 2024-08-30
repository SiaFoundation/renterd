package client

import (
	"context"
	"net/url"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

// Accounts returns all accounts.
func (c *Client) Accounts(ctx context.Context, owner string) (accounts []api.Account, err error) {
	values := url.Values{}
	values.Set("owner", owner)
	err = c.c.WithContext(ctx).GET("/accounts?"+values.Encode(), &accounts)
	return
}

func (c *Client) FundAccount(ctx context.Context, account rhpv3.Account, fcid types.FileContractID, amount types.Currency) (types.Currency, error) {
	var resp api.AccountsFundResponse
	err := c.c.WithContext(ctx).POST("/accounts/fund", api.AccountsFundRequest{
		AccountID:  account,
		Amount:     amount,
		ContractID: fcid,
	}, &resp)
	return resp.Deposit, err
}

// UpdateAccounts saves all accounts.
func (c *Client) UpdateAccounts(ctx context.Context, accounts []api.Account) (err error) {
	err = c.c.WithContext(ctx).POST("/accounts", api.AccountsSaveRequest{
		Accounts: accounts,
	}, nil)
	return
}
