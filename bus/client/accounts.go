package client

import (
	"context"
	"net/url"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

// Accounts returns all accounts.
func (c *Client) Accounts(ctx context.Context, owner string) (accounts []api.Account, err error) {
	values := url.Values{}
	values.Set("owner", owner)
	err = c.c.GET(ctx, "/accounts?"+values.Encode(), &accounts)
	return
}

func (c *Client) FundAccount(ctx context.Context, account rhpv4.Account, fcid types.FileContractID, amount types.Currency) (types.Currency, error) {
	var resp api.AccountsFundResponse
	err := c.c.POST(ctx, "/accounts/fund", api.AccountsFundRequest{
		AccountID:  api.AccountID(account),
		Amount:     amount,
		ContractID: fcid,
	}, &resp)
	return resp.Deposit, err
}

// UpdateAccounts saves all accounts.
func (c *Client) UpdateAccounts(ctx context.Context, accounts []api.Account) (err error) {
	err = c.c.POST(ctx, "/accounts", api.AccountsSaveRequest{
		Accounts: accounts,
	}, nil)
	return
}
