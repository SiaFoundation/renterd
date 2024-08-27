package client

import (
	"context"
	"net/url"

	"go.sia.tech/renterd/api"
)

// Accounts returns all accounts.
func (c *Client) Accounts(ctx context.Context, owner string) (accounts []api.Account, err error) {
	values := url.Values{}
	values.Set("owner", owner)
	err = c.c.WithContext(ctx).GET("/accounts?"+values.Encode(), &accounts)
	return
}

// UpdateAccounts saves all accounts.
func (c *Client) UpdateAccounts(ctx context.Context, accounts []api.Account) (err error) {
	err = c.c.WithContext(ctx).POST("/accounts", api.AccountsSaveRequest{
		Accounts: accounts,
	}, nil)
	return
}
