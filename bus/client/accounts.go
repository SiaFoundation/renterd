package client

import (
	"context"
	"fmt"

	"go.sia.tech/renterd/api"
)

// Accounts returns all accounts.
func (c *Client) Accounts(ctx context.Context, owner string) (accounts []api.Account, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/accounts?owner=%s", owner), &accounts)
	return
}

// UpdateAccounts saves all accounts.
func (c *Client) UpdateAccounts(ctx context.Context, owner string, accounts []api.Account, setUnclean bool) (err error) {
	err = c.c.WithContext(ctx).POST("/accounts", &api.AccountsSaveRequest{
		Accounts:   accounts,
		Owner:      owner,
		SetUnclean: setUnclean,
	}, nil)
	return
}
