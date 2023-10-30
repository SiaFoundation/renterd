package client

import (
	"context"
	"fmt"
	"math/big"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

// Account returns the account for given id.
func (c *Client) Account(ctx context.Context, id rhpv3.Account, host types.PublicKey) (account api.Account, err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/account/%s", id), api.AccountHandlerPOST{
		HostKey: host,
	}, &account)
	return
}

// Accounts returns all accounts.
func (c *Client) Accounts(ctx context.Context) (accounts []api.Account, err error) {
	err = c.c.WithContext(ctx).GET("/accounts", &accounts)
	return
}

// AddBalance adds the given amount to an account's balance, the amount can be negative.
func (c *Client) AddBalance(ctx context.Context, id rhpv3.Account, hk types.PublicKey, amount *big.Int) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/account/%s/add", id), api.AccountsAddBalanceRequest{
		HostKey: hk,
		Amount:  amount,
	}, nil)
	return
}

// LockAccount locks an account.
func (c *Client) LockAccount(ctx context.Context, id rhpv3.Account, hostKey types.PublicKey, exclusive bool, duration time.Duration) (account api.Account, lockID uint64, err error) {
	var resp api.AccountsLockHandlerResponse
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/account/%s/lock", id), api.AccountsLockHandlerRequest{
		HostKey:   hostKey,
		Exclusive: exclusive,
		Duration:  api.DurationMS(duration),
	}, &resp)
	return resp.Account, resp.LockID, err
}

// ResetDrift resets the drift of an account to zero.
func (c *Client) ResetDrift(ctx context.Context, id rhpv3.Account) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/account/%s/resetdrift", id), nil, nil)
	return
}

// SetBalance sets the given account's balance to a certain amount.
func (c *Client) SetBalance(ctx context.Context, id rhpv3.Account, hk types.PublicKey, amount *big.Int) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/account/%s/update", id), api.AccountsUpdateBalanceRequest{
		HostKey: hk,
		Amount:  amount,
	}, nil)
	return
}

// ScheduleSync sets the requiresSync flag of an account.
func (c *Client) ScheduleSync(ctx context.Context, id rhpv3.Account, hk types.PublicKey) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/account/%s/requiressync", id), api.AccountsRequiresSyncRequest{
		HostKey: hk,
	}, nil)
	return
}

// UnlockAccount unlocks an account.
func (c *Client) UnlockAccount(ctx context.Context, id rhpv3.Account, lockID uint64) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/account/%s/unlock", id), api.AccountsUnlockHandlerRequest{
		LockID: lockID,
	}, nil)
	return
}
