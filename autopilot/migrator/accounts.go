package migrator

import (
	"context"
	"fmt"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

const (
	defaultRevisionFetchTimeout = 30 * time.Second

	lockingPrioritySyncing = 30
)

func (m *Migrator) FundAccount(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, desired types.Currency) error {
	// calculate the deposit amount
	acc := m.accounts.ForHost(hk)
	return acc.WithDeposit(func(balance types.Currency) (types.Currency, error) {
		// return early if we have the desired balance
		if balance.Cmp(desired) >= 0 {
			return types.ZeroCurrency, nil
		}
		deposit := desired.Sub(balance)

		// fund the account
		var err error
		deposit, err = m.bus.FundAccount(ctx, acc.ID(), fcid, desired.Sub(balance))
		if err != nil {
			return types.ZeroCurrency, fmt.Errorf("failed to fund account with %v; %w", deposit, err)
		}

		// log the account balance after funding
		m.logger.Debugw("fund account succeeded",
			"balance", balance.ExactString(),
			"deposit", deposit.ExactString(),
		)
		return deposit, nil
	})
}

func (m *Migrator) SyncAccount(ctx context.Context, fcid types.FileContractID, host api.HostInfo) error {
	account := m.accounts.ForHost(host.PublicKey)
	return account.WithSync(func() (types.Currency, error) {
		return m.rhp4Client.AccountBalance(ctx, host.PublicKey, host.SiamuxAddr(), rhpv4.Account(account.ID()))
	})
}
