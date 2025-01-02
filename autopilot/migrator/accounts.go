package migrator

import (
	"context"
	"fmt"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/locking"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
)

const (
	defaultRevisionFetchTimeout = 30 * time.Second

	lockingPrioritySyncing = 30
)

func (m *migrator) FundAccount(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, desired types.Currency) error {
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
			if rhp3.IsBalanceMaxExceeded(err) {
				acc.ScheduleSync()
			}
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

func (m *migrator) SyncAccount(ctx context.Context, fcid types.FileContractID, host api.HostInfo) error {
	// handle v2 host
	if host.IsV2() {
		account := m.accounts.ForHost(host.PublicKey)
		return account.WithSync(func() (types.Currency, error) {
			return m.rhp4Client.AccountBalance(ctx, host.PublicKey, host.V2SiamuxAddr(), rhpv4.Account(account.ID()))
		})
	}

	// attach gouging checker
	gp, err := m.bus.GougingParams(ctx)
	if err != nil {
		return fmt.Errorf("couldn't get gouging parameters; %w", err)
	}
	ctx = gouging.WithChecker(ctx, m.bus, gp)

	// acquire lock
	contractLock, err := locking.NewContractLock(ctx, fcid, lockingPrioritySyncing, m.bus, m.logger)
	if err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = contractLock.Release(releaseCtx)
		cancel()
	}()

	h := m.hostManager.Host(host.PublicKey, fcid, host.SiamuxAddr)

	// fetch revision
	ctx, cancel := context.WithTimeout(ctx, defaultRevisionFetchTimeout)
	defer cancel()
	rev, err := h.FetchRevision(ctx, fcid)
	if err != nil {
		return err
	}

	// sync the account
	err = h.SyncAccount(ctx, &rev)
	if err != nil {
		return fmt.Errorf("failed to sync account; %w", err)
	}
	return nil
}
