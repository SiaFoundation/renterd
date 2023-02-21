package autopilot

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/tracing"
	"go.uber.org/zap"
)

const (
	// accountRefillInterval is the amount of time between refills of ephemeral
	// accounts. If we conservatively assume that a good hosts charges 500 SC /
	// TiB, we can pay for about 2.2 GiB with 1 SC. Since we want to refill
	// ahead of time at 0.5 SC, that makes 1.1 GiB. Considering a 1 Gbps uplink
	// that is shared across 30 uploads, we upload at around 33 Mbps to each
	// host. That means uploading 1.1 GiB to drain 0.5 SC takes around 5
	// minutes.  That's why we assume 30 seconds to be more than frequent enough
	// to refill an account when it's due for another refill.
	// TODO: make configurable
	accountRefillInterval = 30 * time.Second
)

var (
	minBalance = types.Siacoins(1).Div64(2).Big()
	maxBalance = types.Siacoins(1).Big()
)

type accounts struct {
	logger *zap.SugaredLogger
	b      Bus
	w      Worker

	mu                sync.Mutex
	fundingContracts  []api.ContractMetadata
	inProgressRefills map[rhp.Account]struct{}
}

func (ap *Autopilot) newAccounts(w Worker) *accounts {
	return &accounts{
		logger: ap.logger.Named("accounts"),
		w:      w,
	}
}

func (a *accounts) markRefillInProgress(account rhp.Account) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, inProgress := a.inProgressRefills[account]
	if inProgress {
		return false
	}
	a.inProgressRefills[account] = struct{}{}
	return true
}

func (a *accounts) releaseInProgressRefill(account rhp.Account) {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, inProgress := a.inProgressRefills[account]
	if !inProgress {
		panic("releasing a refill that hasn't been in progress")
	}
	delete(a.inProgressRefills, account)
}

func (a *accounts) UpdateContracts(ctx context.Context, cfg api.AutopilotConfig) {
	contracts, err := a.b.Contracts(ctx, cfg.Contracts.Set)
	if err != nil {
		a.logger.Errorw(fmt.Sprintf("failed to fetch contract set for refill: %v", err))
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.fundingContracts = append(a.fundingContracts[:0], contracts...)
}

func (a *accounts) refillWorkersAccountsLoop(ctx context.Context) {
	ticker := time.NewTicker(accountRefillInterval)

	for {
		select {
		case <-ctx.Done():
			return // shutdown
		case <-ticker.C:
		}

		a.refillWorkerAccounts(ctx)
	}
}

// refillWorkerAccounts refills all accounts on a worker that require a refill.
// To avoid slow hosts preventing refills for fast hosts, a separate goroutine
// is used for every host. If a slow host's account is still being refilled by a
// goroutine from a previous call, refillWorkerAccounts will skip that account
// until the previously launched goroutine returns.
func (a *accounts) refillWorkerAccounts(ctx context.Context) {
	ctx, span := tracing.Tracer.Start(ctx, "refillWorkerAccounts")
	defer span.End()

	// Map hosts to contracts to use for funding.
	a.mu.Lock()
	contractForHost := make(map[types.PublicKey]types.FileContractID, len(a.fundingContracts))
	for _, c := range a.fundingContracts {
		contractForHost[c.HostKey] = c.ID
	}
	a.mu.Unlock()

	accounts, err := a.w.Accounts(ctx)
	if err != nil {
		a.logger.Errorw(fmt.Sprintf("failed to fetch accounts for refill: %s", err))
		return
	}

	for _, account := range accounts {
		// Only launch a refill goroutine if no refill is in progress.
		if !a.markRefillInProgress(account.ID) {
			continue // refill already in progress
		}
		go func(account api.Account) (err error) {
			// Remove from in-progress refills once done.
			defer a.releaseInProgressRefill(account.ID)

			// Add tracing.
			ctx, span := tracing.Tracer.Start(ctx, "refillAccount")
			defer span.End()
			span.SetAttributes(attribute.Stringer("account", account.ID))
			span.SetAttributes(attribute.Stringer("host", account.Host))
			span.SetAttributes(attribute.Stringer("balance", account.Balance))
			defer func() {
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, "failed to refill account")
				}
			}()

			// Limit the time a refill can take.
			ctx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()

			// TODO: Check if a host is potentially cheating before refilling.

			// Check if refill is needed and perform it if necessary.
			if account.Balance.Cmp(minBalance) >= 0 {
				a.logger.Debugw("contract doesn't require funding",
					"account", account.ID,
					"host", account.Host,
					"balance", account.Balance,
					"minBalance", minBalance)
				return nil // nothing to do
			}
			contractID, found := contractForHost[account.Host]
			if !found {
				a.logger.Debugw("contract to fund account is missing",
					"account", account.ID,
					"host", account.Host,
					"balance", account.Balance)
				return nil // no contract to fund account
			}
			fundAmt := new(big.Int).Sub(maxBalance, account.Balance)

			fundCurrency, err := types.ParseCurrency(fundAmt.String())
			if err != nil {
				a.logger.Errorw(fmt.Sprintf("failed to parse fundAmt as currency: %s", err),
					"account", account.ID,
					"host", account.Host,
					"balance", account.Balance)
				return err
			}

			if err := a.w.RHPFund(ctx, contractID, account.Host, fundCurrency); err != nil {
				// TODO: depending on the error, resync the account balance with
				// the host through the worker.
				a.logger.Errorw(fmt.Sprintf("failed to fund account: %s", err),
					"account", account.ID,
					"host", account.Host,
					"balance", account.Balance)
				return err
			}
			a.logger.Info("Successfully funded account",
				"account", account.ID,
				"host", account.Host,
				"fundAmt", fundCurrency.String())
			return nil
		}(account)
	}
}
