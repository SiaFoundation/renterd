package autopilot

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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
	inProgressRefills map[types.PublicKey]struct{}
}

func newAccounts(l *zap.SugaredLogger, b Bus, w Worker) *accounts {
	return &accounts{
		b:                 b,
		logger:            l.Named("accounts"),
		w:                 w,
		inProgressRefills: make(map[types.PublicKey]struct{}),
	}
}

func (a *accounts) markRefillInProgress(host types.PublicKey) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, inProgress := a.inProgressRefills[host]
	if inProgress {
		return false
	}
	a.inProgressRefills[host] = struct{}{}
	return true
}

func (a *accounts) markRefillDone(host types.PublicKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, inProgress := a.inProgressRefills[host]
	if !inProgress {
		panic("releasing a refill that hasn't been in progress")
	}
	delete(a.inProgressRefills, host)
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

func (a *accounts) refillWorkersAccountsLoop(stopChan <-chan struct{}) {
	ticker := time.NewTicker(accountRefillInterval)

	for {
		select {
		case <-stopChan:
			return // shutdown
		case <-ticker.C:
		}

		a.refillWorkerAccounts()
	}
}

// refillWorkerAccounts refills all accounts on a worker that require a refill.
// To avoid slow hosts preventing refills for fast hosts, a separate goroutine
// is used for every host. If a slow host's account is still being refilled by a
// goroutine from a previous call, refillWorkerAccounts will skip that account
// until the previously launched goroutine returns.
func (a *accounts) refillWorkerAccounts() {
	ctx, span := tracing.Tracer.Start(context.Background(), "refillWorkerAccounts")
	defer span.End()

	// Map hosts to contracts to use for funding.
	a.mu.Lock()
	contractForHost := make(map[types.PublicKey]api.ContractMetadata, len(a.fundingContracts))
	for _, c := range a.fundingContracts {
		contractForHost[c.HostKey] = c
	}
	a.mu.Unlock()

	accounts, err := a.w.Accounts(ctx)
	if err != nil {
		a.logger.Errorw(fmt.Sprintf("failed to fetch accounts for refill: %s", err))
		return
	}
	accountForHost := make(map[types.PublicKey]api.Account, len(accounts))
	for _, acc := range accounts {
		accountForHost[acc.Host] = acc
	}

	// Fund an account for every contract we have.
	for _, contract := range contractForHost {
		// Only launch a refill goroutine if no refill is in progress.
		if !a.markRefillInProgress(contract.HostKey) {
			continue // refill already in progress
		}
		go func(contract api.ContractMetadata) (err error) {
			// Remove from in-progress refills once done.
			defer a.markRefillDone(contract.HostKey)

			// Fetch the account. This might be zero so use accordingly.
			account := accountForHost[contract.HostKey]
			if account.Balance == nil {
				account.Balance = new(big.Int)
			}

			// Add tracing.
			ctx, span := tracing.Tracer.Start(ctx, "refillAccount")
			defer span.End()
			span.SetAttributes(attribute.Stringer("account", account.ID))
			span.SetAttributes(attribute.Stringer("host", contract.HostKey))
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
					"host", contract.HostKey,
					"balance", account.Balance,
					"minBalance", minBalance)
				return nil // nothing to do
			}
			fundAmt := new(big.Int).Sub(maxBalance, account.Balance)

			fundCurrency, err := types.ParseCurrency(fundAmt.String())
			if err != nil {
				a.logger.Errorw(fmt.Sprintf("failed to parse fundAmt as currency: %s", err),
					"account", account.ID,
					"host", contract.HostKey,
					"balance", account.Balance)
				return err
			}

			if err := a.w.RHPFund(ctx, contract.ID, contract.HostKey, fundCurrency); err != nil {
				// TODO: depending on the error, resync the account balance with
				// the host through the worker.
				a.logger.Errorw(fmt.Sprintf("failed to fund account: %s", err),
					"account", account.ID,
					"host", contract.HostKey,
					"balance", account.Balance)
				return err
			}
			a.logger.Info("Successfully funded account",
				"account", account.ID,
				"host", contract.HostKey,
				"fundAmt", fundCurrency.String())
			return nil
		}(contract)
	}
}
