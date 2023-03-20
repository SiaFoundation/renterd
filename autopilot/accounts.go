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

var (
	minBalance  = types.Siacoins(1).Div64(2).Big()
	maxBalance  = types.Siacoins(1)
	maxNegDrift = new(big.Int).Neg(types.Siacoins(10).Big())
)

type accounts struct {
	logger  *zap.SugaredLogger
	b       Bus
	workers *workerPool

	refillInterval time.Duration

	mu                sync.Mutex
	fundingContracts  []api.ContractMetadata
	inProgressRefills map[types.Hash256]struct{}
}

func newAccounts(l *zap.SugaredLogger, b Bus, workers *workerPool, interval time.Duration) *accounts {
	return &accounts{
		b:                 b,
		inProgressRefills: make(map[types.Hash256]struct{}),
		logger:            l.Named("accounts"),
		refillInterval:    interval,
		workers:           workers,
	}
}

func (a *accounts) markRefillInProgress(workerID string, host types.PublicKey) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	k := types.HashBytes(append([]byte(workerID), host[:]...))
	_, inProgress := a.inProgressRefills[k]
	if inProgress {
		return false
	}
	a.inProgressRefills[k] = struct{}{}
	return true
}

func (a *accounts) markRefillDone(workerID string, host types.PublicKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	k := types.HashBytes(append([]byte(workerID), host[:]...))
	_, inProgress := a.inProgressRefills[k]
	if !inProgress {
		panic("releasing a refill that hasn't been in progress")
	}
	delete(a.inProgressRefills, k)
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
	ticker := time.NewTicker(a.refillInterval)

	for {
		select {
		case <-stopChan:
			return // shutdown
		case <-ticker.C:
		}

		a.workers.withWorkers(func(workers []Worker) {
			for _, w := range workers {
				a.refillWorkerAccounts(w)
			}
		})
	}
}

// refillWorkerAccounts refills all accounts on a worker that require a refill.
// To avoid slow hosts preventing refills for fast hosts, a separate goroutine
// is used for every host. If a slow host's account is still being refilled by a
// goroutine from a previous call, refillWorkerAccounts will skip that account
// until the previously launched goroutine returns.
func (a *accounts) refillWorkerAccounts(w Worker) {
	ctx, span := tracing.Tracer.Start(context.Background(), "refillWorkerAccounts")
	defer span.End()

	workerID, err := w.ID(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to fetch worker id")
		a.logger.Errorw(fmt.Sprintf("failed to fetch worker id for refill: %v", err))
		return
	}

	// Map hosts to contracts to use for funding.
	a.mu.Lock()
	contractForHost := make(map[types.PublicKey]api.ContractMetadata, len(a.fundingContracts))
	for _, c := range a.fundingContracts {
		contractForHost[c.HostKey] = c
	}
	a.mu.Unlock()

	// Fund an account for every contract we have.
	for _, contract := range contractForHost {
		// Only launch a refill goroutine if no refill is in progress.
		if !a.markRefillInProgress(workerID, contract.HostKey) {
			continue // refill already in progress
		}
		go func(contract api.ContractMetadata) (err error) {
			// Remove from in-progress refills once done.
			defer a.markRefillDone(workerID, contract.HostKey)

			// Limit the time a refill can take.
			ctx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()

			// Add tracing.
			ctx, span := tracing.Tracer.Start(ctx, "refillAccount")
			defer span.End()
			span.SetAttributes(attribute.Stringer("host", contract.HostKey))
			defer func() {
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, "failed to refill account")
				}
			}()

			// Fetch the account.
			account, err := w.Account(ctx, contract.HostKey)
			if err != nil {
				return err
			}

			// Add more tracing info.
			span.SetAttributes(attribute.Stringer("account", account.ID))
			span.SetAttributes(attribute.Stringer("balance", account.Balance))

			// Check if a host is potentially cheating before refilling.
			// We only check against the max drift if the account's drift is
			// negative because we don't care if we have more money than
			// expected.
			if account.Drift.Cmp(maxNegDrift) < 0 {
				a.logger.Error("not refilling account since host is potentially cheating",
					"account", account.ID,
					"host", contract.HostKey,
					"balance", account.Balance,
					"drift", account.Drift)
				return fmt.Errorf("drift on account is too large - not funding")
			}

			// Check if a resync is needed.
			if account.RequiresSync {
				err := w.RHPSync(ctx, contract.ID, contract.HostKey, contract.HostIP, contract.SiamuxAddr)
				if err != nil {
					a.logger.Errorw(fmt.Sprintf("failed to sync account's balance: %s", err),
						"account", account.ID,
						"host", contract.HostKey)
					return err
				}
				// Re-fetch account after sync.
				account, err = w.Account(ctx, contract.HostKey)
				if err != nil {
					return err
				}
			}

			// Check if refill is needed and perform it if necessary.
			if account.Balance.Cmp(minBalance) >= 0 {
				return nil // nothing to do
			}

			if err := w.RHPFund(ctx, contract.ID, contract.HostKey, contract.HostIP, contract.SiamuxAddr, maxBalance); err != nil {
				a.logger.Errorw(fmt.Sprintf("failed to fund account: %s", err),
					"account", account.ID,
					"host", contract.HostKey,
					"balance", account.Balance,
					"expected", maxBalance)
				return err
			}
			a.logger.Infow("Successfully funded account",
				"account", account.ID,
				"host", contract.HostKey,
				"balance", maxBalance)
			return nil
		}(contract)
	}
}
