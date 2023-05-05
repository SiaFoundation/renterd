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
	"go.sia.tech/renterd/tracing"
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
	setContracts      map[types.FileContractID]struct{}
	activeContracts   []api.ContractMetadata
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
	if cfg.Contracts.Set == "" {
		a.logger.Warn("could not update contracts, no contract set configured")
		return
	}

	acs, err := a.b.ActiveContracts(ctx)
	if err != nil {
		a.logger.Errorw(fmt.Sprintf("failed to fetch active contracts: %v", err))
		return
	}

	csc, err := a.b.Contracts(ctx, cfg.Contracts.Set)
	if err != nil {
		a.logger.Errorw(fmt.Sprintf("failed to fetch contracts in set %s: %v", cfg.Contracts.Set, err))
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.setContracts = make(map[types.FileContractID]struct{}, len(csc))
	for _, c := range csc {
		a.setContracts[c.ID] = struct{}{}
	}
	a.activeContracts = append(a.activeContracts[:0], acs...)
}

func (a *accounts) refillWorkersAccountsLoop(stopChan <-chan struct{}) {
	ticker := time.NewTicker(a.refillInterval)

	for {
		select {
		case <-stopChan:
			return // shutdown
		case <-ticker.C:
		}

		a.workers.withWorker(func(w Worker) {
			a.refillWorkerAccounts(w)
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

	for hk, contracts := range a.contractsToRefill() {
		// skip if refill is in progress
		if !a.markRefillInProgress(workerID, hk) {
			continue
		}

		// launch a goroutine to refill the account
		go func(hk types.PublicKey, contracts []api.ContractMetadata) error {
			defer a.markRefillDone(workerID, hk)

			// decide whether one of the potentially multiple contracts is in
			// the contract set, we only want to log errors if that is the case
			// because we expect refills to fail often for contracts that are
			// not in the set
			var hasContractSetContract bool
			for _, contract := range contracts {
				if a.isContractSetContract(contract.ID) {
					hasContractSetContract = true
					break
				}
			}

			// make sure we don't hang
			ctx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()

			// add tracing
			ctx, span := tracing.Tracer.Start(ctx, "refillAccount")
			defer span.End()
			defer func() {
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, "failed to refill account")
				}
			}()
			span.SetAttributes(attribute.Stringer("host", hk))

			// fetch the account
			accountID, err := w.Account(ctx, hk)
			if err != nil {
				return err
			}
			account, err := a.b.Account(ctx, accountID, hk)
			if err != nil {
				return err
			}
			span.SetAttributes(attribute.Stringer("account", account.ID))
			span.SetAttributes(attribute.Stringer("balance", account.Balance))

			// check if a host is potentially cheating before refilling.
			// We only check against the max drift if the account's drift is
			// negative because we don't care if we have more money than
			// expected.
			if account.Drift.Cmp(maxNegDrift) < 0 {
				a.logger.Error("not refilling account since host is potentially cheating",
					"account", account.ID,
					"host", hk,
					"balance", account.Balance,
					"drift", account.Drift)
				return fmt.Errorf("drift on account is too large - not funding")
			}

			// check if a resync is needed
			if account.RequiresSync {
				var syncErr error
				for _, contract := range contracts {
					syncErr = w.RHPSync(ctx, contract.ID, contract.HostKey, contract.HostIP, contract.SiamuxAddr)
					if syncErr == nil {
						break
					}
				}
				if syncErr != nil && hasContractSetContract {
					a.logger.Errorw(fmt.Sprintf("failed to sync account's balance: %s", syncErr),
						"account", account.ID,
						"host", hk,
					)
					return syncErr
				}
				account, err = a.b.Account(ctx, accountID, hk)
				if err != nil {
					return err
				}
			}

			// check if refill is needed
			if account.Balance.Cmp(minBalance) >= 0 {
				return nil
			}

			// fund the account
			var fundErr error
			for _, contract := range contracts {
				fundErr = w.RHPFund(ctx, contract.ID, contract.HostKey, contract.HostIP, contract.SiamuxAddr, maxBalance)
				if fundErr == nil {
					break
				}
			}
			if fundErr == nil {
				a.logger.Infow("Successfully funded account",
					"account", account.ID,
					"host", hk,
					"balance", maxBalance,
				)
			} else if hasContractSetContract {
				a.logger.Errorw(fmt.Sprintf("failed to fund account: %s", fundErr),
					"account", account.ID,
					"host", hk,
					"balance", account.Balance,
					"expected", maxBalance)
				return fundErr
			}
			return nil
		}(hk, contracts)
	}
}

func (a *accounts) contractsToRefill() map[types.PublicKey][]api.ContractMetadata {
	a.mu.Lock()
	defer a.mu.Unlock()

	contracts := make(map[types.PublicKey][]api.ContractMetadata)
	for _, contract := range a.activeContracts {
		contracts[contract.HostKey] = append(contracts[contract.HostKey], contract)
	}
	return contracts
}

func (a *accounts) isContractSetContract(id types.FileContractID) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, exists := a.setContracts[id]
	return exists
}
