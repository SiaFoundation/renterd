package autopilot

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	rhpv3 "go.sia.tech/core/rhp/v3"
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
	ap *Autopilot
	a  AccountStore
	c  ContractStore
	l  *zap.SugaredLogger
	w  *workerPool

	refillInterval time.Duration

	mu                sync.Mutex
	inProgressRefills map[types.Hash256]struct{}
}

type AccountStore interface {
	Account(ctx context.Context, id rhpv3.Account, host types.PublicKey) (account api.Account, err error)
	Accounts(ctx context.Context) (accounts []api.Account, err error)
}

type ContractStore interface {
	Contracts(ctx context.Context) ([]api.ContractMetadata, error)
	ContractSetContracts(ctx context.Context, set string) ([]api.ContractMetadata, error)
}

func newAccounts(ap *Autopilot, a AccountStore, c ContractStore, w *workerPool, l *zap.SugaredLogger, refillInterval time.Duration) *accounts {
	return &accounts{
		ap: ap,
		a:  a,
		c:  c,
		l:  l.Named("accounts"),
		w:  w,

		refillInterval:    refillInterval,
		inProgressRefills: make(map[types.Hash256]struct{}),
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

func (a *accounts) refillWorkersAccountsLoop(stopChan <-chan struct{}) {
	ticker := time.NewTicker(a.refillInterval)

	for {
		select {
		case <-stopChan:
			return // shutdown
		case <-ticker.C:
		}

		a.w.withWorker(func(w Worker) {
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

	// fetch config
	cfg, err := a.ap.Config(ctx)
	if err != nil {
		a.l.Errorw(fmt.Sprintf("could not fetch config, err: %v", err))
		return
	}

	// fetch worker id
	workerID, err := w.ID(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to fetch worker id")
		a.l.Errorw(fmt.Sprintf("failed to fetch worker id for refill: %v", err))
		return
	}

	// fetch all contracts
	contracts, err := a.c.Contracts(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to fetch contracts")
		a.l.Errorw(fmt.Sprintf("failed to fetch contracts for refill: %v", err))
		return
	}

	// fetch all contract set contracts
	contractSetContracts, err := a.c.ContractSetContracts(ctx, cfg.Contracts.Set)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("failed to fetch contracts for set '%s'", cfg.Contracts.Set))
		a.l.Errorw(fmt.Sprintf("failed to fetch contract set contracts: %v", err))
		return
	}

	// build a map of contract set contracts
	inContractSet := make(map[types.FileContractID]struct{})
	for _, contract := range contractSetContracts {
		inContractSet[contract.ID] = struct{}{}
	}

	// refill accounts in separate goroutines
	for _, c := range contracts {
		// add logging for contracts in the set
		logger := zap.NewNop().Sugar()
		if _, inSet := inContractSet[c.ID]; inSet {
			logger = a.l
		}

		// launch refill if not already in progress
		if a.markRefillInProgress(workerID, c.HostKey) {
			go func(contract api.ContractMetadata, l *zap.SugaredLogger) {
				rCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
				if accountID, refilled, err := refillWorkerAccount(rCtx, a.a, w, workerID, contract, l); err == nil && refilled {
					a.l.Infow("Successfully funded account",
						"account", accountID,
						"host", contract.HostKey,
						"balance", maxBalance,
					)
				}
				a.markRefillDone(workerID, contract.HostKey)
				cancel()
			}(c, logger)
		}
	}
}

func refillWorkerAccount(ctx context.Context, a AccountStore, w Worker, workerID string, contract api.ContractMetadata, logger *zap.SugaredLogger) (accountID rhpv3.Account, refilled bool, err error) {
	// add tracing
	ctx, span := tracing.Tracer.Start(ctx, "refillAccount")
	span.SetAttributes(attribute.Stringer("host", contract.HostKey))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to refill account")
		}
		span.End()
	}()

	// fetch the account
	accountID, err = w.Account(ctx, contract.HostKey)
	if err != nil {
		return
	}
	var account api.Account
	account, err = a.Account(ctx, accountID, contract.HostKey)
	if err != nil {
		return
	}

	// update span
	span.SetAttributes(attribute.Stringer("account", account.ID))
	span.SetAttributes(attribute.Stringer("balance", account.Balance))

	// check if a host is potentially cheating before refilling.
	// We only check against the max drift if the account's drift is
	// negative because we don't care if we have more money than
	// expected.
	if account.Drift.Cmp(maxNegDrift) < 0 {
		logger.Errorw("not refilling account since host is potentially cheating",
			"account", account.ID,
			"host", contract.HostKey,
			"balance", account.Balance,
			"drift", account.Drift)
		err = fmt.Errorf("drift on account is too large - not funding")
		return
	}

	// check if a resync is needed
	if account.RequiresSync {
		// sync the account
		err = w.RHPSync(ctx, contract.ID, contract.HostKey, contract.HostIP, contract.SiamuxAddr)
		if err != nil {
			logger.Errorw(fmt.Sprintf("failed to sync account's balance: %s", err),
				"account", account.ID,
				"host", contract.HostKey,
			)
			return
		}

		// refetch the account after syncing
		account, err = a.Account(ctx, accountID, contract.HostKey)
		if err != nil {
			return
		}
	}

	// check if refill is needed
	if account.Balance.Cmp(minBalance) >= 0 {
		return
	}

	// fund the account
	err = w.RHPFund(ctx, contract.ID, contract.HostKey, contract.HostIP, contract.SiamuxAddr, maxBalance)
	if err != nil {
		logger.Errorw(fmt.Sprintf("failed to fund account: %s", err),
			"account", account.ID,
			"host", contract.HostKey,
			"balance", account.Balance,
			"expected", maxBalance)
	} else {
		refilled = true
	}
	return
}
