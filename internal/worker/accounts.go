package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	"go.uber.org/zap"
)

var (
	ErrAccountNotFound = errors.New("account doesn't exist")

	errMaxDriftExceeded = errors.New("drift on account is too large")
)

var (
	minBalance  = types.Siacoins(1).Div64(2).Big()
	maxBalance  = types.Siacoins(1)
	maxNegDrift = new(big.Int).Neg(types.Siacoins(10).Big())

	alertAccountRefillID = alerts.RandomAlertID() // constant until restarted
)

type (
	AccountMgrWorker interface {
		FundAccount(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, siamuxAddr string, balance types.Currency) error
		SyncAccount(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, siamuxAddr string) error
	}

	AccountStore interface {
		Accounts(context.Context, string) ([]api.Account, error)
		UpdateAccounts(context.Context, string, []api.Account, bool) error
	}

	ConsensusState interface {
		ConsensusState(ctx context.Context) (api.ConsensusState, error)
	}

	DownloadContracts interface {
		DownloadContracts(ctx context.Context) ([]api.ContractMetadata, error)
	}
)

type (
	AccountMgr struct {
		w                        AccountMgrWorker
		dc                       DownloadContracts
		cs                       ConsensusState
		s                        AccountStore
		key                      types.PrivateKey
		logger                   *zap.SugaredLogger
		owner                    string
		refillInterval           time.Duration
		revisionSubmissionBuffer uint64
		shutdownCtx              context.Context
		shutdownCancel           context.CancelFunc
		wg                       sync.WaitGroup

		mu                  sync.Mutex
		byID                map[rhpv3.Account]*Account
		inProgressRefills   map[types.PublicKey]struct{}
		lastLoggedRefillErr map[types.PublicKey]time.Time
	}

	Account struct {
		key    types.PrivateKey
		logger *zap.SugaredLogger

		rwmu sync.RWMutex

		mu               sync.Mutex
		requiresSyncTime time.Time
		acc              api.Account
	}
)

// NewAccountManager creates a new account manager. It will load all accounts
// from the given store and mark the shutdown as unclean. When Shutdown is
// called it will save all accounts.
func NewAccountManager(key types.PrivateKey, owner string, w AccountMgrWorker, cs ConsensusState, dc DownloadContracts, s AccountStore, refillInterval time.Duration, l *zap.Logger) (*AccountMgr, error) {
	logger := l.Named("accounts").Sugar()

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	a := &AccountMgr{
		w:      w,
		cs:     cs,
		dc:     dc,
		s:      s,
		key:    key,
		logger: logger,
		owner:  owner,

		inProgressRefills:   make(map[types.PublicKey]struct{}),
		lastLoggedRefillErr: make(map[types.PublicKey]time.Time),
		refillInterval:      refillInterval,
		shutdownCtx:         shutdownCtx,
		shutdownCancel:      shutdownCancel,

		byID: make(map[rhpv3.Account]*Account),
	}
	a.wg.Add(1)
	go func() {
		a.run()
		a.wg.Done()
	}()
	return a, nil
}

// Account returns the account with the given id.
func (a *AccountMgr) Account(hostKey types.PublicKey) api.Account {
	acc := a.account(hostKey)
	return acc.convert()
}

// Accounts returns all accounts.
func (a *AccountMgr) Accounts() []api.Account {
	a.mu.Lock()
	defer a.mu.Unlock()
	accounts := make([]api.Account, 0, len(a.byID))
	for _, acc := range a.byID {
		accounts = append(accounts, acc.convert())
	}
	return accounts
}

// ResetDrift resets the drift on an account.
func (a *AccountMgr) ResetDrift(id rhpv3.Account) error {
	a.mu.Lock()
	account, exists := a.byID[id]
	if !exists {
		a.mu.Unlock()
		return ErrAccountNotFound
	}
	a.mu.Unlock()

	account.resetDrift()
	return nil
}

func (a *AccountMgr) Shutdown(ctx context.Context) error {
	accounts := a.Accounts()
	err := a.s.UpdateAccounts(ctx, a.owner, accounts, false)
	if err != nil {
		a.logger.Errorf("failed to save %v accounts: %v", len(accounts), err)
		return err
	}
	a.logger.Infof("successfully saved %v accounts", len(accounts))

	a.shutdownCancel()
	a.wg.Wait()
	return nil
}

func (a *AccountMgr) account(hk types.PublicKey) *Account {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Derive account key.
	accKey := deriveAccountKey(a.key, hk)
	accID := rhpv3.Account(accKey.PublicKey())

	// Create account if it doesn't exist.
	acc, exists := a.byID[accID]
	if !exists {
		acc = &Account{
			key:    accKey,
			logger: a.logger.Named(accID.String()),
			acc: api.Account{
				ID:            accID,
				CleanShutdown: false,
				HostKey:       hk,
				Balance:       big.NewInt(0),
				Drift:         big.NewInt(0),
				RequiresSync:  true, // force sync on new account
			},
		}
		a.byID[accID] = acc
	}
	return acc
}

// ForHost returns an account to use for a given host. If the account
// doesn't exist, a new one is created.
func (a *AccountMgr) ForHost(hk types.PublicKey) *Account {
	return a.account(hk)
}

func (a *AccountMgr) run() {
	// wait for store to become available
	var saved []api.Account
	var err error
	ticker := time.NewTicker(5 * time.Second)
	for {
		aCtx, cancel := context.WithTimeout(a.shutdownCtx, 30*time.Second)
		saved, err = a.s.Accounts(aCtx, a.owner)
		cancel()
		if err == nil {
			break
		}

		a.logger.Warn("failed to fetch accounts from bus - retrying in a few seconds", zap.Error(err))
		select {
		case <-a.shutdownCtx.Done():
			return
		case <-ticker.C:
		}
	}

	// stop ticker
	ticker.Stop()
	select {
	case <-ticker.C:
	default:
	}

	// add accounts
	a.mu.Lock()
	accounts := make(map[rhpv3.Account]*Account, len(saved))
	for _, acc := range saved {
		accKey := deriveAccountKey(a.key, acc.HostKey)
		if rhpv3.Account(accKey.PublicKey()) != acc.ID {
			a.logger.Errorf("account key derivation mismatch %v != %v", accKey.PublicKey(), acc.ID)
			continue
		}
		acc.RequiresSync = true // force sync on reboot
		account := &Account{
			acc:              acc,
			key:              accKey,
			logger:           a.logger.Named(acc.ID.String()),
			requiresSyncTime: time.Now(),
		}
		accounts[account.acc.ID] = account
	}
	a.mu.Unlock()

	// mark the shutdown as unclean, this will be overwritten on shutdown
	err = a.s.UpdateAccounts(a.shutdownCtx, a.owner, nil, true)
	if err != nil {
		a.logger.Error("failed to mark account shutdown as unclean", zap.Error(err))
	}

	ticker = time.NewTicker(a.refillInterval)
	for {
		select {
		case <-a.shutdownCtx.Done():
			return // shutdown
		case <-ticker.C:
		}
		a.refillAccounts()
	}
}

func (a *AccountMgr) markRefillInProgress(hk types.PublicKey) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, inProgress := a.inProgressRefills[hk]
	if inProgress {
		return false
	}
	a.inProgressRefills[hk] = struct{}{}
	return true
}

func (a *AccountMgr) markRefillDone(hk types.PublicKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, inProgress := a.inProgressRefills[hk]
	if !inProgress {
		panic("releasing a refill that hasn't been in progress")
	}
	delete(a.inProgressRefills, hk)
}

// refillWorkerAccounts refills all accounts on a worker that require a refill.
// To avoid slow hosts preventing refills for fast hosts, a separate goroutine
// is used for every host. If a slow host's account is still being refilled by a
// goroutine from a previous call, refillWorkerAccounts will skip that account
// until the previously launched goroutine returns.
func (a *AccountMgr) refillAccounts() {
	// fetch config
	cs, err := a.cs.ConsensusState(a.shutdownCtx)
	if err != nil {
		a.logger.Errorw(fmt.Sprintf("failed to fetch consensus state for refill: %v", err))
		return
	}

	// fetch all contracts
	contracts, err := a.dc.DownloadContracts(a.shutdownCtx)
	if err != nil {
		a.logger.Errorw(fmt.Sprintf("failed to fetch contracts for refill: %v", err))
		return
	} else if len(contracts) == 0 {
		return
	}

	// refill accounts in separate goroutines
	for _, c := range contracts {
		// launch refill if not already in progress
		if a.markRefillInProgress(c.HostKey) {
			go func(contract api.ContractMetadata) {
				defer a.markRefillDone(contract.HostKey)

				rCtx, cancel := context.WithTimeout(a.shutdownCtx, 5*time.Minute)
				defer cancel()

				// refill
				err := a.refillAccount(rCtx, c, cs.BlockHeight, a.revisionSubmissionBuffer)

				// determine whether to log something
				shouldLog := true
				a.mu.Lock()
				if t, exists := a.lastLoggedRefillErr[contract.HostKey]; !exists || err == nil {
					a.lastLoggedRefillErr[contract.HostKey] = time.Now()
				} else if time.Since(t) < time.Hour {
					// only log error once per hour per account
					shouldLog = false
				}
				a.mu.Unlock()

				if err != nil && shouldLog {
					a.logger.Error("failed to refill account for host", zap.Stringer("hostKey", contract.HostKey), zap.Error(err))
				} else {
					a.logger.Infow("successfully refilled account for host", zap.Stringer("hostKey", contract.HostKey), zap.Error(err))
				}
			}(c)
		}
	}
}

func (a *AccountMgr) refillAccount(ctx context.Context, contract api.ContractMetadata, bh, revisionSubmissionBuffer uint64) error {
	// fetch the account
	account := a.Account(contract.HostKey)

	// check if the contract is too close to the proof window to be revised,
	// trying to refill the account would result in the host not returning the
	// revision and returning an obfuscated error
	if (bh + revisionSubmissionBuffer) > contract.WindowStart {
		return fmt.Errorf("contract %v is too close to the proof window to be revised", contract.ID)
	}

	// check if a host is potentially cheating before refilling.
	// We only check against the max drift if the account's drift is
	// negative because we don't care if we have more money than
	// expected.
	if account.Drift.Cmp(maxNegDrift) < 0 {
		// TODO: register alert
		_ = newAccountRefillAlert(account.ID, contract, errMaxDriftExceeded,
			"accountID", account.ID.String(),
			"hostKey", contract.HostKey.String(),
			"balance", account.Balance.String(),
			"drift", account.Drift.String(),
		)
		return fmt.Errorf("not refilling account since host is potentially cheating: %w", errMaxDriftExceeded)
	} else {
		// TODO: dismiss alert on success
	}

	// check if a resync is needed
	if account.RequiresSync {
		// sync the account
		err := a.w.SyncAccount(ctx, contract.ID, contract.HostKey, contract.SiamuxAddr)
		if err != nil {
			return fmt.Errorf("failed to sync account's balance: %w", err)
		}

		// refetch the account after syncing
		account = a.Account(contract.HostKey)
	}

	// check if refill is needed
	if account.Balance.Cmp(minBalance) >= 0 {
		return nil
	}

	// fund the account
	err := a.w.FundAccount(ctx, contract.ID, contract.HostKey, contract.SiamuxAddr, maxBalance)
	if err != nil {
		return fmt.Errorf("failed to fund account: %w", err)
	}
	return nil
}

// WithSync syncs an accounts balance with the bus. To do so, the account is
// locked while the balance is fetched through balanceFn.
func (a *Account) WithSync(balanceFn func() (types.Currency, error)) error {
	a.rwmu.Lock()
	defer a.rwmu.Unlock()

	balance, err := balanceFn()
	if err != nil {
		return err
	}

	a.setBalance(balance.Big())
	return nil
}

func (a *Account) ID() rhpv3.Account {
	return a.acc.ID
}

func (a *Account) Key() types.PrivateKey {
	return a.key
}

// WithDeposit increases the balance of an account by the amount returned by
// amtFn if amtFn doesn't return an error.
func (a *Account) WithDeposit(amtFn func(types.Currency) (types.Currency, error)) error {
	a.rwmu.RLock()
	defer a.rwmu.RUnlock()

	a.mu.Lock()
	balance := types.NewCurrency(a.acc.Balance.Uint64(), new(big.Int).Rsh(a.acc.Balance, 64).Uint64())
	a.mu.Unlock()

	amt, err := amtFn(balance)
	if err != nil {
		return err
	}
	a.addAmount(amt.Big())
	return nil
}

// WithWithdrawal decreases the balance of an account by the amount returned by
// amtFn. The amount is still withdrawn if amtFn returns an error since some
// costs are non-refundable.
func (a *Account) WithWithdrawal(amtFn func() (types.Currency, error)) error {
	a.rwmu.RLock()
	defer a.rwmu.RUnlock()

	// return early if the account needs to sync
	a.mu.Lock()
	if a.acc.RequiresSync {
		a.mu.Unlock()
		return fmt.Errorf("%w; account requires resync", rhp3.ErrBalanceInsufficient)
	}

	// return early if our account is not funded
	if a.acc.Balance.Cmp(big.NewInt(0)) <= 0 {
		a.mu.Unlock()
		return rhp3.ErrBalanceInsufficient
	}
	a.mu.Unlock()

	// execute amtFn
	amt, err := amtFn()

	// in case of an insufficient balance, we schedule a sync
	if rhp3.IsBalanceInsufficient(err) {
		a.scheduleSync()
	}

	// if an amount was returned, we withdraw it
	if !amt.IsZero() {
		a.addAmount(new(big.Int).Neg(amt.Big()))
	}
	return err
}

// AddAmount applies the provided amount to an account through addition. So the
// input can be both a positive or negative number depending on whether a
// withdrawal or deposit is recorded. If the account doesn't exist, it is
// created.
func (a *Account) addAmount(amt *big.Int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Update balance.
	balanceBefore := a.acc.Balance
	a.acc.Balance.Add(a.acc.Balance, amt)

	// Log deposits.
	if amt.Cmp(big.NewInt(0)) > 0 {
		a.logger.Infow("account balance was increased",
			"account", a.acc.ID,
			"host", a.acc.HostKey.String(),
			"amt", amt.String(),
			"balanceBefore", balanceBefore,
			"balanceAfter", a.acc.Balance.String())
	}
}

func (a *Account) convert() api.Account {
	a.mu.Lock()
	defer a.mu.Unlock()
	return api.Account{
		ID:            a.acc.ID,
		CleanShutdown: a.acc.CleanShutdown,
		HostKey:       a.acc.HostKey,
		Balance:       new(big.Int).Set(a.acc.Balance),
		Drift:         new(big.Int).Set(a.acc.Drift),
		RequiresSync:  a.acc.RequiresSync,
	}
}

func (a *Account) resetDrift() {
	a.mu.Lock()
	a.acc.Drift.SetInt64(0)
	a.mu.Unlock()
}

// scheduleSync sets the requiresSync flag of an account.
func (a *Account) scheduleSync() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Only update the sync flag to 'true' if some time has passed since the
	// last time it was set. That way we avoid multiple workers setting it after
	// failing at the same time, causing multiple syncs in the process.
	if time.Since(a.requiresSyncTime) < 30*time.Second {
		a.logger.Warn("not scheduling account sync since it was scheduled too recently", zap.Stringer("account", a.acc.ID))
		return
	}
	a.acc.RequiresSync = true
	a.requiresSyncTime = time.Now()

	// Log scheduling a sync.
	a.logger.Infow("account sync was scheduled",
		"account", a.acc.ID,
		"host", a.acc.HostKey.String(),
		"balance", a.acc.Balance.String(),
		"drift", a.acc.Drift.String())
}

// setBalance sets the balance of a given account to the provided amount. If the
// account doesn't exist, it is created.
// If an account hasn't been saved successfully upon the last shutdown, no drift
// will be added upon the first call to SetBalance.
func (a *Account) setBalance(balance *big.Int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Update balance and drift.
	delta := new(big.Int).Sub(balance, a.acc.Balance)
	balanceBefore := a.acc.Balance.String()
	driftBefore := a.acc.Drift.String()
	if a.acc.CleanShutdown {
		a.acc.Drift = a.acc.Drift.Add(a.acc.Drift, delta)
	}
	a.acc.Balance.Set(balance)
	a.acc.CleanShutdown = true
	a.acc.RequiresSync = false // resetting the balance resets the sync field
	balanceAfter := a.acc.Balance.String()

	// Log resets.
	a.logger.Infow("account balance was reset",
		"account", a.acc.ID,
		"host", a.acc.HostKey.String(),
		"balanceBefore", balanceBefore,
		"balanceAfter", balanceAfter,
		"driftBefore", driftBefore,
		"driftAfter", a.acc.Drift.String(),
		"delta", delta.String())
}

// deriveAccountKey derives an account plus key for a given host and worker.
// Each worker has its own account for a given host. That makes concurrency
// around keeping track of an accounts balance and refilling it a lot easier in
// a multi-worker setup.
func deriveAccountKey(mgrKey types.PrivateKey, hostKey types.PublicKey) types.PrivateKey {
	index := byte(0) // not used yet but can be used to derive more than 1 account per host

	// Append the host for which to create it and the index to the
	// corresponding sub-key.
	subKey := mgrKey
	data := make([]byte, 0, len(subKey)+len(hostKey)+1)
	data = append(data, subKey[:]...)
	data = append(data, hostKey[:]...)
	data = append(data, index)

	seed := types.HashBytes(data)
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

func newAccountRefillAlert(id rhpv3.Account, contract api.ContractMetadata, err error, keysAndValues ...string) alerts.Alert {
	data := map[string]interface{}{
		"error":      err.Error(),
		"accountID":  id.String(),
		"contractID": contract.ID.String(),
		"hostKey":    contract.HostKey.String(),
	}
	for i := 0; i < len(keysAndValues); i += 2 {
		data[keysAndValues[i]] = keysAndValues[i+1]
	}

	return alerts.Alert{
		ID:        alerts.IDForAccount(alertAccountRefillID, id),
		Severity:  alerts.SeverityError,
		Message:   "Ephemeral account refill failed",
		Data:      data,
		Timestamp: time.Now(),
	}
}
