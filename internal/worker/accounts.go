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
	"go.sia.tech/renterd/internal/utils"
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
	AccountFunder interface {
		FundAccount(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, desired types.Currency) error
	}

	AccountSyncer interface {
		SyncAccount(ctx context.Context, fcid types.FileContractID, host api.HostInfo) error
	}

	AccountStore interface {
		Accounts(context.Context, string) ([]api.Account, error)
		UpdateAccounts(context.Context, []api.Account) error
	}

	ConsensusStateStore interface {
		ConsensusState(ctx context.Context) (api.ConsensusState, error)
	}

	ContractStore interface {
		Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error)
	}

	HostStore interface {
		UsableHosts(ctx context.Context) ([]api.HostInfo, error)
	}
)

type (
	AccountMgr struct {
		alerts         alerts.Alerter
		funder         AccountFunder
		syncer         AccountSyncer
		cs             ContractStore
		hs             HostStore
		css            ConsensusStateStore
		s              AccountStore
		key            utils.AccountsKey
		logger         *zap.SugaredLogger
		owner          string
		refillInterval time.Duration
		shutdownCtx    context.Context
		shutdownCancel context.CancelFunc
		wg             sync.WaitGroup

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
func NewAccountManager(key utils.AccountsKey, owner string, alerter alerts.Alerter, funder AccountFunder, syncer AccountSyncer, css ConsensusStateStore, cs ContractStore, hs HostStore, s AccountStore, refillInterval time.Duration, l *zap.Logger) (*AccountMgr, error) {
	logger := l.Named("accounts").Sugar()

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	a := &AccountMgr{
		alerts: alerter,
		funder: funder,
		syncer: syncer,
		cs:     cs,
		hs:     hs,
		css:    css,
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
	err := a.s.UpdateAccounts(ctx, accounts)
	if err != nil {
		a.logger.Errorf("failed to save %v accounts: %v", len(accounts), err)
		return err
	}
	a.logger.Infof("successfully saved %v accounts", len(accounts))

	a.shutdownCancel()

	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return fmt.Errorf("accountMgrShutdown interrupted: %w", context.Cause(ctx))
	case <-done:
	}
	return nil
}

func (a *AccountMgr) account(hk types.PublicKey) *Account {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Derive account key.
	accKey := a.key.DeriveAccountKey(hk)
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
				Owner:         a.owner,
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
		accKey := a.key.DeriveAccountKey(acc.HostKey)
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
	uncleanAccounts := append([]api.Account(nil), saved...)
	for i := range uncleanAccounts {
		uncleanAccounts[i].CleanShutdown = false
	}
	err = a.s.UpdateAccounts(a.shutdownCtx, uncleanAccounts)
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
	// fetch all contracts
	contracts, err := a.cs.Contracts(a.shutdownCtx, api.ContractsOpts{})
	if err != nil {
		a.logger.Errorw(fmt.Sprintf("failed to fetch contracts for refill: %v", err))
		return
	} else if len(contracts) == 0 {
		return
	}

	// fetch all usable hosts
	hosts, err := a.hs.UsableHosts(a.shutdownCtx)
	if err != nil {
		a.logger.Errorw(fmt.Sprintf("failed to fetch usable hosts for refill: %v", err))
		return
	}
	hk2Host := make(map[types.PublicKey]api.HostInfo)
	for _, host := range hosts {
		hk2Host[host.PublicKey] = host
	}

	// refill accounts in separate goroutines
	for _, c := range contracts {
		// launch refill if not already in progress
		if a.markRefillInProgress(c.HostKey) {
			go func(contract api.ContractMetadata) {
				defer a.markRefillDone(contract.HostKey)

				rCtx, cancel := context.WithTimeout(a.shutdownCtx, 5*time.Minute)
				defer cancel()

				host, exists := hk2Host[contract.HostKey]
				if !exists {
					return
				}

				// refill
				refilled, err := a.refillAccount(rCtx, c, host)

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
				} else if refilled {
					a.logger.Infow("successfully refilled account for host", zap.Stringer("hostKey", contract.HostKey), zap.Error(err))
				}
			}(c)
		}
	}
}

func (a *AccountMgr) refillAccount(ctx context.Context, contract api.ContractMetadata, host api.HostInfo) (bool, error) {
	// fetch the account
	account := a.Account(contract.HostKey)

	// check if a host is potentially cheating before refilling.
	// We only check against the max drift if the account's drift is
	// negative because we don't care if we have more money than
	// expected.
	if account.Drift.Cmp(maxNegDrift) < 0 {
		alert := newAccountRefillAlert(account.ID, contract, errMaxDriftExceeded,
			"accountID", account.ID.String(),
			"hostKey", contract.HostKey.String(),
			"balance", account.Balance.String(),
			"drift", account.Drift.String(),
		)
		_ = a.alerts.RegisterAlert(a.shutdownCtx, alert)
		return false, fmt.Errorf("not refilling account since host is potentially cheating: %w", errMaxDriftExceeded)
	} else {
		_ = a.alerts.DismissAlerts(a.shutdownCtx, alerts.IDForAccount(alertAccountRefillID, account.ID))
	}

	// check if a resync is needed
	if account.RequiresSync {
		// sync the account
		err := a.syncer.SyncAccount(ctx, contract.ID, host)
		if err != nil {
			return false, fmt.Errorf("failed to sync account's balance: %w", err)
		}

		// refetch the account after syncing
		account = a.Account(contract.HostKey)
	}

	// check if refill is needed
	if account.Balance.Cmp(minBalance) >= 0 {
		return false, nil
	}

	// fund the account
	err := a.funder.FundAccount(ctx, contract.ID, contract.HostKey, maxBalance)
	if err != nil {
		return false, fmt.Errorf("failed to fund account: %w", err)
	}
	return true, nil
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
		a.ScheduleSync()
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
		Owner:         a.acc.Owner,
		RequiresSync:  a.acc.RequiresSync,
	}
}

func (a *Account) resetDrift() {
	a.mu.Lock()
	a.acc.Drift.SetInt64(0)
	a.mu.Unlock()
}

// scheduleSync sets the requiresSync flag of an account.
func (a *Account) ScheduleSync() {
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
		"account", a.ID,
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

	// save previous values
	prevBalance := new(big.Int).Set(a.acc.Balance)
	prevDrift := new(big.Int).Set(a.acc.Drift)

	// update balance
	a.acc.Balance.Set(balance)

	// update drift
	drift := new(big.Int).Sub(balance, prevBalance)
	if a.acc.CleanShutdown {
		a.acc.Drift = a.acc.Drift.Add(a.acc.Drift, drift)
	}

	// reset fields
	a.acc.CleanShutdown = true
	a.acc.RequiresSync = false

	// log account changes
	a.logger.Infow("account balance was reset",
		zap.Stringer("account", a.acc.ID),
		zap.Stringer("host", a.acc.HostKey),
		zap.Stringer("balanceBefore", prevBalance),
		zap.Stringer("balanceAfter", balance),
		zap.Stringer("driftBefore", prevDrift),
		zap.Stringer("driftAfter", a.acc.Drift),
		zap.Bool("firstDrift", a.acc.Drift.Cmp(big.NewInt(0)) != 0 && prevDrift.Cmp(big.NewInt(0)) == 0),
		zap.Bool("cleanshutdown", a.acc.CleanShutdown),
		zap.Stringer("drift", drift))
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
