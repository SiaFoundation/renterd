package bus

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	busAccountOwner = "bus"
)

var (
	ErrAccountNotFound = errors.New("account doesn't exist")
)

type (
	AccountStore interface {
		Accounts(context.Context, string) ([]api.Account, error)
		SaveAccounts(context.Context, string, []api.Account) error
		SetUncleanShutdown(context.Context, string) error
	}
)

type (
	AccountMgr struct {
		s      AccountStore
		logger *zap.SugaredLogger

		mu   sync.Mutex
		byID map[rhpv3.Account]*account
	}

	account struct {
		mu               sync.Mutex
		locks            map[uint64]*accountLock
		requiresSyncTime time.Time
		api.Account

		rwmu sync.RWMutex
	}

	accountLock struct {
		heldByID uint64
		unlock   func()
		timer    *time.Timer
	}
)

// NewAccountManager creates a new account manager. It will load all accounts
// from the given store and mark the shutdown as unclean. When Shutdown is
// called it will save all accounts.
func NewAccountManager(ctx context.Context, s AccountStore, logger *zap.Logger) (*AccountMgr, error) {
	logger = logger.Named("accounts")

	// load saved accounts
	saved, err := s.Accounts(ctx, busAccountOwner)
	if err != nil {
		return nil, err
	}

	// wrap with a lock
	accounts := make(map[rhpv3.Account]*account, len(saved))
	for _, acc := range saved {
		account := &account{
			Account: acc,
			locks:   map[uint64]*accountLock{},
		}
		accounts[account.ID] = account
	}

	// mark the shutdown as unclean, this will be overwritten on shutdown
	err = s.SetUncleanShutdown(ctx, busAccountOwner)
	if err != nil {
		return nil, fmt.Errorf("failed to mark account shutdown as unclean: %w", err)
	}

	return &AccountMgr{
		s:      s,
		logger: logger.Sugar(),

		byID: accounts,
	}, nil
}

// Account returns the account with the given id.
func (a *AccountMgr) Account(id rhpv3.Account, hostKey types.PublicKey) (api.Account, error) {
	acc := a.account(id, hostKey)
	acc.mu.Lock()
	defer acc.mu.Unlock()
	return acc.convert(), nil
}

// Accounts returns all accounts.
func (a *AccountMgr) Accounts() []api.Account {
	a.mu.Lock()
	defer a.mu.Unlock()
	accounts := make([]api.Account, 0, len(a.byID))
	for _, acc := range a.byID {
		acc.mu.Lock()
		accounts = append(accounts, acc.convert())
		acc.mu.Unlock()
	}
	return accounts
}

// AddAmount applies the provided amount to an account through addition. So the
// input can be both a positive or negative number depending on whether a
// withdrawal or deposit is recorded. If the account doesn't exist, it is
// created.
func (a *AccountMgr) AddAmount(id rhpv3.Account, hk types.PublicKey, amt *big.Int) {
	acc := a.account(id, hk)

	// Update balance.
	acc.mu.Lock()
	balanceBefore := acc.Balance.String()
	acc.Balance.Add(acc.Balance, amt)

	// Log deposits.
	if amt.Cmp(big.NewInt(0)) > 0 {
		a.logger.Infow("account balance was increased",
			"account", acc.ID,
			"host", acc.HostKey.String(),
			"amt", amt.String(),
			"balanceBefore", balanceBefore,
			"balanceAfter", acc.Balance.String())
	}
	acc.mu.Unlock()
}

func (a *AccountMgr) LockAccount(ctx context.Context, id rhpv3.Account, hostKey types.PublicKey, exclusive bool, duration time.Duration) (api.Account, uint64) {
	acc := a.account(id, hostKey)

	// Try to lock the account.
	if exclusive {
		acc.rwmu.Lock()
	} else {
		acc.rwmu.RLock()
	}

	// Create a new lock with an unlock function that can only be called once.
	var once sync.Once
	heldByID := frand.Uint64n(math.MaxUint64) + 1
	lock := &accountLock{
		heldByID: heldByID,
		unlock: func() {
			once.Do(func() {
				if exclusive {
					acc.rwmu.Unlock()
				} else {
					acc.rwmu.RUnlock()
				}
				acc.mu.Lock()
				delete(acc.locks, heldByID)
				acc.mu.Unlock()
			})
		},
	}

	// Spawn a timer that will eventually unlock the lock.
	lock.timer = time.AfterFunc(duration, lock.unlock)

	acc.mu.Lock()
	acc.locks[lock.heldByID] = lock
	account := acc.convert()
	acc.mu.Unlock()
	return account, lock.heldByID
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

// SetBalance sets the balance of a given account to the provided amount. If the
// account doesn't exist, it is created.
// If an account hasn't been saved successfully upon the last shutdown, no drift
// will be added upon the first call to SetBalance.
func (a *AccountMgr) SetBalance(id rhpv3.Account, hk types.PublicKey, balance *big.Int) {
	acc := a.account(id, hk)

	// Update balance and drift.
	acc.mu.Lock()
	delta := new(big.Int).Sub(balance, acc.Balance)
	balanceBefore := acc.Balance.String()
	driftBefore := acc.Drift.String()
	if acc.CleanShutdown {
		acc.Drift = acc.Drift.Add(acc.Drift, delta)
	}
	acc.Balance.Set(balance)
	acc.CleanShutdown = true
	acc.RequiresSync = false // resetting the balance resets the sync field
	balanceAfter := acc.Balance.String()
	acc.mu.Unlock()

	// Log resets.
	a.logger.Infow("account balance was reset",
		"account", acc.ID,
		"host", acc.HostKey.String(),
		"balanceBefore", balanceBefore,
		"balanceAfter", balanceAfter,
		"driftBefore", driftBefore,
		"driftAfter", acc.Drift.String(),
		"delta", delta.String())
}

// ScheduleSync sets the requiresSync flag of an account.
func (a *AccountMgr) ScheduleSync(id rhpv3.Account, hk types.PublicKey) error {
	acc := a.account(id, hk)
	acc.mu.Lock()
	// Only update the sync flag to 'true' if some time has passed since the
	// last time it was set. That way we avoid multiple workers setting it after
	// failing at the same time, causing multiple syncs in the process.
	if time.Since(acc.requiresSyncTime) < 30*time.Second {
		acc.mu.Unlock()
		return api.ErrRequiresSyncSetRecently
	}
	acc.RequiresSync = true
	acc.requiresSyncTime = time.Now()

	// Log scheduling a sync.
	a.logger.Infow("account sync was scheduled",
		"account", acc.ID,
		"host", acc.HostKey.String(),
		"balance", acc.Balance.String(),
		"drift", acc.Drift.String())
	acc.mu.Unlock()

	a.mu.Lock()
	account, exists := a.byID[id]
	defer a.mu.Unlock()
	if !exists {
		return ErrAccountNotFound
	}
	account.resetDrift()
	return nil
}

func (a *AccountMgr) Shutdown(ctx context.Context) error {
	accounts := a.Accounts()
	err := a.s.SaveAccounts(ctx, busAccountOwner, accounts)
	if err != nil {
		a.logger.Errorf("failed to save %v accounts: %v", len(accounts), err)
		return err
	}

	a.logger.Infof("successfully saved %v accounts", len(accounts))
	return nil
}

// UnlockAccount unlocks an account with the given lock id.
func (a *AccountMgr) UnlockAccount(id rhpv3.Account, lockID uint64) error {
	a.mu.Lock()
	acc, exists := a.byID[id]
	if !exists {
		a.mu.Unlock()
		return ErrAccountNotFound
	}
	a.mu.Unlock()

	// Get lock.
	acc.mu.Lock()
	lock, exists := acc.locks[lockID]
	acc.mu.Unlock()
	if !exists {
		return fmt.Errorf("account lock with id %v not found", lockID)
	}

	// Stop timer.
	lock.timer.Stop()
	select {
	case <-lock.timer.C:
	default:
	}

	// Unlock
	lock.unlock()
	return nil
}

func (a *AccountMgr) account(id rhpv3.Account, hk types.PublicKey) *account {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Create account if it doesn't exist.
	acc, exists := a.byID[id]
	if !exists {
		acc = &account{
			Account: api.Account{
				ID:            id,
				CleanShutdown: false,
				HostKey:       hk,
				Balance:       big.NewInt(0),
				Drift:         big.NewInt(0),
				RequiresSync:  false,
			},
			locks: map[uint64]*accountLock{},
		}
		a.byID[id] = acc
	}
	return acc
}

func (a *account) convert() api.Account {
	return api.Account{
		ID:            a.ID,
		Balance:       new(big.Int).Set(a.Balance),
		CleanShutdown: a.CleanShutdown,
		Drift:         new(big.Int).Set(a.Drift),
		HostKey:       a.HostKey,
		RequiresSync:  a.RequiresSync,
	}
}

func (a *account) resetDrift() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Drift.SetInt64(0)
}
