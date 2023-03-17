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
	"lukechampine.com/frand"
)

var errAccountsNotFound = errors.New("account doesn't exist")

type accounts struct {
	mu   sync.Mutex
	byID map[rhpv3.Account]*account
}

type account struct {
	mu               sync.Mutex
	locks            map[uint64]*accountLock
	requiresSyncTime time.Time
	api.Account

	rwmu sync.RWMutex
}

type accountLock struct {
	heldByID uint64
	unlock   func()
	timer    *time.Timer
}

func newAccounts(accs []api.Account) *accounts {
	a := &accounts{
		byID: make(map[rhpv3.Account]*account),
	}
	for _, acc := range accs {
		account := &account{
			Account: acc,
			locks:   map[uint64]*accountLock{},
		}
		a.byID[account.ID] = account
	}
	return a
}

func (a *accounts) LockAccount(ctx context.Context, id rhpv3.Account, hostKey types.PublicKey, exclusive bool, duration time.Duration) (api.Account, uint64) {
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

func (a *accounts) UnlockAccount(id rhpv3.Account, lockID uint64) error {
	a.mu.Lock()
	acc, exists := a.byID[id]
	if !exists {
		a.mu.Unlock()
		return errAccountsNotFound
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

// AddAmount applies the provided amount to an account through addition. So the
// input can be both a positive or negative number depending on whether a
// withdrawal or deposit is recorded. If the account doesn't exist, it is
// created.
func (a *accounts) AddAmount(id rhpv3.Account, hk types.PublicKey, amt *big.Int) {
	acc := a.account(id, hk)

	// Update balance.
	acc.mu.Lock()
	acc.Balance.Add(acc.Balance, amt)
	if amt.Cmp(new(big.Int)) < 0 {
		acc.RequiresSync = false // a successful withdrawal resets the sync flag
	}
	acc.mu.Unlock()
}

// SetBalance sets the balance of a given account to the provided amount. If
// the account doesn't exist, it is created.
func (a *accounts) SetBalance(id rhpv3.Account, hk types.PublicKey, balance *big.Int) {
	acc := a.account(id, hk)

	// Update balance and drift.
	acc.mu.Lock()
	delta := new(big.Int).Sub(balance, acc.Balance)
	acc.Drift = acc.Drift.Add(acc.Drift, delta)
	acc.Balance.Set(balance)
	acc.RequiresSync = false // resetting the balance resets the sync field
	acc.mu.Unlock()
}

// SetRequiresSync sets the requiresSync flag of an account.
func (a *accounts) SetRequiresSync(id rhpv3.Account, hk types.PublicKey, requiresSync bool) error {
	acc := a.account(id, hk)
	acc.mu.Lock()
	// Only update the sync flag to 'true' if some time has passed since the
	// last time it was set. That way we avoid multiple workers setting it after
	// failing at the same time, causing multiple syncs in the process.
	if requiresSync && time.Since(acc.requiresSyncTime) < 30*time.Second {
		acc.mu.Unlock()
		return nil
	}
	acc.RequiresSync = requiresSync
	acc.requiresSyncTime = time.Now()
	acc.mu.Unlock()

	a.mu.Lock()
	account, exists := a.byID[id]
	defer a.mu.Unlock()
	if !exists {
		return errAccountsNotFound
	}
	account.resetDrift()
	return nil
}

func (a *account) convert() api.Account {
	return api.Account{
		ID:           a.ID,
		Balance:      new(big.Int).Set(a.Balance),
		Drift:        new(big.Int).Set(a.Drift),
		Host:         a.Host,
		RequiresSync: a.RequiresSync,
	}
}

// Account returns the account with the given id.
func (a *accounts) Account(id rhpv3.Account, hostKey types.PublicKey) (api.Account, error) {
	acc := a.account(id, hostKey)
	acc.mu.Lock()
	defer acc.mu.Unlock()
	return acc.convert(), nil
}

// Accounts returns all accounts for a given owner. Usually called when workers
// request their accounts on startup.
func (a *accounts) Accounts() []api.Account {
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

// ResetDrift resets the drift on an account.
func (a *accounts) ResetDrift(id rhpv3.Account) error {
	a.mu.Lock()
	account, exists := a.byID[id]
	if !exists {
		a.mu.Unlock()
		return errAccountsNotFound
	}
	a.mu.Unlock()
	account.resetDrift()
	return nil
}

// ToPersist returns all known accounts to be persisted by the storage backend.
// Called once on shutdown.
func (a *accounts) ToPersist() []api.Account {
	a.mu.Lock()
	defer a.mu.Unlock()
	accounts := make([]api.Account, 0, len(a.byID))
	for _, acc := range a.byID {
		acc.mu.Lock()
		accounts = append(accounts, api.Account{
			ID:           acc.ID,
			Balance:      new(big.Int).Set(acc.Balance),
			Drift:        new(big.Int).Set(acc.Drift),
			Host:         acc.Host,
			RequiresSync: acc.RequiresSync,
		})
		acc.mu.Unlock()
	}
	return accounts
}

func (a *accounts) account(id rhpv3.Account, hk types.PublicKey) *account {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Create account if it doesn't exist.
	acc, exists := a.byID[id]
	if !exists {
		acc = &account{
			Account: api.Account{
				ID:           id,
				Host:         hk,
				Balance:      big.NewInt(0),
				Drift:        big.NewInt(0),
				RequiresSync: false,
			},
			locks: map[uint64]*accountLock{},
		}
		a.byID[id] = acc
	}
	return acc
}

func (a *account) resetDrift() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Drift.SetInt64(0)
}
