package bus

import (
	"errors"
	"math/big"
	"sync"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

var errAccountsNotFound = errors.New("account doesn't exist")

type accounts struct {
	mu      sync.Mutex
	byID    map[rhpv3.Account]*account
	byOwner map[string][]*account
}

type account struct {
	mu sync.Mutex
	api.Account
}

func newAccounts(accs []api.Account) *accounts {
	a := &accounts{
		byID:    make(map[rhpv3.Account]*account),
		byOwner: make(map[string][]*account),
	}
	for _, acc := range accs {
		account := &account{
			Account: acc,
		}
		a.byID[account.ID] = account
		a.byOwner[acc.Owner] = append(a.byOwner[acc.Owner], account)
	}
	return a
}

// AddAmount applies the provided amount to an account through addition. So the
// input can be both a positive or negative number depending on whether a
// withdrawal or deposit is recorded. If the account doesn't exist, it is
// created.
func (a *accounts) AddAmount(id rhpv3.Account, owner string, hk types.PublicKey, amt *big.Int) {
	acc := a.account(id, owner, hk)

	// Update balance.
	acc.mu.Lock()
	acc.Balance.Add(acc.Balance, amt)
	acc.mu.Unlock()
}

// SetBalance sets the balance of a given account to the provided amount. If
// the account doesn't exist, it is created.
func (a *accounts) SetBalance(id rhpv3.Account, owner string, hk types.PublicKey, balance, drift *big.Int) {
	acc := a.account(id, owner, hk)

	// Update balance and drift.
	acc.mu.Lock()
	acc.Balance.Set(balance)
	acc.Drift.Set(drift)
	acc.RequiresSync = false
	acc.mu.Unlock()
}

// SetRequiresSync sets the requiresSync flag of an account.
func (a *accounts) SetRequiresSync(id rhpv3.Account, owner string, hk types.PublicKey, requiresSync bool) error {
	acc := a.account(id, owner, hk)
	acc.mu.Lock()
	acc.RequiresSync = requiresSync
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

// Accounts returns all accounts for a given owner. Usually called when workers
// request their accounts on startup.
func (a *accounts) Accounts(owner string) []api.Account {
	a.mu.Lock()
	defer a.mu.Unlock()
	accounts := make([]api.Account, len(a.byOwner[owner]))
	for i, acc := range a.byOwner[owner] {
		acc.mu.Lock()
		accounts[i] = api.Account{
			ID:           acc.ID,
			Balance:      new(big.Int).Set(acc.Balance),
			Drift:        new(big.Int).Set(acc.Drift),
			Host:         acc.Host,
			Owner:        acc.Owner,
			RequiresSync: acc.RequiresSync,
		}
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
			Owner:        acc.Owner,
			RequiresSync: acc.RequiresSync,
		})
		acc.mu.Unlock()
	}
	return accounts
}

func (a *accounts) account(id rhpv3.Account, owner string, hk types.PublicKey) *account {
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
				Owner:        owner,
				RequiresSync: false,
			},
		}
		a.byID[id] = acc
		a.byOwner[owner] = append(a.byOwner[owner], acc)
	}
	return acc
}

func (a *account) resetDrift() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Drift.SetInt64(0)
}
