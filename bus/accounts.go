package bus

import (
	"math/big"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
)

type accounts struct {
	mu      sync.Mutex
	byID    map[rhpv3.Account]*account
	byOwner map[string][]*account
}

type account struct {
	Owner string

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
			Account: api.Account{
				ID:      acc.ID,
				Host:    acc.Host,
				Balance: acc.Balance,
			},
			Owner: acc.Owner,
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
func (a *accounts) SetBalance(id rhpv3.Account, owner string, hk types.PublicKey, balance *big.Int) {
	acc := a.account(id, owner, hk)

	// Update balance.
	acc.mu.Lock()
	acc.Balance = balance
	acc.mu.Unlock()
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
			ID:      acc.ID,
			Balance: acc.Balance,
			Host:    acc.Host,
			Owner:   acc.Owner,
		}
		acc.mu.Unlock()
	}
	return accounts
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
			ID:      acc.ID,
			Balance: acc.Balance,
			Host:    acc.Host,
			Owner:   acc.Owner,
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
				ID:      id,
				Host:    hk,
				Balance: big.NewInt(0),
			},
			Owner: owner,
		}
		a.byID[id] = acc
		a.byOwner[owner] = append(a.byOwner[owner], acc)
	}
	return acc
}
