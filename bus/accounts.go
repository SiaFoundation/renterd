package bus

import (
	"math/big"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/ephemeralaccounts"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
)

type accounts struct {
	mu      sync.Mutex
	byID    map[rhpv3.Account]*account
	byOwner map[string][]*account
}

type account struct {
	ephemeralaccounts.Account

	Owner string
}

func newAccounts(accs []ephemeralaccounts.Account) *accounts {
	a := &accounts{
		byID:    make(map[rhpv3.Account]*account),
		byOwner: make(map[string][]*account),
	}
	for _, acc := range accs {
		account := &account{
			Account: ephemeralaccounts.Account{
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

func (a *accounts) UpdateBalance(id rhpv3.Account, owner string, hk types.PublicKey, amt *big.Int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Create account if it doesn't exist.
	acc, exists := a.byID[id]
	if !exists {
		acc = &account{
			Account: ephemeralaccounts.Account{
				ID:      id,
				Host:    hk,
				Balance: big.NewInt(0),
			},
			Owner: owner,
		}
		a.byID[id] = acc
		a.byOwner[owner] = append(a.byOwner[owner], acc)
	}

	// Update balance.
	acc.Balance.Add(acc.Balance, amt)
}

func (a *accounts) Accounts(owner string) []ephemeralaccounts.Account {
	a.mu.Lock()
	defer a.mu.Unlock()
	accounts := make([]ephemeralaccounts.Account, len(a.byOwner[owner]))
	for i, acc := range a.byOwner[owner] {
		accounts[i] = ephemeralaccounts.Account{
			ID:      acc.ID,
			Balance: acc.Balance,
			Host:    acc.Host,
			Owner:   acc.Owner,
		}
	}
	return accounts
}

func (a *accounts) ToPersist() []ephemeralaccounts.Account {
	a.mu.Lock()
	defer a.mu.Unlock()
	accounts := make([]ephemeralaccounts.Account, 0, len(a.byID))
	for _, acc := range a.byID {
		accounts = append(accounts, ephemeralaccounts.Account{
			ID:      acc.ID,
			Balance: acc.Balance,
			Host:    acc.Host,
			Owner:   acc.Owner,
		})
	}
	return accounts
}
