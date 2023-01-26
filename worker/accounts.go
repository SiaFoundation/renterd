package worker

import (
	"errors"
	"math/big"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/rhp/v3"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
)

type (
	// accounts stores the balance and other metrics of accounts that the
	// worker maintains with a host.
	accounts struct {
		store    AccountStore
		workerID string
		key      types.PrivateKey

		mu       sync.Mutex
		accounts map[rhpv3.Account]*account
	}

	// account contains information regarding a specific account of the
	// worker.
	account struct {
		bus   AccountStore
		id    rhp.Account
		key   types.PrivateKey
		host  types.PublicKey
		owner string

		// The balance is locked by a RWMutex since both withdrawals and
		// deposits can happen in parallel during normal operations. If
		// the account ever goes out of sync, the worker needs to be
		// able to prevent any deposits or withdrawals from the host for
		// the duration of the sync so only syncing acquires an
		// exclusive lock on the mutex.
		mu      sync.RWMutex
		balance *big.Int
	}
)

func newAccounts(workerID string, accountsKey types.PrivateKey, as AccountStore) *accounts {
	return &accounts{
		store:    as,
		accounts: make(map[rhpv3.Account]*account),
		workerID: workerID,
		key:      accountsKey,
	}
}

// All returns information about all accounts to be returned in the API.
func (a *accounts) All() ([]api.Account, error) {
	// Make sure accounts are initialised.
	if err := a.tryInitAccounts(); err != nil {
		return nil, err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	accounts := make([]api.Account, 0, len(a.accounts))
	for _, acc := range a.accounts {
		accounts = append(accounts, api.Account{
			ID:      acc.id,
			Balance: acc.balance,
			Host:    acc.host,
			Owner:   acc.owner,
		})
	}
	return accounts, nil
}

// ForHost returns an account to use for a given host. If the account
// doesn't exist, a new one is created.
func (a *accounts) ForHost(hk types.PublicKey) (*account, error) {
	// Make sure accounts are initialised.
	if err := a.tryInitAccounts(); err != nil {
		return nil, err
	}

	// Key should be set.
	if hk == (types.PublicKey{}) {
		return nil, errors.New("empty host key provided")
	}

	// Create and or return account.
	accountID := rhpv3.Account(a.key.PublicKey())

	a.mu.Lock()
	defer a.mu.Unlock()
	acc, exists := a.accounts[accountID]
	if !exists {
		acc = &account{
			bus:     a.store,
			id:      accountID,
			key:     a.key,
			host:    hk,
			owner:   a.workerID,
			balance: types.ZeroCurrency.Big(),
		}
		a.accounts[accountID] = acc
	}
	return acc, nil
}

// WithDeposit increases the balance of an account by the amount returned by
// amtFn if amtFn doesn't return an error.
func (a *account) WithDeposit(amtFn func() (types.Currency, error)) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	amt, err := amtFn()
	if err != nil {
		return err
	}
	a.balance = a.balance.Add(a.balance, amt.Big())
	return a.bus.AddBalance(a.id, a.owner, a.host, amt.Big())
}

// WithWithdrawal decreases the balance of an account by the amount returned by
// amtFn if amtFn doesn't return an error.
func (a *account) WithWithdrawal(amtFn func() (types.Currency, error)) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	amt, err := amtFn()
	if err != nil {
		return err
	}
	a.balance = a.balance.Sub(a.balance, amt.Big())
	return a.bus.AddBalance(a.id, a.owner, a.host, new(big.Int).Neg(amt.Big()))
}

// WithSync syncs an accounts balance with the bus. To do so, the account is
// locked while the balance is fetched through balanceFn.
func (a *account) WithSync(balanceFn func() types.Currency) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.balance = balanceFn().Big()
	err := a.bus.SetBalance(a.id, a.owner, a.host, a.balance)
	return err
}

// tryInitAccounts is used for lazily initialising the accounts from the bus.
func (a *accounts) tryInitAccounts() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.accounts != nil {
		return nil // already initialised
	}
	a.accounts = make(map[rhpv3.Account]*account)
	accounts, err := a.store.Accounts(a.workerID)
	if err != nil {
		return err
	}
	for _, acc := range accounts {
		a.accounts[rhpv3.Account(acc.ID)] = &account{
			bus:     a.store,
			id:      rhpv3.Account(acc.ID),
			key:     a.deriveAccountKey(acc.Host),
			host:    acc.Host,
			owner:   acc.Owner,
			balance: acc.Balance,
		}
	}
	return nil
}

// deriveAccountKey derives an account plus key for a given host and worker.
// Each worker has its own account for a given host. That makes concurrency
// around keeping track of an accounts balance and refilling it a lot easier in
// a multi-worker setup.
func (a *accounts) deriveAccountKey(hostKey types.PublicKey) types.PrivateKey {
	index := byte(0) // not used yet but can be used to derive more than 1 account per host

	// Append the owner of the account (worker's id), the host for which to
	// create it and the index to the corresponding sub-key.
	subKey := a.key
	data := append(subKey, []byte(a.workerID)...)
	data = append(data, hostKey[:]...)
	data = append(data, index)

	seed := types.HashBytes(data)
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}
