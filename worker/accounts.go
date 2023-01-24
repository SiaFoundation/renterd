package worker

import (
	"errors"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
)

type (
	// accounts stores the balance and other metrics of accounts that the
	// worker maintains with a host.
	accounts struct {
		workerID string
		key      types.PrivateKey

		mu       sync.Mutex
		accounts map[rhpv3.Account]*account
	}

	// account contains information regarding a specific account of the
	// worker.
	account struct {
		id   rhpv3.Account
		key  types.PrivateKey
		host types.PublicKey

		mu      sync.Mutex
		balance types.Currency
	}
)

func newAccounts(workerID string, accountsKey types.PrivateKey) *accounts {
	return &accounts{
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
			id:      accountID,
			host:    hk,
			key:     a.key,
			balance: types.ZeroCurrency,
		}
		a.accounts[accountID] = acc
	}
	return acc, nil
}

// Deposit increases the balance of an account.
func (a *account) Deposit(amt types.Currency) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.balance = a.balance.Add(amt)
	// TODO: notify bus
	return nil
}

// tryInitAccounts is used for lazily initialising the accounts from the bus.
func (a *accounts) tryInitAccounts() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.accounts != nil {
		return nil // already initialised
	}
	a.accounts = make(map[rhpv3.Account]*account)

	// TODO: populate from bus once we have persistence of accounts
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
