package worker

import (
	"errors"
	"math/big"
	"sync"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
)

type (
	// accounts stores the balance and other metrics of accounts that the
	// worker maintains with a host.
	accounts struct {
		bus      AccountStore
		workerID string
		key      consensus.PrivateKey

		mu       sync.Mutex
		accounts map[rhp.Account]*account
	}

	// account contains information regarding a specific account of the
	// worker.
	account struct {
		bus  AccountStore
		id   rhp.Account
		key  consensus.PrivateKey
		host consensus.PublicKey

		mu      sync.Mutex
		balance *big.Int
	}
)

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
func (a *accounts) ForHost(hk consensus.PublicKey) (*account, error) {
	// Make sure accounts are initialised.
	if err := a.tryInitAccounts(); err != nil {
		return nil, err
	}

	// Key should be set.
	if hk == (consensus.PublicKey{}) {
		return nil, errors.New("empty host key provided")
	}

	// Create and or return account.
	accountID := rhp.Account(a.key.PublicKey())

	a.mu.Lock()
	defer a.mu.Unlock()
	acc, exists := a.accounts[accountID]
	if !exists {
		acc = &account{
			bus:     a.bus,
			id:      accountID,
			host:    hk,
			key:     a.key,
			balance: types.ZeroCurrency.Big(),
		}
		a.accounts[accountID] = acc
	}
	return acc, nil
}

// Deposit increases the balance of an account.
func (a *account) Deposit(amt types.Currency) error {
	a.mu.Lock()
	a.balance = a.balance.Add(a.balance, amt.Big())
	a.mu.Unlock()
	return a.bus.UpdateBalance(a.id, amt.Big())
}

// Withdraw decreases the balance of an account.
func (a *account) Withdraw(amt types.Currency) error {
	a.mu.Lock()
	a.balance = a.balance.Sub(a.balance, amt.Big())
	a.mu.Unlock()
	return a.bus.UpdateBalance(a.id, new(big.Int).Neg(amt.Big()))
}

// tryInitAccounts is used for lazily initialising the accounts from the bus.
func (a *accounts) tryInitAccounts() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.accounts != nil {
		return nil // already initialised
	}
	a.accounts = make(map[rhp.Account]*account)
	accounts, err := a.bus.Accounts(a.workerID)
	if err != nil {
		return err
	}
	for _, acc := range accounts {
		a.accounts[rhp.Account(acc.ID)] = &account{
			bus:     a.bus,
			id:      rhp.Account(acc.ID),
			key:     a.deriveAccountKey(acc.Host),
			balance: acc.Balance.Big(),
		}
	}
	return nil
}

// deriveAccountKey derives an account plus key for a given host and worker.
// Each worker has its own account for a given host. That makes concurrency
// around keeping track of an accounts balance and refilling it a lot easier in
// a multi-worker setup.
func (a *accounts) deriveAccountKey(hostKey consensus.PublicKey) consensus.PrivateKey {
	index := byte(0) // not used yet but can be used to derive more than 1 account per host

	// Append the owner of the account (worker's id), the host for which to
	// create it and the index to the corresponding sub-key.
	subKey := a.key
	data := append(subKey, []byte(a.workerID)...)
	data = append(data, hostKey[:]...)
	data = append(data, index)

	seed := blake2b.Sum256(data)
	pk := consensus.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}
