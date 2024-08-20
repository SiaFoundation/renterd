package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
)

const (
	// accountLockingDuration is the time for which an account lock remains
	// reserved on the bus after locking it.
	accountLockingDuration = 30 * time.Second
)

type (
	// accounts stores the balance and other metrics of accounts that the
	// worker maintains with a host.
	accounts struct {
		as  AccountStore
		key types.PrivateKey
	}

	// account contains information regarding a specific account of the
	// worker.
	account struct {
		as   AccountStore
		id   rhpv3.Account
		key  types.PrivateKey
		host types.PublicKey
	}
)

// ForHost returns an account to use for a given host. If the account
// doesn't exist, a new one is created.
func (a *accounts) ForHost(hk types.PublicKey) *account {
	accountID := rhpv3.Account(a.deriveAccountKey(hk).PublicKey())
	return &account{
		as:   a.as,
		id:   accountID,
		key:  a.key,
		host: hk,
	}
}

// deriveAccountKey derives an account plus key for a given host and worker.
// Each worker has its own account for a given host. That makes concurrency
// around keeping track of an accounts balance and refilling it a lot easier in
// a multi-worker setup.
func (a *accounts) deriveAccountKey(hostKey types.PublicKey) types.PrivateKey {
	index := byte(0) // not used yet but can be used to derive more than 1 account per host

	// Append the host for which to create it and the index to the
	// corresponding sub-key.
	subKey := a.key
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

// Balance returns the account balance.
func (a *account) Balance(ctx context.Context) (balance types.Currency, err error) {
	err = withAccountLock(ctx, a.as, a.id, a.host, false, func(account api.Account) error {
		balance = types.NewCurrency(account.Balance.Uint64(), new(big.Int).Rsh(account.Balance, 64).Uint64())
		return nil
	})
	return
}

// WithDeposit increases the balance of an account by the amount returned by
// amtFn if amtFn doesn't return an error.
func (a *account) WithDeposit(ctx context.Context, amtFn func() (types.Currency, error)) error {
	return withAccountLock(ctx, a.as, a.id, a.host, false, func(_ api.Account) error {
		amt, err := amtFn()
		if err != nil {
			return err
		}
		return a.as.AddBalance(ctx, a.id, a.host, amt.Big())
	})
}

// WithSync syncs an accounts balance with the bus. To do so, the account is
// locked while the balance is fetched through balanceFn.
func (a *account) WithSync(ctx context.Context, balanceFn func() (types.Currency, error)) error {
	return withAccountLock(ctx, a.as, a.id, a.host, true, func(_ api.Account) error {
		balance, err := balanceFn()
		if err != nil {
			return err
		}
		return a.as.SetBalance(ctx, a.id, a.host, balance.Big())
	})
}

// WithWithdrawal decreases the balance of an account by the amount returned by
// amtFn. The amount is still withdrawn if amtFn returns an error since some
// costs are non-refundable.
func (a *account) WithWithdrawal(ctx context.Context, amtFn func() (types.Currency, error)) error {
	return withAccountLock(ctx, a.as, a.id, a.host, false, func(account api.Account) error {
		// return early if the account needs to sync
		if account.RequiresSync {
			return fmt.Errorf("%w; account requires resync", rhp3.ErrBalanceInsufficient)
		}

		// return early if our account is not funded
		if account.Balance.Cmp(big.NewInt(0)) <= 0 {
			return rhp3.ErrBalanceInsufficient
		}

		// execute amtFn
		amt, err := amtFn()

		// in case of an insufficient balance, we schedule a sync
		if rhp3.IsBalanceInsufficient(err) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			err = errors.Join(err, a.as.ScheduleSync(ctx, a.id, a.host))
			cancel()
		}

		// if an amount was returned, we withdraw it
		if !amt.IsZero() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			err = errors.Join(err, a.as.AddBalance(ctx, a.id, a.host, new(big.Int).Neg(amt.Big())))
			cancel()
		}

		return err
	})
}

func (w *Worker) initAccounts(as AccountStore) {
	if w.accounts != nil {
		panic("accounts already initialized") // developer error
	}
	w.accounts = &accounts{
		as:  as,
		key: w.deriveSubKey("accountkey"),
	}
}

func withAccountLock(ctx context.Context, as AccountStore, id rhpv3.Account, hk types.PublicKey, exclusive bool, fn func(a api.Account) error) error {
	acc, lockID, err := as.LockAccount(ctx, id, hk, exclusive, accountLockingDuration)
	if err != nil {
		return err
	}
	err = fn(acc)

	// unlock account
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	_ = as.UnlockAccount(ctx, acc.ID, lockID) // ignore error
	cancel()

	return err
}
