package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

var (
	// errBalanceMaxExceeded occurs when a deposit would push the account's
	// balance over the maximum allowed ephemeral account balance.
	errBalanceMaxExceeded = errors.New("ephemeral account maximum balance exceeded")
)

func (w *worker) fundAccount(ctx context.Context, account *account, pt rhpv3.HostPriceTable, hostIP string, hostKey types.PublicKey, amount types.Currency, revision *types.FileContractRevision) error {
	return account.WithDeposit(ctx, func() (types.Currency, error) {
		return amount, withTransportV3(ctx, hostIP, hostKey, func(t *rhpv3.Transport) (err error) {
			rk := w.deriveRenterKey(hostKey)
			cost := amount.Add(pt.FundAccountCost)
			payment, ok := rhpv3.PayByContract(revision, cost, rhpv3.Account{}, rk) // no account needed for funding
			if !ok {
				return errors.New("insufficient funds")
			}
			w.contractSpendingRecorder.Record(revision.ParentID, api.ContractSpending{FundAccount: cost})
			return RPCFundAccount(t, &payment, account.id, pt.UID)
		})
	})
}

func (w *worker) syncAccount(ctx context.Context, account *account, pt rhpv3.HostPriceTable, hostIP string, hostKey types.PublicKey) error {
	account, err := w.accounts.ForHost(hostKey)
	if err != nil {
		return err
	}
	payment := w.preparePayment(hostKey, pt.AccountBalanceCost, pt.HostBlockHeight)
	return account.WithSync(ctx, func() (types.Currency, error) {
		var balance types.Currency
		err := withTransportV3(ctx, hostIP, hostKey, func(t *rhpv3.Transport) error {
			balance, err = RPCAccountBalance(t, &payment, account.id, pt.UID)
			return err
		})
		return balance, err
	})
}

func isMaxBalanceExceeded(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), errBalanceMaxExceeded.Error())
}

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
		id    rhpv3.Account
		key   types.PrivateKey
		host  types.PublicKey
		owner string

		// The balance is locked by a RWMutex in addition to a regular Mutex
		// since both withdrawals and deposits can happen in parallel during
		// normal operations. If the account ever goes out of sync, the worker
		// needs to be able to prevent any deposits or withdrawals from the host
		// for the duration of the sync so only syncing acquires an exclusive
		// lock on the mutex.
		mu        sync.RWMutex
		balanceMu sync.Mutex
		balance   *big.Int
		drift     *big.Int
	}
)

func newAccounts(workerID string, accountsKey types.PrivateKey, as AccountStore) *accounts {
	return &accounts{
		store:    as,
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
		accounts = append(accounts, acc.Convert())
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
	accountID := rhpv3.Account(a.deriveAccountKey(hk).PublicKey())

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
			drift:   types.ZeroCurrency.Big(),
		}
		a.accounts[accountID] = acc
	}
	return acc, nil
}

func (a *account) Balance() types.Currency {
	a.mu.RLock()
	defer a.mu.RUnlock()
	a.balanceMu.Lock()
	defer a.balanceMu.Unlock()
	return types.NewCurrency(a.balance.Uint64(), new(big.Int).Rsh(a.balance, 64).Uint64())
}

func (a *accounts) ResetDrift(ctx context.Context, id rhpv3.Account) error {
	a.mu.Lock()
	account, exists := a.accounts[id]
	if !exists {
		a.mu.Unlock()
		return errors.New("account doesn't exist")
	}
	a.mu.Unlock()
	return account.resetDrift(ctx)
}

func (a *account) Convert() api.Account {
	a.mu.RLock()
	defer a.mu.RUnlock()
	a.balanceMu.Lock()
	defer a.balanceMu.Unlock()
	return api.Account{
		ID:      a.id,
		Balance: new(big.Int).Set(a.balance),
		Drift:   new(big.Int).Set(a.drift),
		Host:    a.host,
		Owner:   a.owner,
	}
}

func (a *account) resetDrift(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.bus.ResetDrift(ctx, a.id); err != nil {
		return err
	}
	a.balanceMu.Lock()
	a.drift.SetInt64(0)
	a.balanceMu.Unlock()
	return nil
}

// WithDeposit increases the balance of an account by the amount returned by
// amtFn if amtFn doesn't return an error.
func (a *account) WithDeposit(ctx context.Context, amtFn func() (types.Currency, error)) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	amt, err := amtFn()
	if err != nil {
		return err
	}
	a.balanceMu.Lock()
	a.balance = a.balance.Add(a.balance, amt.Big())
	a.balanceMu.Unlock()
	return a.bus.AddBalance(ctx, a.id, a.owner, a.host, amt.Big())
}

// WithWithdrawal decreases the balance of an account by the amount returned by
// amtFn if amtFn doesn't return an error.
func (a *account) WithWithdrawal(ctx context.Context, amtFn func() (types.Currency, error)) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	amt, err := amtFn()
	if err != nil {
		return err
	}
	a.balanceMu.Lock()
	a.balance = a.balance.Sub(a.balance, amt.Big())
	a.balanceMu.Unlock()
	return a.bus.AddBalance(ctx, a.id, a.owner, a.host, new(big.Int).Neg(amt.Big()))
}

// WithSync syncs an accounts balance with the bus. To do so, the account is
// locked while the balance is fetched through balanceFn.
func (a *account) WithSync(ctx context.Context, balanceFn func() (types.Currency, error)) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	balance, err := balanceFn()
	if err != nil {
		return err
	}
	a.balanceMu.Lock()
	delta := new(big.Int).Sub(balance.Big(), a.balance)
	a.drift = a.drift.Add(a.drift, delta)
	a.balance = balance.Big()
	newBalance, newDrift := new(big.Int).Set(a.balance), new(big.Int).Set(a.drift)
	a.balanceMu.Unlock()
	return a.bus.SetBalance(ctx, a.id, a.owner, a.host, newBalance, newDrift)
}

// tryInitAccounts is used for lazily initialising the accounts from the bus.
func (a *accounts) tryInitAccounts() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.accounts != nil {
		return nil // already initialised
	}
	a.accounts = make(map[rhpv3.Account]*account)
	accounts, err := a.store.Accounts(context.Background(), a.workerID)
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
			drift:   acc.Drift,
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

// priceTableValidityLeeway is the number of time before the actual expiry of a
// price table when we start considering it invalid.
const priceTableValidityLeeway = 30 * time.Second

type priceTables struct {
	mu          sync.Mutex
	priceTables map[types.PublicKey]*priceTable
}

type priceTable struct {
	pt     *rhpv3.HostPriceTable
	hk     types.PublicKey
	expiry time.Time

	mu            sync.Mutex
	ongoingUpdate *priceTableUpdate
}

type priceTableUpdate struct {
	err  error
	done chan struct{}
	pt   *rhpv3.HostPriceTable
}

func newPriceTables() *priceTables {
	return &priceTables{
		priceTables: make(map[types.PublicKey]*priceTable),
	}
}

// PriceTable returns a price table for the given host and an bool to indicate
// whether it is valid or not.
func (pts *priceTables) PriceTable(hk types.PublicKey) (rhpv3.HostPriceTable, bool) {
	pt := pts.priceTable(hk)
	if pt.pt == nil {
		return rhpv3.HostPriceTable{}, false
	}
	return *pt.pt, time.Now().Before(pt.expiry.Add(priceTableValidityLeeway))
}

// Update updates a price table with the given host using the provided payment
// function to pay for it.
func (pts *priceTables) Update(ctx context.Context, payFn PriceTablePaymentFunc, hostIP string, hk types.PublicKey) (rhpv3.HostPriceTable, error) {
	// Fetch the price table to update.
	pt := pts.priceTable(hk)

	// Check if there is some update going on already. If not, create one.
	pt.mu.Lock()
	ongoing := pt.ongoingUpdate
	var performUpdate bool
	if ongoing == nil {
		ongoing = &priceTableUpdate{
			done: make(chan struct{}),
		}
		pt.ongoingUpdate = ongoing
		performUpdate = true
	}
	pt.mu.Unlock()

	// If this thread is not supposed to perform the update, just block and
	// return the result.
	if !performUpdate {
		select {
		case <-ctx.Done():
			return rhpv3.HostPriceTable{}, errors.New("timeout while blocking for pricetable update")
		case <-ongoing.done:
		}
		if ongoing.err != nil {
			return rhpv3.HostPriceTable{}, ongoing.err
		} else {
			return *ongoing.pt, nil
		}
	}

	// Update price table.
	var hpt rhpv3.HostPriceTable
	err := withTransportV3(ctx, hostIP, hk, func(t *rhpv3.Transport) (err error) {
		hpt, err = RPCPriceTable(t, payFn)
		return err
	})

	pt.mu.Lock()
	defer pt.mu.Unlock()

	// On success we update the pt.
	if err == nil {
		pt.pt = &hpt
		pt.expiry = time.Now().Add(hpt.Validity)
	}
	// Signal that the update is over.
	ongoing.err = err
	close(ongoing.done)
	pt.ongoingUpdate = nil
	return hpt, err
}

// priceTable returns a priceTable from priceTables for the given host or
// creates a new one.
func (pts *priceTables) priceTable(hk types.PublicKey) *priceTable {
	pts.mu.Lock()
	defer pts.mu.Unlock()
	pt, exists := pts.priceTables[hk]
	if !exists {
		pt = &priceTable{
			hk: hk,
		}
		pts.priceTables[hk] = pt
	}
	return pt
}

// preparePriceTableContractPayment prepare a payment function to pay for a
// price table from the given host using the provided revision.
// This way of paying for a price table should only be used if payment by EA is
// not possible or if we already need a contract revision anyway. e.g. funding
// an EA.
func (w *worker) preparePriceTableContractPayment(hk types.PublicKey, revision *types.FileContractRevision) PriceTablePaymentFunc {
	return func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
		// TODO: gouging check on price table

		refundAccount := rhpv3.Account(w.accounts.deriveAccountKey(hk).PublicKey())
		rk := w.deriveRenterKey(hk)
		payment, ok := rhpv3.PayByContract(revision, pt.UpdatePriceTableCost, refundAccount, rk)
		if !ok {
			return nil, errors.New("insufficient funds")
		}
		return &payment, nil
	}
}

// preparePriceTableAccountPayment prepare a payment function to pay for a
// price table from the given host using the provided revision.
// This is the preferred way of paying for a price table since it is faster and
// doesn't require locking a contract.
func (w *worker) preparePriceTableAccountPayment(hk types.PublicKey) PriceTablePaymentFunc {
	return func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
		// TODO: gouging check on price table

		accountKey := w.accounts.deriveAccountKey(hk)
		account := rhpv3.Account(accountKey.PublicKey())
		payment := rhpv3.PayByEphemeralAccount(account, pt.UpdatePriceTableCost, math.MaxUint64, accountKey)
		return &payment, nil
	}
}

func processPayment(s *rhpv3.Stream, payment rhpv3.PaymentMethod) error {
	var paymentType types.Specifier
	switch payment.(type) {
	case *rhpv3.PayByContractRequest:
		paymentType = rhpv3.PaymentTypeContract
	case *rhpv3.PayByEphemeralAccountRequest:
		paymentType = rhpv3.PaymentTypeEphemeralAccount
	default:
		panic("unhandled payment method")
	}
	if err := s.WriteResponse(&paymentType); err != nil {
		return err
	} else if err := s.WriteResponse(payment); err != nil {
		return err
	}
	if _, ok := payment.(*rhpv3.PayByContractRequest); ok {
		var pr rhpv3.PaymentResponse
		if err := s.ReadResponse(&pr, 4096); err != nil {
			return err
		}
		// TODO: return host signature
	}
	return nil
}

// PriceTablePaymentFunc is a function that can be passed in to RPCPriceTable.
// It is called after the price table is received from the host and supposed to
// create a payment for that table and return it. It can also be used to perform
// gouging checks before paying for the table.
type PriceTablePaymentFunc func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error)

// RPCPriceTable calls the UpdatePriceTable RPC.
func RPCPriceTable(t *rhpv3.Transport, paymentFunc PriceTablePaymentFunc) (pt rhpv3.HostPriceTable, err error) {
	defer wrapErr(&err, "PriceTable")
	s := t.DialStream()
	defer s.Close()

	s.SetDeadline(time.Now().Add(5 * time.Second))
	const maxPriceTableSize = 16 * 1024
	var ptr rhpv3.RPCUpdatePriceTableResponse
	if err := s.WriteRequest(rhpv3.RPCUpdatePriceTableID, nil); err != nil {
		return rhpv3.HostPriceTable{}, err
	} else if err := s.ReadResponse(&ptr, maxPriceTableSize); err != nil {
		return rhpv3.HostPriceTable{}, err
	} else if err := json.Unmarshal(ptr.PriceTableJSON, &pt); err != nil {
		return rhpv3.HostPriceTable{}, err
	} else if payment, err := paymentFunc(pt); err != nil {
		return rhpv3.HostPriceTable{}, err
	} else if payment == nil {
		return pt, nil // intended not to pay
	} else if err := processPayment(s, payment); err != nil {
		return rhpv3.HostPriceTable{}, err
	} else if err := s.ReadResponse(&rhpv3.RPCPriceTableResponse{}, 0); err != nil {
		return rhpv3.HostPriceTable{}, err
	}
	return pt, nil
}

// RPCAccountBalance calls the AccountBalance RPC.
func RPCAccountBalance(t *rhpv3.Transport, payment rhpv3.PaymentMethod, account rhpv3.Account, settingsID rhpv3.SettingsID) (bal types.Currency, err error) {
	defer wrapErr(&err, "AccountBalance")
	s := t.DialStream()
	defer s.Close()

	req := rhpv3.RPCAccountBalanceRequest{
		Account: account,
	}
	var resp rhpv3.RPCAccountBalanceResponse
	if err := s.WriteRequest(rhpv3.RPCAccountBalanceID, &settingsID); err != nil {
		return types.ZeroCurrency, err
	} else if err := processPayment(s, payment); err != nil {
		return types.ZeroCurrency, err
	} else if err := s.WriteResponse(&req); err != nil {
		return types.ZeroCurrency, err
	} else if err := s.ReadResponse(&resp, 128); err != nil {
		return types.ZeroCurrency, err
	}
	return resp.Balance, nil
}

// RPCFundAccount calls the FundAccount RPC.
func RPCFundAccount(t *rhpv3.Transport, payment rhpv3.PaymentMethod, account rhpv3.Account, settingsID rhpv3.SettingsID) (err error) {
	defer wrapErr(&err, "FundAccount")
	s := t.DialStream()
	defer s.Close()

	req := rhpv3.RPCFundAccountRequest{
		Account: account,
	}
	var resp rhpv3.RPCFundAccountResponse
	s.SetDeadline(time.Now().Add(15 * time.Second))
	if err := s.WriteRequest(rhpv3.RPCFundAccountID, &settingsID); err != nil {
		return err
	} else if err := s.WriteResponse(&req); err != nil {
		return err
	} else if err := processPayment(s, payment); err != nil {
		return err
	} else if err := s.ReadResponse(&resp, 4096); err != nil {
		return err
	}
	return nil
}

// RPCReadRegistry calls the ExecuteProgram RPC with an MDM program that reads
// the specified registry value.
func RPCReadRegistry(t *rhpv3.Transport, payment rhpv3.PaymentMethod, key rhpv3.RegistryKey) (rv rhpv3.RegistryValue, err error) {
	defer wrapErr(&err, "ReadRegistry")
	s := t.DialStream()
	defer s.Close()

	req := &rhpv3.RPCExecuteProgramRequest{
		FileContractID: types.FileContractID{},
		Program:        []rhpv3.Instruction{&rhpv3.InstrReadRegistry{}},
		ProgramData:    append(key.PublicKey[:], key.Tweak[:]...),
	}
	if err := s.WriteRequest(rhpv3.RPCExecuteProgramID, nil); err != nil {
		return rhpv3.RegistryValue{}, err
	} else if err := processPayment(s, payment); err != nil {
		return rhpv3.RegistryValue{}, err
	} else if err := s.WriteResponse(req); err != nil {
		return rhpv3.RegistryValue{}, err
	}

	var cancellationToken types.Specifier
	s.ReadResponse(&cancellationToken, 16) // unused

	const maxExecuteProgramResponseSize = 16 * 1024
	var resp rhpv3.RPCExecuteProgramResponse
	if err := s.ReadResponse(&resp, maxExecuteProgramResponseSize); err != nil {
		return rhpv3.RegistryValue{}, err
	} else if len(resp.Output) < 64+8+1 {
		return rhpv3.RegistryValue{}, errors.New("invalid output length")
	}
	var sig types.Signature
	copy(sig[:], resp.Output[:64])
	rev := binary.LittleEndian.Uint64(resp.Output[64:72])
	data := resp.Output[72 : len(resp.Output)-1]
	typ := resp.Output[len(resp.Output)-1]
	return rhpv3.RegistryValue{
		Data:      data,
		Revision:  rev,
		Type:      typ,
		Signature: sig,
	}, nil
}

// RPCUpdateRegistry calls the ExecuteProgram RPC with an MDM program that
// updates the specified registry value.
func RPCUpdateRegistry(t *rhpv3.Transport, payment rhpv3.PaymentMethod, key rhpv3.RegistryKey, value rhpv3.RegistryValue) (err error) {
	defer wrapErr(&err, "UpdateRegistry")
	s := t.DialStream()
	defer s.Close()

	var data bytes.Buffer
	e := types.NewEncoder(&data)
	key.Tweak.EncodeTo(e)
	e.WriteUint64(value.Revision)
	value.Signature.EncodeTo(e)
	key.PublicKey.EncodeTo(e)
	e.Write(value.Data)
	e.Flush()
	req := &rhpv3.RPCExecuteProgramRequest{
		FileContractID: types.FileContractID{},
		Program:        []rhpv3.Instruction{&rhpv3.InstrUpdateRegistry{}},
		ProgramData:    data.Bytes(),
	}
	if err := s.WriteRequest(rhpv3.RPCExecuteProgramID, nil); err != nil {
		return err
	} else if err := processPayment(s, payment); err != nil {
		return err
	} else if err := s.WriteResponse(req); err != nil {
		return err
	}

	var cancellationToken types.Specifier
	s.ReadResponse(&cancellationToken, 16) // unused

	const maxExecuteProgramResponseSize = 16 * 1024
	var resp rhpv3.RPCExecuteProgramResponse
	if err := s.ReadResponse(&resp, maxExecuteProgramResponseSize); err != nil {
		return err
	} else if resp.OutputLength != 0 {
		return errors.New("invalid output length")
	}
	return nil
}
