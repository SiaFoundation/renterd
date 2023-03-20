package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"lukechampine.com/frand"
)

const (
	// defaultWithdrawalExpiryBlocks is the number of blocks we add to the
	// current blockheight when we define an expiry block height for withdrawal
	// messages.
	defaultWithdrawalExpiryBlocks = 6

	// responseLeeway is the amount of leeway given to the maxLen when we read
	// the response in the ReadSector RPC
	responseLeeway = 1 << 12 // 4 KiB
)

var (
	// errBalanceSufficient occurs when funding an account to a desired balance
	// that's lower than its current balance.
	errBalanceSufficient = errors.New("ephemeral account balance greater than desired balance")

	// errBalanceInsufficient occurs when a withdrawal failed because the
	// account balance was insufficient.
	errBalanceInsufficient = errors.New("ephemeral account balance was insufficient")

	// errBalanceMaxExceeded occurs when a deposit would push the account's
	// balance over the maximum allowed ephemeral account balance.
	errBalanceMaxExceeded = errors.New("ephemeral account maximum balance exceeded")
)

func (w *worker) fundAccount(ctx context.Context, hk types.PublicKey, siamuxAddr string, balance types.Currency, revision *types.FileContractRevision) error {
	// fetch account
	account, err := w.accounts.ForHost(hk)
	if err != nil {
		return err
	}

	// fetch pricetable
	pt, err := w.priceTable(hk).fetch(ctx, revision)
	if err != nil {
		return err
	}

	// calculate the amount to deposit
	curr := account.Balance()
	if curr.Cmp(balance) >= 0 {
		return fmt.Errorf("%w; %v>%v", errBalanceSufficient, curr, balance)
	}
	amount := balance.Sub(curr)

	// cap the amount by the amount of money left in the contract
	renterFunds := revision.ValidRenterPayout()
	if renterFunds.Cmp(pt.FundAccountCost) <= 0 {
		return fmt.Errorf("insufficient funds to fund account: %v <= %v", renterFunds, pt.FundAccountCost)
	} else if maxAmount := renterFunds.Sub(pt.FundAccountCost); maxAmount.Cmp(amount) < 0 {
		amount = maxAmount
	}

	return account.WithDeposit(ctx, func() (types.Currency, error) {
		return amount, withTransportV3(ctx, siamuxAddr, hk, func(t *rhpv3.Transport) (err error) {
			rk := w.deriveRenterKey(hk)
			cost := amount.Add(pt.FundAccountCost)
			payment, ok := rhpv3.PayByContract(revision, cost, rhpv3.Account{}, rk) // no account needed for funding
			if !ok {
				return errors.New("insufficient funds")
			}
			if err := RPCFundAccount(t, &payment, account.id, pt.UID); err != nil {
				return err
			}
			w.contractSpendingRecorder.Record(revision.ParentID, api.ContractSpending{FundAccount: cost})
			return nil
		})
	})
}

func (w *worker) syncAccount(ctx context.Context, hk types.PublicKey, siamuxAddr string, revision *types.FileContractRevision) error {
	// fetch the account
	account, err := w.accounts.ForHost(hk)
	if err != nil {
		return err
	}

	// fetch pricetable
	pt, err := w.priceTable(hk).fetch(ctx, nil)
	if err != nil {
		return err
	}

	return account.WithSync(ctx, func() (types.Currency, error) {
		var balance types.Currency
		err := withTransportV3(ctx, siamuxAddr, hk, func(t *rhpv3.Transport) error {
			payment := w.preparePayment(hk, pt.AccountBalanceCost, pt.HostBlockHeight)
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

func isBalanceInsufficient(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), errBalanceInsufficient.Error())
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
		rwmu         sync.RWMutex
		mu           sync.Mutex
		balance      *big.Int
		drift        *big.Int
		requiresSync bool
	}

	hostV3 struct {
		acc        *account
		bh         uint64
		fcid       types.FileContractID
		pt         *rhpv3.HostPriceTable
		siamuxAddr string
		sk         types.PrivateKey
	}
)

func (w *worker) initAccounts(as AccountStore) {
	if w.accounts != nil {
		panic("accounts already initialized") // developer error
	}
	w.accounts = &accounts{
		store:    as,
		workerID: w.id,
		key:      w.deriveSubKey("accountkey"),
	}
}

func (w *worker) withHostV3(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, hostIP, siamuxAddr string, fn func(sectorStore) error) (err error) {
	acc, err := w.accounts.ForHost(hostKey)
	if err != nil {
		return err
	}

	pt, err := w.priceTable(hostKey).fetch(ctx, nil)
	if err != nil {
		return err
	}

	return fn(&hostV3{
		acc:        acc,
		bh:         pt.HostBlockHeight,
		fcid:       contractID,
		pt:         pt.HostPriceTable,
		siamuxAddr: siamuxAddr,
		sk:         w.accounts.deriveAccountKey(hostKey),
	})
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
	a.rwmu.RLock()
	defer a.rwmu.RUnlock()
	a.mu.Lock()
	defer a.mu.Unlock()
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
	a.rwmu.RLock()
	defer a.rwmu.RUnlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	return api.Account{
		ID:           a.id,
		Balance:      new(big.Int).Set(a.balance),
		Drift:        new(big.Int).Set(a.drift),
		Host:         a.host,
		Owner:        a.owner,
		RequiresSync: a.requiresSync,
	}
}

func (a *account) resetDrift(ctx context.Context) error {
	a.rwmu.Lock()
	defer a.rwmu.Unlock()
	if err := a.bus.ResetDrift(ctx, a.id); err != nil {
		return err
	}
	a.mu.Lock()
	a.drift.SetInt64(0)
	a.mu.Unlock()
	return nil
}

// WithDeposit increases the balance of an account by the amount returned by
// amtFn if amtFn doesn't return an error.
func (a *account) WithDeposit(ctx context.Context, amtFn func() (types.Currency, error)) error {
	a.rwmu.RLock()
	defer a.rwmu.RUnlock()
	amt, err := amtFn()
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.balance = a.balance.Add(a.balance, amt.Big())
	a.mu.Unlock()
	return a.bus.AddBalance(ctx, a.id, a.owner, a.host, amt.Big())
}

// WithWithdrawal decreases the balance of an account by the amount returned by
// amtFn if amtFn doesn't return an error.
func (a *account) WithWithdrawal(ctx context.Context, amtFn func() (types.Currency, error)) error {
	a.rwmu.RLock()
	defer a.rwmu.RUnlock()

	// return early if our account is not funded
	a.mu.Lock()
	if a.balance.Cmp(big.NewInt(0)) <= 0 {
		a.rwmu.Unlock()
		return errBalanceInsufficient
	}
	a.mu.Unlock()

	amt, err := amtFn()
	if err != nil && strings.Contains(err.Error(), "ephemeral account balance was insufficient") {
		a.mu.Lock()
		requiresSyncBefore := a.requiresSync
		a.requiresSync = true
		a.mu.Unlock()
		if requiresSyncBefore {
			err2 := a.bus.SetRequiresSync(ctx, a.id, a.owner, a.host, true)
			if err2 != nil {
				err = fmt.Errorf("failed to set requiresSync flag on bus: %w", err)
			}
		}
		return err
	}
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.balance = a.balance.Sub(a.balance, amt.Big())
	a.mu.Unlock()
	return a.bus.AddBalance(ctx, a.id, a.owner, a.host, new(big.Int).Neg(amt.Big()))
}

// WithSync syncs an accounts balance with the bus. To do so, the account is
// locked while the balance is fetched through balanceFn.
func (a *account) WithSync(ctx context.Context, balanceFn func() (types.Currency, error)) error {
	a.rwmu.Lock()
	defer a.rwmu.Unlock()
	balance, err := balanceFn()
	if err != nil {
		return err
	}
	a.mu.Lock()
	delta := new(big.Int).Sub(balance.Big(), a.balance)
	a.drift = a.drift.Add(a.drift, delta)
	a.balance = balance.Big()
	newBalance, newDrift := new(big.Int).Set(a.balance), new(big.Int).Set(a.drift)
	a.requiresSync = false
	a.mu.Unlock()
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
			bus:          a.store,
			id:           rhpv3.Account(acc.ID),
			key:          a.deriveAccountKey(acc.Host),
			host:         acc.Host,
			owner:        acc.Owner,
			balance:      acc.Balance,
			drift:        acc.Drift,
			requiresSync: acc.RequiresSync,
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

func (r *hostV3) Contract() types.FileContractID {
	return r.fcid
}

func (r *hostV3) HostKey() types.PublicKey {
	return r.acc.host
}

func (*hostV3) UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte) (types.Hash256, error) {
	panic("not implemented")
}

func (*hostV3) DeleteSectors(ctx context.Context, roots []types.Hash256) error {
	panic("not implemented")
}

func (r *hostV3) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) (err error) {
	// return errGougingHost if gouging checks fail
	if errs := PerformGougingChecks(ctx, nil, r.pt).CanDownload(); len(errs) > 0 {
		return fmt.Errorf("failed to download sector, %w: %v", errGougingHost, errs)
	}
	// return errBalanceInsufficient if balance insufficient
	defer func() {
		if isBalanceInsufficient(err) {
			err = fmt.Errorf("%w %v, err: %v", errNonFundedHost, r.HostKey(), err)
		}
	}()

	return r.acc.WithWithdrawal(ctx, func() (amount types.Currency, err error) {
		err = withTransportV3(ctx, r.siamuxAddr, r.HostKey(), func(t *rhpv3.Transport) error {
			cost, err := readSectorCost(r.pt)
			if err != nil {
				return err
			}

			var refund types.Currency
			payment := rhpv3.PayByEphemeralAccount(r.acc.id, cost, r.bh+defaultWithdrawalExpiryBlocks, r.sk)
			cost, refund, err = RPCReadSector(t, w, r.pt, &payment, offset, length, root, true)
			amount = cost.Sub(refund)
			return err
		})
		return
	})
}

// readSectorCost returns an overestimate for the cost of reading a sector from a host
func readSectorCost(pt *rhpv3.HostPriceTable) (types.Currency, error) {
	cost, overflow := pt.InitBaseCost.AddWithOverflow(pt.ReadBaseCost)
	if overflow {
		return types.ZeroCurrency, errors.New("overflow occurred while calculating read sector cost, base cost overflow")
	}

	ulbw, overflow := pt.UploadBandwidthCost.Mul64WithOverflow(1 << 12) // 4KiB
	if overflow {
		return types.ZeroCurrency, errors.New("overflow occurred while calculating read sector cost, upload bandwidth overflow")
	}

	dlbw, overflow := pt.DownloadBandwidthCost.Mul64WithOverflow(1 << 22) // 4MiB
	if overflow {
		return types.ZeroCurrency, errors.New("overflow occurred while calculating read sector cost, download bandwidth overflow")
	}

	bw, overflow := ulbw.AddWithOverflow(dlbw)
	if overflow {
		return types.ZeroCurrency, errors.New("overflow occurred while calculating read sector cost, bandwidth overflow")
	}

	cost, overflow = cost.AddWithOverflow(bw)
	if overflow {
		return types.ZeroCurrency, errors.New("overflow occurred while calculating read sector cost")
	}

	// overestimate the cost by ~10%
	cost, overflow = cost.Mul64WithOverflow(10)
	if overflow {
		return types.ZeroCurrency, errors.New("overflow occurred while adding leeway to read sector cost")
	}
	return cost.Div64(9), nil
}

// priceTableValidityLeeway is the number of time before the actual expiry of a
// price table when we start considering it invalid.
const priceTableValidityLeeway = -30 * time.Second

type priceTable struct {
	w  *worker
	hk types.PublicKey

	mu     sync.Mutex
	hpt    hostdb.HostPriceTable
	update *priceTableUpdate
}

type priceTableUpdate struct {
	err  error
	done chan struct{}
	hpt  hostdb.HostPriceTable
}

// priceTable returns a price table for the given host
func (w *worker) priceTable(hk types.PublicKey) *priceTable {
	w.priceTablesMu.Lock()
	defer w.priceTablesMu.Unlock()

	if _, exists := w.priceTables[hk]; !exists {
		w.priceTables[hk] = &priceTable{
			w:  w,
			hk: hk,
		}
	}
	return w.priceTables[hk]
}

func (pt *priceTable) ongoingUpdate() (bool, *priceTableUpdate) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	var ongoing bool
	if pt.update == nil {
		pt.update = &priceTableUpdate{done: make(chan struct{})}
	} else {
		ongoing = true
	}

	return ongoing, pt.update
}

func (p *priceTable) fetch(ctx context.Context, revision *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error) {
	// convenience variables
	hk := p.hk
	w := p.w
	b := p.w.bus

	// grab the current price table
	p.mu.Lock()
	hpt = p.hpt
	p.mu.Unlock()

	// price table is valid, no update necessary, return early
	priceTableUpdateLeeway := -time.Duration(frand.Intn(60)) * time.Second
	if time.Now().Before(hpt.Expiry.Add(priceTableValidityLeeway).Add(priceTableUpdateLeeway)) {
		return
	}

	// price table is valid and update ongoing, return early
	ongoing, update := p.ongoingUpdate()
	if ongoing && time.Now().Before(hpt.Expiry.Add(priceTableValidityLeeway)) {
		return
	}

	// price table is being updated, wait for the update
	if ongoing {
		select {
		case <-ctx.Done():
			return hostdb.HostPriceTable{}, errors.New("timeout while blocking for pricetable update")
		case <-update.done:
		}
		return update.hpt, update.err
	}

	// this thread is updating the price table
	defer func() {
		update.hpt = hpt
		update.err = err
		close(update.done)

		p.mu.Lock()
		p.hpt = hpt
		p.update = nil
		p.mu.Unlock()
	}()

	// fetch the host info from the bus, it might have a new price table already
	// in which case we can return without having to update it
	host, err := b.Host(ctx, hk)
	if err == nil && host.PriceTable != nil && time.Now().Before(host.PriceTable.Expiry.Add(priceTableValidityLeeway)) {
		hpt = *host.PriceTable
		return
	}
	hostIP := host.NetAddress
	siamuxAddr := host.Settings.SiamuxAddr()

	payByFC := func() (pt rhpv3.HostPriceTable, err error) {
		// if revision was given, use it to pay for the scan
		if revision != nil {
			_, _, pt, err = w.scanHost(ctx, hk, hostIP, siamuxAddr, w.preparePriceTableContractPayment(hk, revision))
			return
		}

		// otherwise we fetch the active contract
		contract, err := b.ActiveContract(ctx, hk)
		if err != nil {
			return rhpv3.HostPriceTable{}, err
		}

		// and scan the host
		_ = w.withRevision(ctx, contract.ID, hk, hostIP, lockingPriorityPriceTable, lockingDurationPriceTable, func(revision types.FileContractRevision) error {
			_, _, pt, err = w.scanHost(ctx, hk, hostIP, siamuxAddr, w.preparePriceTableContractPayment(hk, &revision))
			return err
		})
		return
	}

	// scan host and pay by EA if possible
	var pt rhpv3.HostPriceTable
	if revision != nil {
		pt, err = payByFC() // pay by FC if a revision is given
	} else if cs, errr := b.ConsensusState(ctx); errr == nil && cs.Synced {
		_, _, pt, err = w.scanHost(ctx, hk, hostIP, siamuxAddr, w.preparePriceTableAccountPayment(hk, cs.BlockHeight))
		if err == nil && pt.Validity < 0 {
			pt, err = payByFC() // fallback to paying by FC
		}
	} else {
		pt, err = payByFC() // fallback to paying by FC
	}

	// only overwrite the price table if it's valid
	if err == nil && pt.Validity <= 0 {
		err = errors.New("failed to get valid price table, likely payment failure")
	} else if err == nil {
		hpt = hostdb.HostPriceTable{
			HostPriceTable: &pt,
			Expiry:         time.Now().Add(pt.Validity),
		}
	}

	return
}

// preparePriceTableContractPayment prepare a payment function to pay for a
// price table from the given host using the provided revision.
//
// NOTE: This way of paying for a price table should only be used if payment by
// EA is not possible or if we already need a contract revision anyway. e.g.
// funding an EA.
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

// preparePriceTableAccountPayment prepare a payment function to pay for a price
// table from the given host using the provided revision.
//
// NOTE: This is the preferred way of paying for a price table since it is
// faster and doesn't require locking a contract.
func (w *worker) preparePriceTableAccountPayment(hk types.PublicKey, bh uint64) PriceTablePaymentFunc {
	return func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
		// TODO: gouging check on price table

		accountKey := w.accounts.deriveAccountKey(hk)
		account := rhpv3.Account(accountKey.PublicKey())
		payment := rhpv3.PayByEphemeralAccount(account, pt.UpdatePriceTableCost, bh+defaultWithdrawalExpiryBlocks, accountKey)
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

	s.SetDeadline(time.Now().Add(15 * time.Second))
	const maxPriceTableSize = 16 * 1024
	var ptr rhpv3.RPCUpdatePriceTableResponse

	if err = s.WriteRequest(rhpv3.RPCUpdatePriceTableID, nil); err != nil {
		return
	} else if err = s.ReadResponse(&ptr, maxPriceTableSize); err != nil {
		return
	} else if err = json.Unmarshal(ptr.PriceTableJSON, &pt); err != nil {
		return
	}

	var tracked bool
	defer func() {
		if !tracked {
			pt.Validity = -time.Second // expiry in the past
		}
	}()

	var payment rhpv3.PaymentMethod
	if payment, err = paymentFunc(pt); err != nil {
		return pt, nil // failed to pay
	} else if payment == nil {
		return pt, nil // intended not to pay
	}

	if err = processPayment(s, payment); err != nil {
		return pt, nil // failed to pay
	} else if err = s.ReadResponse(&rhpv3.RPCPriceTableResponse{}, 0); err != nil {
		return pt, nil // failed to pay
	}

	tracked = true
	return
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

// RPCReadSector calls the ExecuteProgram RPC with a ReadSector instruction.
func RPCReadSector(t *rhpv3.Transport, w io.Writer, pt *rhpv3.HostPriceTable, payment rhpv3.PaymentMethod, offset, length uint64, merkleRoot types.Hash256, merkleProof bool) (cost, refund types.Currency, err error) {
	defer wrapErr(&err, "ReadSector")
	s := t.DialStream()
	defer s.Close()

	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	e.WriteUint64(length)
	e.WriteUint64(offset)
	merkleRoot.EncodeTo(e)
	e.Flush()

	req := rhpv3.RPCExecuteProgramRequest{
		FileContractID: types.FileContractID{},
		Program: []rhpv3.Instruction{&rhpv3.InstrReadSector{
			LengthOffset:     0,
			OffsetOffset:     8,
			MerkleRootOffset: 16,
			ProofRequired:    true,
		}},
		ProgramData: buf.Bytes(),
	}

	var cancellationToken types.Specifier
	var resp rhpv3.RPCExecuteProgramResponse
	if err = s.WriteRequest(rhpv3.RPCExecuteProgramID, &pt.UID); err != nil {
		return
	} else if err = processPayment(s, payment); err != nil {
		return
	} else if err = s.WriteResponse(&req); err != nil {
		return
	} else if err = s.ReadResponse(&cancellationToken, 16); err != nil {
		return
	} else if err = s.ReadResponse(&resp, modules.SectorSize+responseLeeway); err != nil {
		return
	}

	// check response error
	if err = resp.Error; err != nil {
		refund = resp.FailureRefund
		return
	}
	cost = resp.TotalCost

	// build proof
	proof := make([]crypto.Hash, len(resp.Proof))
	for i, h := range resp.Proof {
		proof[i] = crypto.Hash(h)
	}

	// verify proof
	proofStart := int(offset) / crypto.SegmentSize
	proofEnd := int(offset+length) / crypto.SegmentSize
	if !crypto.VerifyRangeProof(resp.Output, proof, proofStart, proofEnd, crypto.Hash(merkleRoot)) {
		err = errors.New("proof verification failed")
		return
	}

	_, err = w.Write(resp.Output)
	return
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
