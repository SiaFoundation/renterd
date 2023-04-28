package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/siad/crypto"
	"lukechampine.com/frand"
)

const (
	// accountLockingDuration is the time for which an account lock remains
	// reserved on the bus after locking it.
	accountLockingDuration = 30 * time.Second

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

	// errTransportClosed is returned when using a transportV3 which was already
	// closed.
	errTransportClosed = errors.New("transport closed")
)

// transportV3 is a reference-counted wrapper for rhpv3.Transport.
type transportV3 struct {
	mu       sync.Mutex
	refCount uint64
	t        *rhpv3.Transport
}

// Close decrements the refcounter and closes the transport if the refcounter
// reaches 0.
func (t *transportV3) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Decrement refcounter.
	t.refCount--

	// Close the transport if the refcounter is zero.
	if t.refCount == 0 {
		err := t.t.Close()
		t.t = nil
		return err
	}
	return nil
}

// DialStream dials a new stream on the transport.
func (t *transportV3) DialStream() (*rhpv3.Stream, error) {
	t.mu.Lock()
	transport := t.t
	t.mu.Unlock()
	if transport == nil {
		return nil, errTransportClosed
	}
	return transport.DialStream(), nil
}

// transportPoolV3 is a pool of rhpv3.Transports which allows for reusing them.
type transportPoolV3 struct {
	mu   sync.Mutex
	pool map[string]*transportV3
}

func newTransportPoolV3() *transportPoolV3 {
	return &transportPoolV3{
		pool: make(map[string]*transportV3),
	}
}

func (p *transportPoolV3) newTransport(ctx context.Context, siamuxAddr string, hostKey types.PublicKey) (*transportV3, error) {
	// Get or create a transport for the given siamux address.
	p.mu.Lock()
	t, found := p.pool[siamuxAddr]
	if !found {
		t = &transportV3{}
		p.pool[siamuxAddr] = t
	}

	// Lock the transport and increment its refcounter.
	t.mu.Lock()
	defer t.mu.Unlock()

	// Unlock the pool now that the transport is locked.
	p.mu.Unlock()

	// Init the transport if necessary.
	if t.t == nil {
		conn, err := dial(ctx, siamuxAddr, hostKey)
		if err != nil {
			return nil, err
		}
		t.t, err = rhpv3.NewRenterTransport(conn, hostKey)
		if err != nil {
			return nil, err
		}
	}

	// Increment the refcounter upon success.
	t.refCount++
	return t, nil
}

func (p *transportPoolV3) withTransportV3(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, fn func(*transportV3) error) (err error) {
	t, err := p.newTransport(ctx, siamuxAddr, hostKey)
	if err != nil {
		return err
	}
	var once sync.Once
	onceClose := func() { t.Close() }

	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			once.Do(onceClose)
		}
	}()
	defer once.Do(onceClose)
	return fn(t)
}

// FetchRevision fetches the latest revision of a contract and uses an account
// as the primary payment method. If the account balance is insufficient, it
// falls back to using the contract as a payment method.
func (w *worker) FetchRevision(ctx context.Context, timeout time.Duration, contract api.ContractMetadata, bh uint64, lockPriority int) (types.FileContractRevision, error) {
	timeoutCtx := func() (context.Context, context.CancelFunc) {
		if timeout > 0 {
			return context.WithTimeout(ctx, timeout)
		}
		return ctx, func() {}
	}

	// Try to fetch the revision with an account first.
	ctx, cancel := timeoutCtx()
	defer cancel()
	rev, err := w.FetchRevisionWithAccount(ctx, contract.HostKey, contract.SiamuxAddr, bh, contract.ID)
	if err != nil && !isBalanceInsufficient(err) {
		return types.FileContractRevision{}, err
	} else if err == nil {
		return rev, nil
	}

	// Fall back to using the contract to pay for the revision.
	ctx, cancel = timeoutCtx()
	defer cancel()
	contractLock, err := w.AcquireContract(ctx, contract.ID, lockPriority)
	if err != nil {
		return types.FileContractRevision{}, err
	}
	defer contractLock.Release(ctx)
	return w.FetchRevisionWithContract(ctx, contract.HostKey, contract.SiamuxAddr, contract.ID)
}

func (w *worker) FetchRevisionWithAccount(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, bh uint64, contractID types.FileContractID) (rev types.FileContractRevision, err error) {
	acc, err := w.accounts.ForHost(hostKey)
	if err != nil {
		return types.FileContractRevision{}, err
	}
	err = acc.WithWithdrawal(ctx, func() (types.Currency, error) {
		var cost types.Currency
		return cost, w.transportPoolV3.withTransportV3(ctx, hostKey, siamuxAddr, func(t *transportV3) (err error) {
			rev, err = RPCLatestRevision(t, contractID, func(revision *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error) {
				// Fetch pt.
				pt, err := w.priceTables.fetch(ctx, hostKey, nil)
				if err != nil {
					return rhpv3.HostPriceTable{}, nil, fmt.Errorf("failed to fetch pricetable, err: %v", err)
				}
				// Check pt.
				if breakdown := GougingCheckerFromContext(ctx).Check(nil, &pt.HostPriceTable); breakdown.Gouging() {
					return rhpv3.HostPriceTable{}, nil, fmt.Errorf("failed to fetch revision, %w: %v", errGougingHost, breakdown.Reasons())
				}
				cost = pt.LatestRevisionCost
				payment := rhpv3.PayByEphemeralAccount(acc.id, cost, bh+defaultWithdrawalExpiryBlocks, w.accounts.deriveAccountKey(hostKey))
				return pt.HostPriceTable, &payment, nil
			})
			if err != nil {
				return err
			}
			return nil
		})
	})
	return rev, err
}

// FetchRevisionWithContract fetches the latest revision of a contract and uses
// a contract to pay for it. If no pricetable is provided, a new one is
// requested.
func (w *worker) FetchRevisionWithContract(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, contractID types.FileContractID) (rev types.FileContractRevision, err error) {
	acc, err := w.accounts.ForHost(hostKey)
	if err != nil {
		return types.FileContractRevision{}, err
	}
	err = w.transportPoolV3.withTransportV3(ctx, hostKey, siamuxAddr, func(t *transportV3) (err error) {
		rev, err = RPCLatestRevision(t, contractID, func(revision *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error) {
			// Fetch pt.
			pt, err := w.priceTables.fetch(ctx, hostKey, revision)
			if err != nil {
				return rhpv3.HostPriceTable{}, nil, fmt.Errorf("failed to fetch pricetable, err: %v", err)
			}
			// Check pt.
			if breakdown := GougingCheckerFromContext(ctx).Check(nil, &pt.HostPriceTable); breakdown.Gouging() {
				return rhpv3.HostPriceTable{}, nil, fmt.Errorf("failed to fetch revision, %w: %v", errGougingHost, breakdown.Reasons())
			}
			// Pay for the revision.
			payment, ok := rhpv3.PayByContract(revision, pt.LatestRevisionCost, acc.id, w.deriveRenterKey(hostKey))
			if !ok {
				return rhpv3.HostPriceTable{}, nil, errors.New("insufficient funds")
			}
			return pt.HostPriceTable, &payment, nil
		})
		return err
	})
	return rev, err
}

func (w *worker) fundAccount(ctx context.Context, hk types.PublicKey, siamuxAddr string, balance types.Currency, revision *types.FileContractRevision) error {
	// fetch account
	account, err := w.accounts.ForHost(hk)
	if err != nil {
		return err
	}

	// fetch pricetable
	pt, err := w.priceTables.fetch(ctx, hk, revision)
	if err != nil {
		return err
	}

	// calculate the amount to deposit
	curr, err := account.Balance(ctx)
	if err != nil {
		return err
	}
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
		return amount, w.transportPoolV3.withTransportV3(ctx, hk, siamuxAddr, func(t *transportV3) (err error) {
			rk := w.deriveRenterKey(hk)
			cost := amount.Add(pt.FundAccountCost)
			payment, ok := rhpv3.PayByContract(revision, cost, rhpv3.Account{}, rk) // no account needed for funding
			if !ok {
				return errors.New("insufficient funds")
			}
			if err := RPCFundAccount(t, &payment, account.id, pt.UID); err != nil {
				return fmt.Errorf("failed to fund account with %v;%w", amount, err)
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
	pt, err := w.priceTables.fetch(ctx, hk, revision)
	if err != nil {
		return err
	}

	return account.WithSync(ctx, func() (types.Currency, error) {
		var balance types.Currency
		err := w.transportPoolV3.withTransportV3(ctx, hk, siamuxAddr, func(t *transportV3) error {
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
		store AccountStore
		key   types.PrivateKey
	}

	// account contains information regarding a specific account of the
	// worker.
	account struct {
		bus  AccountStore
		id   rhpv3.Account
		key  types.PrivateKey
		host types.PublicKey
	}

	hostV3 struct {
		acc           *account
		bh            uint64
		fcid          types.FileContractID
		pt            rhpv3.HostPriceTable
		siamuxAddr    string
		sk            types.PrivateKey
		transportPool *transportPoolV3
	}
)

func (w *worker) initAccounts(as AccountStore) {
	if w.accounts != nil {
		panic("accounts already initialized") // developer error
	}
	w.accounts = &accounts{
		store: as,
		key:   w.deriveSubKey("accountkey"),
	}
}

func (w *worker) withHostV3(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, siamuxAddr string, fn func(sectorStore) error) (err error) {
	acc, err := w.accounts.ForHost(hostKey)
	if err != nil {
		return err
	}

	pt, err := w.priceTables.fetch(ctx, hostKey, nil)
	if err != nil {
		return err
	}

	return fn(&hostV3{
		acc:           acc,
		bh:            pt.HostBlockHeight,
		fcid:          contractID,
		pt:            pt.HostPriceTable,
		siamuxAddr:    siamuxAddr,
		sk:            w.accounts.deriveAccountKey(hostKey),
		transportPool: w.transportPoolV3,
	})
}

// ForHost returns an account to use for a given host. If the account
// doesn't exist, a new one is created.
func (a *accounts) ForHost(hk types.PublicKey) (*account, error) {
	// Key should be set.
	if hk == (types.PublicKey{}) {
		return nil, errors.New("empty host key provided")
	}

	// Return account.
	accountID := rhpv3.Account(a.deriveAccountKey(hk).PublicKey())
	return &account{
		bus:  a.store,
		id:   accountID,
		key:  a.key,
		host: hk,
	}, nil
}

// WithDeposit increases the balance of an account by the amount returned by
// amtFn if amtFn doesn't return an error.
func (a *account) WithDeposit(ctx context.Context, amtFn func() (types.Currency, error)) error {
	_, lockID, err := a.bus.LockAccount(ctx, a.id, a.host, false, accountLockingDuration)
	if err != nil {
		return err
	}
	defer a.bus.UnlockAccount(ctx, a.id, lockID)

	amt, err := amtFn()
	if err != nil {
		return err
	}
	return a.bus.AddBalance(ctx, a.id, a.host, amt.Big())
}

func (a *account) Balance(ctx context.Context) (types.Currency, error) {
	account, lockID, err := a.bus.LockAccount(ctx, a.id, a.host, false, accountLockingDuration)
	if err != nil {
		return types.Currency{}, err
	}
	defer a.bus.UnlockAccount(ctx, a.id, lockID)
	return types.NewCurrency(account.Balance.Uint64(), new(big.Int).Rsh(account.Balance, 64).Uint64()), nil
}

// WithWithdrawal decreases the balance of an account by the amount returned by
// amtFn if amtFn doesn't return an error.
func (a *account) WithWithdrawal(ctx context.Context, amtFn func() (types.Currency, error)) error {
	account, lockID, err := a.bus.LockAccount(ctx, a.id, a.host, false, accountLockingDuration)
	if err != nil {
		return err
	}
	defer a.bus.UnlockAccount(ctx, a.id, lockID)

	// return early if our account is not funded
	if account.Balance.Cmp(big.NewInt(0)) <= 0 {
		return errBalanceInsufficient
	}

	amt, err := amtFn()
	if err != nil && isBalanceInsufficient(err) {
		err2 := a.bus.ScheduleSync(ctx, a.id, a.host)
		if err2 != nil {
			err = fmt.Errorf("failed to set requiresSync flag on bus: %w", err)
		}
		return err
	}
	if err != nil {
		return err
	}
	return a.bus.AddBalance(ctx, a.id, a.host, new(big.Int).Neg(amt.Big()))
}

// WithSync syncs an accounts balance with the bus. To do so, the account is
// locked while the balance is fetched through balanceFn.
func (a *account) WithSync(ctx context.Context, balanceFn func() (types.Currency, error)) error {
	_, lockID, err := a.bus.LockAccount(ctx, a.id, a.host, true, accountLockingDuration)
	if err != nil {
		return err
	}
	defer a.bus.UnlockAccount(ctx, a.id, lockID)
	balance, err := balanceFn()
	if err != nil {
		return err
	}
	return a.bus.SetBalance(ctx, a.id, a.host, balance.Big())
}

// deriveAccountKey derives an account plus key for a given host and worker.
// Each worker has its own account for a given host. That makes concurrency
// around keeping track of an accounts balance and refilling it a lot easier in
// a multi-worker setup.
func (a *accounts) deriveAccountKey(hostKey types.PublicKey) types.PrivateKey {
	index := byte(0) // not used yet but can be used to derive more than 1 account per host

	// Append the the host for which to create it and the index to the
	// corresponding sub-key.
	subKey := a.key
	data := append(subKey, hostKey[:]...)
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
	if breakdown := GougingCheckerFromContext(ctx).Check(nil, &r.pt); breakdown.Gouging() {
		return fmt.Errorf("failed to download sector, %w: %v", errGougingHost, breakdown.Reasons())
	}
	// return errBalanceInsufficient if balance insufficient
	defer func() {
		if isBalanceInsufficient(err) {
			err = fmt.Errorf("%w %v, err: %v", errInsufficientBalance, r.HostKey(), err)
		}
	}()

	return r.acc.WithWithdrawal(ctx, func() (amount types.Currency, err error) {
		err = r.transportPool.withTransportV3(ctx, r.HostKey(), r.siamuxAddr, func(t *transportV3) error {
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
func readSectorCost(pt rhpv3.HostPriceTable) (types.Currency, error) {
	rc := pt.BaseCost()
	rc = rc.Add(pt.ReadSectorCost(rhpv2.SectorSize))
	cost, _ := rc.Total()

	// overestimate the cost by 5%
	cost, overflow := cost.Mul64WithOverflow(21)
	if overflow {
		return types.ZeroCurrency, errors.New("overflow occurred while adding leeway to read sector cost")
	}
	return cost.Div64(20), nil
}

// priceTableValidityLeeway is the number of time before the actual expiry of a
// price table when we start considering it invalid.
const priceTableValidityLeeway = -30 * time.Second

type priceTables struct {
	w *worker

	mu          sync.Mutex
	priceTables map[types.PublicKey]*priceTable
}

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

func (w *worker) initPriceTables() {
	if w.priceTables != nil {
		panic("priceTables already initialized") // developer error
	}
	w.priceTables = &priceTables{
		w:           w,
		priceTables: make(map[types.PublicKey]*priceTable),
	}
}

// fetch returns a price table for the given host
func (pts *priceTables) fetch(ctx context.Context, hk types.PublicKey, revision *types.FileContractRevision) (hostdb.HostPriceTable, error) {
	pts.mu.Lock()
	pt, exists := pts.priceTables[hk]
	if !exists {
		pt = &priceTable{
			w:  pts.w,
			hk: hk,
		}
		pts.priceTables[hk] = pt
	}
	pts.mu.Unlock()

	return pt.fetch(ctx, revision)
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
	if !hpt.Expiry.IsZero() {
		total := int(math.Floor(hpt.HostPriceTable.Validity.Seconds() * 0.1))
		priceTableUpdateLeeway := -time.Duration(frand.Intn(total)) * time.Second
		if time.Now().Before(hpt.Expiry.Add(priceTableValidityLeeway).Add(priceTableUpdateLeeway)) {
			return
		}
	}

	// price table is valid and update ongoing, return early
	ongoing, update := p.ongoingUpdate()
	if ongoing && !hpt.Expiry.IsZero() && time.Now().Before(hpt.Expiry.Add(priceTableValidityLeeway)) {
		return
	}

	// price table is being updated, wait for the update
	if ongoing {
		select {
		case <-ctx.Done():
			return hostdb.HostPriceTable{}, fmt.Errorf("%w; timeout while blocking for pricetable update", ctx.Err())
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
		if err == nil {
			p.hpt = hpt
		}
		p.update = nil
		p.mu.Unlock()
	}()

	// fetch the host, return early if it has a valid price table
	host, err := b.Host(ctx, hk)
	if err == nil && host.Scanned && time.Now().Before(host.PriceTable.Expiry.Add(priceTableValidityLeeway)) {
		hpt = host.PriceTable
		return
	}

	// sanity check the host has been scanned before fetching the price table
	if !host.Scanned {
		return hostdb.HostPriceTable{}, fmt.Errorf("host %v was not scanned", hk)
	}

	// otherwise fetch it
	return w.fetchPriceTable(ctx, hk, host.Settings.SiamuxAddr(), revision)
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

// Renew renews a contract with a host. To avoid an edge case where the contract
// is drained and can therefore not be used to pay for the revision, we simply
// don't pay for it.
func (w *worker) Renew(ctx context.Context, rrr api.RHPRenewRequest, cs api.ConsensusState, renterKey types.PrivateKey) (_ rhpv2.ContractRevision, _ []types.Transaction, err error) {
	var rev rhpv2.ContractRevision
	var txnSet []types.Transaction
	var renewErr error
	err = w.transportPoolV3.withTransportV3(ctx, rrr.HostKey, rrr.SiamuxAddr, func(t *transportV3) (err error) {
		_, err = RPCLatestRevision(t, rrr.ContractID, func(revision *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error) {
			// Renew contract.
			rev, txnSet, renewErr = w.RPCRenew(ctx, rrr, cs, t, revision, renterKey)
			return rhpv3.HostPriceTable{}, nil, nil
		})
		return err
	})
	if err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}
	return rev, txnSet, renewErr
}

// RPCPriceTable calls the UpdatePriceTable RPC.
func RPCPriceTable(t *transportV3, paymentFunc PriceTablePaymentFunc) (pt rhpv3.HostPriceTable, err error) {
	defer wrapErr(&err, "PriceTable")
	s, err := t.DialStream()
	if err != nil {
		return rhpv3.HostPriceTable{}, err
	}
	defer s.Close()

	s.SetDeadline(time.Now().Add(15 * time.Second))
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
func RPCAccountBalance(t *transportV3, payment rhpv3.PaymentMethod, account rhpv3.Account, settingsID rhpv3.SettingsID) (bal types.Currency, err error) {
	defer wrapErr(&err, "AccountBalance")
	s, err := t.DialStream()
	if err != nil {
		return types.ZeroCurrency, err
	}
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
func RPCFundAccount(t *transportV3, payment rhpv3.PaymentMethod, account rhpv3.Account, settingsID rhpv3.SettingsID) (err error) {
	defer wrapErr(&err, "FundAccount")
	s, err := t.DialStream()
	if err != nil {
		return err
	}
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

// RPCLatestRevision calls the LatestRevision RPC. The paymentFunc allows for
// fetching a pricetable using the fetched revision to pay for it. If
// paymentFunc returns 'nil' as payment, the host is not paid.
func RPCLatestRevision(t *transportV3, contractID types.FileContractID, paymentFunc func(rev *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error)) (_ types.FileContractRevision, err error) {
	defer wrapErr(&err, "LatestRevision")
	s, err := t.DialStream()
	if err != nil {
		return types.FileContractRevision{}, err
	}
	defer s.Close()
	req := rhpv3.RPCLatestRevisionRequest{
		ContractID: contractID,
	}
	var resp rhpv3.RPCLatestRevisionResponse
	if err := s.WriteRequest(rhpv3.RPCLatestRevisionID, &req); err != nil {
		return types.FileContractRevision{}, err
	} else if err := s.ReadResponse(&resp, 4096); err != nil {
		return types.FileContractRevision{}, err
	} else if pt, payment, err := paymentFunc(&resp.Revision); err != nil || payment == nil {
		return types.FileContractRevision{}, err
	} else if err := s.WriteResponse(&pt.UID); err != nil {
		return types.FileContractRevision{}, err
	} else if err := processPayment(s, payment); err != nil {
		return types.FileContractRevision{}, err
	}
	return resp.Revision, nil
}

// RPCReadSector calls the ExecuteProgram RPC with a ReadSector instruction.
func RPCReadSector(t *transportV3, w io.Writer, pt rhpv3.HostPriceTable, payment rhpv3.PaymentMethod, offset, length uint64, merkleRoot types.Hash256, merkleProof bool) (cost, refund types.Currency, err error) {
	defer wrapErr(&err, "ReadSector")
	s, err := t.DialStream()
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, err
	}
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
	} else if err = s.ReadResponse(&resp, rhpv2.SectorSize+responseLeeway); err != nil {
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
func RPCReadRegistry(t *transportV3, payment rhpv3.PaymentMethod, key rhpv3.RegistryKey) (rv rhpv3.RegistryValue, err error) {
	defer wrapErr(&err, "ReadRegistry")
	s, err := t.DialStream()
	if err != nil {
		return rhpv3.RegistryValue{}, err
	}
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

func RPCAppendSector(t *transportV3, renterKey types.PrivateKey, pt rhpv3.HostPriceTable, rev *types.FileContractRevision, payment rhpv3.PaymentMethod, collateral types.Currency, sector *[rhpv2.SectorSize]byte) (cost, refund types.Currency, err error) {
	defer wrapErr(&err, "AppendSector")
	s, err := t.DialStream()
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, err
	}
	defer s.Close()

	req := rhpv3.RPCExecuteProgramRequest{
		FileContractID: types.FileContractID{},
		Program: []rhpv3.Instruction{&rhpv3.InstrAppendSector{
			SectorDataOffset: 0,
			ProofRequired:    true,
		}},
		ProgramData: (*sector)[:],
	}

	var cancellationToken types.Specifier
	var executeResp rhpv3.RPCExecuteProgramResponse
	if err = s.WriteRequest(rhpv3.RPCExecuteProgramID, &pt.UID); err != nil {
		return
	} else if err = processPayment(s, payment); err != nil {
		return
	} else if err = s.WriteResponse(&req); err != nil {
		return
	} else if err = s.ReadResponse(&cancellationToken, 16); err != nil {
		return
	} else if err = s.ReadResponse(&executeResp, 4096); err != nil {
		return
	}

	// check response error
	if err = executeResp.Error; err != nil {
		refund = executeResp.FailureRefund
		return
	}
	cost = executeResp.TotalCost

	// TODO: verify proof

	// finalize the program with a new revision.
	newRevision := *rev
	newValid, newMissed := updateRevisionOutputs(&newRevision, types.ZeroCurrency, collateral)
	newRevision.RevisionNumber++
	newRevision.FileMerkleRoot = executeResp.NewMerkleRoot

	finalizeReq := rhpv3.RPCFinalizeProgramRequest{
		Signature:         renterKey.SignHash(hashRevision(newRevision)),
		ValidProofValues:  newValid,
		MissedProofValues: newMissed,
		RevisionNumber:    newRevision.RevisionNumber,
	}

	var finalizeResp rhpv3.RPCFinalizeProgramResponse
	if err = s.WriteResponse(&finalizeReq); err != nil {
		return
	} else if err = s.ReadResponse(&finalizeResp, 64); err != nil {
		return
	}

	return
}

func (w *worker) RPCRenew(ctx context.Context, rrr api.RHPRenewRequest, cs api.ConsensusState, t *transportV3, rev *types.FileContractRevision, renterKey types.PrivateKey) (_ rhpv2.ContractRevision, _ []types.Transaction, err error) {
	defer wrapErr(&err, "RPCRenew")
	s, err := t.DialStream()
	if err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}
	defer s.Close()

	// Try to get a valid pricetable.
	var ptUID rhpv3.SettingsID
	ptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var pt rhpv3.HostPriceTable
	hpt, err := w.priceTables.fetch(ptCtx, rrr.HostKey, nil)
	if err == nil {
		pt = hpt.HostPriceTable
		ptUID = hpt.UID
	} else {
		w.logger.Warnf("failed to fetch valid pricetable for renew: %v", err)
	}

	// Send the ptUID.
	if err = s.WriteRequest(rhpv3.RPCRenewContractID, &ptUID); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// If we didn't have a valid pricetable, read the temporary one from the
	// host.
	if ptUID == (rhpv3.SettingsID{}) {
		var ptResp rhpv3.RPCUpdatePriceTableResponse
		if err = s.ReadResponse(&ptResp, 4096); err != nil {
			return rhpv2.ContractRevision{}, nil, err
		}
		if err = json.Unmarshal(ptResp.PriceTableJSON, &pt); err != nil {
			return rhpv2.ContractRevision{}, nil, err
		}
	}

	// Perform gouging checks.
	if gc := GougingCheckerFromContext(ctx).Check(nil, &pt); gc.Gouging() {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("host gouging during renew: %v", gc.Reasons())
	}

	// Prepare the signed transaction that contains the final revision as well
	// as the new contract
	wprr, err := w.bus.WalletPrepareRenew(ctx, *rev, rrr.HostAddress, rrr.RenterAddress, renterKey, rrr.RenterFunds, rrr.NewCollateral, rrr.HostKey, pt, rrr.EndHeight, rrr.WindowSize)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// Starting from here, we need to make sure to release the txn on error.
	defer w.discardTxnOnErr(ctx, wprr.TransactionSet[len(wprr.TransactionSet)-1], "RPCRenew", &err)

	txnSet := wprr.TransactionSet
	parents, txn := txnSet[:len(txnSet)-1], txnSet[len(txnSet)-1]

	// Sign only the revision and contract. We can't sign everything because
	// then the host can't add its own outputs.
	h := types.NewHasher()
	txn.FileContracts[0].EncodeTo(h.E)
	txn.FileContractRevisions[0].EncodeTo(h.E)
	finalRevisionSignature := renterKey.SignHash(h.Sum())

	// Send the request.
	req := rhpv3.RPCRenewContractRequest{
		TransactionSet:         txnSet,
		RenterKey:              rev.UnlockConditions.PublicKeys[0],
		FinalRevisionSignature: finalRevisionSignature,
	}
	if err = s.WriteResponse(&req); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// Incorporate the host's additions.
	var hostAdditions rhpv3.RPCRenewContractHostAdditions
	if err = s.ReadResponse(&hostAdditions, 4096); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}
	parents = append(parents, hostAdditions.Parents...)
	txn.SiacoinInputs = append(txn.SiacoinInputs, hostAdditions.SiacoinInputs...)
	txn.SiacoinOutputs = append(txn.SiacoinOutputs, hostAdditions.SiacoinOutputs...)
	finalRevRenterSig := types.TransactionSignature{
		ParentID:       types.Hash256(rev.ParentID),
		PublicKeyIndex: 0, // renter key is first
		CoveredFields: types.CoveredFields{
			FileContracts:         []uint64{0},
			FileContractRevisions: []uint64{0},
		},
		Signature: finalRevisionSignature[:],
	}
	finalRevHostSig := types.TransactionSignature{
		ParentID:       types.Hash256(rev.ParentID),
		PublicKeyIndex: 1,
		CoveredFields: types.CoveredFields{
			FileContracts:         []uint64{0},
			FileContractRevisions: []uint64{0},
		},
		Signature: hostAdditions.FinalRevisionSignature[:],
	}
	txn.Signatures = []types.TransactionSignature{finalRevRenterSig, finalRevHostSig}

	// Sign the inputs we funded the txn with and cover the whole txn including
	// the existing signatures.
	cf := types.CoveredFields{
		WholeTransaction: true,
		Signatures:       []uint64{0, 1},
	}
	cf = wallet.ExplicitCoveredFields(txn)
	if err := w.bus.WalletSign(ctx, &txn, wprr.ToSign, cf); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// Create a new no-op revision and sign it.
	noOpRevision := initialRevision(txn, rev.UnlockConditions.PublicKeys[1], renterKey.PublicKey().UnlockKey())
	h = types.NewHasher()
	noOpRevision.EncodeTo(h.E)
	renterNoOpSig := renterKey.SignHash(h.Sum())
	renterNoOpRevisionSignature := types.TransactionSignature{
		ParentID:       types.Hash256(noOpRevision.ParentID),
		PublicKeyIndex: 0, // renter key is first
		CoveredFields: types.CoveredFields{
			FileContractRevisions: []uint64{0},
		},
		Signature: renterNoOpSig[:],
	}

	// Send the newly added signatures to the host and the signature for the
	// initial no-op revision.
	rs := rhpv3.RPCRenewSignatures{
		TransactionSignatures: txn.Signatures[2:],
		RevisionSignature:     renterNoOpRevisionSignature,
	}
	if err = s.WriteResponse(&rs); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// Receive the host's signatures.
	var hostSigs rhpv3.RPCRenewSignatures
	if err = s.ReadResponse(&hostSigs, 4096); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// Add the parents to get the full txnSet.
	txnSet = append(parents, txn)

	return rhpv2.ContractRevision{
		Revision:   noOpRevision,
		Signatures: [2]types.TransactionSignature{renterNoOpRevisionSignature, hostSigs.RevisionSignature},
	}, txnSet, nil
}

// initialRevision returns the first revision of a file contract formation
// transaction.
func initialRevision(formationTxn types.Transaction, hostPubKey, renterPubKey types.UnlockKey) types.FileContractRevision {
	fc := formationTxn.FileContracts[0]
	return types.FileContractRevision{
		ParentID: formationTxn.FileContractID(0),
		UnlockConditions: types.UnlockConditions{
			PublicKeys:         []types.UnlockKey{renterPubKey, hostPubKey},
			SignaturesRequired: 2,
		},
		FileContract: types.FileContract{
			Filesize:           fc.Filesize,
			FileMerkleRoot:     fc.FileMerkleRoot,
			WindowStart:        fc.WindowStart,
			WindowEnd:          fc.WindowEnd,
			ValidProofOutputs:  fc.ValidProofOutputs,
			MissedProofOutputs: fc.MissedProofOutputs,
			UnlockHash:         fc.UnlockHash,
			RevisionNumber:     1,
		},
	}
}

// RPCUpdateRegistry calls the ExecuteProgram RPC with an MDM program that
// updates the specified registry value.
func RPCUpdateRegistry(t *transportV3, payment rhpv3.PaymentMethod, key rhpv3.RegistryKey, value rhpv3.RegistryValue) (err error) {
	defer wrapErr(&err, "UpdateRegistry")
	s, err := t.DialStream()
	if err != nil {
		return err
	}
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
