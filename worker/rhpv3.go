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
	"net"
	"strings"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/mux/v1"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/metrics"
	"go.sia.tech/siad/crypto"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	// accountLockingDuration is the time for which an account lock remains
	// reserved on the bus after locking it.
	accountLockingDuration = 30 * time.Second

	// defaultRPCResponseMaxSize is the default maxSize we use whenever we read
	// an RPC response.
	defaultRPCResponseMaxSize = 100 * 1024 // 100 KiB

	// defaultWithdrawalExpiryBlocks is the number of blocks we add to the
	// current blockheight when we define an expiry block height for withdrawal
	// messages.
	defaultWithdrawalExpiryBlocks = 6

	// responseLeeway is the amount of leeway given to the maxLen when we read
	// the response in the ReadSector RPC
	responseLeeway = 1 << 12 // 4 KiB
)

var (
	// errBalanceInsufficient occurs when a withdrawal failed because the
	// account balance was insufficient.
	errBalanceInsufficient = errors.New("ephemeral account balance was insufficient")

	// errBalanceMaxExceeded occurs when a deposit would push the account's
	// balance over the maximum allowed ephemeral account balance.
	errBalanceMaxExceeded = errors.New("ephemeral account maximum balance exceeded")

	// errMaxRevisionReached occurs when trying to revise a contract that has
	// already reached the highest possible revision number. Usually happens
	// when trying to use a renewed contract.
	errMaxRevisionReached = errors.New("contract has reached the maximum number of revisions")

	// errPriceTableExpired is returned by the host when the price table that
	// corresponds to the id it was given is already expired and thus no longer
	// valid.
	errPriceTableExpired = errors.New("price table requested is expired")

	// errPriceTableNotFound is returned by the host when it can not find a
	// price table that corresponds with the id we sent it.
	errPriceTableNotFound = errors.New("price table not found")

	// errSectorNotFound is returned by the host when it can not find the
	// requested sector.
	errSectorNotFoundOld = errors.New("could not find the desired sector")
	errSectorNotFound    = errors.New("sector not found")

	// errWithdrawalsInactive occurs when the host is (perhaps temporarily)
	// unsynced and has disabled its account manager.
	errWithdrawalsInactive = errors.New("ephemeral account withdrawals are inactive because the host is not synced")
)

func isBalanceInsufficient(err error) bool { return isError(err, errBalanceInsufficient) }
func isBalanceMaxExceeded(err error) bool  { return isError(err, errBalanceMaxExceeded) }
func isClosedStream(err error) bool {
	return isError(err, mux.ErrClosedStream) || isError(err, net.ErrClosed)
}
func isInsufficientFunds(err error) bool  { return isError(err, ErrInsufficientFunds) }
func isMaxRevisionReached(err error) bool { return isError(err, errMaxRevisionReached) }
func isPriceTableExpired(err error) bool  { return isError(err, errPriceTableExpired) }
func isPriceTableNotFound(err error) bool { return isError(err, errPriceTableNotFound) }
func isSectorNotFound(err error) bool {
	return isError(err, errSectorNotFound) || isError(err, errSectorNotFoundOld)
}
func isWithdrawalsInactive(err error) bool { return isError(err, errWithdrawalsInactive) }

func isError(err error, target error) bool {
	if err == nil {
		return err == target
	}
	// compare error first
	if errors.Is(err, target) {
		return true
	}
	// then compare the string in case the error was returned by a host
	return strings.Contains(strings.ToLower(err.Error()), strings.ToLower(target.Error()))
}

// transportV3 is a reference-counted wrapper for rhpv3.Transport.
type transportV3 struct {
	refCount uint64 // locked by pool

	mu         sync.Mutex
	hostKey    types.PublicKey
	siamuxAddr string
	t          *rhpv3.Transport
}

type streamV3 struct {
	cancel context.CancelFunc
	*rhpv3.Stream
}

// Close closes the stream and cancels the goroutine launched by DialStream.
func (s *streamV3) Close() error {
	s.cancel()
	return s.Stream.Close()
}

// DialStream dials a new stream on the transport.
func (t *transportV3) DialStream(ctx context.Context) (*streamV3, error) {
	t.mu.Lock()
	if t.t == nil {
		start := time.Now()
		newTransport, err := dialTransport(ctx, t.siamuxAddr, t.hostKey)
		if err != nil {
			t.mu.Unlock()
			return nil, fmt.Errorf("DialStream: could not dial transport: %w (%v)", err, time.Since(start))
		}
		t.t = newTransport
	}
	transport := t.t
	t.mu.Unlock()

	// Close the stream when the context is closed to unblock any reads or
	// writes.
	stream := transport.DialStream()

	// Apply a sane timeout to the stream.
	if err := stream.SetDeadline(time.Now().Add(5 * time.Minute)); err != nil {
		_ = stream.Close()
		return nil, err
	}

	// Make sure the stream is closed when the context is closed.
	doneCtx, doneFn := context.WithCancel(ctx)
	go func() {
		select {
		case <-doneCtx.Done():
		case <-ctx.Done():
			_ = stream.Close()
		}
	}()
	return &streamV3{
		Stream: stream,
		cancel: doneFn,
	}, nil
}

// transportPoolV3 is a pool of rhpv3.Transports which allows for reusing them.
type transportPoolV3 struct {
	mu   sync.Mutex
	pool map[string]*transportV3
}

func newTransportPoolV3(w *worker) *transportPoolV3 {
	return &transportPoolV3{
		pool: make(map[string]*transportV3),
	}
}

func dialTransport(ctx context.Context, siamuxAddr string, hostKey types.PublicKey) (*rhpv3.Transport, error) {
	// Dial host.
	conn, err := dial(ctx, siamuxAddr)
	if err != nil {
		return nil, err
	}

	// Upgrade to rhpv3.Transport.
	var t *rhpv3.Transport
	done := make(chan struct{})
	go func() {
		t, err = rhpv3.NewRenterTransport(conn, hostKey)
		close(done)
	}()
	select {
	case <-ctx.Done():
		conn.Close()
		<-done
		return nil, ctx.Err()
	case <-done:
		return t, err
	}
}

func (p *transportPoolV3) withTransportV3(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, fn func(context.Context, *transportV3) error) (err error) {
	var mr ephemeralMetricsRecorder
	defer func() {
		// TODO: record metrics
	}()
	ctx = metrics.WithRecorder(ctx, &mr)

	// Create or fetch transport.
	p.mu.Lock()
	t, found := p.pool[siamuxAddr]
	if !found {
		t = &transportV3{
			hostKey:    hostKey,
			siamuxAddr: siamuxAddr,
		}
		p.pool[siamuxAddr] = t
	}
	t.refCount++
	p.mu.Unlock()

	// Execute function.
	err = fn(ctx, t)

	// Decrement refcounter again and clean up pool.
	p.mu.Lock()
	t.refCount--
	if t.refCount == 0 {
		// Cleanup
		if t.t != nil {
			_ = t.t.Close()
			t.t = nil
		}
		delete(p.pool, siamuxAddr)
	}
	p.mu.Unlock()
	return err
}

// FetchRevision tries to fetch a contract revision from the host. We pass in
// the blockHeight instead of using the blockHeight from the pricetable since we
// might not have a price table.
func (h *host) FetchRevision(ctx context.Context, fetchTimeout time.Duration, blockHeight uint64) (types.FileContractRevision, error) {
	timeoutCtx := func() (context.Context, context.CancelFunc) {
		if fetchTimeout > 0 {
			return context.WithTimeout(ctx, fetchTimeout)
		}
		return ctx, func() {}
	}
	// Try to fetch the revision with an account first.
	ctx, cancel := timeoutCtx()
	defer cancel()
	rev, err := h.fetchRevisionWithAccount(ctx, h.HostKey(), h.siamuxAddr, blockHeight, h.fcid)
	if err != nil && !(isBalanceInsufficient(err) || isWithdrawalsInactive(err) || isClosedStream(err)) { // TODO: checking for a closed stream here can be removed once the withdrawal timeout on the host side is removed
		return types.FileContractRevision{}, fmt.Errorf("unable to fetch revision with account: %v", err)
	} else if err == nil {
		return rev, nil
	}

	// Fall back to using the contract to pay for the revision.
	ctx, cancel = timeoutCtx()
	defer cancel()
	rev, err = h.fetchRevisionWithContract(ctx, h.HostKey(), h.siamuxAddr, h.fcid)
	if err != nil && !isInsufficientFunds(err) {
		return types.FileContractRevision{}, fmt.Errorf("unable to fetch revision with contract: %v", err)
	} else if err == nil {
		return rev, nil
	}

	// If we don't have enough money in the contract, try again without paying.
	ctx, cancel = timeoutCtx()
	defer cancel()
	rev, err = h.fetchRevisionNoPayment(ctx, h.HostKey(), h.siamuxAddr, h.fcid)
	if err != nil {
		return types.FileContractRevision{}, fmt.Errorf("unable to fetch revision without payment: %v", err)
	}
	return rev, nil
}

func (h *host) fetchRevisionWithAccount(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, bh uint64, contractID types.FileContractID) (rev types.FileContractRevision, err error) {
	err = h.acc.WithWithdrawal(ctx, func() (types.Currency, error) {
		var cost types.Currency
		return cost, h.transportPool.withTransportV3(ctx, hostKey, siamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
			rev, err = RPCLatestRevision(ctx, t, contractID, func(rev *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error) {
				// Fetch pt.
				pt, err := h.priceTable(ctx, nil)
				if err != nil {
					return rhpv3.HostPriceTable{}, nil, fmt.Errorf("failed to fetch pricetable, err: %w", err)
				}
				cost = pt.LatestRevisionCost.Add(pt.UpdatePriceTableCost) // add cost of fetching the pricetable since we might need a new one and it's better to stay pessimistic
				payment := rhpv3.PayByEphemeralAccount(h.acc.id, cost, bh+defaultWithdrawalExpiryBlocks, h.accountKey)
				return pt, &payment, nil
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
// a contract to pay for it.
func (h *host) fetchRevisionWithContract(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, contractID types.FileContractID) (rev types.FileContractRevision, err error) {
	err = h.transportPool.withTransportV3(ctx, hostKey, siamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
		rev, err = RPCLatestRevision(ctx, t, contractID, func(rev *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error) {
			// Fetch pt.
			pt, err := h.priceTable(ctx, rev)
			if err != nil {
				return rhpv3.HostPriceTable{}, nil, fmt.Errorf("failed to fetch pricetable, err: %v", err)
			}
			// Pay for the revision.
			payment, err := payByContract(rev, pt.LatestRevisionCost, h.acc.id, h.renterKey)
			if err != nil {
				return rhpv3.HostPriceTable{}, nil, err
			}
			return pt, &payment, nil
		})
		return err
	})
	return rev, err
}

func (h *host) fetchRevisionNoPayment(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, contractID types.FileContractID) (rev types.FileContractRevision, err error) {
	err = h.transportPool.withTransportV3(ctx, hostKey, siamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
		_, err = RPCLatestRevision(ctx, t, contractID, func(r *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error) {
			rev = *r
			return rhpv3.HostPriceTable{}, nil, nil
		})
		return err
	})
	return rev, err
}

func (h *host) FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error {
	// fetch pricetable
	pt, err := h.priceTable(ctx, rev)
	if err != nil {
		return err
	}

	// calculate the amount to deposit
	curr, err := h.acc.Balance(ctx)
	if err != nil {
		return err
	}
	if curr.Cmp(balance) >= 0 {
		return nil
	}
	amount := balance.Sub(curr)

	// cap the amount by the amount of money left in the contract
	renterFunds := rev.ValidRenterPayout()
	possibleFundCost := pt.FundAccountCost.Add(pt.UpdatePriceTableCost)
	if renterFunds.Cmp(possibleFundCost) <= 0 {
		return fmt.Errorf("insufficient funds to fund account: %v <= %v", renterFunds, possibleFundCost)
	} else if maxAmount := renterFunds.Sub(possibleFundCost); maxAmount.Cmp(amount) < 0 {
		amount = maxAmount
	}

	return h.acc.WithDeposit(ctx, func() (types.Currency, error) {
		return amount, h.transportPool.withTransportV3(ctx, h.HostKey(), h.siamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
			cost := amount.Add(pt.FundAccountCost)
			payment, err := payByContract(rev, cost, rhpv3.Account{}, h.renterKey) // no account needed for funding
			if err != nil {
				return err
			}
			if err := RPCFundAccount(ctx, t, &payment, h.acc.id, pt.UID); err != nil {
				return fmt.Errorf("failed to fund account with %v;%w", amount, err)
			}
			h.contractSpendingRecorder.Record(rev.ParentID, rev.RevisionNumber, rev.Filesize, api.ContractSpending{FundAccount: cost})
			return nil
		})
	})
}

func (h *host) SyncAccount(ctx context.Context, rev *types.FileContractRevision) error {
	// fetch pricetable
	pt, err := h.priceTable(ctx, rev)
	if err != nil {
		return err
	}

	return h.acc.WithSync(ctx, func() (types.Currency, error) {
		var balance types.Currency
		err := h.transportPool.withTransportV3(ctx, h.HostKey(), h.siamuxAddr, func(ctx context.Context, t *transportV3) error {
			payment, err := payByContract(rev, pt.AccountBalanceCost, h.acc.id, h.renterKey)
			if err != nil {
				return err
			}
			balance, err = RPCAccountBalance(ctx, t, &payment, h.acc.id, pt.UID)
			return err
		})
		return balance, err
	})
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

	host struct {
		acc                      *account
		bus                      Bus
		contractSpendingRecorder *contractSpendingRecorder
		fcid                     types.FileContractID
		logger                   *zap.SugaredLogger
		mr                       *ephemeralMetricsRecorder
		siamuxAddr               string
		renterKey                types.PrivateKey
		accountKey               types.PrivateKey
		transportPool            *transportPoolV3
		priceTables              *priceTables
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

func (w *worker) initTransportPool() {
	if w.transportPoolV3 != nil {
		panic("transport pool already initialized") // developer error
	}
	w.transportPoolV3 = newTransportPoolV3(w)
}

// ForHost returns an account to use for a given host. If the account
// doesn't exist, a new one is created.
func (a *accounts) ForHost(hk types.PublicKey) *account {
	accountID := rhpv3.Account(a.deriveAccountKey(hk).PublicKey())
	return &account{
		bus:  a.store,
		id:   accountID,
		key:  a.key,
		host: hk,
	}
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
// amtFn. The amount is still withdrawn if amtFn returns an error since some
// costs are non-refundable.
func (a *account) WithWithdrawal(ctx context.Context, amtFn func() (types.Currency, error)) error {
	account, lockID, err := a.bus.LockAccount(ctx, a.id, a.host, false, accountLockingDuration)
	if err != nil {
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		a.bus.UnlockAccount(unlockCtx, a.id, lockID)
		cancel()
	}()

	// return early if the account needs to sync
	if account.RequiresSync {
		return fmt.Errorf("%w; account requires resync", errBalanceInsufficient)
	}

	// return early if our account is not funded
	if account.Balance.Cmp(big.NewInt(0)) <= 0 {
		return errBalanceInsufficient
	}

	// execute amtFn
	amt, err := amtFn()
	if isBalanceInsufficient(err) {
		// in case of an insufficient balance, we schedule a sync
		scheduleCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err2 := a.bus.ScheduleSync(scheduleCtx, a.id, a.host)
		if err2 != nil {
			err = fmt.Errorf("%w; failed to set requiresSync flag on bus, error: %v", err, err2)
		}
	}

	// if the amount is zero, we are done
	if amt.IsZero() {
		return err
	}

	// if an amount was returned, we withdraw it.
	addCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errAdd := a.bus.AddBalance(addCtx, a.id, a.host, new(big.Int).Neg(amt.Big()))
	if errAdd != nil {
		err = fmt.Errorf("%w; failed to add balance to account, error: %v", err, errAdd)
	}
	return err
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

func (r *host) Contract() types.FileContractID {
	return r.fcid
}

func (r *host) HostKey() types.PublicKey {
	return r.acc.host
}

// priceTable fetches a price table from the host. If a revision is provided, it
// will be used to pay for the price table. The returned price table is
// guaranteed to be safe to use.
func (h *host) priceTable(ctx context.Context, rev *types.FileContractRevision) (rhpv3.HostPriceTable, error) {
	pt, err := h.priceTables.fetch(ctx, h.HostKey(), rev)
	if err != nil {
		return rhpv3.HostPriceTable{}, err
	}
	gc, err := GougingCheckerFromContext(ctx)
	if err != nil {
		return rhpv3.HostPriceTable{}, err
	}
	if breakdown := gc.Check(nil, &pt.HostPriceTable); breakdown.Gouging() {
		return rhpv3.HostPriceTable{}, fmt.Errorf("host price table gouging: %v", breakdown)
	}
	return pt.HostPriceTable, nil
}

func (h *host) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32) (err error) {
	pt, err := h.priceTable(ctx, nil)
	if err != nil {
		return err
	}
	// return errBalanceInsufficient if balance insufficient
	defer func() {
		if isBalanceInsufficient(err) {
			err = fmt.Errorf("%w %v, err: %v", errBalanceInsufficient, h.HostKey(), err)
		}
	}()

	return h.acc.WithWithdrawal(ctx, func() (amount types.Currency, err error) {
		err = h.transportPool.withTransportV3(ctx, h.HostKey(), h.siamuxAddr, func(ctx context.Context, t *transportV3) error {
			cost, err := readSectorCost(pt, uint64(length))
			if err != nil {
				return err
			}

			var refund types.Currency
			payment := rhpv3.PayByEphemeralAccount(h.acc.id, cost, pt.HostBlockHeight+defaultWithdrawalExpiryBlocks, h.accountKey)
			cost, refund, err = RPCReadSector(ctx, t, w, pt, &payment, offset, length, root)
			amount = cost.Sub(refund)
			return err
		})
		return
	})
}

// UploadSector uploads a sector to the host.
func (h *host) UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) (root types.Hash256, err error) {
	// fetch price table
	pt, err := h.priceTable(ctx, nil)
	if err != nil {
		return types.Hash256{}, err
	}

	// prepare payment
	//
	// TODO: change to account payments once we have the means to check for an
	// insufficient balance error
	expectedCost, _, _, err := uploadSectorCost(pt, rev.WindowEnd)
	if err != nil {
		return types.Hash256{}, err
	}
	if rev.RevisionNumber == math.MaxUint64 {
		return types.Hash256{}, fmt.Errorf("revision number has reached max, fcid %v", rev.ParentID)
	}
	payment, ok := rhpv3.PayByContract(&rev, expectedCost, h.acc.id, h.renterKey)
	if !ok {
		return types.Hash256{}, errors.New("failed to create payment")
	}

	var cost types.Currency
	err = h.transportPool.withTransportV3(ctx, h.HostKey(), h.siamuxAddr, func(ctx context.Context, t *transportV3) error {
		root, cost, err = RPCAppendSector(ctx, t, h.renterKey, pt, &rev, &payment, sector)
		return err
	})
	if err != nil {
		return types.Hash256{}, err
	}

	// record spending
	h.contractSpendingRecorder.Record(rev.ParentID, rev.RevisionNumber, rev.Filesize, api.ContractSpending{Uploads: cost})
	return root, nil
}

// padBandwitdh pads the bandwidth to the next multiple of 1460 bytes.  1460
// bytes is the maximum size of a TCP packet when using IPv4.
// TODO: once hostd becomes the only host implementation we can simplify this.
func padBandwidth(pt rhpv3.HostPriceTable, rc rhpv3.ResourceCost) rhpv3.ResourceCost {
	padCost := func(cost, paddingSize types.Currency) types.Currency {
		if paddingSize.IsZero() {
			return cost // might happen if bandwidth is free
		}
		return cost.Add(paddingSize).Sub(types.NewCurrency64(1)).Div(paddingSize).Mul(paddingSize)
	}
	minPacketSize := uint64(1460)
	minIngress := pt.UploadBandwidthCost.Mul64(minPacketSize)
	minEgress := pt.DownloadBandwidthCost.Mul64(3*minPacketSize + responseLeeway)
	rc.Ingress = padCost(rc.Ingress, minIngress)
	rc.Egress = padCost(rc.Egress, minEgress)
	return rc
}

// readSectorCost returns an overestimate for the cost of reading a sector from a host
func readSectorCost(pt rhpv3.HostPriceTable, length uint64) (types.Currency, error) {
	rc := pt.BaseCost()
	rc = rc.Add(pt.ReadSectorCost(length))
	rc = padBandwidth(pt, rc)
	cost, _ := rc.Total()

	// overestimate the cost by 10%
	cost, overflow := cost.Mul64WithOverflow(11)
	if overflow {
		return types.ZeroCurrency, errors.New("overflow occurred while adding leeway to read sector cost")
	}
	return cost.Div64(10), nil
}

// uploadSectorCost returns an overestimate for the cost of uploading a sector
// to a host
func uploadSectorCost(pt rhpv3.HostPriceTable, windowEnd uint64) (cost, collateral, storage types.Currency, _ error) {
	rc := pt.BaseCost()
	rc = rc.Add(pt.AppendSectorCost(windowEnd - pt.HostBlockHeight))
	rc = padBandwidth(pt, rc)
	cost, collateral = rc.Total()

	// overestimate the cost by 10%
	cost, overflow := cost.Mul64WithOverflow(11)
	if overflow {
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("overflow occurred while adding leeway to read sector cost")
	}
	return cost.Div64(10), collateral, rc.Storage, nil
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
func (pts *priceTables) fetch(ctx context.Context, hk types.PublicKey, rev *types.FileContractRevision) (hostdb.HostPriceTable, error) {
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

	return pt.fetch(ctx, rev)
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

func (p *priceTable) fetch(ctx context.Context, rev *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error) {
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
	return w.fetchPriceTable(ctx, hk, host.Settings.SiamuxAddr(), rev)
}

// preparePriceTableContractPayment prepare a payment function to pay for a
// price table from the given host using the provided revision.
//
// NOTE: This way of paying for a price table should only be used if payment by
// EA is not possible or if we already need a contract revision anyway. e.g.
// funding an EA.
func (h *host) preparePriceTableContractPayment(rev *types.FileContractRevision) PriceTablePaymentFunc {
	return func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
		// TODO: gouging check on price table

		refundAccount := rhpv3.Account(h.accountKey.PublicKey())
		payment, err := payByContract(rev, pt.UpdatePriceTableCost, refundAccount, h.renterKey)
		if err != nil {
			return nil, err
		}
		return &payment, nil
	}
}

// preparePriceTableAccountPayment prepare a payment function to pay for a price
// table from the given host using the provided revision.
//
// NOTE: This is the preferred way of paying for a price table since it is
// faster and doesn't require locking a contract.
func (h *host) preparePriceTableAccountPayment(bh uint64) PriceTablePaymentFunc {
	return func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
		// TODO: gouging check on price table

		account := rhpv3.Account(h.accountKey.PublicKey())
		payment := rhpv3.PayByEphemeralAccount(account, pt.UpdatePriceTableCost, bh+defaultWithdrawalExpiryBlocks, h.accountKey)
		return &payment, nil
	}
}

func processPayment(s *streamV3, payment rhpv3.PaymentMethod) error {
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
		if err := s.ReadResponse(&pr, defaultRPCResponseMaxSize); err != nil {
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
func (h *host) Renew(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, err error) {
	// Try to get a valid pricetable.
	ptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var pt *rhpv3.HostPriceTable
	hpt, err := h.priceTables.fetch(ptCtx, h.HostKey(), nil)
	if err == nil {
		pt = &hpt.HostPriceTable
	} else {
		h.logger.Debugf("unable to fetch price table for renew: %v", err)
	}

	var rev rhpv2.ContractRevision
	var txnSet []types.Transaction
	var renewErr error
	err = h.transportPool.withTransportV3(ctx, h.HostKey(), h.siamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
		_, err = RPCLatestRevision(ctx, t, h.fcid, func(revision *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error) {
			// Renew contract.
			rev, txnSet, renewErr = RPCRenew(ctx, rrr, h.bus, t, pt, *revision, h.renterKey, h.logger)
			return rhpv3.HostPriceTable{}, nil, nil
		})
		return err
	})
	if err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}
	return rev, txnSet, renewErr
}

func (h *host) FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error) {
	// fetchPT is a helper function that performs the RPC given a payment function
	fetchPT := func(paymentFn PriceTablePaymentFunc) (hpt hostdb.HostPriceTable, err error) {
		err = h.transportPool.withTransportV3(ctx, h.HostKey(), h.siamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
			defer recordPriceTableUpdate(ctx, h.siamuxAddr, h.HostKey(), &hpt, &err)()

			pt, err := RPCPriceTable(ctx, t, paymentFn)
			if err != nil {
				return err
			}
			hpt = hostdb.HostPriceTable{
				HostPriceTable: pt,
				Expiry:         time.Now().Add(pt.Validity),
			}
			return nil
		})
		return
	}

	// pay by contract if a revision is given
	if rev != nil {
		return fetchPT(h.preparePriceTableContractPayment(rev))
	}

	// pay by account
	cs, err := h.bus.ConsensusState(ctx)
	if err != nil {
		return hostdb.HostPriceTable{}, err
	}
	return fetchPT(h.preparePriceTableAccountPayment(cs.BlockHeight))
}

// RPCPriceTable calls the UpdatePriceTable RPC.
func RPCPriceTable(ctx context.Context, t *transportV3, paymentFunc PriceTablePaymentFunc) (pt rhpv3.HostPriceTable, err error) {
	defer wrapErr(&err, "PriceTable")
	start := time.Now()
	s, err := t.DialStream(ctx)
	if err != nil {
		return rhpv3.HostPriceTable{}, err
	}
	defer s.Close()

	const maxPriceTableSize = 16 * 1024
	var ptr rhpv3.RPCUpdatePriceTableResponse
	if err := s.WriteRequest(rhpv3.RPCUpdatePriceTableID, nil); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("couldn't send RPCUpdatePriceTableID: %w (%v)", err, time.Since(start))
	} else if err := s.ReadResponse(&ptr, maxPriceTableSize); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("couldn't read RPCUpdatePriceTableResponse: %w (%v)", err, time.Since(start))
	} else if err := json.Unmarshal(ptr.PriceTableJSON, &pt); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("couldn't unmarshal price table: %w (%v)", err, time.Since(start))
	} else if payment, err := paymentFunc(pt); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("couldn't create payment: %w (%v)", err, time.Since(start))
	} else if payment == nil {
		return pt, nil // intended not to pay
	} else if err := processPayment(s, payment); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("couldn't process payment: %w (%v)", err, time.Since(start))
	} else if err := s.ReadResponse(&rhpv3.RPCPriceTableResponse{}, 0); err != nil {
		return rhpv3.HostPriceTable{}, fmt.Errorf("couldn't read RPCPriceTableResponse: %w (%v)", err, time.Since(start))
	}
	return pt, nil
}

// RPCAccountBalance calls the AccountBalance RPC.
func RPCAccountBalance(ctx context.Context, t *transportV3, payment rhpv3.PaymentMethod, account rhpv3.Account, settingsID rhpv3.SettingsID) (bal types.Currency, err error) {
	defer wrapErr(&err, "AccountBalance")
	s, err := t.DialStream(ctx)
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
func RPCFundAccount(ctx context.Context, t *transportV3, payment rhpv3.PaymentMethod, account rhpv3.Account, settingsID rhpv3.SettingsID) (err error) {
	defer wrapErr(&err, "FundAccount")
	s, err := t.DialStream(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	req := rhpv3.RPCFundAccountRequest{
		Account: account,
	}
	var resp rhpv3.RPCFundAccountResponse
	if err := s.WriteRequest(rhpv3.RPCFundAccountID, &settingsID); err != nil {
		return err
	} else if err := s.WriteResponse(&req); err != nil {
		return err
	} else if err := processPayment(s, payment); err != nil {
		return err
	} else if err := s.ReadResponse(&resp, defaultRPCResponseMaxSize); err != nil {
		return err
	}
	return nil
}

// RPCLatestRevision calls the LatestRevision RPC. The paymentFunc allows for
// fetching a pricetable using the fetched revision to pay for it. If
// paymentFunc returns 'nil' as payment, the host is not paid.
func RPCLatestRevision(ctx context.Context, t *transportV3, contractID types.FileContractID, paymentFunc func(rev *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error)) (_ types.FileContractRevision, err error) {
	defer wrapErr(&err, "LatestRevision")
	s, err := t.DialStream(ctx)
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
	} else if err := s.ReadResponse(&resp, defaultRPCResponseMaxSize); err != nil {
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
func RPCReadSector(ctx context.Context, t *transportV3, w io.Writer, pt rhpv3.HostPriceTable, payment rhpv3.PaymentMethod, offset, length uint32, merkleRoot types.Hash256) (cost, refund types.Currency, err error) {
	defer wrapErr(&err, "ReadSector")
	s, err := t.DialStream(ctx)
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, err
	}
	defer s.Close()

	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	e.WriteUint64(uint64(length))
	e.WriteUint64(uint64(offset))
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
func RPCReadRegistry(ctx context.Context, t *transportV3, payment rhpv3.PaymentMethod, key rhpv3.RegistryKey) (rv rhpv3.RegistryValue, err error) {
	defer wrapErr(&err, "ReadRegistry")
	s, err := t.DialStream(ctx)
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

func RPCAppendSector(ctx context.Context, t *transportV3, renterKey types.PrivateKey, pt rhpv3.HostPriceTable, rev *types.FileContractRevision, payment rhpv3.PaymentMethod, sector *[rhpv2.SectorSize]byte) (sectorRoot types.Hash256, cost types.Currency, err error) {
	defer wrapErr(&err, "AppendSector")

	// sanity check revision first
	if rev.RevisionNumber == math.MaxUint64 {
		return types.Hash256{}, types.ZeroCurrency, errMaxRevisionReached
	}

	s, err := t.DialStream(ctx)
	if err != nil {
		return types.Hash256{}, types.ZeroCurrency, err
	}
	defer s.Close()

	req := rhpv3.RPCExecuteProgramRequest{
		FileContractID: rev.ParentID,
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
	} else if err = s.ReadResponse(&executeResp, defaultRPCResponseMaxSize); err != nil {
		return
	}

	// compute expected collateral and refund
	expectedCost, expectedCollateral, expectedRefund, err := uploadSectorCost(pt, rev.WindowEnd)
	if err != nil {
		return types.Hash256{}, types.ZeroCurrency, err
	}

	// apply leeways.
	// TODO: remove once most hosts use hostd. Then we can check for exact values.
	expectedCollateral = expectedCollateral.Mul64(9).Div64(10)
	expectedCost = expectedCost.Mul64(11).Div64(10)
	expectedRefund = expectedRefund.Mul64(9).Div64(10)

	// check if the cost, collateral and refund match our expectation.
	if executeResp.TotalCost.Cmp(expectedCost) > 0 {
		return types.Hash256{}, types.ZeroCurrency, fmt.Errorf("cost exceeds expectation: %v > %v", executeResp.TotalCost.String(), expectedCost.String())
	}
	if executeResp.FailureRefund.Cmp(expectedRefund) < 0 {
		return types.Hash256{}, types.ZeroCurrency, fmt.Errorf("insufficient refund: %v < %v", executeResp.FailureRefund.String(), expectedRefund.String())
	}
	if executeResp.AdditionalCollateral.Cmp(expectedCollateral) < 0 {
		return types.Hash256{}, types.ZeroCurrency, fmt.Errorf("insufficient collateral: %v < %v", executeResp.AdditionalCollateral.String(), expectedCollateral.String())
	}

	// set the cost and refund
	cost = executeResp.TotalCost
	defer func() {
		if err != nil {
			cost = types.ZeroCurrency
			if executeResp.FailureRefund.Cmp(cost) < 0 {
				cost = cost.Sub(executeResp.FailureRefund)
			}
		}
	}()

	// check response error
	if err = executeResp.Error; err != nil {
		return
	}
	cost = executeResp.TotalCost

	// include the refund in the collateral
	collateral := executeResp.AdditionalCollateral.Add(executeResp.FailureRefund)

	// check proof
	sectorRoot = rhpv2.SectorRoot(sector)
	if rev.Filesize == 0 {
		// For the first upload to a contract we don't get a proof. So we just
		// assert that the new contract root matches the root of the sector.
		if rev.Filesize == 0 && executeResp.NewMerkleRoot != sectorRoot {
			return types.Hash256{}, types.ZeroCurrency, fmt.Errorf("merkle root doesn't match the sector root upon first upload to contract: %v != %v", executeResp.NewMerkleRoot, sectorRoot)
		}
	} else {
		// Otherwise we make sure the proof was transmitted and verify it.
		actions := []rhpv2.RPCWriteAction{{Type: rhpv2.RPCWriteActionAppend}} // TODO: change once rhpv3 support is available
		if !rhpv2.VerifyDiffProof(actions, rev.Filesize/rhpv2.SectorSize, executeResp.Proof, []types.Hash256{}, rev.FileMerkleRoot, executeResp.NewMerkleRoot, []types.Hash256{sectorRoot}) {
			return types.Hash256{}, types.ZeroCurrency, errors.New("proof verification failed")
		}
	}

	// finalize the program with a new revision.
	newRevision := *rev
	newValid, newMissed, err := updateRevisionOutputs(&newRevision, types.ZeroCurrency, collateral)
	if err != nil {
		return types.Hash256{}, types.ZeroCurrency, err
	}
	newRevision.Filesize += rhpv2.SectorSize
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

	// read one more time to receive a potential error in case finalising the
	// contract fails after receiving the RPCFinalizeProgramResponse. This also
	// guarantees that the program is finalised before we return.
	// TODO: remove once most hosts use hostd.
	errFinalise := s.ReadResponse(&finalizeResp, 64)
	if errFinalise != nil &&
		!errors.Is(errFinalise, io.EOF) &&
		!errors.Is(errFinalise, mux.ErrClosedConn) &&
		!errors.Is(errFinalise, mux.ErrClosedStream) &&
		!errors.Is(errFinalise, mux.ErrPeerClosedStream) &&
		!errors.Is(errFinalise, mux.ErrPeerClosedConn) {
		err = errFinalise
		return
	}

	*rev = newRevision
	return
}

func RPCRenew(ctx context.Context, rrr api.RHPRenewRequest, bus Bus, t *transportV3, pt *rhpv3.HostPriceTable, rev types.FileContractRevision, renterKey types.PrivateKey, l *zap.SugaredLogger) (_ rhpv2.ContractRevision, _ []types.Transaction, err error) {
	defer wrapErr(&err, "RPCRenew")
	s, err := t.DialStream(ctx)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to dial stream: %w", err)
	}
	defer s.Close()

	// Send the ptUID.
	var ptUID rhpv3.SettingsID
	if pt != nil {
		ptUID = pt.UID
	}
	if err = s.WriteRequest(rhpv3.RPCRenewContractID, &ptUID); err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to send ptUID: %w", err)
	}

	// If we didn't have a valid pricetable, read the temporary one from the
	// host.
	if ptUID == (rhpv3.SettingsID{}) {
		var ptResp rhpv3.RPCUpdatePriceTableResponse
		if err = s.ReadResponse(&ptResp, defaultRPCResponseMaxSize); err != nil {
			return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to read RPCUpdatePriceTableResponse: %w", err)
		}
		pt = new(rhpv3.HostPriceTable)
		if err = json.Unmarshal(ptResp.PriceTableJSON, pt); err != nil {
			return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to unmarshal price table: %w", err)
		}
	}

	// Perform gouging checks.
	gc, err := GougingCheckerFromContext(ctx)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to get gouging checker: %w", err)
	}
	if breakdown := gc.Check(nil, pt); breakdown.Gouging() {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("host gouging during renew: %v", breakdown.Reasons())
	}

	// Prepare the signed transaction that contains the final revision as well
	// as the new contract
	wprr, err := bus.WalletPrepareRenew(ctx, rev, rrr.HostAddress, rrr.RenterAddress, renterKey, rrr.RenterFunds, rrr.NewCollateral, *pt, rrr.EndHeight, rrr.WindowSize)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to prepare renew: %w", err)
	}

	// Starting from here, we need to make sure to release the txn on error.
	defer discardTxnOnErr(ctx, bus, l, wprr.TransactionSet[len(wprr.TransactionSet)-1], "RPCRenew", &err)

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
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to send RPCRenewContractRequest: %w", err)
	}

	// Incorporate the host's additions.
	var hostAdditions rhpv3.RPCRenewContractHostAdditions
	if err = s.ReadResponse(&hostAdditions, defaultRPCResponseMaxSize); err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to read RPCRenewContractHostAdditions: %w", err)
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
	if err := bus.WalletSign(ctx, &txn, wprr.ToSign, cf); err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to sign transaction: %w", err)
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
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to send RPCRenewSignatures: %w", err)
	}

	// Receive the host's signatures.
	var hostSigs rhpv3.RPCRenewSignatures
	if err = s.ReadResponse(&hostSigs, defaultRPCResponseMaxSize); err != nil {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed to read RPCRenewSignatures: %w", err)
	}
	txn.Signatures = append(txn.Signatures, hostSigs.TransactionSignatures...)

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
func RPCUpdateRegistry(ctx context.Context, t *transportV3, payment rhpv3.PaymentMethod, key rhpv3.RegistryKey, value rhpv3.RegistryValue) (err error) {
	defer wrapErr(&err, "UpdateRegistry")
	s, err := t.DialStream(ctx)
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

func payByContract(rev *types.FileContractRevision, amount types.Currency, refundAcct rhpv3.Account, sk types.PrivateKey) (rhpv3.PayByContractRequest, error) {
	if rev.RevisionNumber == math.MaxUint64 {
		return rhpv3.PayByContractRequest{}, errMaxRevisionReached
	}
	payment, ok := rhpv3.PayByContract(rev, amount, refundAcct, sk)
	if !ok {
		return rhpv3.PayByContractRequest{}, ErrInsufficientFunds
	}
	return payment, nil
}
