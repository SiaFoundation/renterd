package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/mux/v1"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/siad/crypto"
	"go.uber.org/zap"
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
	defaultWithdrawalExpiryBlocks = 12

	// maxPriceTableSize defines the maximum size of a price table
	maxPriceTableSize = 16 * 1024

	// responseLeeway is the amount of leeway given to the maxLen when we read
	// the response in the ReadSector RPC
	responseLeeway = 1 << 12 // 4 KiB
)

var (
	// errHost is used to wrap rpc errors returned by the host.
	errHost = errors.New("host responded with error")

	// errTransport is used to wrap rpc errors caused by the transport.
	errTransport = errors.New("transport error")

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

	// errWithdrawalExpired is returned by the host when the withdrawal request
	// has an expiry block height that is in the past.
	errWithdrawalExpired = errors.New("withdrawal request expired")
)

// IsErrHost indicates whether an error was returned by a host as part of an RPC.
func IsErrHost(err error) bool {
	return utils.IsErr(err, errHost)
}

func isBalanceInsufficient(err error) bool { return utils.IsErr(err, errBalanceInsufficient) }
func isBalanceMaxExceeded(err error) bool  { return utils.IsErr(err, errBalanceMaxExceeded) }
func isClosedStream(err error) bool {
	return utils.IsErr(err, mux.ErrClosedStream) || utils.IsErr(err, net.ErrClosed)
}
func isInsufficientFunds(err error) bool  { return utils.IsErr(err, ErrInsufficientFunds) }
func isPriceTableExpired(err error) bool  { return utils.IsErr(err, errPriceTableExpired) }
func isPriceTableGouging(err error) bool  { return utils.IsErr(err, errPriceTableGouging) }
func isPriceTableNotFound(err error) bool { return utils.IsErr(err, errPriceTableNotFound) }
func isSectorNotFound(err error) bool {
	return utils.IsErr(err, errSectorNotFound) || utils.IsErr(err, errSectorNotFoundOld)
}
func isWithdrawalsInactive(err error) bool { return utils.IsErr(err, errWithdrawalsInactive) }
func isWithdrawalExpired(err error) bool   { return utils.IsErr(err, errWithdrawalExpired) }

// wrapRPCErr extracts the innermost error, wraps it in either a errHost or
// errTransport and finally wraps it using the provided fnName.
func wrapRPCErr(err *error, fnName string) {
	if *err == nil {
		return
	}
	innerErr := *err
	for errors.Unwrap(innerErr) != nil {
		innerErr = errors.Unwrap(innerErr)
	}
	if errors.As(*err, new(*rhpv3.RPCError)) {
		*err = fmt.Errorf("%w: '%w'", errHost, innerErr)
	} else {
		*err = fmt.Errorf("%w: '%w'", errTransport, innerErr)
	}
	*err = fmt.Errorf("%s: %w", fnName, *err)
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

func (s *streamV3) ReadResponse(resp rhpv3.ProtocolObject, maxLen uint64) (err error) {
	defer wrapRPCErr(&err, "ReadResponse")
	return s.Stream.ReadResponse(resp, maxLen)
}

func (s *streamV3) WriteResponse(resp rhpv3.ProtocolObject) (err error) {
	defer wrapRPCErr(&err, "WriteResponse")
	return s.Stream.WriteResponse(resp)
}

func (s *streamV3) ReadRequest(req rhpv3.ProtocolObject, maxLen uint64) (err error) {
	defer wrapRPCErr(&err, "ReadRequest")
	return s.Stream.ReadRequest(req, maxLen)
}

func (s *streamV3) WriteRequest(rpcID types.Specifier, req rhpv3.ProtocolObject) (err error) {
	defer wrapRPCErr(&err, "WriteRequest")
	return s.Stream.WriteRequest(rpcID, req)
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

func newTransportPoolV3() *transportPoolV3 {
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

// FetchRevision tries to fetch a contract revision from the host.
func (h *host) FetchRevision(ctx context.Context, fetchTimeout time.Duration) (types.FileContractRevision, error) {
	timeoutCtx := func() (context.Context, context.CancelFunc) {
		if fetchTimeout > 0 {
			return context.WithTimeout(ctx, fetchTimeout)
		}
		return ctx, func() {}
	}

	// Try to fetch the revision with an account first.
	ctx, cancel := timeoutCtx()
	defer cancel()
	rev, err := h.fetchRevisionWithAccount(ctx, h.hk, h.siamuxAddr, h.fcid)
	if err != nil && !(isBalanceInsufficient(err) || isWithdrawalsInactive(err) || isWithdrawalExpired(err) || isClosedStream(err)) { // TODO: checking for a closed stream here can be removed once the withdrawal timeout on the host side is removed
		return types.FileContractRevision{}, fmt.Errorf("unable to fetch revision with account: %v", err)
	} else if err == nil {
		return rev, nil
	}

	// Fall back to using the contract to pay for the revision.
	ctx, cancel = timeoutCtx()
	defer cancel()
	rev, err = h.fetchRevisionWithContract(ctx, h.hk, h.siamuxAddr, h.fcid)
	if err != nil && !isInsufficientFunds(err) {
		return types.FileContractRevision{}, fmt.Errorf("unable to fetch revision with contract: %v", err)
	} else if err == nil {
		return rev, nil
	}

	// If we don't have enough money in the contract, try again without paying.
	ctx, cancel = timeoutCtx()
	defer cancel()
	rev, err = h.fetchRevisionNoPayment(ctx, h.hk, h.siamuxAddr, h.fcid)
	if err != nil {
		return types.FileContractRevision{}, fmt.Errorf("unable to fetch revision without payment: %v", err)
	}
	return rev, nil
}

func (h *host) fetchRevisionWithAccount(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, fcid types.FileContractID) (rev types.FileContractRevision, err error) {
	err = h.acc.WithWithdrawal(ctx, func() (types.Currency, error) {
		var cost types.Currency
		return cost, h.transportPool.withTransportV3(ctx, hostKey, siamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
			rev, err = RPCLatestRevision(ctx, t, fcid, func(rev *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error) {
				pt, err := h.priceTable(ctx, nil)
				if err != nil {
					return rhpv3.HostPriceTable{}, nil, fmt.Errorf("failed to fetch pricetable, err: %w", err)
				}
				cost = pt.LatestRevisionCost.Add(pt.UpdatePriceTableCost) // add cost of fetching the pricetable since we might need a new one and it's better to stay pessimistic
				payment := rhpv3.PayByEphemeralAccount(h.acc.id, cost, pt.HostBlockHeight+defaultWithdrawalExpiryBlocks, h.accountKey)
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

func (w *worker) initAccounts(as AccountStore) {
	if w.accounts != nil {
		panic("accounts already initialized") // developer error
	}
	w.accounts = &accounts{
		as:  as,
		key: w.deriveSubKey("accountkey"),
	}
}

func (w *worker) initTransportPool() {
	if w.transportPoolV3 != nil {
		panic("transport pool already initialized") // developer error
	}
	w.transportPoolV3 = newTransportPoolV3()
}

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
			return fmt.Errorf("%w; account requires resync", errBalanceInsufficient)
		}

		// return early if our account is not funded
		if account.Balance.Cmp(big.NewInt(0)) <= 0 {
			return errBalanceInsufficient
		}

		// execute amtFn
		amt, err := amtFn()

		// in case of an insufficient balance, we schedule a sync
		if isBalanceInsufficient(err) {
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

// priceTable fetches a price table from the host. If a revision is provided, it
// will be used to pay for the price table. The returned price table is
// guaranteed to be safe to use.
func (h *host) priceTable(ctx context.Context, rev *types.FileContractRevision) (rhpv3.HostPriceTable, error) {
	pt, err := h.priceTables.fetch(ctx, h.hk, rev)
	if err != nil {
		return rhpv3.HostPriceTable{}, err
	}
	gc, err := GougingCheckerFromContext(ctx, false)
	if err != nil {
		return rhpv3.HostPriceTable{}, err
	}
	if breakdown := gc.Check(nil, &pt.HostPriceTable); breakdown.Gouging() {
		return rhpv3.HostPriceTable{}, fmt.Errorf("%w: %v", errPriceTableGouging, breakdown)
	}
	return pt.HostPriceTable, nil
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

// RPCPriceTable calls the UpdatePriceTable RPC.
func RPCPriceTable(ctx context.Context, t *transportV3, paymentFunc PriceTablePaymentFunc) (_ hostdb.HostPriceTable, err error) {
	defer wrapErr(&err, "PriceTable")

	s, err := t.DialStream(ctx)
	if err != nil {
		return hostdb.HostPriceTable{}, err
	}
	defer s.Close()

	var pt rhpv3.HostPriceTable
	var ptr rhpv3.RPCUpdatePriceTableResponse
	if err := s.WriteRequest(rhpv3.RPCUpdatePriceTableID, nil); err != nil {
		return hostdb.HostPriceTable{}, fmt.Errorf("couldn't send RPCUpdatePriceTableID: %w", err)
	} else if err := s.ReadResponse(&ptr, maxPriceTableSize); err != nil {
		return hostdb.HostPriceTable{}, fmt.Errorf("couldn't read RPCUpdatePriceTableResponse: %w", err)
	} else if err := json.Unmarshal(ptr.PriceTableJSON, &pt); err != nil {
		return hostdb.HostPriceTable{}, fmt.Errorf("couldn't unmarshal price table: %w", err)
	} else if payment, err := paymentFunc(pt); err != nil {
		return hostdb.HostPriceTable{}, fmt.Errorf("couldn't create payment: %w", err)
	} else if payment == nil {
		return hostdb.HostPriceTable{
			HostPriceTable: pt,
			Expiry:         time.Now(),
		}, nil // intended not to pay
	} else if err := processPayment(s, payment); err != nil {
		return hostdb.HostPriceTable{}, fmt.Errorf("couldn't process payment: %w", err)
	} else if err := s.ReadResponse(&rhpv3.RPCPriceTableResponse{}, 0); err != nil {
		return hostdb.HostPriceTable{}, fmt.Errorf("couldn't read RPCPriceTableResponse: %w", err)
	} else {
		return hostdb.HostPriceTable{
			HostPriceTable: pt,
			Expiry:         time.Now().Add(pt.Validity),
		}, nil
	}
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

func RPCAppendSector(ctx context.Context, t *transportV3, renterKey types.PrivateKey, pt rhpv3.HostPriceTable, rev *types.FileContractRevision, payment rhpv3.PaymentMethod, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte) (cost types.Currency, err error) {
	defer wrapErr(&err, "AppendSector")

	// sanity check revision first
	if rev.RevisionNumber == math.MaxUint64 {
		return types.ZeroCurrency, errMaxRevisionReached
	}

	s, err := t.DialStream(ctx)
	if err != nil {
		return types.ZeroCurrency, err
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
		return types.ZeroCurrency, err
	}

	// apply leeways.
	// TODO: remove once most hosts use hostd. Then we can check for exact values.
	expectedCollateral = expectedCollateral.Mul64(9).Div64(10)
	expectedCost = expectedCost.Mul64(11).Div64(10)
	expectedRefund = expectedRefund.Mul64(9).Div64(10)

	// check if the cost, collateral and refund match our expectation.
	if executeResp.TotalCost.Cmp(expectedCost) > 0 {
		return types.ZeroCurrency, fmt.Errorf("cost exceeds expectation: %v > %v", executeResp.TotalCost.String(), expectedCost.String())
	}
	if executeResp.FailureRefund.Cmp(expectedRefund) < 0 {
		return types.ZeroCurrency, fmt.Errorf("insufficient refund: %v < %v", executeResp.FailureRefund.String(), expectedRefund.String())
	}
	if executeResp.AdditionalCollateral.Cmp(expectedCollateral) < 0 {
		return types.ZeroCurrency, fmt.Errorf("insufficient collateral: %v < %v", executeResp.AdditionalCollateral.String(), expectedCollateral.String())
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
	if rev.Filesize == 0 {
		// For the first upload to a contract we don't get a proof. So we just
		// assert that the new contract root matches the root of the sector.
		if rev.Filesize == 0 && executeResp.NewMerkleRoot != sectorRoot {
			return types.ZeroCurrency, fmt.Errorf("merkle root doesn't match the sector root upon first upload to contract: %v != %v", executeResp.NewMerkleRoot, sectorRoot)
		}
	} else {
		// Otherwise we make sure the proof was transmitted and verify it.
		actions := []rhpv2.RPCWriteAction{{Type: rhpv2.RPCWriteActionAppend}} // TODO: change once rhpv3 support is available
		if !rhpv2.VerifyDiffProof(actions, rev.Filesize/rhpv2.SectorSize, executeResp.Proof, []types.Hash256{}, rev.FileMerkleRoot, executeResp.NewMerkleRoot, []types.Hash256{sectorRoot}) {
			return types.ZeroCurrency, errors.New("proof verification failed")
		}
	}

	// finalize the program with a new revision.
	newRevision := *rev
	newValid, newMissed, err := updateRevisionOutputs(&newRevision, types.ZeroCurrency, collateral)
	if err != nil {
		return types.ZeroCurrency, err
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

func RPCRenew(ctx context.Context, rrr api.RHPRenewRequest, bus Bus, t *transportV3, pt *rhpv3.HostPriceTable, rev types.FileContractRevision, renterKey types.PrivateKey, l *zap.SugaredLogger) (_ rhpv2.ContractRevision, _ []types.Transaction, _ types.Currency, err error) {
	defer wrapErr(&err, "RPCRenew")
	s, err := t.DialStream(ctx)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, types.ZeroCurrency, fmt.Errorf("failed to dial stream: %w", err)
	}
	defer s.Close()

	// Send the ptUID.
	var ptUID rhpv3.SettingsID
	if pt != nil {
		ptUID = pt.UID
	}
	if err = s.WriteRequest(rhpv3.RPCRenewContractID, &ptUID); err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, fmt.Errorf("failed to send ptUID: %w", err)
	}

	// If we didn't have a valid pricetable, read the temporary one from the
	// host.
	if ptUID == (rhpv3.SettingsID{}) {
		var ptResp rhpv3.RPCUpdatePriceTableResponse
		if err = s.ReadResponse(&ptResp, defaultRPCResponseMaxSize); err != nil {
			return rhpv2.ContractRevision{}, nil, types.Currency{}, fmt.Errorf("failed to read RPCUpdatePriceTableResponse: %w", err)
		}
		pt = new(rhpv3.HostPriceTable)
		if err = json.Unmarshal(ptResp.PriceTableJSON, pt); err != nil {
			return rhpv2.ContractRevision{}, nil, types.Currency{}, fmt.Errorf("failed to unmarshal price table: %w", err)
		}
	}

	// Perform gouging checks.
	gc, err := GougingCheckerFromContext(ctx, false)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, fmt.Errorf("failed to get gouging checker: %w", err)
	}
	if breakdown := gc.Check(nil, pt); breakdown.Gouging() {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, fmt.Errorf("host gouging during renew: %v", breakdown)
	}

	// Prepare the signed transaction that contains the final revision as well
	// as the new contract
	wprr, err := bus.WalletPrepareRenew(ctx, rev, rrr.HostAddress, rrr.RenterAddress, renterKey, rrr.RenterFunds, rrr.MinNewCollateral, *pt, rrr.EndHeight, rrr.WindowSize, rrr.ExpectedNewStorage)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, fmt.Errorf("failed to prepare renew: %w", err)
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
		return rhpv2.ContractRevision{}, nil, types.Currency{}, fmt.Errorf("failed to send RPCRenewContractRequest: %w", err)
	}

	// Incorporate the host's additions.
	var hostAdditions rhpv3.RPCRenewContractHostAdditions
	if err = s.ReadResponse(&hostAdditions, defaultRPCResponseMaxSize); err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, fmt.Errorf("failed to read RPCRenewContractHostAdditions: %w", err)
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
		return rhpv2.ContractRevision{}, nil, types.Currency{}, fmt.Errorf("failed to sign transaction: %w", err)
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
		return rhpv2.ContractRevision{}, nil, types.Currency{}, fmt.Errorf("failed to send RPCRenewSignatures: %w", err)
	}

	// Receive the host's signatures.
	var hostSigs rhpv3.RPCRenewSignatures
	if err = s.ReadResponse(&hostSigs, defaultRPCResponseMaxSize); err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, fmt.Errorf("failed to read RPCRenewSignatures: %w", err)
	}
	txn.Signatures = append(txn.Signatures, hostSigs.TransactionSignatures...)

	// Add the parents to get the full txnSet.
	txnSet = parents
	txnSet = append(txnSet, txn)

	return rhpv2.ContractRevision{
		Revision:   noOpRevision,
		Signatures: [2]types.TransactionSignature{renterNoOpRevisionSignature, hostSigs.RevisionSignature},
	}, txnSet, pt.ContractPrice, nil
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
