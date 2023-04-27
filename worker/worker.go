package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"mime"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gotd/contrib/http_range"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/metrics"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
	"lukechampine.com/frand"
)

const (
	lockingPriorityActiveContractRevision = 100 // highest
	lockingPriorityRenew                  = 80
	lockingPriorityPriceTable             = 60
	lockingPriorityFunding                = 40
	lockingPrioritySyncing                = 20
	lockingPriorityUpload                 = 1 // lowest

	queryStringParamContractSet = "contractset"
	queryStringParamMinShards   = "minshards"
	queryStringParamTotalShards = "totalshards"
)

// rangedResponseWriter is a wrapper around http.ResponseWriter. The difference
// to the standard http.ResponseWriter is that it allows for overriding the
// default status code that is sent upon the first call to Write with a custom
// one.
type rangedResponseWriter struct {
	rw                http.ResponseWriter
	defaultStatusCode int
	headerWritten     bool
}

func (rw *rangedResponseWriter) Write(p []byte) (int, error) {
	if !rw.headerWritten {
		contentType := rw.Header().Get("Content-Type")
		if contentType == "" {
			rw.Header().Set("Content-Type", http.DetectContentType(p))
		}
		rw.WriteHeader(rw.defaultStatusCode)
	}
	return rw.rw.Write(p)
}

func (rw *rangedResponseWriter) Header() http.Header {
	return rw.rw.Header()
}

func (rw *rangedResponseWriter) WriteHeader(statusCode int) {
	rw.headerWritten = true
	rw.rw.WriteHeader(statusCode)
}

func errToStr(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

type InteractionResult struct {
	Error string `json:"error,omitempty"`
}

type ephemeralMetricsRecorder struct {
	ms []metrics.Metric
	mu sync.Mutex
}

func (mr *ephemeralMetricsRecorder) RecordMetric(m metrics.Metric) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	mr.ms = append(mr.ms, m)
}

func (mr *ephemeralMetricsRecorder) interactions() []hostdb.Interaction {
	// TODO: merge/filter metrics?
	var his []hostdb.Interaction
	mr.mu.Lock()
	defer mr.mu.Unlock()
	for _, m := range mr.ms {
		if hi, ok := toHostInteraction(m); ok {
			his = append(his, hi)
		}
	}
	return his
}

// MetricHostDial contains metrics relating to a host dial.
type MetricHostDial struct {
	HostKey   types.PublicKey
	HostIP    string
	Timestamp time.Time
	Elapsed   time.Duration
	Err       error
}

// IsMetric implements metrics.Metric.
func (MetricHostDial) IsMetric() {}

// IsSuccess implements metrics.Metric.
func (m MetricHostDial) IsSuccess() bool { return m.Err == nil }

func dial(ctx context.Context, hostIP string, hostKey types.PublicKey) (net.Conn, error) {
	start := time.Now()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", hostIP)
	metrics.Record(ctx, MetricHostDial{
		HostKey:   hostKey,
		HostIP:    hostIP,
		Timestamp: start,
		Elapsed:   time.Since(start),
		Err:       err,
	})
	return conn, err
}

func toHostInteraction(m metrics.Metric) (hostdb.Interaction, bool) {
	transform := func(hk types.PublicKey, timestamp time.Time, typ string, err error, res interface{}) (hostdb.Interaction, bool) {
		b, _ := json.Marshal(InteractionResult{Error: errToStr(err)})
		hi := hostdb.Interaction{
			Host:      hk,
			Timestamp: timestamp,
			Type:      typ,
			Result:    json.RawMessage(b),
			Success:   err == nil,
		}
		return hi, true
	}

	switch m := m.(type) {
	case MetricHostDial:
		return transform(m.HostKey, m.Timestamp, "dial", m.Err, struct {
			HostIP    string        `json:"hostIP"`
			Timestamp time.Time     `json:"timestamp"`
			Elapsed   time.Duration `json:"elapsed"`
		}{m.HostIP, m.Timestamp, m.Elapsed})
	case MetricRPC:
		return transform(m.HostKey, m.Timestamp, "rhpv2 rpc", m.Err, struct {
			RPC        string         `json:"RPC"`
			Timestamp  time.Time      `json:"timestamp"`
			Elapsed    time.Duration  `json:"elapsed"`
			Contract   string         `json:"contract"`
			Uploaded   uint64         `json:"uploaded"`
			Downloaded uint64         `json:"downloaded"`
			Cost       types.Currency `json:"cost"`
			Collateral types.Currency `json:"collateral"`
		}{m.RPC.String(), m.Timestamp, m.Elapsed, m.Contract.String(), m.Uploaded, m.Downloaded, m.Cost, m.Collateral})
	default:
		return hostdb.Interaction{}, false
	}
}

type AccountStore interface {
	Accounts(ctx context.Context) ([]api.Account, error)
	AddBalance(ctx context.Context, id rhpv3.Account, hk types.PublicKey, amt *big.Int) error

	LockAccount(ctx context.Context, id rhpv3.Account, hostKey types.PublicKey, exclusive bool, duration time.Duration) (api.Account, uint64, error)
	UnlockAccount(ctx context.Context, id rhpv3.Account, lockID uint64) error

	ResetDrift(ctx context.Context, id rhpv3.Account) error
	SetBalance(ctx context.Context, id rhpv3.Account, hk types.PublicKey, amt *big.Int) error
	ScheduleSync(ctx context.Context, id rhpv3.Account, hk types.PublicKey) error
}

type contractReleaser interface {
	Release(context.Context) error
}

type contractLocker interface {
	AcquireContract(ctx context.Context, fcid types.FileContractID, priority int) (_ contractReleaser, err error)
}

type ContractLocker interface {
	AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error)
	KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error)
	ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error)
}

// A Bus is the source of truth within a renterd system.
type Bus interface {
	AccountStore
	ContractLocker

	BroadcastTransaction(ctx context.Context, txns []types.Transaction) error
	ConsensusState(ctx context.Context) (api.ConsensusState, error)

	ActiveContracts(ctx context.Context) ([]api.ContractMetadata, error)
	Contracts(ctx context.Context, set string) ([]api.ContractMetadata, error)
	RecordInteractions(ctx context.Context, interactions []hostdb.Interaction) error
	RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error

	Host(ctx context.Context, hostKey types.PublicKey) (hostdb.HostInfo, error)

	DownloadParams(ctx context.Context) (api.DownloadParams, error)
	GougingParams(ctx context.Context) (api.GougingParams, error)
	UploadParams(ctx context.Context) (api.UploadParams, error)

	Object(ctx context.Context, path, prefix string, offset, limit int) (object.Object, []api.ObjectMetadata, error)
	AddObject(ctx context.Context, path string, o object.Object, usedContracts map[types.PublicKey]types.FileContractID) error
	DeleteObject(ctx context.Context, path string) error

	Accounts(ctx context.Context) ([]api.Account, error)
	UpdateSlab(ctx context.Context, s object.Slab, goodContracts map[types.PublicKey]types.FileContractID) error

	WalletDiscard(ctx context.Context, txn types.Transaction) error
	WalletPrepareForm(ctx context.Context, renterAddress types.Address, renterKey types.PrivateKey, renterFunds, hostCollateral types.Currency, hostKey types.PublicKey, hostSettings rhpv2.HostSettings, endHeight uint64) (txns []types.Transaction, err error)
	WalletPrepareRenew(ctx context.Context, revision types.FileContractRevision, hostAddress, renterAddress types.Address, renterKey types.PrivateKey, renterFunds, newCollateral types.Currency, hostKey types.PublicKey, pt rhpv3.HostPriceTable, endHeight, windowSize uint64) (api.WalletPrepareRenewResponse, error)
	WalletSign(ctx context.Context, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error
}

// deriveSubKey can be used to derive a sub-masterkey from the worker's
// masterkey to use for a specific purpose. Such as deriving more keys for
// ephemeral accounts.
func (w *worker) deriveSubKey(purpose string) types.PrivateKey {
	seed := blake2b.Sum256(append(w.masterKey[:], []byte(purpose)...))
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

// TODO: deriving the renter key from the host key using the master key only
// works if we persist a hash of the renter's master key in the database and
// compare it on startup, otherwise there's no way of knowing the derived key is
// usuable
// NOTE: Instead of hashing the masterkey and comparing, we could use random
// bytes + the HMAC thereof as the salt. e.g. 32 bytes + 32 bytes HMAC. Then
// whenever we read a specific salt we can verify that is was created with a
// given key. That would eventually allow different masterkeys to coexist in the
// same bus.
//
// TODO: instead of deriving a renter key use a randomly generated salt so we're
// not limited to one key per host
func (w *worker) deriveRenterKey(hostKey types.PublicKey) types.PrivateKey {
	seed := blake2b.Sum256(append(w.deriveSubKey("renterkey"), hostKey[:]...))
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

// A worker talks to Sia hosts to perform contract and storage operations within
// a renterd system.
type worker struct {
	id        string
	bus       Bus
	pool      *sessionPool
	masterKey [32]byte

	accounts    *accounts
	priceTables *priceTables

	busFlushInterval time.Duration

	interactionsMu         sync.Mutex
	interactions           []hostdb.Interaction
	interactionsFlushTimer *time.Timer

	contractSpendingRecorder *contractSpendingRecorder

	contractLockingDuration time.Duration
	downloadSectorTimeout   time.Duration
	uploadSectorTimeout     time.Duration
	downloadMaxOverdrive    int
	uploadMaxOverdrive      int

	transportPoolV3 *transportPoolV3
	logger          *zap.SugaredLogger
}

func (w *worker) recordPriceTableUpdate(hostKey types.PublicKey, pt hostdb.HostPriceTable, err error) {
	hi := hostdb.Interaction{
		Host:      hostKey,
		Timestamp: time.Now(),
		Type:      hostdb.InteractionTypePriceTableUpdate,
		Success:   err == nil,
	}
	if err == nil {
		hi.Result, _ = json.Marshal(hostdb.PriceTableUpdateResult{
			PriceTable: pt,
		})
	} else {
		hi.Result, _ = json.Marshal(hostdb.PriceTableUpdateResult{
			Error: errToStr(err),
		})
	}
	w.recordInteractions([]hostdb.Interaction{hi})
}

func (w *worker) recordScan(hostKey types.PublicKey, pt rhpv3.HostPriceTable, settings rhpv2.HostSettings, err error) {
	hi := hostdb.Interaction{
		Host:      hostKey,
		Timestamp: time.Now(),
		Type:      hostdb.InteractionTypeScan,
		Success:   err == nil,
	}
	if err == nil {
		hi.Result, _ = json.Marshal(hostdb.ScanResult{
			PriceTable: pt,
			Settings:   settings,
		})
	} else {
		hi.Result, _ = json.Marshal(hostdb.ScanResult{
			Error: errToStr(err),
		})
	}
	w.recordInteractions([]hostdb.Interaction{hi})
}

func (w *worker) withTransportV2(ctx context.Context, hostKey types.PublicKey, hostIP string, fn func(*rhpv2.Transport) error) (err error) {
	var mr ephemeralMetricsRecorder
	defer func() {
		w.recordInteractions(mr.interactions())
	}()
	ctx = metrics.WithRecorder(ctx, &mr)
	conn, err := dial(ctx, hostIP, hostKey)
	if err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			conn.Close()
		}
	}()
	defer func() {
		close(done)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	t, err := rhpv2.NewRenterTransport(conn, hostKey)
	if err != nil {
		return err
	}
	defer t.Close()
	return fn(t)
}

func (w *worker) withHostV2(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, hostIP string, fn func(sectorStore) error) (err error) {
	return w.withHostsV2(ctx, []api.ContractMetadata{{
		ID:      contractID,
		HostKey: hostKey,
		HostIP:  hostIP,
	}}, func(ss []sectorStore) error {
		return fn(ss[0])
	})
}

func (w *worker) withRevisionV3(ctx context.Context, contractID types.FileContractID, hk types.PublicKey, siamuxAddr string, lockPriority int, fn func(revision types.FileContractRevision) error) error {
	// acquire contract lock
	contractLock, err := w.AcquireContract(ctx, contractID, lockPriority)
	if err != nil {
		return fmt.Errorf("%v: %w", "failed to acquire contract for funding EA", err)
	}
	defer func() {
		if err := contractLock.Release(ctx); err != nil {
			w.logger.Errorw(fmt.Sprintf("failed to release contract, err: %v", err), "hk", hk, "fcid", contractID)
		}
	}()

	// fetch contract revision
	rev, err := w.FetchRevisionWithContract(ctx, hk, siamuxAddr, contractID)
	if err != nil {
		return err
	}
	return fn(rev)
}

func (w *worker) unlockHosts(hosts []sectorStore) {
	// apply a pessimistic timeout, ensuring unlocking the contract or force
	// closing the session does not deadlock and keep this goroutine around
	// forever. Use a background context as the parent to avoid timing out
	// the unlock when 'withHosts' returns and the parent context gets
	// closed.
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	var wg sync.WaitGroup
	for _, h := range hosts {
		wg.Add(1)
		go func(ss *sharedSession) {
			w.pool.unlockContract(ctx, ss)
			wg.Done()
		}(h.(*sharedSession))
	}
	wg.Wait()
}

func (w *worker) withHostsV2(ctx context.Context, contracts []api.ContractMetadata, fn func([]sectorStore) error) (err error) {
	var hosts []sectorStore
	for _, c := range contracts {
		hosts = append(hosts, w.pool.session(c.HostKey, c.HostIP, c.ID, w.deriveRenterKey(c.HostKey)))
	}
	done := make(chan struct{})

	// Unlock hosts either after the context is closed or the function is done
	// executing.
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
		}
		w.unlockHosts(hosts)
	}()
	defer func() {
		close(done)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	err = fn(hosts)
	return err
}

func (w *worker) rhpScanHandler(jc jape.Context) {
	var rsr api.RHPScanRequest
	if jc.Decode(&rsr) != nil {
		return
	}

	ctx := jc.Request.Context()
	if rsr.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(jc.Request.Context(), rsr.Timeout)
		defer cancel()
	}

	// defer scan result
	var err error
	var settings rhpv2.HostSettings
	var priceTable rhpv3.HostPriceTable
	defer func() {
		w.recordScan(rsr.HostKey, priceTable, settings, err)
	}()

	// fetch the host settings
	start := time.Now()
	err = w.withTransportV2(ctx, rsr.HostKey, rsr.HostIP, func(t *rhpv2.Transport) (err error) {
		if settings, err = RPCSettings(ctx, t); err == nil {
			// NOTE: we overwrite the NetAddress with the host address here since we
			// just used it to dial the host we know it's valid
			settings.NetAddress = rsr.HostIP
		}
		return err
	})
	elapsed := time.Since(start)

	// fetch the host pricetable
	if err == nil {
		err = w.transportPoolV3.withTransportV3(ctx, rsr.HostKey, settings.SiamuxAddr(), func(t *transportV3) (err error) {
			priceTable, err = RPCPriceTable(t, func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) { return nil, nil })
			return err
		})
	}

	// check error
	var errStr string
	if err != nil {
		errStr = err.Error()
	}

	jc.Encode(api.RHPScanResponse{
		Ping:      api.ParamDuration(elapsed),
		ScanError: errStr,
		Settings:  settings,
	})
}

func (w *worker) fetchActiveContracts(ctx context.Context, metadatas []api.ContractMetadata, timeout time.Duration, bh uint64) (contracts []api.Contract, errs HostErrorSet) {
	// create requests channel
	reqs := make(chan api.ContractMetadata)

	// create worker function
	var mu sync.Mutex
	worker := func() {
		for metadata := range reqs {
			rev, err := w.FetchRevision(ctx, timeout, metadata, bh, lockingPriorityActiveContractRevision)
			mu.Lock()
			if err != nil {
				errs = append(errs, &HostError{HostKey: metadata.HostKey, Err: err})
			} else {
				contracts = append(contracts, api.Contract{
					ContractMetadata: metadata,
					Revision:         rev,
				})
			}
			mu.Unlock()
		}
	}

	// launch all workers
	var wg sync.WaitGroup
	for t := 0; t < 10 && t < len(metadatas); t++ {
		wg.Add(1)
		go func() {
			worker()
			wg.Done()
		}()
	}

	// launch all requests
	for _, metadata := range metadatas {
		reqs <- metadata
	}
	close(reqs)

	// wait until they're done
	wg.Wait()
	return
}

func (w *worker) fetchPriceTable(ctx context.Context, hk types.PublicKey, siamuxAddr string, revision *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error) {
	defer func() { w.recordPriceTableUpdate(hk, hpt, err) }()

	// fetchPT is a helper function that performs the RPC given a payment function
	fetchPT := func(paymentFn PriceTablePaymentFunc) (hpt hostdb.HostPriceTable, err error) {
		err = w.transportPoolV3.withTransportV3(ctx, hk, siamuxAddr, func(t *transportV3) (err error) {
			pt, err := RPCPriceTable(t, paymentFn)
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
	if revision != nil {
		return fetchPT(w.preparePriceTableContractPayment(hk, revision))
	}

	// pay by account
	cs, err := w.bus.ConsensusState(ctx)
	if err != nil {
		return hostdb.HostPriceTable{}, err
	}
	return fetchPT(w.preparePriceTableAccountPayment(hk, cs.BlockHeight))
}

func (w *worker) rhpPriceTableHandler(jc jape.Context) {
	var rptr api.RHPPriceTableRequest
	if jc.Decode(&rptr) != nil {
		return
	}

	var pt rhpv3.HostPriceTable
	if jc.Check("could not get price table", w.transportPoolV3.withTransportV3(jc.Request.Context(), rptr.HostKey, rptr.SiamuxAddr, func(t *transportV3) (err error) {
		pt, err = RPCPriceTable(t, func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) { return nil, nil })
		return
	})) != nil {
		return
	}

	jc.Encode(hostdb.HostPriceTable{
		HostPriceTable: pt,
		Expiry:         time.Now().Add(pt.Validity),
	})
}

func (w *worker) discardTxnOnErr(ctx context.Context, txn types.Transaction, errContext string, err *error) {
	if *err == nil {
		return
	}
	_, span := tracing.Tracer.Start(ctx, "discardTxn")
	defer span.End()
	// Attach the span to a new context derived from the background context.
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	timeoutCtx = trace.ContextWithSpan(timeoutCtx, span)
	if err := w.bus.WalletDiscard(timeoutCtx, txn); err != nil {
		w.logger.Errorf("%v: failed to discard txn: %v", err)
	}
}

func (w *worker) rhpFormHandler(jc jape.Context) {
	ctx := jc.Request.Context()
	var rfr api.RHPFormRequest
	if jc.Decode(&rfr) != nil {
		return
	}

	// apply a pessimistic timeout on contract formations
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	hostIP, hostKey, renterFunds := rfr.HostIP, rfr.HostKey, rfr.RenterFunds
	renterAddress, endHeight, hostCollateral := rfr.RenterAddress, rfr.EndHeight, rfr.HostCollateral
	renterKey := w.deriveRenterKey(hostKey)

	var contract rhpv2.ContractRevision
	var txnSet []types.Transaction
	ctx = WithGougingChecker(ctx, gp)
	err = w.withTransportV2(ctx, rfr.HostKey, hostIP, func(t *rhpv2.Transport) (err error) {
		hostSettings, err := RPCSettings(ctx, t)
		if err != nil {
			return err
		}
		// NOTE: we overwrite the NetAddress with the host address here since we
		// just used it to dial the host we know it's valid
		hostSettings.NetAddress = hostIP

		if breakdown := GougingCheckerFromContext(ctx).Check(&hostSettings, nil); breakdown.Gouging() {
			return fmt.Errorf("failed to form contract, gouging check failed: %v", breakdown.Reasons())
		}

		renterTxnSet, err := w.bus.WalletPrepareForm(ctx, renterAddress, renterKey, renterFunds, hostCollateral, hostKey, hostSettings, endHeight)
		if err != nil {
			return err
		}
		defer w.discardTxnOnErr(ctx, renterTxnSet[len(renterTxnSet)-1], "rhpFormHandler", &err)

		contract, txnSet, err = RPCFormContract(ctx, t, renterKey, renterTxnSet)
		if err != nil {
			return err
		}
		return
	})
	if jc.Check("couldn't form contract", err) != nil {
		return
	}

	// broadcast the transaction set
	err = w.bus.BroadcastTransaction(jc.Request.Context(), txnSet)
	if err != nil && !isErrDuplicateTransactionSet(err) {
		w.logger.Warnf("failed to broadcast formation txn set: %v", err)
	}

	jc.Encode(api.RHPFormResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (w *worker) rhpRenewHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode request
	var rrr api.RHPRenewRequest
	if jc.Decode(&rrr) != nil {
		return
	}

	// get consensus state
	cs, err := w.bus.ConsensusState(ctx)
	if jc.Check("could not get consensus state", err) != nil {
		return
	}

	// attach gouging checker
	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	ctx = WithGougingChecker(ctx, gp)
	rk := w.deriveRenterKey(rrr.HostKey)

	// renew the contract
	renewed, txnSet, err := w.Renew(ctx, rrr, cs, rk)
	if jc.Check("couldn't renew contract", err) != nil {
		return
	}

	// broadcast the transaction set
	err = w.bus.BroadcastTransaction(jc.Request.Context(), txnSet)
	if err != nil && !isErrDuplicateTransactionSet(err) {
		w.logger.Warnf("failed to broadcast renewal txn set: %v", err)
	}

	// send the response
	jc.Encode(api.RHPRenewResponse{
		ContractID:     renewed.ID(),
		Contract:       renewed,
		TransactionSet: txnSet,
	})
}

func (w *worker) rhpFundHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode request
	var rfr api.RHPFundRequest
	if jc.Decode(&rfr) != nil {
		return
	}

	// attach gouging checker
	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	ctx = WithGougingChecker(ctx, gp)

	// fund the account
	jc.Check("couldn't fund account", w.withRevisionV3(ctx, rfr.ContractID, rfr.HostKey, rfr.SiamuxAddr, lockingPriorityFunding, func(revision types.FileContractRevision) (err error) {
		err = w.fundAccount(ctx, rfr.HostKey, rfr.SiamuxAddr, rfr.Balance, &revision)
		if isMaxBalanceExceeded(err) {
			// sync the account
			err = w.syncAccount(ctx, rfr.HostKey, rfr.SiamuxAddr, &revision)
			if err != nil {
				w.logger.Errorw(fmt.Sprintf("failed to sync account: %v", err), "host", rfr.HostKey)
				return
			}

			// try funding the account again
			err = w.fundAccount(ctx, rfr.HostKey, rfr.SiamuxAddr, rfr.Balance, &revision)
			if errors.Is(err, errBalanceSufficient) {
				w.logger.Debugf("account balance for host %v restored after sync", rfr.HostKey)
				return nil
			}

			// funding failed after syncing the account successfully
			if err != nil {
				w.logger.Errorw(fmt.Sprintf("failed to fund account after syncing: %v", err), "host", rfr.HostKey, "balance", rfr.Balance)
			}
		}
		return
	}))
}

func (w *worker) rhpRegistryReadHandler(jc jape.Context) {
	var rrrr api.RHPRegistryReadRequest
	if jc.Decode(&rrrr) != nil {
		return
	}
	var value rhpv3.RegistryValue
	err := w.transportPoolV3.withTransportV3(jc.Request.Context(), rrrr.HostKey, rrrr.SiamuxAddr, func(t *transportV3) (err error) {
		value, err = RPCReadRegistry(t, &rrrr.Payment, rrrr.RegistryKey)
		return
	})
	if jc.Check("couldn't read registry", err) != nil {
		return
	}
	jc.Encode(value)
}

func (w *worker) rhpRegistryUpdateHandler(jc jape.Context) {
	var rrur api.RHPRegistryUpdateRequest
	if jc.Decode(&rrur) != nil {
		return
	}
	var pt rhpv3.HostPriceTable   // TODO
	rc := pt.UpdateRegistryCost() // TODO: handle refund
	cost, _ := rc.Total()
	payment := w.preparePayment(rrur.HostKey, cost, pt.HostBlockHeight)
	err := w.transportPoolV3.withTransportV3(jc.Request.Context(), rrur.HostKey, rrur.SiamuxAddr, func(t *transportV3) (err error) {
		return RPCUpdateRegistry(t, &payment, rrur.RegistryKey, rrur.RegistryValue)
	})
	if jc.Check("couldn't update registry", err) != nil {
		return
	}
}

func (w *worker) rhpSyncHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode the request
	var rsr api.RHPSyncRequest
	if jc.Decode(&rsr) != nil {
		return
	}

	// fetch gouging params
	up, err := w.bus.UploadParams(ctx)
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, up.GougingParams)

	// sync the account
	jc.Check("couldn't sync account", w.withRevisionV3(ctx, rsr.ContractID, rsr.HostKey, rsr.SiamuxAddr, lockingPrioritySyncing, func(revision types.FileContractRevision) error {
		return w.syncAccount(ctx, rsr.HostKey, rsr.SiamuxAddr, &revision)
	}))
}

func (w *worker) slabMigrateHandler(jc jape.Context) {
	ctx := jc.Request.Context()
	var slab object.Slab
	if jc.Decode(&slab) != nil {
		return
	}

	// fetch the upload parameters
	up, err := w.bus.UploadParams(ctx)
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}

	// cancel the upload if consensus is not synced
	if !up.ConsensusState.Synced {
		w.logger.Errorf("migration cancelled, err: %v", api.ErrConsensusNotSynced)
		jc.Error(api.ErrConsensusNotSynced, http.StatusServiceUnavailable)
		return
	}

	// allow overriding contract set
	var contractset string
	if jc.DecodeForm(queryStringParamContractSet, &contractset) != nil {
		return
	} else if contractset != "" {
		up.ContractSet = contractset
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, up.GougingParams)

	// attach contract spending recorder to the context.
	ctx = WithContractSpendingRecorder(ctx, w.contractSpendingRecorder)

	contracts, err := w.bus.Contracts(ctx, up.ContractSet)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	w.pool.setCurrentHeight(up.CurrentHeight)
	err = migrateSlab(ctx, w, &slab, contracts, w, w.downloadSectorTimeout, w.uploadSectorTimeout, w.logger)
	if jc.Check("couldn't migrate slabs", err) != nil {
		return
	}

	usedContracts := make(map[types.PublicKey]types.FileContractID)
	for _, ss := range slab.Shards {
		if _, exists := usedContracts[ss.Host]; exists {
			continue
		}

		for _, c := range contracts {
			if c.HostKey == ss.Host {
				usedContracts[ss.Host] = c.ID
				break
			}
		}
	}

	if jc.Check("couldn't update slab", w.bus.UpdateSlab(ctx, slab, usedContracts)) != nil {
		return
	}
}

func (w *worker) objectsHandlerGET(jc jape.Context) {
	ctx := jc.Request.Context()
	jc.Custom(nil, []api.ObjectMetadata{})

	var off int
	if jc.DecodeForm("offset", &off) != nil {
		return
	}
	limit := -1
	if jc.DecodeForm("limit", &limit) != nil {
		return
	}
	var prefix string
	if jc.DecodeForm("prefix", &prefix) != nil {
		return
	}

	path := strings.TrimPrefix(jc.PathParam("path"), "/")
	obj, entries, err := w.bus.Object(ctx, path, prefix, off, limit)
	if err != nil && strings.Contains(err.Error(), api.ErrObjectNotFound.Error()) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("couldn't get object or entries", err) != nil {
		return
	}

	if path == "" || strings.HasSuffix(path, "/") {
		jc.Encode(entries)
		return
	}
	if len(obj.Slabs) == 0 {
		jc.Error(errors.New("object has no data"), http.StatusInternalServerError)
		return
	}

	dp, err := w.bus.DownloadParams(ctx)
	if jc.Check("couldn't fetch download parameters from bus", err) != nil {
		return
	}

	// allow overriding contract set
	var contractset string
	if jc.DecodeForm(queryStringParamContractSet, &contractset) != nil {
		return
	} else if contractset != "" {
		dp.ContractSet = contractset
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, dp.GougingParams)

	// NOTE: ideally we would use http.ServeContent in this handler, but that
	// has performance issues. If we implemented io.ReadSeeker in the most
	// straightforward fashion, we would need one (or more!) RHP RPCs for each
	// Read call. We can improve on this to some degree by buffering, but
	// without knowing the exact ranges being requested, this will always be
	// suboptimal. Thus, sadly, we have to roll our own range support.
	ranges, err := http_range.ParseRange(jc.Request.Header.Get("Range"), obj.Size())
	if err != nil {
		jc.Error(err, http.StatusRequestedRangeNotSatisfiable)
		return
	}
	var offset int64
	length := obj.Size()
	status := http.StatusOK
	if len(ranges) > 0 {
		status = http.StatusPartialContent
		jc.ResponseWriter.Header().Set("Content-Range", ranges[0].ContentRange(obj.Size()))
		offset, length = ranges[0].Start, ranges[0].Length
	}
	jc.ResponseWriter.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	jc.ResponseWriter.Header().Set("Accept-Ranges", "bytes")
	if ext := filepath.Ext(path); ext != "" {
		if mimeType := mime.TypeByExtension(ext); mimeType != "" {
			jc.ResponseWriter.Header().Set("Content-Type", mimeType)
		}
	}
	rw := rangedResponseWriter{rw: jc.ResponseWriter, defaultStatusCode: status}

	// keep track of recent timings per host so we can favour faster hosts
	performance := make(map[types.PublicKey]int64)

	// fetch contracts
	set, err := w.bus.Contracts(ctx, dp.ContractSet)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	// build contract map
	availableContracts := make(map[types.PublicKey]api.ContractMetadata)
	for _, contract := range set {
		availableContracts[contract.HostKey] = contract
	}

	cw := obj.Key.Decrypt(&rw, offset)
	for i, ss := range slabsForDownload(obj.Slabs, offset, length) {
		// fetch available hosts for the slab
		hostMap := make(map[types.PublicKey]api.ContractMetadata)
		availableShards := 0
		for _, shard := range ss.Shards {
			if _, available := availableContracts[shard.Host]; !available {
				continue
			}
			availableShards++
			hostMap[shard.Host] = availableContracts[shard.Host]
		}

		// check if enough slabs are available
		if availableShards < int(ss.MinShards) {
			err = fmt.Errorf("not enough available shards to download the slab, %d<%d", availableShards, ss.MinShards)
			w.logger.Errorf("couldn't download object '%v' slab %d, err: %v", path, i, err)
			if i == 0 {
				jc.Error(err, http.StatusInternalServerError)
			}
			return
		}

		// flatten host map to get a slice of contracts which is deduplicated
		// already and contains only contracts relevant to the slab.
		contracts := make([]api.ContractMetadata, 0, len(hostMap))
		for _, c := range hostMap {
			contracts = append(contracts, c)
		}

		// make sure consecutive slabs are downloaded from hosts that performed
		// well on previous slab downloads
		sort.SliceStable(contracts, func(i, j int) bool {
			return performance[contracts[i].HostKey] < performance[contracts[j].HostKey]
		})

		timings, err := downloadSlab(ctx, w, cw, ss, contracts, w.downloadSectorTimeout, w.downloadMaxOverdrive, w.logger)

		// update historic host performance
		//
		// NOTE: we only update if we don't have a datapoint yet or if the
		// returned timing differs from the default. This ensures we don't
		// necessarily want to try downloading from all hosts and we don't reset
		// a host's performance to the default timing.
		for i, timing := range timings {
			if _, exists := performance[contracts[i].HostKey]; !exists || timing != int64(defaultSectorDownloadTiming) {
				performance[contracts[i].HostKey] = timing
			}
		}
		if err != nil {
			w.logger.Errorf("couldn't download object '%v' slab %d, err: %v", path, i, err)
			if i == 0 {
				jc.Error(err, http.StatusInternalServerError)
			}
			return
		}
	}
}

func (w *worker) objectsHandlerPUT(jc jape.Context) {
	jc.Custom((*[]byte)(nil), nil)
	ctx := jc.Request.Context()

	// fetch the path
	path := strings.TrimPrefix(jc.PathParam("path"), "/")

	// fetch the upload parameters
	up, err := w.bus.UploadParams(ctx)
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}

	// cancel the upload if consensus is not synced
	if !up.ConsensusState.Synced {
		w.logger.Errorf("upload cancelled, err: %v", api.ErrConsensusNotSynced)
		jc.Error(api.ErrConsensusNotSynced, http.StatusServiceUnavailable)
		return
	}

	// allow overriding the redundancy settings
	rs := up.RedundancySettings
	if jc.DecodeForm(queryStringParamMinShards, &rs.MinShards) != nil {
		return
	}
	if jc.DecodeForm(queryStringParamTotalShards, &rs.TotalShards) != nil {
		return
	}
	if jc.Check("invalid redundancy settings", rs.Validate()) != nil {
		return
	}

	// allow overriding contract set
	var contractset string
	if jc.DecodeForm(queryStringParamContractSet, &contractset) != nil {
		return
	} else if contractset != "" {
		up.ContractSet = contractset
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, up.GougingParams)

	// attach contract spending recorder to the context.
	ctx = WithContractSpendingRecorder(ctx, w.contractSpendingRecorder)

	o := object.Object{
		Key: object.GenerateEncryptionKey(),
	}
	w.pool.setCurrentHeight(up.CurrentHeight)
	usedContracts := make(map[types.PublicKey]types.FileContractID)

	// fetch contracts
	contracts, err := w.bus.Contracts(ctx, up.ContractSet)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	// randomize order of contracts so we don't always upload to the same hosts
	frand.Shuffle(len(contracts), func(i, j int) { contracts[i], contracts[j] = contracts[j], contracts[i] })

	// keep track of slow hosts so we can avoid them in consecutive slab uploads
	slow := make(map[types.PublicKey]int)

	cr := o.Key.Encrypt(jc.Request.Body)
	var objectSize int
	for {
		var s object.Slab
		var length int
		var slowHosts []int

		lr := io.LimitReader(cr, int64(rs.MinShards)*rhpv2.SectorSize)
		// move slow hosts to the back of the array
		sort.SliceStable(contracts, func(i, j int) bool {
			return slow[contracts[i].HostKey] < slow[contracts[j].HostKey]
		})

		// upload the slab
		start := time.Now()
		s, length, slowHosts, err = uploadSlab(ctx, w, lr, uint8(rs.MinShards), uint8(rs.TotalShards), contracts, w, w.uploadSectorTimeout, w.uploadMaxOverdrive, w.logger)
		for _, h := range slowHosts {
			slow[contracts[h].HostKey]++
		}
		if err == io.EOF {
			break
		} else if jc.Check(fmt.Sprintf("uploading slab failed after %v", time.Since(start)), err); err != nil {
			w.logger.Errorf("couldn't upload object '%v' slab %d, err: %v", path, len(o.Slabs), err)
			return
		}

		objectSize += length
		o.Slabs = append(o.Slabs, object.SlabSlice{
			Slab:   s,
			Offset: 0,
			Length: uint32(length),
		})

		for _, ss := range s.Shards {
			if _, ok := usedContracts[ss.Host]; !ok {
				for _, c := range contracts {
					if c.HostKey == ss.Host {
						usedContracts[ss.Host] = c.ID
						break
					}
				}
			}
		}
	}

	if jc.Check("couldn't add object", w.bus.AddObject(ctx, path, o, usedContracts)) != nil {
		return
	}
}

func (w *worker) objectsHandlerDELETE(jc jape.Context) {
	jc.Check("couldn't delete object", w.bus.DeleteObject(jc.Request.Context(), jc.PathParam("path")))
}

func (w *worker) rhpActiveContractsHandlerGET(jc jape.Context) {
	ctx := jc.Request.Context()
	busContracts, err := w.bus.ActiveContracts(ctx)
	if jc.Check("failed to fetch contracts from bus", err) != nil {
		return
	}
	if len(busContracts) == 0 {
		jc.Encode(api.ContractsResponse{Contracts: nil})
		return
	}

	var hosttimeout time.Duration
	if jc.DecodeForm("hosttimeout", (*api.ParamDuration)(&hosttimeout)) != nil {
		return
	}

	cs, err := w.bus.ConsensusState(ctx)
	if jc.Check("could not get consensus state", err) != nil {
		return
	}
	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	ctx = WithGougingChecker(ctx, gp)

	contracts, errs := w.fetchActiveContracts(ctx, busContracts, hosttimeout, cs.BlockHeight)
	resp := api.ContractsResponse{Contracts: contracts}
	if errs != nil {
		resp.Error = errs.Error()
	}
	jc.Encode(resp)
}

func (w *worker) preparePayment(hk types.PublicKey, amt types.Currency, blockHeight uint64) rhpv3.PayByEphemeralAccountRequest {
	pk := w.accounts.deriveAccountKey(hk)
	return rhpv3.PayByEphemeralAccount(rhpv3.Account(pk.PublicKey()), amt, blockHeight+6, pk) // 1 hour valid
}

func (w *worker) idHandlerGET(jc jape.Context) {
	jc.Encode(w.id)
}

func (w *worker) accountHandlerGET(jc jape.Context) {
	var hostKey types.PublicKey
	if jc.DecodeParam("hostkey", &hostKey) != nil {
		return
	}
	account := rhpv3.Account(w.accounts.deriveAccountKey(hostKey).PublicKey())
	jc.Encode(account)
}

// New returns an HTTP handler that serves the worker API.
func New(masterKey [32]byte, id string, b Bus, contractLockingDuration, sessionLockTimeout, sessionReconectTimeout, sessionTTL, busFlushInterval, downloadSectorTimeout, uploadSectorTimeout time.Duration, maxDownloadOverdrive, maxUploadOverdrive int, l *zap.Logger) *worker {
	w := &worker{
		contractLockingDuration: contractLockingDuration,
		id:                      id,
		bus:                     b,
		pool:                    newSessionPool(sessionLockTimeout, sessionReconectTimeout, sessionTTL),
		masterKey:               masterKey,
		busFlushInterval:        busFlushInterval,
		downloadSectorTimeout:   downloadSectorTimeout,
		uploadSectorTimeout:     uploadSectorTimeout,
		downloadMaxOverdrive:    maxDownloadOverdrive,
		uploadMaxOverdrive:      maxUploadOverdrive,
		logger:                  l.Sugar().Named("worker").Named(id),
		transportPoolV3:         newTransportPoolV3(),
	}
	w.initAccounts(b)
	w.initContractSpendingRecorder()
	w.initPriceTables()
	return w
}

// Handler returns an HTTP handler that serves the worker API.
func (w *worker) Handler() http.Handler {
	return jape.Mux(tracing.TracedRoutes("worker", map[string]jape.Handler{
		"GET    /account/:hostkey": w.accountHandlerGET,
		"GET    /id":               w.idHandlerGET,

		"GET    /rhp/contracts/active": w.rhpActiveContractsHandlerGET,
		"POST   /rhp/scan":             w.rhpScanHandler,
		"POST   /rhp/form":             w.rhpFormHandler,
		"POST   /rhp/renew":            w.rhpRenewHandler,
		"POST   /rhp/fund":             w.rhpFundHandler,
		"POST   /rhp/sync":             w.rhpSyncHandler,
		"POST   /rhp/pricetable":       w.rhpPriceTableHandler,
		"POST   /rhp/registry/read":    w.rhpRegistryReadHandler,
		"POST   /rhp/registry/update":  w.rhpRegistryUpdateHandler,

		"POST   /slab/migrate": w.slabMigrateHandler,

		"GET    /objects/*path": w.objectsHandlerGET,
		"PUT    /objects/*path": w.objectsHandlerPUT,
		"DELETE /objects/*path": w.objectsHandlerDELETE,
	}))
}

// Shutdown shuts down the worker.
func (w *worker) Shutdown(_ context.Context) error {
	w.interactionsMu.Lock()
	if w.interactionsFlushTimer != nil {
		w.interactionsFlushTimer.Stop()
		w.flushInteractions()
	}
	w.interactionsMu.Unlock()

	// Stop contract spending recorder.
	w.contractSpendingRecorder.Stop()
	return nil
}

func (w *worker) recordInteractions(interactions []hostdb.Interaction) {
	w.interactionsMu.Lock()
	defer w.interactionsMu.Unlock()

	// Append interactions to buffer.
	w.interactions = append(w.interactions, interactions...)

	// If a thread was scheduled to flush the buffer we are done.
	if w.interactionsFlushTimer != nil {
		return
	}
	// Otherwise we schedule a flush.
	w.interactionsFlushTimer = time.AfterFunc(w.busFlushInterval, func() {
		w.interactionsMu.Lock()
		w.flushInteractions()
		w.interactionsMu.Unlock()
	})
}

func (w *worker) flushInteractions() {
	if len(w.interactions) > 0 {
		ctx, span := tracing.Tracer.Start(context.Background(), "worker: flushInteractions")
		defer span.End()
		if err := w.bus.RecordInteractions(ctx, w.interactions); err != nil {
			w.logger.Errorw(fmt.Sprintf("failed to record interactions: %v", err))
		} else {
			w.interactions = nil
		}
	}
	w.interactionsFlushTimer = nil
}

type contractLock struct {
	lockID uint64
	fcid   types.FileContractID
	d      time.Duration
	locker ContractLocker
	logger *zap.SugaredLogger

	ctx      context.Context
	stopChan chan struct{}
}

func newContractLock(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration, locker ContractLocker, logger *zap.SugaredLogger) *contractLock {
	return &contractLock{
		lockID: lockID,
		fcid:   fcid,
		d:      d,
		locker: locker,
		logger: logger,

		ctx:      ctx,
		stopChan: make(chan struct{}),
	}
}

func (cl *contractLock) Release(ctx context.Context) error {
	_, span := tracing.Tracer.Start(ctx, "tracedContractLocker.ReleaseContract")
	defer span.End()

	// Stop background loop.
	close(cl.stopChan)

	// Create a new context with the span that times out after a certain amount
	// of time.  That's because the context passed to this method might be
	// cancelled but we still want to release the contract.
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	timeoutCtx = trace.ContextWithSpan(timeoutCtx, span)

	// Release the contract.
	err := cl.locker.ReleaseContract(timeoutCtx, cl.fcid, cl.lockID)
	if err != nil {
		span.SetStatus(codes.Error, "failed to release contract")
		span.RecordError(err)
	}
	span.SetAttributes(attribute.Stringer("contract", cl.fcid))
	return err
}

func (cl *contractLock) keepaliveLoop() {
	// Create ticker for half the duration of the lock.
	t := time.NewTicker(cl.d / 2)

	// Cleanup
	defer func() {
		t.Stop()
		select {
		case <-t.C:
		default:
		}
	}()

	// Loop until stopped.
	for {
		select {
		case <-cl.ctx.Done():
			return // interrupted
		case <-cl.stopChan:
			return // released
		case <-t.C:
		}
		if err := cl.locker.KeepaliveContract(cl.ctx, cl.fcid, cl.lockID, cl.d); err != nil {
			cl.logger.Errorw(fmt.Sprintf("failed to send keepalive: %v", err), "contract", cl.fcid, "lockID", cl.lockID)
			return
		}
	}
}

func (w *worker) AcquireContract(ctx context.Context, fcid types.FileContractID, priority int) (_ contractReleaser, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "tracedContractLocker.AcquireContract")
	defer span.End()
	span.SetAttributes(attribute.Stringer("contract", fcid))
	span.SetAttributes(attribute.Int("priority", priority))
	lockID, err := w.bus.AcquireContract(ctx, fcid, priority, w.contractLockingDuration)
	if err != nil {
		span.SetStatus(codes.Error, "failed to acquire contract")
		span.RecordError(err)
		return nil, err
	}
	cl := newContractLock(ctx, fcid, lockID, w.contractLockingDuration, w.bus, w.logger)
	go cl.keepaliveLoop()
	return cl, nil
}

func isErrDuplicateTransactionSet(err error) bool {
	return err != nil && strings.Contains(err.Error(), modules.ErrDuplicateTransactionSet.Error())
}
