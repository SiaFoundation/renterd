package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
)

const (
	defaultRevisionFetchTimeout = 30 * time.Second

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

var privateSubnets []*net.IPNet

func init() {
	for _, subnet := range []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"100.64.0.0/10",
	} {
		_, subnet, err := net.ParseCIDR(subnet)
		if err != nil {
			panic(fmt.Sprintf("failed to parse subnet: %v", err))
		}
		privateSubnets = append(privateSubnets, subnet)
	}
}

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

type (
	consensusState interface {
		ConsensusState(ctx context.Context) (api.ConsensusState, error)
	}

	revisionUnlocker interface {
		Release(context.Context) error
	}

	revisionLocker interface {
		withRevision(ctx context.Context, timeout time.Duration, contractID types.FileContractID, hk types.PublicKey, siamuxAddr string, lockPriority int, blockHeight uint64, fn func(rev types.FileContractRevision) error) error
	}
)

type ContractLocker interface {
	AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error)
	KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error)
	ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error)
}

// A Bus is the source of truth within a renterd system.
type Bus interface {
	consensusState

	AccountStore
	ContractLocker

	BroadcastTransaction(ctx context.Context, txns []types.Transaction) error

	Contracts(ctx context.Context) ([]api.ContractMetadata, error)
	ContractSetContracts(ctx context.Context, set string) ([]api.ContractMetadata, error)
	RecordInteractions(ctx context.Context, interactions []hostdb.Interaction) error
	RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error

	Host(ctx context.Context, hostKey types.PublicKey) (hostdb.HostInfo, error)

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
	allowPrivateIPs bool
	id              string
	bus             Bus
	pool            *sessionPool
	masterKey       [32]byte

	uploader *uploader

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
	downloadMaxOverdrive    uint64
	uploadMaxOverdrive      uint64

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

func (w *worker) withHostV2(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, hostIP string, fn func(hostV2) error) (err error) {
	host := w.pool.session(hostKey, hostIP, contractID, w.deriveRenterKey(hostKey))
	done := make(chan struct{})

	// Unlock hosts either after the context is closed or the function is done
	// executing.
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
		}
		w.unlockHost(host)
	}()
	defer func() {
		close(done)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	err = fn(host)
	return err
}

func (w *worker) newHostV3(ctx context.Context, contractID types.FileContractID, hostKey types.PublicKey, siamuxAddr string) (_ hostV3, err error) {
	acc, err := w.accounts.ForHost(hostKey)
	if err != nil {
		return nil, err
	}

	return &host{
		acc:                      acc,
		bus:                      w.bus,
		contractSpendingRecorder: w.contractSpendingRecorder,
		logger:                   w.logger.Named(hostKey.String()[:4]),
		fcid:                     contractID,
		siamuxAddr:               siamuxAddr,
		renterKey:                w.deriveRenterKey(hostKey),
		accountKey:               w.accounts.deriveAccountKey(hostKey),
		transportPool:            w.transportPoolV3,
		priceTables:              w.priceTables,
	}, nil
}

func (w *worker) withRevision(ctx context.Context, fetchTimeout time.Duration, contractID types.FileContractID, hk types.PublicKey, siamuxAddr string, lockPriority int, blockHeight uint64, fn func(rev types.FileContractRevision) error) error {
	// lock the revision for the duration of the operation.
	contractLock, err := w.acquireRevision(ctx, contractID, lockPriority)
	if err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = contractLock.Release(releaseCtx)
		cancel()
	}()

	h, err := w.newHostV3(ctx, contractID, hk, siamuxAddr)
	if err != nil {
		return err
	}
	rev, err := h.FetchRevision(ctx, fetchTimeout, blockHeight)
	if err != nil {
		return err
	}

	before := rev.RevisionNumber

	err = fn(rev)
	if err != nil && !strings.Contains(err.Error(), "revision number was not incremented") {
		after := rev.RevisionNumber
		rev, err := h.FetchRevision(ctx, fetchTimeout, blockHeight)
		if err != nil {
			return err
		}
		host := rev.RevisionNumber
		fmt.Printf("DEBUG PJ | %d -> %d -> %d (%v)\n", before, after, host, err)
	}
	return err
}

func (w *worker) unlockHost(host hostV2) {
	// apply a pessimistic timeout, ensuring unlocking the contract or force
	// closing the session does not deadlock and keep this goroutine around
	// forever. Use a background context as the parent to avoid timing out
	// the unlock when 'withHosts' returns and the parent context gets
	// closed.
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	w.pool.unlockContract(ctx, host.(*sharedSession))
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

	// scan host
	var errStr string
	settings, priceTable, elapsed, err := w.scanHost(ctx, rsr.HostKey, rsr.HostIP)
	if err != nil {
		errStr = err.Error()
	}

	jc.Encode(api.RHPScanResponse{
		Ping:      api.ParamDuration(elapsed),
		ScanError: errStr,
		Settings:  settings,
	})
}

func (w *worker) fetchContracts(ctx context.Context, metadatas []api.ContractMetadata, timeout time.Duration, blockHeight uint64) (contracts []api.Contract, errs HostErrorSet) {
	// create requests channel
	reqs := make(chan api.ContractMetadata)

	// create worker function
	var mu sync.Mutex
	worker := func() {
		for md := range reqs {
			var revision types.FileContractRevision
			err := w.withRevision(ctx, timeout, md.ID, md.HostKey, md.SiamuxAddr, lockingPriorityActiveContractRevision, blockHeight, func(rev types.FileContractRevision) error {
				revision = rev
				return nil
			})
			mu.Lock()
			if err != nil {
				errs = append(errs, &HostError{HostKey: md.HostKey, Err: err})
				contracts = append(contracts, api.Contract{
					ContractMetadata: md,
				})
			} else {
				contracts = append(contracts, api.Contract{
					ContractMetadata: md,
					Revision:         &revision,
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

func (w *worker) fetchPriceTable(ctx context.Context, hk types.PublicKey, siamuxAddr string, rev *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error) {
	defer func() { w.recordPriceTableUpdate(hk, hpt, err) }()

	h, err := w.newHostV3(ctx, types.FileContractID{}, hk, siamuxAddr)
	if err != nil {
		return hostdb.HostPriceTable{}, err
	}
	hpt, err = h.FetchPriceTable(ctx, rev)
	if err != nil {
		return hostdb.HostPriceTable{}, err
	}
	return hpt, nil
}

func (w *worker) rhpPriceTableHandler(jc jape.Context) {
	var rptr api.RHPPriceTableRequest
	if jc.Decode(&rptr) != nil {
		return
	}

	var pt rhpv3.HostPriceTable
	if jc.Check("could not get price table", w.transportPoolV3.withTransportV3(jc.Request.Context(), rptr.HostKey, rptr.SiamuxAddr, func(t *transportV3) (err error) {
		pt, err = RPCPriceTable(jc.Request.Context(), t, func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) { return nil, nil })
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
	discardTxnOnErr(ctx, w.bus, w.logger, txn, errContext, err)
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
	ctx = WithGougingChecker(ctx, w.bus, gp)
	err = w.withTransportV2(ctx, rfr.HostKey, hostIP, func(t *rhpv2.Transport) (err error) {
		hostSettings, err := RPCSettings(ctx, t)
		if err != nil {
			return err
		}
		// NOTE: we overwrite the NetAddress with the host address here since we
		// just used it to dial the host we know it's valid
		hostSettings.NetAddress = hostIP

		gc, err := GougingCheckerFromContext(ctx)
		if err != nil {
			return err
		}
		if breakdown := gc.Check(&hostSettings, nil); breakdown.Gouging() {
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

	// attach gouging checker
	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	ctx = WithGougingChecker(ctx, w.bus, gp)

	// renew the contract
	h, err := w.newHostV3(ctx, rrr.ContractID, rrr.HostKey, rrr.SiamuxAddr)
	if jc.Check("failed to create host for renewal", err) != nil {
		return
	}
	renewed, txnSet, err := h.Renew(ctx, rrr)
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
	ctx = WithGougingChecker(ctx, w.bus, gp)

	// create host

	// fund the account
	jc.Check("couldn't fund account", w.withRevision(ctx, defaultRevisionFetchTimeout, rfr.ContractID, rfr.HostKey, rfr.SiamuxAddr, lockingPriorityFunding, gp.ConsensusState.BlockHeight, func(revision types.FileContractRevision) (err error) {
		h, err := w.newHostV3(ctx, revision.ParentID, rfr.HostKey, rfr.SiamuxAddr)
		if err != nil {
			return err
		}
		err = h.FundAccount(ctx, rfr.Balance, &revision)
		if isMaxBalanceExceeded(err) {
			// sync the account
			err = h.SyncAccount(ctx, &revision)
			if err != nil {
				w.logger.Errorw(fmt.Sprintf("failed to sync account: %v", err), "host", rfr.HostKey)
				return
			}

			// try funding the account again
			err = h.FundAccount(ctx, rfr.Balance, &revision)
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
		value, err = RPCReadRegistry(jc.Request.Context(), t, &rrrr.Payment, rrrr.RegistryKey)
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
	// TODO: refactor to a w.RegistryUpdate method that calls host.RegistryUpdate.
	payment := preparePayment(w.accounts.deriveAccountKey(rrur.HostKey), cost, pt.HostBlockHeight)
	err := w.transportPoolV3.withTransportV3(jc.Request.Context(), rrur.HostKey, rrur.SiamuxAddr, func(t *transportV3) (err error) {
		return RPCUpdateRegistry(jc.Request.Context(), t, &payment, rrur.RegistryKey, rrur.RegistryValue)
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
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// sync the account
	h, err := w.newHostV3(ctx, rsr.ContractID, rsr.HostKey, rsr.SiamuxAddr)
	if jc.Check("failed to create host for renewal", err) != nil {
		return
	}
	jc.Check("couldn't sync account", w.withRevision(ctx, defaultRevisionFetchTimeout, rsr.ContractID, rsr.HostKey, rsr.SiamuxAddr, lockingPrioritySyncing, up.CurrentHeight, func(revision types.FileContractRevision) error {
		return h.SyncAccount(ctx, &revision)
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
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// attach contract spending recorder to the context.
	ctx = WithContractSpendingRecorder(ctx, w.contractSpendingRecorder)

	// fetch all contracts
	dlContracts, err := w.bus.Contracts(ctx)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	// update uploader contracts
	ulContracts, err := w.bus.ContractSetContracts(ctx, up.ContractSet)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}
	w.uploader.update(ulContracts, up.CurrentHeight)

	err = migrateSlab(ctx, w.uploader, w, &slab, dlContracts, ulContracts, w, w.downloadSectorTimeout, w.uploadSectorTimeout, w.logger)
	if jc.Check("couldn't migrate slabs", err) != nil {
		return
	}

	usedContracts := make(map[types.PublicKey]types.FileContractID)
	for _, ss := range slab.Shards {
		if _, exists := usedContracts[ss.Host]; exists {
			continue
		}

		for _, c := range ulContracts {
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

func (w *worker) uploadsStatshandlerGET(jc jape.Context) {
	stats := w.uploader.Stats()

	estimates := make([]api.HostEstimate, 0, 10)
	for _, stat := range stats.topTenHosts {
		estimates = append(estimates, api.HostEstimate{HK: stat.hk, Estimate: stat.estimate})
	}

	jc.Encode(api.UploadStatsResponse{
		OverdrivePct:   math.Floor(stats.overdrivePct*100*100) / 100,
		QueuesHealthy:  stats.queuesHealthy,
		QueuesSpeedAvg: stats.queuesSpeedAvg,
		QueuesTotal:    stats.queuesTotal,
		TopTenHosts:    estimates,
	})
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

	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("couldn't fetch gouging parameters from bus", err) != nil {
		return
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, gp)

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

	// fetch all contracts
	contracts, err := w.bus.Contracts(ctx)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	// build contract map
	availableContracts := make(map[types.PublicKey]api.ContractMetadata)
	for _, contract := range contracts {
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
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// update uploader contracts
	contracts, err := w.bus.ContractSetContracts(ctx, up.ContractSet)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}
	w.uploader.update(contracts, up.CurrentHeight)

	// upload the object
	object, err := w.uploader.upload(ctx, jc.Request.Body, rs)
	if jc.Check("couldn't upload object", err) != nil {
		return
	}

	// build used contracts map
	h2c := make(map[types.PublicKey]types.FileContractID)
	for _, c := range contracts {
		h2c[c.HostKey] = c.ID
	}
	used := make(map[types.PublicKey]types.FileContractID)
	for _, s := range object.Slabs {
		for _, ss := range s.Shards {
			used[ss.Host] = h2c[ss.Host]
		}
	}

	// persist the object
	if jc.Check("couldn't add object", w.bus.AddObject(ctx, path, object, used)) != nil {
		return
	}
}

func (w *worker) objectsHandlerDELETE(jc jape.Context) {
	jc.Check("couldn't delete object", w.bus.DeleteObject(jc.Request.Context(), jc.PathParam("path")))
}

func (w *worker) rhpContractsHandlerGET(jc jape.Context) {
	ctx := jc.Request.Context()
	busContracts, err := w.bus.Contracts(ctx)
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

	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	ctx = WithGougingChecker(ctx, w.bus, gp)

	contracts, errs := w.fetchContracts(ctx, busContracts, hosttimeout, gp.ConsensusState.BlockHeight)
	resp := api.ContractsResponse{Contracts: contracts}
	if errs != nil {
		resp.Error = errs.Error()
	}
	jc.Encode(resp)
}

func preparePayment(accountKey types.PrivateKey, amt types.Currency, blockHeight uint64) rhpv3.PayByEphemeralAccountRequest {
	return rhpv3.PayByEphemeralAccount(rhpv3.Account(accountKey.PublicKey()), amt, blockHeight+6, accountKey) // 1 hour valid
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
func New(masterKey [32]byte, id string, b Bus, contractLockingDuration, sessionLockTimeout, sessionReconectTimeout, sessionTTL, busFlushInterval, downloadSectorTimeout, uploadSectorTimeout time.Duration, maxDownloadOverdrive, maxUploadOverdrive uint64, allowPrivateIPs bool, l *zap.Logger) (*worker, error) {
	if contractLockingDuration == 0 {
		return nil, errors.New("contract lock duration must be positive")
	}
	if sessionLockTimeout == 0 {
		return nil, errors.New("session lock timeout must be positive")
	}
	if sessionReconectTimeout == 0 {
		return nil, errors.New("session reconnect timeout must be positive")
	}
	if sessionTTL == 0 {
		return nil, errors.New("session TTL must be positive")
	}
	if busFlushInterval == 0 {
		return nil, errors.New("bus flush interval must be positive")
	}
	if downloadSectorTimeout == 0 {
		return nil, errors.New("download sector timeout must be positive")
	}
	if uploadSectorTimeout == 0 {
		return nil, errors.New("upload sector timeout must be positive")
	}

	w := &worker{
		allowPrivateIPs:         allowPrivateIPs,
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
	w.initUploader()
	return w, nil
}

// Handler returns an HTTP handler that serves the worker API.
func (w *worker) Handler() http.Handler {
	return jape.Mux(tracing.TracedRoutes("worker", map[string]jape.Handler{
		"GET    /account/:hostkey": w.accountHandlerGET,
		"GET    /id":               w.idHandlerGET,

		"GET    /rhp/contracts":       w.rhpContractsHandlerGET,
		"POST   /rhp/scan":            w.rhpScanHandler,
		"POST   /rhp/form":            w.rhpFormHandler,
		"POST   /rhp/renew":           w.rhpRenewHandler,
		"POST   /rhp/fund":            w.rhpFundHandler,
		"POST   /rhp/sync":            w.rhpSyncHandler,
		"POST   /rhp/pricetable":      w.rhpPriceTableHandler,
		"POST   /rhp/registry/read":   w.rhpRegistryReadHandler,
		"POST   /rhp/registry/update": w.rhpRegistryUpdateHandler,

		"POST   /slab/migrate": w.slabMigrateHandler,

		"GET    /stats/uploads": w.uploadsStatshandlerGET,

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

	// Stop the uploader.
	w.uploader.Stop()
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

	stopCtx       context.Context
	stopCtxCancel context.CancelFunc
}

func newContractLock(fcid types.FileContractID, lockID uint64, d time.Duration, locker ContractLocker, logger *zap.SugaredLogger) *contractLock {
	ctx, cancel := context.WithCancel(context.Background())
	return &contractLock{
		lockID: lockID,
		fcid:   fcid,
		d:      d,
		locker: locker,
		logger: logger,

		stopCtx:       ctx,
		stopCtxCancel: cancel,
	}
}

func (cl *contractLock) Release(ctx context.Context) error {
	// Stop background loop.
	cl.stopCtxCancel()

	// Release the contract.
	return cl.locker.ReleaseContract(ctx, cl.fcid, cl.lockID)
}

func (cl *contractLock) keepaliveLoop() {
	// Create ticker for half the duration of the lock.
	t := time.NewTicker(cl.d / 5)

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
		case <-cl.stopCtx.Done():
			return // released
		case <-t.C:
		}
		if err := cl.locker.KeepaliveContract(cl.stopCtx, cl.fcid, cl.lockID, cl.d); err != nil && !errors.Is(err, context.Canceled) {
			cl.logger.Errorw(fmt.Sprintf("failed to send keepalive: %v", err), "contract", cl.fcid, "lockID", cl.lockID)
			return
		}
	}
}

func (w *worker) acquireRevision(ctx context.Context, fcid types.FileContractID, priority int) (_ revisionUnlocker, err error) {
	lockID, err := w.bus.AcquireContract(ctx, fcid, priority, w.contractLockingDuration)
	if err != nil {
		return nil, err
	}
	cl := newContractLock(fcid, lockID, w.contractLockingDuration, w.bus, w.logger)
	go cl.keepaliveLoop()
	return cl, nil
}

func (w *worker) scanHost(ctx context.Context, hostKey types.PublicKey, hostIP string) (rhpv2.HostSettings, rhpv3.HostPriceTable, time.Duration, error) {
	// resolve hostIP. We don't want to scan hosts on private networks.
	if !w.allowPrivateIPs {
		addrs, err := (&net.Resolver{}).LookupAddr(ctx, hostIP)
		if err != nil {
			return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, err
		}
		for _, addr := range addrs {
			if isPrivateIP(net.ParseIP(addr)) {
				return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, errors.New("host is on a private network")
			}
		}
	}

	// fetch the host settings
	start := time.Now()
	var settings rhpv2.HostSettings
	err := w.withTransportV2(ctx, hostKey, hostIP, func(t *rhpv2.Transport) (err error) {
		if settings, err = RPCSettings(ctx, t); err == nil {
			// NOTE: we overwrite the NetAddress with the host address here since we
			// just used it to dial the host we know it's valid
			settings.NetAddress = hostIP
		}
		return err
	})
	elapsed := time.Since(start)

	// fetch the host pricetable
	var pt rhpv3.HostPriceTable
	if err == nil {
		err = w.transportPoolV3.withTransportV3(ctx, hostKey, settings.SiamuxAddr(), func(t *transportV3) (err error) {
			pt, err = RPCPriceTable(ctx, t, func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) { return nil, nil })
			return err
		})
	}
	return settings, pt, elapsed, err
}

func discardTxnOnErr(ctx context.Context, bus Bus, l *zap.SugaredLogger, txn types.Transaction, errContext string, err *error) {
	if *err == nil {
		return
	}
	_, span := tracing.Tracer.Start(ctx, "discardTxn")
	defer span.End()
	// Attach the span to a new context derived from the background context.
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	timeoutCtx = trace.ContextWithSpan(timeoutCtx, span)
	if err := bus.WalletDiscard(timeoutCtx, txn); err != nil {
		l.Errorf("%v: failed to discard txn: %v", err)
	}
}

func isErrDuplicateTransactionSet(err error) bool {
	return err != nil && strings.Contains(err.Error(), modules.ErrDuplicateTransactionSet.Error())
}

func isPrivateIP(addr net.IP) bool {
	if addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() {
		return true
	}

	for _, block := range privateSubnets {
		if block.Contains(addr) {
			return true
		}
	}
	return false
}
