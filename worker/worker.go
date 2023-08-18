package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	"go.sia.tech/renterd/alerts"
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
	batchSizeFetchSectors = uint64(130000) // ~4MiB of roots

	defaultRevisionFetchTimeout = 30 * time.Second

	lockingPriorityActiveContractRevision = 100 // highest
	lockingPriorityRenew                  = 80
	lockingPriorityPriceTable             = 60
	lockingPriorityFunding                = 40
	lockingPrioritySyncing                = 20
	lockingPriorityUpload                 = 1 // lowest
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
	alerts.Alerter
	consensusState

	AccountStore
	ContractLocker

	SyncerPeers(ctx context.Context) (resp []string, err error)

	BroadcastTransaction(ctx context.Context, txns []types.Transaction) error

	Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
	ContractRoots(ctx context.Context, id types.FileContractID) ([]types.Hash256, []types.Hash256, error)
	Contracts(ctx context.Context) ([]api.ContractMetadata, error)
	ContractSetContracts(ctx context.Context, set string) ([]api.ContractMetadata, error)
	RecordInteractions(ctx context.Context, interactions []hostdb.Interaction) error
	RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error
	RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error)

	Host(ctx context.Context, hostKey types.PublicKey) (hostdb.HostInfo, error)

	GougingParams(ctx context.Context) (api.GougingParams, error)
	UploadParams(ctx context.Context) (api.UploadParams, error)

	Object(ctx context.Context, path string, opts ...api.ObjectsOption) (api.Object, []api.ObjectMetadata, error)
	AddObject(ctx context.Context, path, contractSet string, o object.Object, usedContracts map[types.PublicKey]types.FileContractID) error
	DeleteObject(ctx context.Context, path string, batch bool) error

	MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab, usedContracts map[types.PublicKey]types.FileContractID) error
	PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) ([]api.PackedSlab, error)

	Accounts(ctx context.Context) ([]api.Account, error)
	UpdateSlab(ctx context.Context, s object.Slab, contractSet string, goodContracts map[types.PublicKey]types.FileContractID) error

	TrackUpload(ctx context.Context, uID api.UploadID) error
	AddUploadingSector(ctx context.Context, uID api.UploadID, id types.FileContractID, root types.Hash256) error
	FinishUpload(ctx context.Context, uID api.UploadID) error

	WalletDiscard(ctx context.Context, txn types.Transaction) error
	WalletFund(ctx context.Context, txn *types.Transaction, amount types.Currency) ([]types.Hash256, []types.Transaction, error)
	WalletPrepareForm(ctx context.Context, renterAddress types.Address, renterKey types.PublicKey, renterFunds, hostCollateral types.Currency, hostKey types.PublicKey, hostSettings rhpv2.HostSettings, endHeight uint64) (txns []types.Transaction, err error)
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

type hostV2 interface {
	Contract() types.FileContractID
	HostKey() types.PublicKey
}

type hostV3 interface {
	hostV2

	DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32) error
	FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error)
	FetchRevision(ctx context.Context, fetchTimeout time.Duration, blockHeight uint64) (types.FileContractRevision, error)
	FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error
	Renew(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, err error)
	SyncAccount(ctx context.Context, rev *types.FileContractRevision) error
	UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) (types.Hash256, error)
}

type hostProvider interface {
	newHostV3(types.FileContractID, types.PublicKey, string) hostV3
}

// A worker talks to Sia hosts to perform contract and storage operations within
// a renterd system.
type worker struct {
	allowPrivateIPs bool
	id              string
	bus             Bus
	masterKey       [32]byte

	downloadManager *downloadManager
	uploadManager   *uploadManager

	accounts    *accounts
	priceTables *priceTables

	busFlushInterval time.Duration

	interactionsMu         sync.Mutex
	interactions           []hostdb.Interaction
	interactionsFlushTimer *time.Timer

	contractSpendingRecorder *contractSpendingRecorder
	contractLockingDuration  time.Duration

	transportPoolV3 *transportPoolV3
	logger          *zap.SugaredLogger
}

func dial(ctx context.Context, hostIP string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", hostIP)
	return conn, err
}

func (w *worker) withTransportV2(ctx context.Context, hostKey types.PublicKey, hostIP string, fn func(*rhpv2.Transport) error) (err error) {
	var mr ephemeralMetricsRecorder
	defer func() {
		w.recordInteractions(mr.interactions())
	}()
	ctx = metrics.WithRecorder(ctx, &mr)
	conn, err := dial(ctx, hostIP)
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

func (w *worker) newHostV3(contractID types.FileContractID, hostKey types.PublicKey, siamuxAddr string) hostV3 {
	return &host{
		acc:                      w.accounts.ForHost(hostKey),
		bus:                      w.bus,
		contractSpendingRecorder: w.contractSpendingRecorder,
		mr:                       &ephemeralMetricsRecorder{},
		logger:                   w.logger.Named(hostKey.String()[:4]),
		fcid:                     contractID,
		siamuxAddr:               siamuxAddr,
		renterKey:                w.deriveRenterKey(hostKey),
		accountKey:               w.accounts.deriveAccountKey(hostKey),
		transportPool:            w.transportPoolV3,
		priceTables:              w.priceTables,
	}
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

	h := w.newHostV3(contractID, hk, siamuxAddr)
	rev, err := h.FetchRevision(ctx, fetchTimeout, blockHeight)
	if err != nil {
		return err
	}

	return fn(rev)
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

	// only scan hosts if we are online
	peers, err := w.bus.SyncerPeers(jc.Request.Context())
	if jc.Check("failed to fetch peers from bus", err) != nil {
		return
	}
	if len(peers) == 0 {
		jc.Error(errors.New("not connected to the internet"), http.StatusServiceUnavailable)
		return
	}

	// scan host
	var errStr string
	settings, priceTable, elapsed, err := w.scanHost(ctx, rsr.HostKey, rsr.HostIP)
	if err != nil {
		errStr = err.Error()
	}

	// record scan
	var mr ephemeralMetricsRecorder
	recordScan(&mr, elapsed, rsr.HostIP, rsr.HostKey, priceTable, settings, err)
	w.recordInteractions(mr.interactions())

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
	for t := 0; t < 20 && t < len(metadatas); t++ {
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
	h := w.newHostV3(types.FileContractID{}, hk, siamuxAddr)
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

	ctx := jc.Request.Context()
	if rptr.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(jc.Request.Context(), rptr.Timeout)
		defer cancel()
	}

	var pt rhpv3.HostPriceTable
	if jc.Check("could not get price table", w.transportPoolV3.withTransportV3(ctx, rptr.HostKey, rptr.SiamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
		pt, err = RPCPriceTable(ctx, t, func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) { return nil, nil })
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

		renterTxnSet, err := w.bus.WalletPrepareForm(ctx, renterAddress, renterKey.PublicKey(), renterFunds, hostCollateral, hostKey, hostSettings, endHeight)
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
		w.logger.Errorf("failed to broadcast formation txn set: %v", err)
	}

	jc.Encode(api.RHPFormResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (w *worker) rhpBroadcastHandler(jc jape.Context) {
	var fcid types.FileContractID
	if jc.DecodeParam("id", &fcid) != nil {
		return
	}

	// Fetch contract from bus.
	ctx := jc.Request.Context()
	c, err := w.bus.Contract(ctx, fcid)
	if jc.Check("could not get contract", err) != nil {
		return
	}
	rk := w.deriveRenterKey(c.HostKey)

	rev, err := w.FetchSignedRevision(ctx, c.HostIP, c.HostKey, rk, fcid, time.Minute)
	if jc.Check("could not fetch revision", err) != nil {
		return
	}
	// Create txn with revision.
	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{rev.Revision},
		Signatures:            rev.Signatures[:],
	}
	// Fund the txn. We pass 0 here since we only need the wallet to fund
	// the fee.
	toSign, parents, err := w.bus.WalletFund(ctx, &txn, types.ZeroCurrency)
	if jc.Check("failed to fund transaction", err) != nil {
		return
	}
	// Sign the txn.
	err = w.bus.WalletSign(ctx, &txn, toSign, types.CoveredFields{
		WholeTransaction: true,
	})
	if jc.Check("failed to sign transaction", err) != nil {
		_ = w.bus.WalletDiscard(ctx, txn)
		return
	}
	// Broadcast the txn.
	txnSet := append(parents, txn)
	err = w.bus.BroadcastTransaction(ctx, txnSet)
	if jc.Check("failed to broadcast transaction", err) != nil {
		_ = w.bus.WalletDiscard(ctx, txn)
		return
	}
}

func (w *worker) rhpContractRootsHandlerGET(jc jape.Context) {
	// decode fcid
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	// fetch the contract from the bus
	ctx := jc.Request.Context()
	c, err := w.bus.Contract(ctx, id)
	if errors.Is(err, api.ErrContractNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("couldn't fetch contract", err) != nil {
		return
	}

	// fetch the roots from the host
	renterKey := w.deriveRenterKey(c.HostKey)
	roots, err := w.FetchContractRoots(ctx, c.HostIP, c.HostKey, renterKey, id, c.RevisionNumber, time.Minute)
	if jc.Check("couldn't fetch contract roots from host", err) == nil {
		jc.Encode(roots)
	}
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
	cs, err := w.bus.ConsensusState(ctx)
	if jc.Check("could not get consensus state", err) != nil {
		return
	}
	ctx = WithGougingChecker(ctx, w.bus, gp)

	// renew the contract
	var renewed rhpv2.ContractRevision
	var txnSet []types.Transaction
	if jc.Check("couldn't renew contract", w.withRevision(ctx, defaultRevisionFetchTimeout, rrr.ContractID, rrr.HostKey, rrr.SiamuxAddr, lockingPriorityRenew, cs.BlockHeight, func(_ types.FileContractRevision) (err error) {
		h := w.newHostV3(rrr.ContractID, rrr.HostKey, rrr.SiamuxAddr)
		renewed, txnSet, err = h.Renew(ctx, rrr)
		return err
	})) != nil {
		return
	}

	// broadcast the transaction set
	err = w.bus.BroadcastTransaction(jc.Request.Context(), txnSet)
	if err != nil && !isErrDuplicateTransactionSet(err) {
		w.logger.Errorf("failed to broadcast renewal txn set: %v", err)
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

	// fund the account
	jc.Check("couldn't fund account", w.withRevision(ctx, defaultRevisionFetchTimeout, rfr.ContractID, rfr.HostKey, rfr.SiamuxAddr, lockingPriorityFunding, gp.ConsensusState.BlockHeight, func(rev types.FileContractRevision) (err error) {
		h := w.newHostV3(rev.ParentID, rfr.HostKey, rfr.SiamuxAddr)
		err = h.FundAccount(ctx, rfr.Balance, &rev)
		if isBalanceMaxExceeded(err) {
			// sync the account
			err = h.SyncAccount(ctx, &rev)
			if err != nil {
				w.logger.Debugf(fmt.Sprintf("failed to sync account: %v", err), "host", rfr.HostKey)
				return
			}

			// try funding the account again
			err = h.FundAccount(ctx, rfr.Balance, &rev)
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
	err := w.transportPoolV3.withTransportV3(jc.Request.Context(), rrrr.HostKey, rrrr.SiamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
		value, err = RPCReadRegistry(ctx, t, &rrrr.Payment, rrrr.RegistryKey)
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
	err := w.transportPoolV3.withTransportV3(jc.Request.Context(), rrur.HostKey, rrur.SiamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
		return RPCUpdateRegistry(ctx, t, &payment, rrur.RegistryKey, rrur.RegistryValue)
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
	h := w.newHostV3(rsr.ContractID, rsr.HostKey, rsr.SiamuxAddr)
	jc.Check("couldn't sync account", w.withRevision(ctx, defaultRevisionFetchTimeout, rsr.ContractID, rsr.HostKey, rsr.SiamuxAddr, lockingPrioritySyncing, up.CurrentHeight, func(rev types.FileContractRevision) error {
		return h.SyncAccount(ctx, &rev)
	}))
}

func (w *worker) slabMigrateHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode the slab
	var slab object.Slab
	if jc.Decode(&slab) != nil {
		return
	}

	// fetch the upload parameters
	up, err := w.bus.UploadParams(ctx)
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}

	// NOTE: migrations do not use the default contract set but instead require
	// the user to specify the contract set through the query string parameter,
	// this to avoid accidentally migration to the default set if the autopilot
	// configuration is missing a contract set
	up.ContractSet = ""

	// decode the contract set from the query string
	var contractset string
	if jc.DecodeForm("contractset", &contractset) != nil {
		return
	} else if contractset != "" {
		up.ContractSet = contractset
	}

	// cancel the migration if no contract set is specified
	if up.ContractSet == "" {
		jc.Error(fmt.Errorf("migrations require the contract set to be passed as a query string parameter; %w", api.ErrContractSetNotSpecified), http.StatusBadRequest)
		return
	}

	// cancel the upload if consensus is not synced
	if !up.ConsensusState.Synced {
		w.logger.Errorf("migration cancelled, err: %v", api.ErrConsensusNotSynced)
		jc.Error(api.ErrConsensusNotSynced, http.StatusServiceUnavailable)
		return
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// fetch all contracts
	dlContracts, err := w.bus.Contracts(ctx)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	// fetch upload contracts
	ulContracts, err := w.bus.ContractSetContracts(ctx, up.ContractSet)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	err = migrateSlab(ctx, w.downloadManager, w.uploadManager, &slab, dlContracts, ulContracts, up.CurrentHeight, w.logger)
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

	if jc.Check("couldn't update slab", w.bus.UpdateSlab(ctx, slab, up.ContractSet, usedContracts)) != nil {
		return
	}
}

func (w *worker) downloadsStatsHandlerGET(jc jape.Context) {
	stats := w.downloadManager.Stats()

	// prepare downloaders stats
	var healthy uint64
	var dss []api.DownloaderStats
	for hk, stat := range stats.downloaders {
		if stat.healthy {
			healthy++
		}
		dss = append(dss, api.DownloaderStats{
			HostKey:                    hk,
			AvgSectorDownloadSpeedMBPS: stat.avgSpeedMBPS,
			NumDownloads:               stat.numDownloads,
		})
	}
	sort.SliceStable(dss, func(i, j int) bool {
		return dss[i].AvgSectorDownloadSpeedMBPS > dss[j].AvgSectorDownloadSpeedMBPS
	})

	// encode response
	jc.Encode(api.DownloadStatsResponse{
		AvgDownloadSpeedMBPS: math.Ceil(stats.avgDownloadSpeedMBPS*100) / 100,
		AvgOverdrivePct:      math.Floor(stats.avgOverdrivePct*100*100) / 100,
		HealthyDownloaders:   healthy,
		NumDownloaders:       uint64(len(stats.downloaders)),
		DownloadersStats:     dss,
	})
}

func (w *worker) uploadsStatsHandlerGET(jc jape.Context) {
	stats := w.uploadManager.Stats()

	// prepare upload stats
	var uss []api.UploaderStats
	for hk, mbps := range stats.uploadSpeedsMBPS {
		uss = append(uss, api.UploaderStats{
			HostKey:                  hk,
			AvgSectorUploadSpeedMBPS: mbps,
		})
	}
	sort.SliceStable(uss, func(i, j int) bool {
		return uss[i].AvgSectorUploadSpeedMBPS > uss[j].AvgSectorUploadSpeedMBPS
	})

	// encode response
	jc.Encode(api.UploadStatsResponse{
		AvgSlabUploadSpeedMBPS: math.Ceil(stats.avgSlabUploadSpeedMBPS*100) / 100,
		AvgOverdrivePct:        math.Floor(stats.avgOverdrivePct*100*100) / 100,
		HealthyUploaders:       stats.healthyUploaders,
		NumUploaders:           stats.numUploaders,
		UploadersStats:         uss,
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

	path := jc.PathParam("path")
	obj, entries, err := w.bus.Object(ctx, path, api.ObjectsWithPrefix(prefix), api.ObjectsWithOffset(off), api.ObjectsWithLimit(limit))
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
	if len(obj.Slabs) == 0 && obj.PartialSlab == nil {
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
	ranges, err := http_range.ParseRange(jc.Request.Header.Get("Range"), obj.Size)
	if err != nil {
		jc.Error(err, http.StatusRequestedRangeNotSatisfiable)
		return
	}
	var offset int64
	length := obj.Size
	status := http.StatusOK
	if len(ranges) > 0 {
		status = http.StatusPartialContent
		jc.ResponseWriter.Header().Set("Content-Range", ranges[0].ContentRange(obj.Size))
		offset, length = ranges[0].Start, ranges[0].Length
		if offset < 0 || length < 0 || offset+length > obj.Size {
			jc.Error(fmt.Errorf("invalid range: %v %v", offset, length), http.StatusRequestedRangeNotSatisfiable)
			return
		}
	}
	jc.ResponseWriter.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	jc.ResponseWriter.Header().Set("Accept-Ranges", "bytes")
	if ext := filepath.Ext(path); ext != "" {
		if mimeType := mime.TypeByExtension(ext); mimeType != "" {
			jc.ResponseWriter.Header().Set("Content-Type", mimeType)
		}
	}
	rw := rangedResponseWriter{rw: jc.ResponseWriter, defaultStatusCode: status}

	// fetch all contracts
	contracts, err := w.bus.Contracts(ctx)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	// download the object
	if jc.Check(fmt.Sprintf("couldn't download object '%v'", path), w.downloadManager.DownloadObject(ctx, &rw, obj.Object, uint64(offset), uint64(length), contracts)) != nil {
		return
	}
}

func (w *worker) objectsHandlerPUT(jc jape.Context) {
	jc.Custom((*[]byte)(nil), nil)
	ctx := jc.Request.Context()

	// fetch the upload parameters
	up, err := w.bus.UploadParams(ctx)
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}

	// decode the contract set from the query string
	var contractset string
	if jc.DecodeForm("contractset", &contractset) != nil {
		return
	} else if contractset != "" {
		up.ContractSet = contractset
	}

	// cancel the upload if no contract set is specified
	if up.ContractSet == "" {
		jc.Error(api.ErrContractSetNotSpecified, http.StatusBadRequest)
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
	if jc.DecodeForm("minshards", &rs.MinShards) != nil {
		return
	}
	if jc.DecodeForm("totalshards", &rs.TotalShards) != nil {
		return
	}
	if jc.Check("invalid redundancy settings", rs.Validate()) != nil {
		return
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// update uploader contracts
	contracts, err := w.bus.ContractSetContracts(ctx, up.ContractSet)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	// upload the object
	obj, err := w.uploadManager.Upload(ctx, jc.Request.Body, rs, contracts, up.CurrentHeight, up.UploadPacking)
	if jc.Check("couldn't upload object", err) != nil {
		return
	}

	// build used contracts map
	h2c := make(map[types.PublicKey]types.FileContractID)
	for _, c := range contracts {
		h2c[c.HostKey] = c.ID
	}
	used := make(map[types.PublicKey]types.FileContractID)
	for _, s := range obj.Slabs {
		for _, ss := range s.Shards {
			used[ss.Host] = h2c[ss.Host]
		}
	}

	// persist the object
	if jc.Check("couldn't add object", w.bus.AddObject(ctx, jc.PathParam("path"), up.ContractSet, obj, used)) != nil {
		return
	}

	// if partial uploads are not enabled we are done.
	if !up.UploadPacking {
		return
	}

	// if partial uploads are enabled, check whether we have a full slab now
	packedSlabs, err := w.bus.PackedSlabsForUpload(jc.Request.Context(), 5*time.Minute, uint8(rs.MinShards), uint8(rs.TotalShards), up.ContractSet, 2)
	if jc.Check("couldn't fetch packed slabs from bus", err) != nil {
		return
	}

	for _, ps := range packedSlabs {
		// upload packed slab.
		key := object.GenerateEncryptionKey()
		shards := (&object.PartialSlab{
			MinShards:   uint8(rs.MinShards),
			TotalShards: uint8(rs.TotalShards),
			Data:        ps.Data,
		}).Encrypt(key)

		sectors, err := w.uploadManager.Migrate(ctx, shards, contracts, up.CurrentHeight)
		if jc.Check("couldn't upload packed slab", err) != nil {
			return
		}
		slab := object.Slab{
			Key:       key,
			MinShards: uint8(rs.MinShards),
			Shards:    sectors,
		}

		// build used contracts map
		h2c := make(map[types.PublicKey]types.FileContractID)
		for _, c := range contracts {
			h2c[c.HostKey] = c.ID
		}
		used := make(map[types.PublicKey]types.FileContractID)
		for _, ss := range slab.Shards {
			used[ss.Host] = h2c[ss.Host]
		}

		// mark packed slab as uploaded.
		err = w.bus.MarkPackedSlabsUploaded(jc.Request.Context(), []api.UploadedPackedSlab{
			{
				BufferID: ps.BufferID,
				Key:      slab.Key,
				Shards:   sectors,
			},
		}, used)
		if jc.Check("couldn't mark packed slabs uploaded", err) != nil {
			return
		}
	}
}

func (w *worker) objectsHandlerDELETE(jc jape.Context) {
	var batch bool
	if jc.DecodeForm("batch", &batch) != nil {
		return
	}
	err := w.bus.DeleteObject(jc.Request.Context(), jc.PathParam("path"), batch)
	if err != nil && strings.Contains(err.Error(), api.ErrObjectNotFound.Error()) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	jc.Check("couldn't delete object", err)
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
func New(masterKey [32]byte, id string, b Bus, contractLockingDuration, busFlushInterval, downloadOverdriveTimeout, uploadOverdriveTimeout time.Duration, downloadMaxOverdrive, uploadMaxOverdrive uint64, allowPrivateIPs bool, l *zap.Logger) (*worker, error) {
	if contractLockingDuration == 0 {
		return nil, errors.New("contract lock duration must be positive")
	}
	if busFlushInterval == 0 {
		return nil, errors.New("bus flush interval must be positive")
	}
	if downloadOverdriveTimeout == 0 {
		return nil, errors.New("download overdrive timeout must be positive")
	}
	if uploadOverdriveTimeout == 0 {
		return nil, errors.New("upload overdrive timeout must be positive")
	}

	w := &worker{
		allowPrivateIPs:         allowPrivateIPs,
		contractLockingDuration: contractLockingDuration,
		id:                      id,
		bus:                     b,
		masterKey:               masterKey,
		busFlushInterval:        busFlushInterval,
		logger:                  l.Sugar().Named("worker").Named(id),
	}
	w.initTransportPool()
	w.initAccounts(b)
	w.initContractSpendingRecorder()
	w.initPriceTables()
	w.initDownloadManager(downloadMaxOverdrive, downloadOverdriveTimeout, l.Sugar().Named("downloadmanager"))
	w.initUploadManager(uploadMaxOverdrive, uploadOverdriveTimeout, l.Sugar().Named("uploadmanager"))
	return w, nil
}

// Handler returns an HTTP handler that serves the worker API.
func (w *worker) Handler() http.Handler {
	return jape.Mux(tracing.TracedRoutes("worker", map[string]jape.Handler{
		"GET    /account/:hostkey": w.accountHandlerGET,
		"GET    /id":               w.idHandlerGET,

		"GET    /rhp/contracts":              w.rhpContractsHandlerGET,
		"POST   /rhp/contract/:id/broadcast": w.rhpBroadcastHandler,
		"GET    /rhp/contract/:id/roots":     w.rhpContractRootsHandlerGET,
		"POST   /rhp/scan":                   w.rhpScanHandler,
		"POST   /rhp/form":                   w.rhpFormHandler,
		"POST   /rhp/renew":                  w.rhpRenewHandler,
		"POST   /rhp/fund":                   w.rhpFundHandler,
		"POST   /rhp/sync":                   w.rhpSyncHandler,
		"POST   /rhp/pricetable":             w.rhpPriceTableHandler,
		"POST   /rhp/registry/read":          w.rhpRegistryReadHandler,
		"POST   /rhp/registry/update":        w.rhpRegistryUpdateHandler,

		"GET    /stats/downloads": w.downloadsStatsHandlerGET,
		"GET    /stats/uploads":   w.uploadsStatsHandlerGET,
		"POST   /slab/migrate":    w.slabMigrateHandler,

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

	// Stop the downloader.
	w.downloadManager.Stop()

	// Stop the uploader.
	w.uploadManager.Stop()
	return nil
}

type contractLock struct {
	lockID uint64
	fcid   types.FileContractID
	d      time.Duration
	locker ContractLocker
	logger *zap.SugaredLogger

	stopCtx       context.Context
	stopCtxCancel context.CancelFunc
	stopWG        sync.WaitGroup
}

func newContractLock(fcid types.FileContractID, lockID uint64, d time.Duration, locker ContractLocker, logger *zap.SugaredLogger) *contractLock {
	ctx, cancel := context.WithCancel(context.Background())
	cl := &contractLock{
		lockID: lockID,
		fcid:   fcid,
		d:      d,
		locker: locker,
		logger: logger,

		stopCtx:       ctx,
		stopCtxCancel: cancel,
	}
	cl.stopWG.Add(1)
	go func() {
		cl.keepaliveLoop()
		cl.stopWG.Done()
	}()
	return cl
}

func (cl *contractLock) Release(ctx context.Context) error {
	// Stop background loop.
	cl.stopCtxCancel()
	cl.stopWG.Wait()

	// Release the contract.
	return cl.locker.ReleaseContract(ctx, cl.fcid, cl.lockID)
}

func (cl *contractLock) keepaliveLoop() {
	// Create ticker for 20% of the lock duration.
	start := time.Now()
	var lastUpdate time.Time
	tickDuration := cl.d / 5
	t := time.NewTicker(tickDuration)

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
			cl.logger.Errorw(fmt.Sprintf("failed to send keepalive: %v", err),
				"contract", cl.fcid,
				"lockID", cl.lockID,
				"loopStart", start,
				"timeSinceLastUpdate", time.Since(lastUpdate),
				"tickDuration", tickDuration)
			return
		}
		lastUpdate = time.Now()
	}
}

func (w *worker) acquireRevision(ctx context.Context, fcid types.FileContractID, priority int) (_ revisionUnlocker, err error) {
	lockID, err := w.bus.AcquireContract(ctx, fcid, priority, w.contractLockingDuration)
	if err != nil {
		return nil, err
	}
	return newContractLock(fcid, lockID, w.contractLockingDuration, w.bus, w.logger), nil
}

func (w *worker) scanHost(ctx context.Context, hostKey types.PublicKey, hostIP string) (rhpv2.HostSettings, rhpv3.HostPriceTable, time.Duration, error) {
	// resolve hostIP. We don't want to scan hosts on private networks.
	if !w.allowPrivateIPs {
		host, _, err := net.SplitHostPort(hostIP)
		if err != nil {
			return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, err
		}
		addrs, err := (&net.Resolver{}).LookupIPAddr(ctx, host)
		if err != nil {
			return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, err
		}
		for _, addr := range addrs {
			if isPrivateIP(addr.IP) {
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
		err = w.transportPoolV3.withTransportV3(ctx, hostKey, settings.SiamuxAddr(), func(ctx context.Context, t *transportV3) (err error) {
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
