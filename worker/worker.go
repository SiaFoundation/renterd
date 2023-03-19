package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/tracing"
	"go.sia.tech/renterd/metrics"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
	"lukechampine.com/frand"
)

const (
	lockingPriorityRenew   = 100 // highest
	lockingPriorityFunding = 90

	lockingDurationRenew   = time.Minute
	lockingDurationFunding = 30 * time.Second

	queryStringParamContractSet = "contractset"
	queryStringParamMinShards   = "minshards"
	queryStringParamTotalShards = "totalshards"
)

// parseRange parses a Range header string as per RFC 7233. Only the first range
// is returned. If no range is specified, parseRange returns 0, size.
func parseRange(s string, size int64) (offset, length int64, _ error) {
	if s == "" {
		return 0, size, nil
	}
	const b = "bytes="
	if !strings.HasPrefix(s, b) {
		return 0, 0, errors.New("invalid range")
	}
	rs := strings.Split(s[len(b):], ",")
	if len(rs) == 0 {
		return 0, 0, errors.New("invalid range")
	}
	ra := strings.TrimSpace(rs[0])
	if ra == "" {
		return 0, 0, errors.New("invalid range")
	}
	i := strings.Index(ra, "-")
	if i < 0 {
		return 0, 0, errors.New("invalid range")
	}
	start, end := strings.TrimSpace(ra[:i]), strings.TrimSpace(ra[i+1:])
	if start == "" {
		if end == "" || end[0] == '-' {
			return 0, 0, errors.New("invalid range")
		}
		i, err := strconv.ParseInt(end, 10, 64)
		if i < 0 || err != nil {
			return 0, 0, errors.New("invalid range")
		}
		if i > size {
			i = size
		}
		offset = size - i
		length = size - offset
	} else {
		i, err := strconv.ParseInt(start, 10, 64)
		if err != nil || i < 0 {
			return 0, 0, errors.New("invalid range")
		} else if i >= size {
			return 0, 0, errors.New("invalid range")
		}
		offset = i
		if end == "" {
			length = size - offset
		} else {
			i, err := strconv.ParseInt(end, 10, 64)
			if err != nil || offset > i {
				return 0, 0, errors.New("invalid range")
			}
			if i >= size {
				i = size - 1
			}
			length = i - offset + 1
		}
	}
	return offset, length, nil
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
	Accounts(ctx context.Context, owner string) ([]api.Account, error)
	AddBalance(ctx context.Context, id rhpv3.Account, owner string, hk types.PublicKey, amt *big.Int) error
	ResetDrift(ctx context.Context, id rhpv3.Account) error
	SetBalance(ctx context.Context, id rhpv3.Account, owner string, hk types.PublicKey, amt, drift *big.Int) error
	SetRequiresSync(ctx context.Context, id rhpv3.Account, owner string, hk types.PublicKey, requiresSync bool) error
}

type contractLocker interface {
	AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error)
	ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error)
}

// A Bus is the source of truth within a renterd system.
type Bus interface {
	AccountStore
	contractLocker

	ConsensusState(ctx context.Context) (api.ConsensusState, error)

	ActiveContracts(ctx context.Context) ([]api.ContractMetadata, error)
	Contracts(ctx context.Context, set string) ([]api.ContractMetadata, error)
	RecordInteractions(ctx context.Context, interactions []hostdb.Interaction) error
	RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error

	Host(ctx context.Context, hostKey types.PublicKey) (hostdb.HostInfo, error)

	DownloadParams(ctx context.Context) (api.DownloadParams, error)
	GougingParams(ctx context.Context) (api.GougingParams, error)
	UploadParams(ctx context.Context) (api.UploadParams, error)

	Object(ctx context.Context, path, prefix string, offset, limit int) (object.Object, []string, error)
	AddObject(ctx context.Context, path string, o object.Object, usedContracts map[types.PublicKey]types.FileContractID) error
	DeleteObject(ctx context.Context, path string) error

	Accounts(ctx context.Context, owner string) ([]api.Account, error)
	UpdateSlab(ctx context.Context, s object.Slab, goodContracts map[types.PublicKey]types.FileContractID) error

	WalletDiscard(ctx context.Context, txn types.Transaction) error
	WalletPrepareForm(ctx context.Context, renterAddress types.Address, renterKey types.PrivateKey, renterFunds, hostCollateral types.Currency, hostKey types.PublicKey, hostSettings rhpv2.HostSettings, endHeight uint64) (txns []types.Transaction, err error)
	WalletPrepareRenew(ctx context.Context, contract types.FileContractRevision, renterAddress types.Address, renterKey types.PrivateKey, renterFunds, newCollateral types.Currency, hostKey types.PublicKey, hostSettings rhpv2.HostSettings, endHeight uint64) ([]types.Transaction, types.Currency, error)
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

	downloadSectorTimeout time.Duration
	uploadSectorTimeout   time.Duration

	logger *zap.SugaredLogger
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

func (w *worker) withTransportV2(ctx context.Context, hostIP string, hostKey types.PublicKey, fn func(*rhpv2.Transport) error) (err error) {
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

func withTransportV3(ctx context.Context, siamuxAddr string, hostKey types.PublicKey, fn func(*rhpv3.Transport) error) (err error) {
	conn, err := dial(ctx, siamuxAddr, hostKey)
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
	t, err := rhpv3.NewRenterTransport(conn, hostKey)
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

	var settings rhpv2.HostSettings
	start := time.Now()
	pingErr := w.withTransportV2(ctx, rsr.HostIP, rsr.HostKey, func(t *rhpv2.Transport) (err error) {
		settings, err = RPCSettings(ctx, t)
		return err
	})
	elapsed := time.Since(start)

	var pt rhpv3.HostPriceTable
	ptErr := withTransportV3(ctx, settings.SiamuxAddr(), rsr.HostKey, func(t *rhpv3.Transport) (err error) {
		pt, err = RPCPriceTable(t, func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) { return nil, nil })
		return err
	})

	w.recordScan(rsr.HostKey, pt, settings, pingErr)

	var scanErrStr string
	if pingErr != nil {
		scanErrStr = pingErr.Error()
	}
	if ptErr != nil {
		if scanErrStr != "" {
			scanErrStr += "; "
		}
		scanErrStr += ptErr.Error()
	}
	jc.Encode(api.RHPScanResponse{
		Ping:      api.ParamDuration(elapsed),
		ScanError: scanErrStr,
		Settings:  settings,
	})
}

func (w *worker) rhpPriceTableHandler(jc jape.Context) {
	var rptr api.RHPPriceTableRequest
	if jc.Decode(&rptr) != nil {
		return
	}

	var pt rhpv3.HostPriceTable
	if jc.Check("could not get price table", withTransportV3(jc.Request.Context(), rptr.SiamuxAddr, rptr.HostKey, func(t *rhpv3.Transport) (err error) {
		pt, err = RPCPriceTable(t, func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) { return nil, nil })
		return
	})) != nil {
		return
	}

	jc.Encode(pt)
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
	err = w.withTransportV2(ctx, hostIP, rfr.HostKey, func(t *rhpv2.Transport) (err error) {
		hostSettings, err := RPCSettings(ctx, t)
		if err != nil {
			return err
		}

		if errs := PerformGougingChecks(ctx, &hostSettings, nil).CanForm(); len(errs) > 0 {
			return fmt.Errorf("failed to form contract, gouging check failed: %v", errs)
		}

		renterTxnSet, err := w.bus.WalletPrepareForm(ctx, renterAddress, renterKey, renterFunds, hostCollateral, hostKey, hostSettings, endHeight)
		if err != nil {
			return err
		}

		contract, txnSet, err = RPCFormContract(ctx, t, renterKey, renterTxnSet)
		if err != nil {
			w.bus.WalletDiscard(ctx, renterTxnSet[len(renterTxnSet)-1])
			return err
		}
		return
	})
	if jc.Check("couldn't form contract", err) != nil {
		return
	}
	jc.Encode(api.RHPFormResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (w *worker) rhpRenewHandler(jc jape.Context) {
	ctx := jc.Request.Context()
	var rrr api.RHPRenewRequest
	if jc.Decode(&rrr) != nil {
		return
	}

	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	lockID, err := w.bus.AcquireContract(jc.Request.Context(), rrr.ContractID, lockingPriorityRenew, lockingDurationRenew)
	if jc.Check("could not lock contract for renewal", err) != nil {
		return
	}
	defer func() {
		_ = w.bus.ReleaseContract(ctx, rrr.ContractID, lockID) // TODO: log error
	}()

	hostIP, hostKey, toRenewID, renterFunds, newCollateral := rrr.HostIP, rrr.HostKey, rrr.ContractID, rrr.RenterFunds, rrr.NewCollateral
	renterAddress, endHeight := rrr.RenterAddress, rrr.EndHeight
	renterKey := w.deriveRenterKey(hostKey)

	var contract rhpv2.ContractRevision
	var txnSet []types.Transaction
	ctx = WithGougingChecker(jc.Request.Context(), gp)
	err = w.withHostV2(ctx, toRenewID, hostKey, hostIP, func(ss sectorStore) error {
		session := ss.(*sharedSession)
		contract, txnSet, err = session.RenewContract(ctx, func(rev types.FileContractRevision, host rhpv2.HostSettings) ([]types.Transaction, types.Currency, func(), error) {
			renterTxnSet, finalPayment, err := w.bus.WalletPrepareRenew(ctx, rev, renterAddress, renterKey, renterFunds, newCollateral, hostKey, host, endHeight)
			if err != nil {
				return nil, types.Currency{}, nil, err
			}
			return renterTxnSet, finalPayment, func() { w.bus.WalletDiscard(ctx, renterTxnSet[len(renterTxnSet)-1]) }, nil
		})
		return err
	})
	if jc.Check("couldn't renew contract", err) != nil {
		return
	}
	jc.Encode(api.RHPRenewResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (w *worker) rhpFundHandler(jc jape.Context) {
	ctx := jc.Request.Context()
	var rfr api.RHPFundRequest
	if jc.Decode(&rfr) != nil {
		return
	}

	// Get account for the host.
	account, err := w.accounts.ForHost(rfr.HostKey)
	if jc.Check("failed to get account for provided host", err) != nil {
		return
	}

	// Get IP of host.
	h, err := w.bus.Host(ctx, rfr.HostKey)
	if jc.Check("failed to fetch host", err) != nil {
		return
	}
	siamuxAddr := h.Settings.SiamuxAddr()

	// Get contract revision.
	lockID, err := w.bus.AcquireContract(jc.Request.Context(), rfr.ContractID, lockingPriorityFunding, lockingDurationFunding)
	if jc.Check("failed to acquire contract for funding EA", err) != nil {
		return
	}
	defer func() {
		_ = w.bus.ReleaseContract(ctx, rfr.ContractID, lockID) // TODO: log error
	}()

	// Get contract revision.
	var revision types.FileContractRevision
	err = w.withHostV2(jc.Request.Context(), rfr.ContractID, rfr.HostKey, h.NetAddress, func(ss sectorStore) error {
		rev, err := ss.(*sharedSession).Revision(jc.Request.Context())
		if err != nil {
			return err
		}
		revision = rev.Revision
		return nil
	})
	if jc.Check(fmt.Sprintf("failed to fetch revision from host '%s'", siamuxAddr), err) != nil {
		return
	}

	// Get price table.
	pt, ptValid := w.priceTables.PriceTable(rfr.HostKey)
	if !ptValid {
		paymentFunc := w.preparePriceTableContractPayment(rfr.HostKey, &revision)
		pt, err = w.priceTables.Update(jc.Request.Context(), paymentFunc, siamuxAddr, rfr.HostKey)
		if jc.Check("failed to update outdated price table", err) != nil {
			return
		}
	}

	// Calculate the fund amount
	balance := account.Balance()
	if balance.Cmp(rfr.Balance) >= 0 {
		jc.Error(fmt.Errorf("account balance %v is already greater than or equal to requested balance %v", balance, rfr.Balance), http.StatusBadRequest)
		return
	}
	fundAmount := rfr.Balance.Sub(balance)

	// Fund account.
	err = w.fundAccount(ctx, account, pt, siamuxAddr, rfr.HostKey, fundAmount, &revision)

	// If funding failed due to an exceeded max balance, we sync the account and
	// try funding the account again.
	if isMaxBalanceExceeded(err) {
		err = w.syncAccount(ctx, pt, siamuxAddr, rfr.HostKey)
		if err != nil {
			w.logger.Errorw(fmt.Sprintf("failed to sync account: %v", err), "host", rfr.HostKey)
		}

		balance = account.Balance()
		if balance.Cmp(rfr.Balance) < 0 {
			fundAmount = rfr.Balance.Sub(balance)
			err = w.fundAccount(ctx, account, pt, siamuxAddr, rfr.HostKey, fundAmount, &revision)
			if err != nil {
				w.logger.Errorw(fmt.Sprintf("failed to fund account right after a sync: %v", err), "host", rfr.HostKey, "balance", balance, "requested", rfr.Balance, "fund", fundAmount)
			}
		}
	}
	if jc.Check("couldn't fund account", err) != nil {
		return
	}
}

func (w *worker) rhpRegistryReadHandler(jc jape.Context) {
	var rrrr api.RHPRegistryReadRequest
	if jc.Decode(&rrrr) != nil {
		return
	}
	var value rhpv3.RegistryValue
	err := withTransportV3(jc.Request.Context(), rrrr.HostIP, rrrr.HostKey, func(t *rhpv3.Transport) (err error) {
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
	err := withTransportV3(jc.Request.Context(), rrur.HostIP, rrur.HostKey, func(t *rhpv3.Transport) (err error) {
		return RPCUpdateRegistry(t, &payment, rrur.RegistryKey, rrur.RegistryValue)
	})
	if jc.Check("couldn't update registry", err) != nil {
		return
	}
}

func (w *worker) rhpSyncHandler(jc jape.Context) {
	ctx := jc.Request.Context()
	var rsr api.RHPSyncRequest
	if jc.Decode(&rsr) != nil {
		return
	}

	// Get IP of host.
	h, err := w.bus.Host(ctx, rsr.HostKey)
	if jc.Check("failed to fetch host", err) != nil {
		return
	}
	hostIP := h.Settings.NetAddress
	siamuxAddr := h.Settings.SiamuxAddr()

	// Get contract revision.
	lockID, err := w.bus.AcquireContract(jc.Request.Context(), rsr.ContractID, lockingPriorityFunding, lockingDurationFunding)
	if jc.Check("failed to acquire contract for funding EA", err) != nil {
		return
	}
	defer func() {
		if err := w.bus.ReleaseContract(ctx, rsr.ContractID, lockID); err != nil {
			w.logger.Warnf("failed to release lock for contract %v: %v", rsr.ContractID, err)
		}
	}()

	// Get contract revision.
	var revision types.FileContractRevision
	err = w.withHostV2(jc.Request.Context(), rsr.ContractID, rsr.HostKey, hostIP, func(ss sectorStore) error {
		rev, err := ss.(*sharedSession).Revision(jc.Request.Context())
		if err != nil {
			return err
		}
		revision = rev.Revision
		return nil
	})
	if jc.Check("failed to fetch revision", err) != nil {
		return
	}

	// Get price table.
	pt, ptValid := w.priceTables.PriceTable(rsr.HostKey)
	if !ptValid {
		paymentFunc := w.preparePriceTableContractPayment(rsr.HostKey, &revision)
		pt, err = w.priceTables.Update(jc.Request.Context(), paymentFunc, siamuxAddr, rsr.HostKey)
		if jc.Check("failed to update outdated price table", err) != nil {
			return
		}
	}

	// Sync account.
	err = w.syncAccount(ctx, pt, siamuxAddr, rsr.HostKey)
	if err != nil {
		w.logger.Errorw(fmt.Sprintf("failed to sync account: %v", err), "host", rsr.HostKey)
	}
	if jc.Check("couldn't fund account", err) != nil {
		return
	}
}

func (w *worker) slabMigrateHandler(jc jape.Context) {
	ctx := jc.Request.Context()
	var slab object.Slab
	if jc.Decode(&slab) != nil {
		return
	}

	up, err := w.bus.UploadParams(ctx)
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
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
	err = migrateSlab(ctx, w, &slab, contracts, w.bus, w.downloadSectorTimeout, w.uploadSectorTimeout)
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
	jc.Custom(nil, []string{})

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

	if strings.HasSuffix(path, "/") {
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
	offset, length, err := parseRange(jc.Request.Header.Get("Range"), obj.Size())
	if err != nil {
		jc.Error(err, http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if length < obj.Size() {
		jc.ResponseWriter.WriteHeader(http.StatusPartialContent)
		jc.ResponseWriter.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, obj.Size()))
	}
	jc.ResponseWriter.Header().Set("Content-Length", strconv.FormatInt(length, 10))

	// keep track of bad hosts so we can avoid them in consecutive slab downloads
	badHosts := make(map[types.PublicKey]int)

	// fetch contracts
	set, err := w.bus.Contracts(ctx, dp.ContractSet)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	// build contract map
	contracts := make(map[types.PublicKey]api.ContractMetadata)
	for _, contract := range set {
		contracts[contract.HostKey] = contract
	}

	// create a function that returns the contracts for a given slab
	contractsForSlab := func(s object.Slab) (c []api.ContractMetadata) {
		for _, shard := range s.Shards {
			if contract, exists := contracts[shard.Host]; exists {
				c = append(c, contract)
			}
		}
		return
	}

	cw := obj.Key.Decrypt(jc.ResponseWriter, offset)
	for i, ss := range slabsForDownload(obj.Slabs, offset, length) {
		// fetch contracts for the slab
		contracts := contractsForSlab(ss.Slab)
		if len(contracts) < int(ss.MinShards) {
			err = fmt.Errorf("not enough contracts to download the slab, %d<%d", len(contracts), ss.MinShards)
			w.logger.Errorf("couldn't download object '%v' slab %d, err: %v", path, i, err)
			if i == 0 {
				jc.Error(err, http.StatusInternalServerError)
			}
			return
		}

		// randomize order of contracts so we don't always download from the same hosts
		frand.Shuffle(len(contracts), func(i, j int) { contracts[i], contracts[j] = contracts[j], contracts[i] })

		// move bad hosts to the back of the array, a bad host is a host that
		// timed out, is out of funds or is gouging its prices
		sort.SliceStable(contracts, func(i, j int) bool {
			return badHosts[contracts[i].HostKey] < badHosts[contracts[j].HostKey]
		})

		badHostIndices, err := downloadSlab(ctx, w, cw, ss, contracts, w.downloadSectorTimeout)
		for _, h := range badHostIndices {
			badHosts[contracts[h].HostKey]++
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

	up, err := w.bus.UploadParams(ctx)
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}
	rs := up.RedundancySettings

	// allow overriding the redundancy settings
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
		s, length, slowHosts, err = uploadSlab(ctx, w, lr, uint8(rs.MinShards), uint8(rs.TotalShards), contracts, &tracedContractLocker{w.bus}, w.uploadSectorTimeout)
		for _, h := range slowHosts {
			slow[contracts[h].HostKey]++
		}
		if err == io.EOF {
			break
		} else if jc.Check("couldn't upload slab", err); err != nil {
			return
		}

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

	path := strings.TrimPrefix(jc.PathParam("path"), "/")
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

	var hosttimeout api.ParamDuration
	if jc.DecodeForm("hosttimeout", &hosttimeout) != nil {
		return
	}

	// fetch all contracts
	var contracts []api.Contract
	err = w.withHostsV2(jc.Request.Context(), busContracts, func(ss []sectorStore) error {
		var errs HostErrorSet
		for i, store := range ss {
			func() {
				ctx := jc.Request.Context()
				if hosttimeout > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, time.Duration(hosttimeout))
					defer cancel()
				}

				rev, err := store.(*sharedSession).Revision(ctx)
				if err != nil {
					errs = append(errs, &HostError{HostKey: store.HostKey(), Err: err})
					return
				}
				contracts = append(contracts, api.Contract{
					ContractMetadata: busContracts[i],
					Revision:         rev.Revision,
				})
			}()
		}
		if len(errs) > 0 {
			return fmt.Errorf("couldn't retrieve contract(s): %s", errs.Error())
		}
		return nil
	})

	resp := api.ContractsResponse{Contracts: contracts}
	if err != nil {
		resp.Error = err.Error()
	}
	jc.Encode(resp)
}

func (w *worker) preparePayment(hk types.PublicKey, amt types.Currency, blockHeight uint64) rhpv3.PayByEphemeralAccountRequest {
	pk := w.accounts.deriveAccountKey(hk)
	return rhpv3.PayByEphemeralAccount(rhpv3.Account(pk.PublicKey()), amt, blockHeight+6, pk) // 1 hour valid
}

func (w *worker) accountHandlerGET(jc jape.Context) {
	var host types.PublicKey
	if jc.DecodeParam("id", &host) != nil {
		return
	}
	account, err := w.accounts.ForHost(host)
	if jc.Check("failed to fetch accounts", err) != nil {
		return
	}
	jc.Encode(account.Convert())
}

func (w *worker) accountsHandlerGET(jc jape.Context) {
	accounts, err := w.accounts.All()
	if jc.Check("failed to fetch accounts", err) != nil {
		return
	}
	jc.Encode(accounts)
}

func (w *worker) idHandlerGET(jc jape.Context) {
	jc.Encode(w.id)
}

// New returns an HTTP handler that serves the worker API.
func New(masterKey [32]byte, id string, b Bus, sessionReconectTimeout, sessionTTL, busFlushInterval, downloadSectorTimeout, uploadSectorTimeout time.Duration, l *zap.Logger) *worker {
	w := &worker{
		id:                    id,
		bus:                   b,
		pool:                  newSessionPool(sessionReconectTimeout, sessionTTL),
		priceTables:           newPriceTables(),
		masterKey:             masterKey,
		busFlushInterval:      busFlushInterval,
		downloadSectorTimeout: downloadSectorTimeout,
		uploadSectorTimeout:   uploadSectorTimeout,
		logger:                l.Sugar().Named("worker").Named(id),
	}
	w.initAccounts(b)
	w.initContractSpendingRecorder()
	return w
}

func (w *worker) accountsResetDriftHandlerPOST(jc jape.Context) {
	var id rhpv3.Account
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	if jc.Check("failed to reset drift", w.accounts.ResetDrift(jc.Request.Context(), id)) != nil {
		return
	}
}

// Handler returns an HTTP handler that serves the worker API.
func (w *worker) Handler() http.Handler {
	return jape.Mux(tracing.TracedRoutes("worker", map[string]jape.Handler{
		"GET    /accounts":                w.accountsHandlerGET,
		"GET    /accounts/host/:id":       w.accountHandlerGET,
		"POST   /accounts/:id/resetdrift": w.accountsResetDriftHandlerPOST,

		"GET    /id": w.idHandlerGET,

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

// tracedContractLocker is a helper type that wraps a contractLocker and adds tracing to its methods.
type tracedContractLocker struct {
	l contractLocker
}

func (l *tracedContractLocker) AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "tracedContractLocker.AcquireContract")
	defer span.End()
	lockID, err = l.l.AcquireContract(ctx, fcid, priority, d)
	if err != nil {
		span.SetStatus(codes.Error, "failed to acquire contract")
		span.RecordError(err)
	}
	span.SetAttributes(attribute.Stringer("contract", fcid))
	span.SetAttributes(attribute.Int("priority", priority))
	return
}

func (l *tracedContractLocker) ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error) {
	ctx, span := tracing.Tracer.Start(ctx, "tracedContractLocker.ReleaseContract")
	defer span.End()
	err = l.l.ReleaseContract(ctx, fcid, lockID)
	if err != nil {
		span.SetStatus(codes.Error, "failed to release contract")
		span.RecordError(err)
	}
	span.SetAttributes(attribute.Stringer("contract", fcid))
	return
}
