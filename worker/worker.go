package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/ephemeralaccounts"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/metrics"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"golang.org/x/crypto/blake2b"
)

const (
	lockingPriorityRenew   = 100 // highest
	lockingPriorityFunding = 90

	lockingDurationRenew   = time.Minute
	lockingDurationFunding = 30 * time.Second
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
	case rhpv2.MetricRPC:
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
	Accounts(owner string) ([]ephemeralaccounts.Account, error)
	AddBalance(id rhpv3.Account, owner string, hk types.PublicKey, amt *big.Int) error
	SetBalance(id rhpv3.Account, owner string, hk types.PublicKey, amt *big.Int) error
}

type contractLocker interface {
	AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error)
	ReleaseContract(fcid types.FileContractID, lockID uint64) (err error)
}

// A Bus is the source of truth within a renterd system.
type Bus interface {
	AccountStore
	contractLocker

	ActiveContracts() ([]api.ContractMetadata, error)
	Contracts(set string) ([]api.ContractMetadata, error)
	ContractsForSlab(shards []object.Sector, contractSetName string) ([]api.ContractMetadata, error)
	RecordInteractions(interactions []hostdb.Interaction) error

	Host(hostKey types.PublicKey) (hostdb.Host, error)

	DownloadParams() (api.DownloadParams, error)
	GougingParams() (api.GougingParams, error)
	UploadParams() (api.UploadParams, error)

	Object(key string) (object.Object, []string, error)
	AddObject(key string, o object.Object, usedContracts map[types.PublicKey]types.FileContractID) error
	DeleteObject(key string) error

	Accounts(owner string) ([]ephemeralaccounts.Account, error)
	UpdateSlab(s object.Slab, goodContracts map[types.PublicKey]types.FileContractID) error

	WalletDiscard(txn types.Transaction) error
	WalletPrepareForm(renterKey types.PrivateKey, hostKey types.PublicKey, renterFunds types.Currency, renterAddress types.Address, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (txns []types.Transaction, err error)
	WalletPrepareRenew(contract types.FileContractRevision, renterKey types.PrivateKey, hostKey types.PublicKey, renterFunds types.Currency, renterAddress types.Address, endHeight uint64, hostSettings rhpv2.HostSettings) ([]types.Transaction, types.Currency, error)
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
}

func (w *worker) recordScan(hostKey types.PublicKey, settings rhpv2.HostSettings, err error) error {
	hi := hostdb.Interaction{
		Host:      hostKey,
		Timestamp: time.Now(),
		Type:      hostdb.InteractionTypeScan,
		Success:   err == nil,
	}
	if err == nil {
		hi.Result, _ = json.Marshal(hostdb.ScanResult{
			Settings: settings,
		})
	} else {
		hi.Result, _ = json.Marshal(hostdb.ScanResult{
			Error: errToStr(err),
		})
	}
	return w.bus.RecordInteractions([]hostdb.Interaction{hi})
}

func (w *worker) withTransportV2(ctx context.Context, hostIP string, hostKey types.PublicKey, fn func(*rhpv2.Transport) error) (err error) {
	var mr ephemeralMetricsRecorder
	defer func() {
		// TODO: handle error
		_ = w.bus.RecordInteractions(mr.interactions())
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

func (w *worker) withTransportV3(ctx context.Context, hostIP string, hostKey types.PublicKey, fn func(*rhpv3.Transport) error) (err error) {
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
	t, err := rhpv3.NewRenterTransport(conn, hostKey)
	if err != nil {
		return err
	}
	defer t.Close()
	return fn(t)
}

func (w *worker) withHosts(ctx context.Context, contracts []api.ContractMetadata, fn func([]sectorStore) error) (err error) {
	var hosts []sectorStore
	for _, c := range contracts {
		hosts = append(hosts, w.pool.session(c.HostKey, c.HostIP, c.ID, w.deriveRenterKey(c.HostKey)))
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			for _, h := range hosts {
				w.pool.unlockContract(h.(*session))
			}
		case <-ctx.Done():
			for _, h := range hosts {
				w.pool.forceClose(h.(*session))
			}
		}
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
	scanErr := w.withTransportV2(ctx, rsr.HostIP, rsr.HostKey, func(t *rhpv2.Transport) (err error) {
		settings, err = rhpv2.RPCSettings(ctx, t)
		return err
	})
	elapsed := time.Since(start)

	err := w.recordScan(rsr.HostKey, settings, scanErr)
	if jc.Check("failed to record scan", err) != nil {
		return
	}
	var scanErrStr string
	if scanErr != nil {
		scanErrStr = scanErr.Error()
	}
	jc.Encode(api.RHPScanResponse{
		Ping:      api.Duration(elapsed),
		ScanError: scanErrStr,
		Settings:  settings,
	})
}

func (w *worker) rhpFormHandler(jc jape.Context) {
	var rfr api.RHPFormRequest
	if jc.Decode(&rfr) != nil {
		return
	}

	gp, err := w.bus.GougingParams()
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	hostIP, hostKey, renterFunds := rfr.HostIP, rfr.HostKey, rfr.RenterFunds
	renterAddress, endHeight, hostCollateral := rfr.RenterAddress, rfr.EndHeight, rfr.HostCollateral
	rk := w.deriveRenterKey(hostKey)

	var contract rhpv2.ContractRevision
	var txnSet []types.Transaction
	ctx := WithGougingChecker(jc.Request.Context(), gp)
	err = w.withTransportV2(ctx, hostIP, rfr.HostKey, func(t *rhpv2.Transport) (err error) {
		settings, err := rhpv2.RPCSettings(ctx, t)
		if err != nil {
			return err
		}

		if errs := PerformGougingChecks(ctx, settings).CanForm(); len(errs) > 0 {
			return fmt.Errorf("failed to form contract, gouging check failed: %v", errs)
		}

		renterTxnSet, err := w.bus.WalletPrepareForm(rk, hostKey, renterFunds, renterAddress, hostCollateral, endHeight, settings)
		if err != nil {
			return err
		}

		contract, txnSet, err = rhpv2.RPCFormContract(t, rk, renterTxnSet)
		if err != nil {
			w.bus.WalletDiscard(renterTxnSet[len(renterTxnSet)-1])
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
	var rrr api.RHPRenewRequest
	if jc.Decode(&rrr) != nil {
		return
	}

	gp, err := w.bus.GougingParams()
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	lockID, err := w.bus.AcquireContract(jc.Request.Context(), rrr.ContractID, lockingPriorityRenew, lockingDurationRenew)
	if jc.Check("could not lock contract for renewal", err) != nil {
		return
	}
	defer func() {
		if err := w.bus.ReleaseContract(rrr.ContractID, lockID); err != nil {
			// TODO: log
		}
	}()

	hostIP, hostKey, toRenewID, renterFunds := rrr.HostIP, rrr.HostKey, rrr.ContractID, rrr.RenterFunds
	renterAddress, endHeight := rrr.RenterAddress, rrr.EndHeight
	rk := w.deriveRenterKey(hostKey)

	var contract rhpv2.ContractRevision
	var txnSet []types.Transaction
	ctx := WithGougingChecker(jc.Request.Context(), gp)
	err = w.withTransportV2(ctx, hostIP, hostKey, func(t *rhpv2.Transport) error {
		settings, err := rhpv2.RPCSettings(ctx, t)
		if err != nil {
			return err
		}

		if errs := PerformGougingChecks(ctx, settings).CanForm(); len(errs) > 0 {
			return fmt.Errorf("failed to renew contract, gouging check failed: %v", errs)
		}

		session, err := rhpv2.RPCLock(ctx, t, toRenewID, rk, 5*time.Second)
		if err != nil {
			return err
		}
		rev := session.Revision()

		renterTxnSet, finalPayment, err := w.bus.WalletPrepareRenew(rev.Revision, rk, hostKey, renterFunds, renterAddress, endHeight, settings)
		if err != nil {
			return err
		}

		contract, txnSet, err = session.RenewContract(renterTxnSet, finalPayment)
		if err != nil {
			w.bus.WalletDiscard(renterTxnSet[len(renterTxnSet)-1])
			return err
		}
		return nil
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
	h, err := w.bus.Host(rfr.HostKey)
	if jc.Check("failed to fetch host", err) != nil {
		return
	}
	hostIP := h.Settings.SiamuxAddr()

	// Get contract revision.
	lockID, err := w.bus.AcquireContract(jc.Request.Context(), rfr.ContractID, lockingPriorityRenew, lockingPriorityFunding)
	if jc.Check("failed to acquire contract for funding EA", err) != nil {
		return
	}
	defer func() {
		if err := w.bus.ReleaseContract(rfr.ContractID, lockID); err != nil {
			// TODO: log
		}
	}()

	// Get contract revision.
	var revision types.FileContractRevision
	cm := api.ContractMetadata{
		ID:      rfr.ContractID,
		HostIP:  hostIP,
		HostKey: rfr.HostKey,
	}
	err = w.withHosts(jc.Request.Context(), []api.ContractMetadata{cm}, func(ss []sectorStore) error {
		rev, err := ss[0].(*session).Revision(jc.Request.Context())
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
	pt, ptValid := w.priceTables.PriceTable(rfr.HostKey)
	if !ptValid {
		paymentFunc := w.preparePriceTableContractPayment(rfr.HostKey, &revision)
		pt, err = w.priceTables.Update(jc.Request.Context(), paymentFunc, hostIP, rfr.HostKey)
		if jc.Check("failed to update outdated price table", err) != nil {
			return
		}
	}

	// Fund account.
	err = account.WithDeposit(func() (types.Currency, error) {
		return rfr.Amount, w.withTransportV3(jc.Request.Context(), hostIP, rfr.HostKey, func(t *rhpv3.Transport) (err error) {
			rk := w.deriveRenterKey(rfr.HostKey)
			payment, ok := rhpv3.PayByContract(&revision, rfr.Amount.Add(pt.FundAccountCost), rhpv3.Account{}, rk) // no account needed for funding
			if !ok {
				return errors.New("insufficient funds")
			}
			return rhpv3.RPCFundAccount(t, &payment, account.id, pt.ID)
		})
	})
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
	err := w.withTransportV3(jc.Request.Context(), rrrr.HostIP, rrrr.HostKey, func(t *rhpv3.Transport) (err error) {
		value, err = rhpv3.RPCReadRegistry(t, &rrrr.Payment, rrrr.RegistryKey)
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
	var pt rhpv3.HostPriceTable        // TODO
	cost, _ := pt.UpdateRegistryCost() // TODO: handle refund
	payment := w.preparePayment(rrur.HostKey, cost)
	err := w.withTransportV3(jc.Request.Context(), rrur.HostIP, rrur.HostKey, func(t *rhpv3.Transport) (err error) {
		return rhpv3.RPCUpdateRegistry(t, &payment, rrur.RegistryKey, rrur.RegistryValue)
	})
	if jc.Check("couldn't update registry", err) != nil {
		return
	}
}

func (w *worker) slabMigrateHandler(jc jape.Context) {
	var slab object.Slab
	if jc.Decode(&slab) != nil {
		return
	}

	up, err := w.bus.UploadParams()
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}

	// attach gouging checker to the context
	ctx := WithGougingChecker(jc.Request.Context(), up.GougingParams)

	contracts, err := w.bus.Contracts(up.ContractSet)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	w.pool.setCurrentHeight(up.CurrentHeight)
	err = w.withHosts(ctx, contracts, func(hosts []sectorStore) error {
		return migrateSlab(ctx, &slab, hosts, w.bus)
	})
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

	if jc.Check("couldn't update slab", w.bus.UpdateSlab(slab, usedContracts)) != nil {
		return
	}
}

func (w *worker) objectsKeyHandlerGET(jc jape.Context) {
	jc.Custom(nil, []string{})

	o, es, err := w.bus.Object(jc.PathParam("key"))
	if jc.Check("couldn't get object or entries", err) != nil {
		return
	}
	if len(es) > 0 {
		jc.Encode(es)
		return
	}

	dp, err := w.bus.DownloadParams()
	if jc.Check("couldn't fetch download parameters from bus", err) != nil {
		return
	}

	// attach gouging checker to the context
	ctx := WithGougingChecker(jc.Request.Context(), dp.GougingParams)

	// NOTE: ideally we would use http.ServeContent in this handler, but that
	// has performance issues. If we implemented io.ReadSeeker in the most
	// straightforward fashion, we would need one (or more!) RHP RPCs for each
	// Read call. We can improve on this to some degree by buffering, but
	// without knowing the exact ranges being requested, this will always be
	// suboptimal. Thus, sadly, we have to roll our own range support.
	offset, length, err := parseRange(jc.Request.Header.Get("Range"), o.Size())
	if err != nil {
		jc.Error(err, http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if length < o.Size() {
		jc.ResponseWriter.WriteHeader(http.StatusPartialContent)
		jc.ResponseWriter.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, o.Size()))
	}
	jc.ResponseWriter.Header().Set("Content-Length", strconv.FormatInt(length, 10))

	cw := o.Key.Decrypt(jc.ResponseWriter, offset)
	for _, ss := range slabsForDownload(o.Slabs, offset, length) {
		contracts, err := w.bus.ContractsForSlab(ss.Shards, dp.ContractSet)
		if err != nil {
			_ = err // NOTE: can't write error because we may have already written to the response
			return
		}

		if len(contracts) < int(ss.MinShards) {
			err = fmt.Errorf("not enough contracts to download the slab, %d<%d", len(contracts), ss.MinShards)
			_ = err // TODO: can we do better UX wise (?) could ask the DB before initiating the first download
			return
		}

		err = w.withHosts(ctx, contracts, func(hosts []sectorStore) error {
			return downloadSlab(ctx, cw, ss, hosts, w.bus)
		})
		if err != nil {
			_ = err // NOTE: can't write error because we may have already written to the response
			return
		}
	}
}

func (w *worker) objectsKeyHandlerPUT(jc jape.Context) {
	jc.Custom((*[]byte)(nil), nil)

	up, err := w.bus.UploadParams()
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}
	rs := up.RedundancySettings

	// allow overriding the redundancy settings
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
	ctx := WithGougingChecker(jc.Request.Context(), up.GougingParams)

	o := object.Object{
		Key: object.GenerateEncryptionKey(),
	}
	w.pool.setCurrentHeight(up.CurrentHeight)
	usedContracts := make(map[types.PublicKey]types.FileContractID)

	// fetch contracts
	contracts, err := w.bus.Contracts(up.ContractSet)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	cr := o.Key.Encrypt(jc.Request.Body)
	for {
		var s object.Slab
		var length int

		lr := io.LimitReader(cr, int64(rs.MinShards)*rhpv2.SectorSize)
		if err := w.withHosts(ctx, contracts, func(hosts []sectorStore) (err error) {
			s, length, err = uploadSlab(ctx, lr, uint8(rs.MinShards), uint8(rs.TotalShards), hosts, w.bus)
			return err
		}); err == io.EOF {
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

	if jc.Check("couldn't add object", w.bus.AddObject(jc.PathParam("key"), o, usedContracts)) != nil {
		return
	}
}

func (w *worker) objectsKeyHandlerDELETE(jc jape.Context) {
	jc.Check("couldn't delete object", w.bus.DeleteObject(jc.PathParam("key")))
}

func (w *worker) rhpActiveContractsHandlerGET(jc jape.Context) {
	busContracts, err := w.bus.ActiveContracts()
	if jc.Check("failed to fetch contracts from bus", err) != nil {
		return
	}

	var hosttimeout api.Duration
	if jc.DecodeForm("hosttimeout", &hosttimeout) != nil {
		return
	}

	// fetch all contracts
	var contracts []api.Contract
	err = w.withHosts(jc.Request.Context(), busContracts, func(ss []sectorStore) error {
		var errs []error
		for i, store := range ss {
			func() {
				ctx := jc.Request.Context()
				if hosttimeout > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, time.Duration(hosttimeout))
					defer cancel()
				}

				rev, err := store.(*session).Revision(ctx)
				if err != nil {
					errs = append(errs, err)
					return
				}
				contracts = append(contracts, api.Contract{
					ContractMetadata: busContracts[i],
					Revision:         rev.Revision,
				})
			}()
		}
		if err := joinErrors(errs); err != nil {
			return fmt.Errorf("couldn't retrieve %d contract(s): %w", len(errs), err)
		}
		return nil
	})

	resp := api.ContractsResponse{Contracts: contracts}
	if err != nil {
		resp.Error = err.Error()
	}
	jc.Encode(resp)
}

func joinErrors(errs []error) error {
	filtered := errs[:0]
	for _, err := range errs {
		if err != nil {
			filtered = append(filtered, err)
		}
	}

	switch len(filtered) {
	case 0:
		return nil
	case 1:
		return filtered[0]
	default:
		strs := make([]string, len(filtered))
		for i := range strs {
			strs[i] = filtered[i].Error()
		}
		return errors.New(strings.Join(strs, ";"))
	}
}

func (w *worker) preparePayment(hk types.PublicKey, amt types.Currency) rhpv3.PayByEphemeralAccountRequest {
	pk := w.accounts.deriveAccountKey(hk)
	return rhpv3.PayByEphemeralAccount(rhpv3.Account(pk.PublicKey()), amt, math.MaxUint64, pk)
}

func (w *worker) accountsHandlerGET(jc jape.Context) {
	accounts, err := w.accounts.All()
	if jc.Check("failed to fetch accounts", err) != nil {
		return
	}
	jc.Encode(accounts)
}

// New returns an HTTP handler that serves the worker API.
func New(masterKey [32]byte, b Bus, sessionReconectTimeout, sessionTTL time.Duration) (http.Handler, error) {
	w := &worker{
		id:        "worker", // TODO: make this configurable for multi-worker setup.
		bus:       b,
		pool:      newSessionPool(sessionReconectTimeout, sessionTTL),
		masterKey: masterKey,
	}
	w.priceTables = newPriceTables(w.withTransportV3)
	w.accounts = newAccounts(w.id, w.deriveSubKey("accountkey"), b)

	return jape.Mux(map[string]jape.Handler{
		"GET    /accounts": w.accountsHandlerGET,

		"GET    /rhp/contracts/active": w.rhpActiveContractsHandlerGET,
		"POST   /rhp/scan":             w.rhpScanHandler,
		"POST   /rhp/form":             w.rhpFormHandler,
		"POST   /rhp/renew":            w.rhpRenewHandler,
		"POST   /rhp/fund":             w.rhpFundHandler,
		"POST   /rhp/registry/read":    w.rhpRegistryReadHandler,
		"POST   /rhp/registry/update":  w.rhpRegistryUpdateHandler,

		"POST   /slab/migrate": w.slabMigrateHandler,

		"GET    /objects/*key": w.objectsKeyHandlerGET,
		"PUT    /objects/*key": w.objectsKeyHandlerPUT,
		"DELETE /objects/*key": w.objectsKeyHandlerDELETE,
	}), nil
}
