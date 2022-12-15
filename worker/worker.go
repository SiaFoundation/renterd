package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/metrics"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
)

type Contract struct {
	bus.Contract `json:"contract"`
	Revision     rhpv2.ContractRevision `json:"revision"`
}

// EndHeight returns the height at which the host is no longer obligated to
// store contract data.
func (c Contract) EndHeight() uint64 {
	return uint64(c.Revision.EndHeight())
}

// FileSize returns the current Size of the contract.
func (c Contract) FileSize() uint64 {
	return c.Revision.Revision.NewFileSize
}

// HostKey returns the public key of the host.
func (c Contract) HostKey() (pk PublicKey) {
	return c.Revision.HostKey()
}

// RenterFunds returns the funds remaining in the contract's Renter payout.
func (c Contract) RenterFunds() types.Currency {
	return c.Revision.RenterFunds()
}

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

type ScanResult struct {
	InteractionResult
	Settings rhpv2.HostSettings `json:"settings,omitempty"`
}

func IsSuccessfulInteraction(i hostdb.Interaction) bool {
	var result InteractionResult
	if err := json.Unmarshal(i.Result, &result); err != nil {
		return false
	}
	return result.Error == ""
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
	for _, m := range mr.ms {
		if hi, ok := toHostInteraction(m); ok {
			his = append(his, hi)
		}
	}
	return his
}

// MetricHostDial contains metrics relating to a host dial.
type MetricHostDial struct {
	HostKey   consensus.PublicKey
	HostIP    string
	Timestamp time.Time
	Elapsed   time.Duration
	Err       error
}

// IsMetric implements metrics.Metric.
func (MetricHostDial) IsMetric() {}

func dial(ctx context.Context, hostIP string, hostKey consensus.PublicKey) (net.Conn, error) {
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
	transform := func(timestamp time.Time, typ string, err error, res interface{}) (hostdb.Interaction, bool) {
		b, _ := json.Marshal(InteractionResult{Error: errToStr(err)})
		hi := hostdb.Interaction{
			Timestamp: timestamp,
			Type:      typ,
			Result:    json.RawMessage(b),
		}
		return hi, true
	}

	switch m := m.(type) {
	case MetricHostDial:
		return transform(m.Timestamp, "dial", m.Err, struct {
			HostIP    string        `json:"hostIP"`
			Timestamp time.Time     `json:"timestamp"`
			Elapsed   time.Duration `json:"elapsed"`
		}{m.HostIP, m.Timestamp, m.Elapsed})
	case rhpv2.MetricRPC:
		return transform(m.Timestamp, "rhpv2 rpc", m.Err, struct {
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

type contractCapability struct {
	HostKey   consensus.PublicKey
	HostIP    string
	ID        types.FileContractID
	RenterKey consensus.PrivateKey
}

func parseContractCapabilities(contracts []bus.Contract, masterKey [32]byte) []contractCapability {
	ccs := make([]contractCapability, len(contracts))
	for i, c := range contracts {
		ccs[i] = contractCapability{
			HostKey:   c.HostKey,
			HostIP:    c.HostIP,
			ID:        c.ID,
			RenterKey: deriveRenterKey(masterKey, c.HostKey),
		}
	}
	return ccs
}

// A Bus is the source of truth within a renterd system.
type Bus interface {
	RecordHostInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error
	ContractSetContracts(name string) ([]bus.Contract, error)
	ContractsForSlab(shards []object.Sector) ([]bus.Contract, error)
	Contracts() ([]bus.Contract, error)

	UploadParams() (bus.UploadParams, error)
	MigrateParams(slab object.Slab) (bus.MigrateParams, error)

	Object(key string) (object.Object, []string, error)
	AddObject(key string, o object.Object, usedContracts map[consensus.PublicKey]types.FileContractID) error
	DeleteObject(key string) error

	WalletDiscard(txn types.Transaction) error
	WalletPrepareForm(renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, hostCollateral types.Currency, endHeight uint64, hostSettings rhpv2.HostSettings) (txns []types.Transaction, err error)
	WalletPrepareRenew(contract types.FileContractRevision, renterKey consensus.PrivateKey, hostKey consensus.PublicKey, renterFunds types.Currency, renterAddress types.UnlockHash, endHeight uint64, hostSettings rhpv2.HostSettings) ([]types.Transaction, types.Currency, error)
}

// TODO: deriving the renter key from the host key using the master key only
// works if we persist a hash of the renter's master key in the database and
// compare it on startup, otherwise there's no way of knowing the derived key is
// usuable
//
// TODO: instead of deriving a renter key use a randomly generated salt so we're
// not limited to one key per host
func deriveRenterKey(masterKey [32]byte, hostKey consensus.PublicKey) consensus.PrivateKey {
	seed := blake2b.Sum256(append(masterKey[:], hostKey[:]...))
	pk := consensus.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

// A worker talks to Sia hosts to perform contract and storage operations within
// a renterd system.
type worker struct {
	bus       Bus
	pool      *sessionPool
	masterKey [32]byte
}

func (w *worker) recordScan(hostKey consensus.PublicKey, settings rhpv2.HostSettings, err error) error {
	hi := hostdb.Interaction{
		Timestamp: time.Now(),
		Type:      "scan",
	}
	if err == nil {
		hi.Result, _ = json.Marshal(ScanResult{
			Settings: settings,
		})
	} else {
		hi.Result, _ = json.Marshal(ScanResult{
			InteractionResult: InteractionResult{
				Error: errToStr(err),
			},
		})
	}
	return w.bus.RecordHostInteraction(hostKey, hi)
}

func (w *worker) withTransportV2(ctx context.Context, hostIP string, hostKey consensus.PublicKey, fn func(*rhpv2.Transport) error) (err error) {
	var mr ephemeralMetricsRecorder
	defer func() {
		// TODO: send all interactions in one request
		for _, hi := range mr.interactions() {
			// TODO: handle error
			_ = w.bus.RecordHostInteraction(hostKey, hi)
		}
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

func (w *worker) withTransportV3(ctx context.Context, hostIP string, hostKey consensus.PublicKey, fn func(*rhpv3.Transport) error) (err error) {
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

func (w *worker) withHosts(ctx context.Context, contracts []contractCapability, fn func([]sectorStore) error) (err error) {
	var hosts []sectorStore
	for _, c := range contracts {
		hosts = append(hosts, w.pool.session(ctx, c.HostKey, c.HostIP, c.ID, c.RenterKey))
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

func (w *worker) rhpPreparePaymentHandler(jc jape.Context) {
	var rppr RHPPreparePaymentRequest
	if jc.Decode(&rppr) == nil {
		jc.Encode(rhpv3.PayByEphemeralAccount(rppr.Account, rppr.Amount, rppr.Expiry, rppr.AccountKey))
	}
}

func (w *worker) rhpScanHandler(jc jape.Context) {
	var rsr RHPScanRequest
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
	err := w.withTransportV2(ctx, rsr.HostIP, rsr.HostKey, func(t *rhpv2.Transport) (err error) {
		settings, err = rhpv2.RPCSettings(ctx, t)
		return err
	})
	elapsed := time.Since(start)
	w.recordScan(rsr.HostKey, settings, err)

	if jc.Check("couldn't scan host", err) != nil {
		return
	}
	jc.Encode(RHPScanResponse{
		Settings: settings,
		Ping:     Duration(elapsed),
	})
}

func (w *worker) rhpFormHandler(jc jape.Context) {
	var rfr RHPFormRequest
	if jc.Decode(&rfr) != nil {
		return
	}

	hostSettings, hostKey, renterFunds := rfr.HostSettings, rfr.HostKey, rfr.RenterFunds
	renterAddress, endHeight, hostCollateral := rfr.RenterAddress, rfr.EndHeight, rfr.HostCollateral
	rk := deriveRenterKey(w.masterKey, hostKey)

	var contract rhpv2.ContractRevision
	var txnSet []types.Transaction
	err := w.withTransportV2(jc.Request.Context(), hostSettings.NetAddress, rfr.HostKey, func(t *rhpv2.Transport) (err error) {
		renterTxnSet, err := w.bus.WalletPrepareForm(rk, hostKey, renterFunds, renterAddress, hostCollateral, endHeight, hostSettings)
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
	jc.Encode(RHPFormResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (w *worker) rhpRenewHandler(jc jape.Context) {
	var rrr RHPRenewRequest
	if jc.Decode(&rrr) != nil {
		return
	}
	hostSettings, hostKey, toRenewID, renterFunds := rrr.HostSettings, rrr.HostKey, rrr.ContractID, rrr.RenterFunds
	renterAddress, endHeight := rrr.RenterAddress, rrr.EndHeight
	rk := deriveRenterKey(w.masterKey, hostKey)

	var contract rhpv2.ContractRevision
	var txnSet []types.Transaction
	err := w.withTransportV2(jc.Request.Context(), hostSettings.NetAddress, hostKey, func(t *rhpv2.Transport) error {
		session, err := rhpv2.RPCLock(jc.Request.Context(), t, toRenewID, rk, 5*time.Second)
		if err != nil {
			return err
		}
		rev := session.Revision()

		renterTxnSet, finalPayment, err := w.bus.WalletPrepareRenew(rev.Revision, rk, hostKey, renterFunds, renterAddress, endHeight, hostSettings)
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
	jc.Encode(RHPRenewResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (w *worker) rhpFundHandler(jc jape.Context) {
	var rfr RHPFundRequest
	if jc.Decode(&rfr) != nil {
		return
	}
	rk := deriveRenterKey(w.masterKey, rfr.HostKey)
	err := w.withTransportV3(jc.Request.Context(), rfr.HostIP, rfr.HostKey, func(t *rhpv3.Transport) (err error) {
		// The FundAccount RPC requires a SettingsID, which we also have to pay
		// for. To simplify things, we pay for the SettingsID using the full
		// amount, with the "refund" going to the desired account; we then top
		// up the account to cover the cost of the two RPCs.
		payment, ok := rhpv3.PayByContract(&rfr.Contract, rfr.Amount, rfr.Account, rk)
		if !ok {
			return errors.New("insufficient funds")
		}
		priceTable, err := rhpv3.RPCPriceTable(t, &payment)
		if err != nil {
			return err
		}
		payment, ok = rhpv3.PayByContract(&rfr.Contract, priceTable.UpdatePriceTableCost.Add(priceTable.FundAccountCost), rhpv3.ZeroAccount, rk)
		if !ok {
			return errors.New("insufficient funds")
		}
		return rhpv3.RPCFundAccount(t, &payment, rfr.Account, priceTable.ID)
	})
	if jc.Check("couldn't fund account", err) != nil {
		return
	}
}

func (w *worker) rhpRegistryReadHandler(jc jape.Context) {
	var rrrr RHPRegistryReadRequest
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
	var rrur RHPRegistryUpdateRequest
	if jc.Decode(&rrur) != nil {
		return
	}
	err := w.withTransportV3(jc.Request.Context(), rrur.HostIP, rrur.HostKey, func(t *rhpv3.Transport) (err error) {
		return rhpv3.RPCUpdateRegistry(t, &rrur.Payment, rrur.RegistryKey, rrur.RegistryValue)
	})
	if jc.Check("couldn't update registry", err) != nil {
		return
	}
}

func (w *worker) slabsMigrateHandler(jc jape.Context) {
	var slab object.Slab
	if jc.Decode(&slab) != nil {
		return
	}

	mp, err := w.bus.MigrateParams(slab)
	if jc.Check("couldn't fetch migration parameters from bus", err) != nil {
		return
	}

	bFrom, err := w.bus.ContractSetContracts(mp.FromContracts)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}
	from := parseContractCapabilities(bFrom, [32]byte{}) // TODO

	bTo, err := w.bus.ContractSetContracts(mp.ToContracts)
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}
	to := parseContractCapabilities(bTo, [32]byte{}) // TODO

	w.pool.setCurrentHeight(mp.CurrentHeight)
	err = w.withHosts(jc.Request.Context(), append(from, to...), func(hosts []sectorStore) error {
		return migrateSlab(jc.Request.Context(), &slab, hosts[:len(from)], hosts[len(from):])
	})
	if jc.Check("couldn't migrate slabs", err) != nil {
		return
	}

	// TODO: After migration, we need to notify the bus of the changes to the slab.
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
		contracts, err := w.bus.ContractsForSlab(ss.Shards)
		if err != nil {
			_ = err // NOTE: can't write error because we may have already written to the response
			return
		}

		// TODO: Lock contracts
		cs := parseContractCapabilities(contracts, [32]byte{}) // TODO
		err = w.withHosts(jc.Request.Context(), cs, func(hosts []sectorStore) error {
			return downloadSlab(jc.Request.Context(), cw, ss, hosts)
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

	o := object.Object{
		Key: object.GenerateEncryptionKey(),
	}
	w.pool.setCurrentHeight(up.CurrentHeight)
	usedContracts := make(map[consensus.PublicKey]types.FileContractID)
	for {
		// fetch contracts
		bcs, err := w.bus.ContractSetContracts(up.ContractSet)
		if jc.Check("couldn't fetch contracts from bus", err) != nil {
			return
		}
		cs := parseContractCapabilities(bcs, [32]byte{}) // TODO

		// TODO: Lock contracts

		r := io.LimitReader(jc.Request.Body, int64(up.MinShards)*rhpv2.SectorSize)
		var s object.Slab
		var length int
		err = w.withHosts(jc.Request.Context(), cs, func(hosts []sectorStore) (err error) {
			s, length, err = uploadSlab(jc.Request.Context(), r, up.MinShards, up.TotalShards, hosts)
			return err
		})
		if err == io.EOF {
			break
		} else if jc.Check("couldn't upload slab", err) != nil {
			return
		}
		o.Slabs = append(o.Slabs, object.SlabSlice{
			Slab:   s,
			Offset: 0,
			Length: uint32(length),
		})

		for _, ss := range s.Shards {
			if _, ok := usedContracts[ss.Host]; !ok {
				for _, c := range cs {
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

func (w *worker) rhpContractsHandlerGET(jc jape.Context) {
	busContracts, err := w.bus.Contracts()
	if jc.Check("failed to fetch contracts from bus", err) != nil {
		return
	}
	cc := parseContractCapabilities(busContracts, w.masterKey)

	var contracts []Contract
	err = w.withHosts(jc.Request.Context(), cc, func(ss []sectorStore) error {
		for i, store := range ss {
			rev, err := store.(*session).Revision()
			if err != nil {
				return err
			}
			contracts = append(contracts, Contract{
				Contract: busContracts[i],
				Revision: rev,
			})
		}
		return nil
	})
	if jc.Check("failed to fetch contracts", err) != nil {
		return
	}
	jc.Encode(contracts)
}

// New returns an HTTP handler that serves the worker API.
func New(masterKey [32]byte, b Bus) http.Handler {
	w := &worker{
		bus:       b,
		pool:      newSessionPool(),
		masterKey: masterKey,
	}
	return jape.Mux(map[string]jape.Handler{
		"GET    /rhp/contracts":       w.rhpContractsHandlerGET,
		"POST   /rhp/prepare/payment": w.rhpPreparePaymentHandler,
		"POST   /rhp/scan":            w.rhpScanHandler,
		"POST   /rhp/form":            w.rhpFormHandler,
		"POST   /rhp/renew":           w.rhpRenewHandler,
		"POST   /rhp/fund":            w.rhpFundHandler,
		"POST   /rhp/registry/read":   w.rhpRegistryReadHandler,
		"POST   /rhp/registry/update": w.rhpRegistryUpdateHandler,

		"POST   /slabs/migrate": w.slabsMigrateHandler,

		"GET    /objects/*key": w.objectsKeyHandlerGET,
		"PUT    /objects/*key": w.objectsKeyHandlerPUT,
		"DELETE /objects/*key": w.objectsKeyHandlerDELETE,
	})
}
