package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/renterd/slab"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/siad/types"
)

type (
	// A ChainManager manages blockchain state.
	ChainManager interface {
		TipState() consensus.State
	}

	// A Syncer can connect to other peers and synchronize the blockchain.
	Syncer interface {
		Addr() string
		Peers() []string
		Connect(addr string) error
		BroadcastTransaction(txn types.Transaction, dependsOn []types.Transaction)
	}

	// A TransactionPool can validate and relay unconfirmed transactions.
	TransactionPool interface {
		RecommendedFee() types.Currency
		Transactions() []types.Transaction
		AddTransactionSet(txns []types.Transaction) error
		UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error)
	}

	// A Wallet can spend and receive siacoins.
	Wallet interface {
		Balance() types.Currency
		Address() types.UnlockHash
		UnspentOutputs() ([]wallet.SiacoinElement, error)
		Transactions(since time.Time, max int) ([]wallet.Transaction, error)
		FundTransaction(cs consensus.State, txn *types.Transaction, amount types.Currency, pool []types.Transaction) ([]types.OutputID, error)
		ReleaseInputs(txn types.Transaction)
		SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.OutputID, cf types.CoveredFields) error
	}

	// A HostDB stores information about hosts.
	HostDB interface {
		SelectHosts(n int, filter func(hostdb.Host) bool) ([]hostdb.Host, error)
		Host(hostKey consensus.PublicKey) (hostdb.Host, error)
		SetScore(hostKey consensus.PublicKey, score float64) error
		RecordInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error
	}

	// An RHP implements the renter-host protocol.
	RHP interface {
		Settings(ctx context.Context, hostIP string, hostKey consensus.PublicKey) (rhpv2.HostSettings, error)
		FormContract(ctx context.Context, cs consensus.State, hostIP string, hostKey consensus.PublicKey, renterKey consensus.PrivateKey, txns []types.Transaction) (rhpv2.Contract, []types.Transaction, error)
		RenewContract(ctx context.Context, cs consensus.State, hostIP string, hostKey consensus.PublicKey, renterKey consensus.PrivateKey, contractID types.FileContractID, txns []types.Transaction, finalPayment types.Currency) (rhpv2.Contract, []types.Transaction, error)
		FundAccount(ctx context.Context, hostIP string, hostKey consensus.PublicKey, contract types.FileContractRevision, renterKey consensus.PrivateKey, account rhpv3.Account, amount types.Currency) (rhpv2.Contract, error)
		ReadRegistry(ctx context.Context, hostIP string, hostKey consensus.PublicKey, payment rhpv3.PaymentMethod, registryKey rhpv3.RegistryKey) (rhpv3.RegistryValue, error)
		UpdateRegistry(ctx context.Context, hostIP string, hostKey consensus.PublicKey, payment rhpv3.PaymentMethod, registryKey rhpv3.RegistryKey, registryValue rhpv3.RegistryValue) error
	}

	// A ContractStore stores contracts.
	ContractStore interface {
		Contracts() ([]rhpv2.Contract, error)
		Contract(id types.FileContractID) (rhpv2.Contract, error)
		AddContract(c rhpv2.Contract) error
		RemoveContract(id types.FileContractID) error
	}

	// A SlabMover uploads, downloads, and migrates slabs.
	SlabMover interface {
		UploadSlabs(ctx context.Context, r io.Reader, m, n uint8, currentHeight uint64, contracts []Contract) ([]slab.Slab, error)
		DownloadSlabs(ctx context.Context, w io.Writer, slabs []slab.Slice, offset, length int64, contracts []Contract) error
		DeleteSlabs(ctx context.Context, slabs []slab.Slab, contracts []Contract) error
		MigrateSlabs(ctx context.Context, slabs []slab.Slab, currentHeight uint64, from, to []Contract) error
	}

	// An ObjectStore stores objects.
	ObjectStore interface {
		List(key string) []string
		Get(key string) (object.Object, error)
		Put(key string, o object.Object) error
		Delete(key string) error
	}
)

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	// encode nil slices as [] and nil maps as {} (instead of null)
	if val := reflect.ValueOf(v); val.Kind() == reflect.Slice && val.Len() == 0 {
		w.Write([]byte("[]\n"))
		return
	} else if val.Kind() == reflect.Map && val.Len() == 0 {
		w.Write([]byte("{}\n"))
		return
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "\t")
	enc.Encode(v)
}

func readJSON(w http.ResponseWriter, req *http.Request, v interface{}) bool {
	if err := json.NewDecoder(req.Body).Decode(v); err != nil {
		http.Error(w, fmt.Sprintf("couldn't decode request type (%T): %v", v, err), http.StatusBadRequest)
		return false
	}
	return true
}

func readPathParam(w http.ResponseWriter, ps httprouter.Params, param string, v interface{}) bool {
	var err error
	switch v := v.(type) {
	case interface{ UnmarshalText([]byte) error }:
		err = v.UnmarshalText([]byte(ps.ByName(param)))
	case interface{ LoadString(string) error }:
		err = v.LoadString(ps.ByName(param))
	default:
		panic("unsupported type")
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("couldn't parse param %q: %v", param, err), http.StatusBadRequest)
		return false
	}
	return true
}

func check(w http.ResponseWriter, ctx string, err error) bool {
	if err != nil {
		http.Error(w, fmt.Sprintf("%v: %v", ctx, err), http.StatusInternalServerError)
		return true
	}
	return false
}

// AuthMiddleware enforces HTTP Basic Authentication on the provided handler.
func AuthMiddleware(handler http.Handler, requiredPass string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if _, password, ok := req.BasicAuth(); !ok || password != requiredPass {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, req)
	})
}

type server struct {
	s   Syncer
	cm  ChainManager
	tp  TransactionPool
	w   Wallet
	hdb HostDB
	rhp RHP
	cs  ContractStore
	sm  SlabMover
	os  ObjectStore
}

func (s *server) syncerPeersHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	writeJSON(w, s.s.Peers())
}

func (s *server) syncerConnectHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var addr string
	if readJSON(w, req, &addr) {
		check(w, "couldn't connect to peer", s.s.Connect(addr))
	}
}

func (s *server) consensusTipHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	writeJSON(w, s.cm.TipState().Index)
}

func (s *server) txpoolTransactionsHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	writeJSON(w, s.tp.Transactions())
}

func (s *server) txpoolBroadcastHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var txnSet []types.Transaction
	if readJSON(w, req, &txnSet) {
		check(w, "couldn't broadcast transaction set", s.tp.AddTransactionSet(txnSet))
	}
}

func (s *server) walletBalanceHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	writeJSON(w, s.w.Balance())
}

func (s *server) walletAddressHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	writeJSON(w, s.w.Address())
}

func (s *server) walletTransactionsHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var since time.Time
	if v := req.FormValue("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if check(w, "invalid 'since' value", err) {
			return
		}
		since = t
	}
	max := -1
	if v := req.FormValue("max"); v != "" {
		t, err := strconv.Atoi(v)
		if check(w, "invalid 'max' value", err) {
			return
		}
		max = t
	}
	txns, err := s.w.Transactions(since, max)
	if !check(w, "couldn't load transactions", err) {
		writeJSON(w, txns)
	}
}

func (s *server) walletOutputsHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	utxos, err := s.w.UnspentOutputs()
	if !check(w, "couldn't load outputs", err) {
		writeJSON(w, utxos)
	}
}

func (s *server) walletFundHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var wfr WalletFundRequest
	if !readJSON(w, req, &wfr) {
		return
	}
	txn := wfr.Transaction
	fee := s.tp.RecommendedFee().Mul64(uint64(len(encoding.Marshal(txn))))
	txn.MinerFees = []types.Currency{fee}
	toSign, err := s.w.FundTransaction(s.cm.TipState(), &wfr.Transaction, wfr.Amount.Add(txn.MinerFees[0]), s.tp.Transactions())
	if check(w, "couldn't fund transaction", err) {
		return
	}
	parents, err := s.tp.UnconfirmedParents(txn)
	if check(w, "couldn't load transaction dependencies", err) {
		s.w.ReleaseInputs(txn)
		return
	}
	writeJSON(w, WalletFundResponse{
		Transaction: txn,
		ToSign:      toSign,
		DependsOn:   parents,
	})
}

func (s *server) walletSignHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var wsr WalletSignRequest
	if !readJSON(w, req, &wsr) {
		return
	}
	err := s.w.SignTransaction(s.cm.TipState(), &wsr.Transaction, wsr.ToSign, wsr.CoveredFields)
	if !check(w, "couldn't sign transaction", err) {
		writeJSON(w, wsr.Transaction)
	}
}

func (s *server) walletDiscardHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var txn types.Transaction
	if readJSON(w, req, &txn) {
		s.w.ReleaseInputs(txn)
	}
}

func (s *server) walletPrepareFormHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var wpfr WalletPrepareFormRequest
	if !readJSON(w, req, &wpfr) {
		return
	}
	fc := rhpv2.PrepareContractFormation(wpfr.RenterKey, wpfr.HostKey, wpfr.RenterFunds, wpfr.HostCollateral, wpfr.EndHeight, wpfr.HostSettings, wpfr.RenterAddress)
	cost := rhpv2.ContractFormationCost(fc, wpfr.HostSettings.ContractPrice)
	txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}
	txn.MinerFees = []types.Currency{s.tp.RecommendedFee().Mul64(uint64(len(encoding.Marshal(txn))))}
	toSign, err := s.w.FundTransaction(s.cm.TipState(), &txn, cost.Add(txn.MinerFees[0]), s.tp.Transactions())
	if !check(w, "couldn't fund transaction", err) {
		return
	}
	cf := wallet.ExplicitCoveredFields(txn)
	err = s.w.SignTransaction(s.cm.TipState(), &txn, toSign, cf)
	if check(w, "couldn't sign transaction", err) {
		s.w.ReleaseInputs(txn)
		return
	}
	parents, err := s.tp.UnconfirmedParents(txn)
	if check(w, "couldn't load transaction dependencies", err) {
		s.w.ReleaseInputs(txn)
		return
	}
	writeJSON(w, append(parents, txn))
}

func (s *server) walletPrepareRenewHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var wprr WalletPrepareRenewRequest
	if !readJSON(w, req, &wprr) {
		return
	}
	fc := rhpv2.PrepareContractRenewal(wprr.Contract, wprr.RenterKey, wprr.HostKey, wprr.RenterFunds, wprr.HostCollateral, wprr.EndHeight, wprr.HostSettings, wprr.RenterAddress)
	cost := rhpv2.ContractRenewalCost(fc, wprr.HostSettings.ContractPrice)
	finalPayment := wprr.HostSettings.BaseRPCPrice
	if finalPayment.Cmp(wprr.Contract.ValidRenterPayout()) > 0 {
		finalPayment = wprr.Contract.ValidRenterPayout()
	}
	txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}
	txn.MinerFees = []types.Currency{s.tp.RecommendedFee().Mul64(uint64(len(encoding.Marshal(txn))))}
	toSign, err := s.w.FundTransaction(s.cm.TipState(), &txn, cost.Add(txn.MinerFees[0]), s.tp.Transactions())
	if !check(w, "couldn't fund transaction", err) {
		return
	}
	cf := wallet.ExplicitCoveredFields(txn)
	err = s.w.SignTransaction(s.cm.TipState(), &txn, toSign, cf)
	if check(w, "couldn't sign transaction", err) {
		s.w.ReleaseInputs(txn)
		return
	}
	parents, err := s.tp.UnconfirmedParents(txn)
	if check(w, "couldn't load transaction dependencies", err) {
		s.w.ReleaseInputs(txn)
		return
	}
	writeJSON(w, WalletPrepareRenewResponse{
		TransactionSet: append(parents, txn),
		FinalPayment:   finalPayment,
	})
}

func (s *server) hostsHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// TODO: support filtering via query params
	hosts, err := s.hdb.SelectHosts(-1, func(hostdb.Host) bool { return true })
	if !check(w, "couldn't load hosts", err) {
		writeJSON(w, hosts)
	}
}

func (s *server) hostsPubkeyHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var pk PublicKey
	if !readPathParam(w, ps, "pubkey", &pk) {
		return
	}
	host, err := s.hdb.Host(pk)
	if !check(w, "couldn't load host", err) {
		writeJSON(w, host)
	}
}

func (s *server) hostsScoreHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var score float64
	var pk PublicKey
	if readJSON(w, req, &score) && readPathParam(w, ps, "pubkey", &pk) {
		check(w, "couldn't set score", s.hdb.SetScore(pk, score))
	}
}

func (s *server) hostsInteractionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var hi hostdb.Interaction
	var pk PublicKey
	if readJSON(w, req, &hi) && readPathParam(w, ps, "pubkey", &pk) {
		check(w, "couldn't record interaction", s.hdb.RecordInteraction(pk, hi))
	}
}

func (s *server) rhpPrepareFormHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var rpfr RHPPrepareFormRequest
	if !readJSON(w, req, &rpfr) {
		return
	}
	fc := rhpv2.PrepareContractFormation(rpfr.RenterKey, rpfr.HostKey, rpfr.RenterFunds, rpfr.HostCollateral, rpfr.EndHeight, rpfr.HostSettings, rpfr.RenterAddress)
	cost := rhpv2.ContractFormationCost(fc, rpfr.HostSettings.ContractPrice)
	writeJSON(w, RHPPrepareFormResponse{
		Contract: fc,
		Cost:     cost,
	})
}

func (s *server) rhpPrepareRenewHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var rprr RHPPrepareRenewRequest
	if !readJSON(w, req, &rprr) {
		return
	}
	fc := rhpv2.PrepareContractRenewal(rprr.Contract, rprr.RenterKey, rprr.HostKey, rprr.RenterFunds, rprr.HostCollateral, rprr.EndHeight, rprr.HostSettings, rprr.RenterAddress)
	cost := rhpv2.ContractRenewalCost(fc, rprr.HostSettings.ContractPrice)
	finalPayment := rprr.HostSettings.BaseRPCPrice
	if finalPayment.Cmp(rprr.Contract.ValidRenterPayout()) > 0 {
		finalPayment = rprr.Contract.ValidRenterPayout()
	}
	writeJSON(w, RHPPrepareRenewResponse{
		Contract:     fc,
		Cost:         cost,
		FinalPayment: finalPayment,
	})
}

func (s *server) rhpPreparePaymentHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var rppr RHPPreparePaymentRequest
	if readJSON(w, req, &rppr) {
		writeJSON(w, rhpv3.PayByEphemeralAccount(rppr.Account, rppr.Amount, rppr.Expiry, rppr.AccountKey))
	}
}

func (s *server) rhpScanHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var rsr RHPScanRequest
	if !readJSON(w, req, &rsr) {
		return
	}
	settings, err := s.rhp.Settings(req.Context(), rsr.HostIP, rsr.HostKey)
	if !check(w, "couldn't scan host", err) {
		writeJSON(w, settings)
	}
}

func (s *server) rhpFormHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var rfr RHPFormRequest
	if !readJSON(w, req, &rfr) {
		return
	}
	var cs consensus.State
	cs.Index.Height = uint64(rfr.TransactionSet[len(rfr.TransactionSet)-1].FileContracts[0].WindowStart)
	contract, txnSet, err := s.rhp.FormContract(req.Context(), cs, rfr.HostIP, rfr.HostKey, rfr.RenterKey, rfr.TransactionSet)
	if check(w, "couldn't form contract", err) {
		return
	}
	writeJSON(w, RHPFormResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (s *server) rhpRenewHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var rrr RHPRenewRequest
	if !readJSON(w, req, &rrr) {
		return
	}
	var cs consensus.State
	cs.Index.Height = uint64(rrr.TransactionSet[len(rrr.TransactionSet)-1].FileContracts[0].WindowStart)
	contract, txnSet, err := s.rhp.RenewContract(req.Context(), cs, rrr.HostIP, rrr.HostKey, rrr.RenterKey, rrr.ContractID, rrr.TransactionSet, rrr.FinalPayment)
	if check(w, "couldn't renew contract", err) {
		return
	}
	writeJSON(w, RHPRenewResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (s *server) rhpFundHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var rfr RHPFundRequest
	if !readJSON(w, req, &rfr) {
		return
	}
	_, err := s.rhp.FundAccount(req.Context(), rfr.HostIP, rfr.HostKey, rfr.Contract, rfr.RenterKey, rfr.Account, rfr.Amount)
	check(w, "couldn't fund account", err)
}

func (s *server) rhpRegistryReadHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var rrrr RHPRegistryReadRequest
	if !readJSON(w, req, &rrrr) {
		return
	}
	value, err := s.rhp.ReadRegistry(req.Context(), rrrr.HostIP, rrrr.HostKey, &rrrr.Payment, rrrr.RegistryKey)
	if !check(w, "couldn't read registry", err) {
		writeJSON(w, value)
	}
}

func (s *server) rhpRegistryUpdateHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var rrur RHPRegistryUpdateRequest
	if !readJSON(w, req, &rrur) {
		return
	}
	err := s.rhp.UpdateRegistry(req.Context(), rrur.HostIP, rrur.HostKey, &rrur.Payment, rrur.RegistryKey, rrur.RegistryValue)
	check(w, "couldn't update registry", err)
}

func (s *server) contractsHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	cs, err := s.cs.Contracts()
	if !check(w, "couldn't load contracts", err) {
		writeJSON(w, cs)
	}
}

func (s *server) contractsIDHandlerGET(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var id types.FileContractID
	if !readPathParam(w, ps, "id", &id) {
		return
	}
	c, err := s.cs.Contract(id)
	if !check(w, "couldn't load contract", err) {
		writeJSON(w, c)
	}
}

func (s *server) contractsIDHandlerPUT(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var id types.FileContractID
	var c rhpv2.Contract
	if !readPathParam(w, ps, "id", &id) && !readJSON(w, req, &c) {
		return
	}
	if c.ID() != id {
		http.Error(w, "contract ID mismatch", http.StatusBadRequest)
		return
	}
	check(w, "couldn't store contract", s.cs.AddContract(c))
}

func (s *server) contractsIDHandlerDELETE(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var id types.FileContractID
	if !readPathParam(w, ps, "id", &id) {
		return
	}
	check(w, "couldn't remove contract", s.cs.RemoveContract(id))
}

func (s *server) slabsUploadHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var sur SlabsUploadRequest
	if err := json.NewDecoder(strings.NewReader(req.PostFormValue("meta"))).Decode(&sur); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f, _, err := req.FormFile("data")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	slabs, err := s.sm.UploadSlabs(req.Context(), f, sur.MinShards, sur.TotalShards, sur.CurrentHeight, sur.Contracts)
	if !check(w, "couldn't upload slabs", err) {
		writeJSON(w, slabs)
	}
}

func (s *server) slabsDownloadHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var sdr SlabsDownloadRequest
	if !readJSON(w, req, &sdr) {
		return
	}
	if sdr.Length == 0 {
		for _, ss := range sdr.Slabs {
			sdr.Length += int64(ss.Length)
		}
	}
	// TODO: if we encounter an error halfway through the download, it's too
	// late to change the response code and send an error message. Not sure how
	// best to handle this.
	err := s.sm.DownloadSlabs(req.Context(), w, sdr.Slabs, sdr.Offset, sdr.Length, sdr.Contracts)
	check(w, "couldn't download slabs", err)
}

func (s *server) slabsMigrateHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var smr SlabsMigrateRequest
	if readJSON(w, req, &smr) {
		err := s.sm.MigrateSlabs(req.Context(), smr.Slabs, smr.CurrentHeight, smr.From, smr.To)
		check(w, "couldn't migrate slabs", err)
	}
}

func (s *server) slabsDeleteHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var sdr SlabsDeleteRequest
	if readJSON(w, req, &sdr) {
		err := s.sm.DeleteSlabs(req.Context(), sdr.Slabs, sdr.Contracts)
		check(w, "couldn't delete slabs", err)
	}
}

func (s *server) objectsKeyHandlerGET(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	if strings.HasSuffix(ps.ByName("key"), "/") {
		writeJSON(w, s.os.List(ps.ByName("key")))
		return
	}
	o, err := s.os.Get(ps.ByName("key"))
	if !check(w, "couldn't load object", err) {
		writeJSON(w, o)
	}
}

func (s *server) objectsKeyHandlerPUT(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var o object.Object
	if readJSON(w, req, &o) {
		check(w, "couldn't store object", s.os.Put(ps.ByName("key"), o))
	}
}

func (s *server) objectsKeyHandlerDELETE(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	check(w, "couldn't delete object", s.os.Delete(ps.ByName("key")))
}

// NewServer returns an HTTP handler that serves the renterd API.
func NewServer(cm ChainManager, s Syncer, tp TransactionPool, w Wallet, hdb HostDB, rhp RHP, cs ContractStore, sm SlabMover, os ObjectStore) http.Handler {
	srv := server{
		cm:  cm,
		s:   s,
		tp:  tp,
		w:   w,
		hdb: hdb,
		rhp: rhp,
		cs:  cs,
		sm:  sm,
		os:  os,
	}
	mux := httprouter.New()

	mux.GET("/syncer/peers", srv.syncerPeersHandler)
	mux.POST("/syncer/connect", srv.syncerConnectHandler)

	mux.GET("/consensus/tip", srv.consensusTipHandler)

	mux.GET("/txpool/transactions", srv.txpoolTransactionsHandler)
	mux.POST("/txpool/broadcast", srv.txpoolBroadcastHandler)

	mux.GET("/wallet/balance", srv.walletBalanceHandler)
	mux.GET("/wallet/address", srv.walletAddressHandler)
	mux.GET("/wallet/transactions", srv.walletTransactionsHandler)
	mux.GET("/wallet/outputs", srv.walletOutputsHandler)
	mux.POST("/wallet/fund", srv.walletFundHandler)
	mux.POST("/wallet/sign", srv.walletSignHandler)
	mux.POST("/wallet/discard", srv.walletDiscardHandler)
	mux.POST("/wallet/prepare/form", srv.walletPrepareFormHandler)
	mux.POST("/wallet/prepare/renew", srv.walletPrepareRenewHandler)

	mux.GET("/hosts", srv.hostsHandler)
	mux.GET("/hosts/:pubkey", srv.hostsPubkeyHandler)
	mux.PUT("/hosts/:pubkey/score", srv.hostsScoreHandler)
	mux.POST("/hosts/:pubkey/interaction", srv.hostsInteractionHandler)

	mux.POST("/rhp/prepare/form", srv.rhpPrepareFormHandler)
	mux.POST("/rhp/prepare/renew", srv.rhpPrepareRenewHandler)
	mux.POST("/rhp/prepare/payment", srv.rhpPreparePaymentHandler)
	mux.POST("/rhp/scan", srv.rhpScanHandler)
	mux.POST("/rhp/form", srv.rhpFormHandler)
	mux.POST("/rhp/renew", srv.rhpRenewHandler)
	mux.POST("/rhp/fund", srv.rhpFundHandler)
	mux.POST("/rhp/registry/read", srv.rhpRegistryReadHandler)
	mux.POST("/rhp/registry/update", srv.rhpRegistryUpdateHandler)

	mux.GET("/contracts", srv.contractsHandler)
	mux.GET("/contracts/:id", srv.contractsIDHandlerGET)
	mux.PUT("/contracts/:id", srv.contractsIDHandlerPUT)
	mux.DELETE("/contracts/:id", srv.contractsIDHandlerDELETE)

	mux.POST("/slabs/upload", srv.slabsUploadHandler)
	mux.POST("/slabs/download", srv.slabsDownloadHandler)
	mux.POST("/slabs/migrate", srv.slabsMigrateHandler)
	mux.POST("/slabs/delete", srv.slabsDeleteHandler)

	mux.GET("/objects/*key", srv.objectsKeyHandlerGET)
	mux.PUT("/objects/*key", srv.objectsKeyHandlerPUT)
	mux.DELETE("/objects/*key", srv.objectsKeyHandlerDELETE)

	return mux
}

// NewStatelessServer returns an HTTP handler that serves the stateless renterd API.
func NewStatelessServer(rhp RHP, sm SlabMover) http.Handler {
	srv := server{
		rhp: rhp,
		sm:  sm,
	}
	mux := httprouter.New()

	mux.POST("/rhp/prepare/form", srv.rhpPrepareFormHandler)
	mux.POST("/rhp/prepare/renew", srv.rhpPrepareRenewHandler)
	mux.POST("/rhp/prepare/payment", srv.rhpPreparePaymentHandler)
	mux.POST("/rhp/scan", srv.rhpScanHandler)
	mux.POST("/rhp/form", srv.rhpFormHandler)
	mux.POST("/rhp/renew", srv.rhpRenewHandler)
	mux.POST("/rhp/fund", srv.rhpFundHandler)
	mux.POST("/rhp/registry/read", srv.rhpRegistryReadHandler)
	mux.POST("/rhp/registry/update", srv.rhpRegistryUpdateHandler)

	mux.POST("/slabs/upload", srv.slabsUploadHandler)
	mux.POST("/slabs/download", srv.slabsDownloadHandler)
	mux.POST("/slabs/migrate", srv.slabsMigrateHandler)
	mux.POST("/slabs/delete", srv.slabsDeleteHandler)

	return mux
}
