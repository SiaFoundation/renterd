package api

import (
	"context"
	"encoding/base64"
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

func paramError(w http.ResponseWriter, v interface{}, err error) {
	http.Error(w, fmt.Sprintf("couldn't decode request type (%T): %v", v, err), http.StatusBadRequest)
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
	if err := json.NewDecoder(req.Body).Decode(&addr); err != nil {
		paramError(w, addr, err)
		return
	}
	if err := s.s.Connect(addr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
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
	if err := json.NewDecoder(req.Body).Decode(&txnSet); err != nil {
		paramError(w, txnSet, err)
		return
	}
	if err := s.tp.AddTransactionSet(txnSet); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
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
		if err != nil {
			http.Error(w, "invalid since value: "+err.Error(), http.StatusBadRequest)
			return
		}
		since = t
	}
	max := -1
	if v := req.FormValue("max"); v != "" {
		t, err := strconv.Atoi(v)
		if err != nil {
			http.Error(w, "invalid max value: "+err.Error(), http.StatusBadRequest)
			return
		}
		max = t
	}
	txns, err := s.w.Transactions(since, max)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, txns)
}

func (s *server) walletOutputsHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	utxos, err := s.w.UnspentOutputs()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, utxos)
}

func (s *server) walletFundHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var wfr WalletFundRequest
	if err := json.NewDecoder(req.Body).Decode(&wfr); err != nil {
		paramError(w, wfr, err)
		return
	}
	txn := wfr.Transaction
	fee := s.tp.RecommendedFee().Mul64(uint64(len(encoding.Marshal(txn))))
	txn.MinerFees = []types.Currency{fee}
	toSign, err := s.w.FundTransaction(s.cm.TipState(), &wfr.Transaction, wfr.Amount.Add(txn.MinerFees[0]), s.tp.Transactions())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	parents, err := s.tp.UnconfirmedParents(txn)
	if err != nil {
		s.w.ReleaseInputs(txn)
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
	if err := json.NewDecoder(req.Body).Decode(&wsr); err != nil {
		paramError(w, wsr, err)
		return
	}
	if err := s.w.SignTransaction(s.cm.TipState(), &wsr.Transaction, wsr.ToSign, wsr.CoveredFields); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, wsr.Transaction)
}

func (s *server) walletDiscardHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var txn types.Transaction
	if err := json.NewDecoder(req.Body).Decode(&txn); err != nil {
		paramError(w, txn, err)
		return
	}
	s.w.ReleaseInputs(txn)
}

func (s *server) walletPrepareFormHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var wpfr WalletPrepareFormRequest
	if err := json.NewDecoder(req.Body).Decode(&wpfr); err != nil {
		paramError(w, wpfr, err)
		return
	}
	fc := rhpv2.PrepareContractFormation(wpfr.RenterKey, wpfr.HostKey, wpfr.RenterFunds, wpfr.HostCollateral, wpfr.EndHeight, wpfr.HostSettings, wpfr.RenterAddress)
	cost := rhpv2.ContractFormationCost(fc, wpfr.HostSettings.ContractPrice)
	txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}
	txn.MinerFees = []types.Currency{s.tp.RecommendedFee().Mul64(uint64(len(encoding.Marshal(txn))))}
	_, err := s.w.FundTransaction(s.cm.TipState(), &txn, cost.Add(txn.MinerFees[0]), s.tp.Transactions())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	toSign, err := s.w.FundTransaction(s.cm.TipState(), &txn, cost.Add(txn.MinerFees[0]), nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cf := wallet.ExplicitCoveredFields(txn)
	if err := s.w.SignTransaction(s.cm.TipState(), &txn, toSign, cf); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	parents, err := s.tp.UnconfirmedParents(txn)
	if err != nil {
		s.w.ReleaseInputs(txn)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, append(parents, txn))
}

func (s *server) walletPrepareRenewHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var wprr WalletPrepareRenewRequest
	if err := json.NewDecoder(req.Body).Decode(&wprr); err != nil {
		paramError(w, wprr, err)
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
	_, err := s.w.FundTransaction(s.cm.TipState(), &txn, cost.Add(txn.MinerFees[0]), s.tp.Transactions())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	toSign, err := s.w.FundTransaction(s.cm.TipState(), &txn, cost.Add(txn.MinerFees[0]), nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cf := wallet.ExplicitCoveredFields(txn)
	if err := s.w.SignTransaction(s.cm.TipState(), &txn, toSign, cf); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	parents, err := s.tp.UnconfirmedParents(txn)
	if err != nil {
		s.w.ReleaseInputs(txn)
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, hosts)
}

func (s *server) hostsPubkeyHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var pk consensus.PublicKey
	if err := pk.UnmarshalText([]byte(ps.ByName("pubkey"))); err != nil {
		paramError(w, pk, err)
		return
	}
	host, err := s.hdb.Host(pk)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, host)
}

func (s *server) hostsScoreHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var score float64
	if err := json.NewDecoder(req.Body).Decode(&score); err != nil {
		paramError(w, score, err)
		return
	}
	var pk consensus.PublicKey
	if err := pk.UnmarshalText([]byte(ps.ByName("pubkey"))); err != nil {
		paramError(w, pk, err)
		return
	}
	err := s.hdb.SetScore(pk, score)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) hostsInteractionHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var hi hostdb.Interaction
	if err := json.NewDecoder(req.Body).Decode(&hi); err != nil {
		paramError(w, hi, err)
		return
	}
	var pk consensus.PublicKey
	if err := pk.UnmarshalText([]byte(ps.ByName("pubkey"))); err != nil {
		paramError(w, pk, err)
		return
	}
	err := s.hdb.RecordInteraction(pk, hi)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) rhpPrepareFormHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var rpfr RHPPrepareFormRequest
	if err := json.NewDecoder(req.Body).Decode(&rpfr); err != nil {
		paramError(w, rpfr, err)
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
	if err := json.NewDecoder(req.Body).Decode(&rprr); err != nil {
		paramError(w, rprr, err)
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
	if err := json.NewDecoder(req.Body).Decode(&rppr); err != nil {
		paramError(w, rppr, err)
		return
	}
	payment := rhpv3.PayByEphemeralAccount(rppr.Account, rppr.Amount, rppr.Expiry, rppr.AccountKey)
	writeJSON(w, payment)
}

func (s *server) rhpScanHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var rsr RHPScanRequest
	if err := json.NewDecoder(req.Body).Decode(&rsr); err != nil {
		paramError(w, rsr, err)
		return
	}
	settings, err := s.rhp.Settings(req.Context(), rsr.HostIP, rsr.HostKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, settings)
}

func (s *server) rhpFormHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var rfr RHPFormRequest
	if err := json.NewDecoder(req.Body).Decode(&rfr); err != nil {
		paramError(w, rfr, err)
		return
	}
	var cs consensus.State
	cs.Index.Height = uint64(rfr.TransactionSet[len(rfr.TransactionSet)-1].FileContracts[0].WindowStart)
	contract, txnSet, err := s.rhp.FormContract(req.Context(), cs, rfr.HostIP, rfr.HostKey, rfr.RenterKey, rfr.TransactionSet)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, RHPFormResponse{
		Contract: Contract{
			HostKey:   contract.HostKey(),
			HostIP:    rfr.HostIP,
			ID:        contract.ID(),
			RenterKey: rfr.RenterKey,
		},
		TransactionSet: txnSet,
	})
}

func (s *server) rhpRenewHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var rrr RHPRenewRequest
	if err := json.NewDecoder(req.Body).Decode(&rrr); err != nil {
		paramError(w, rrr, err)
		return
	}
	var cs consensus.State
	cs.Index.Height = uint64(rrr.TransactionSet[len(rrr.TransactionSet)-1].FileContracts[0].WindowStart)
	contract, txnSet, err := s.rhp.RenewContract(req.Context(), cs, rrr.HostIP, rrr.HostKey, rrr.RenterKey, rrr.ContractID, rrr.TransactionSet, rrr.FinalPayment)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, RHPRenewResponse{
		Contract: Contract{
			HostKey:   contract.HostKey(),
			HostIP:    rrr.HostIP,
			ID:        contract.ID(),
			RenterKey: rrr.RenterKey,
		},
		TransactionSet: txnSet,
	})
}

func (s *server) rhpFundHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	var rfr RHPFundRequest
	if err := json.NewDecoder(req.Body).Decode(&rfr); err != nil {
		paramError(w, rfr, err)
		return
	}
	_, err := s.rhp.FundAccount(req.Context(), rfr.HostIP, rfr.HostKey, rfr.Contract, rfr.RenterKey, rfr.Account, rfr.Amount)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) rhpRegistryHandlerGET(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var hostKey consensus.PublicKey
	if err := hostKey.UnmarshalText([]byte("ed25519:" + ps.ByName("host"))); err != nil {
		paramError(w, hostKey, err)
		return
	}
	var registryKey rhpv3.RegistryKey
	if err := registryKey.PublicKey.LoadString("ed25519:" + ps.ByName("key")); err != nil {
		paramError(w, registryKey.PublicKey, err)
		return
	} else if err := registryKey.Tweak.LoadString(ps.ByName("tweak")); err != nil {
		paramError(w, registryKey.Tweak, err)
		return
	}
	hostIP := req.FormValue("hostIP")
	var payment rhpv3.PayByEphemeralAccountRequest
	if b, err := base64.StdEncoding.DecodeString(req.FormValue("payment")); err != nil {
		paramError(w, payment, err)
		return
	} else if err := encoding.Unmarshal(b, &payment); err != nil {
		paramError(w, payment, err)
		return
	}

	value, err := s.rhp.ReadRegistry(req.Context(), hostIP, hostKey, &payment, registryKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, value)
}

func (s *server) rhpRegistryHandlerPUT(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var hostKey consensus.PublicKey
	if err := hostKey.UnmarshalText([]byte("ed25519:" + ps.ByName("host"))); err != nil {
		paramError(w, hostKey, err)
		return
	}
	var registryKey rhpv3.RegistryKey
	if err := registryKey.PublicKey.LoadString("ed25519:" + ps.ByName("key")); err != nil {
		paramError(w, registryKey.PublicKey, err)
		return
	} else if err := registryKey.Tweak.LoadString(ps.ByName("tweak")); err != nil {
		paramError(w, registryKey.Tweak, err)
		return
	}

	hostIP := req.FormValue("hostIP")
	var payment rhpv3.PayByEphemeralAccountRequest
	if b, err := base64.StdEncoding.DecodeString(req.FormValue("payment")); err != nil {
		paramError(w, payment, err)
		return
	} else if err := encoding.Unmarshal(b, &payment); err != nil {
		paramError(w, payment, err)
		return
	}

	var value rhpv3.RegistryValue
	if err := json.NewDecoder(req.Body).Decode(&value); err != nil {
		paramError(w, value, err)
		return
	}

	err := s.rhp.UpdateRegistry(req.Context(), hostIP, hostKey, &payment, registryKey, value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) contractsHandler(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	cs, err := s.cs.Contracts()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, cs)
}

func (s *server) contractsIDHandlerGET(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var id types.FileContractID
	if err := id.LoadString(ps.ByName("id")); err != nil {
		paramError(w, id, err)
		return
	}
	c, err := s.cs.Contract(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, c)
}

func (s *server) contractsIDHandlerPUT(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var id types.FileContractID
	if err := id.LoadString(ps.ByName("id")); err != nil {
		paramError(w, id, err)
		return
	}
	var c rhpv2.Contract
	if err := json.NewDecoder(req.Body).Decode(&c); err != nil {
		paramError(w, c, err)
		return
	} else if c.ID() != id {
		http.Error(w, "contract ID mismatch", http.StatusBadRequest)
		return
	}
	if err := s.cs.AddContract(c); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) contractsIDHandlerDELETE(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var id types.FileContractID
	if err := id.LoadString(ps.ByName("id")); err != nil {
		paramError(w, id, err)
		return
	}
	if err := s.cs.RemoveContract(id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) slabsUploadHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var sur SlabsUploadRequest
	if err := json.NewDecoder(strings.NewReader(req.PostFormValue("meta"))).Decode(&sur); err != nil {
		paramError(w, sur, err)
		return
	}
	f, _, err := req.FormFile("data")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	slabs, err := s.sm.UploadSlabs(req.Context(), f, sur.MinShards, sur.TotalShards, sur.CurrentHeight, sur.Contracts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, slabs)
}

func (s *server) slabsDownloadHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var sdr SlabsDownloadRequest
	if err := json.NewDecoder(req.Body).Decode(&sdr); err != nil {
		paramError(w, sdr, err)
		return
	}
	if sdr.Length == 0 {
		for _, ss := range sdr.Slabs {
			sdr.Length += int64(ss.Length)
		}
	}
	err := s.sm.DownloadSlabs(req.Context(), w, sdr.Slabs, sdr.Offset, sdr.Length, sdr.Contracts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) slabsMigrateHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var smr SlabsMigrateRequest
	if err := json.NewDecoder(req.Body).Decode(&smr); err != nil {
		paramError(w, smr, err)
		return
	}
	err := s.sm.MigrateSlabs(req.Context(), smr.Slabs, smr.CurrentHeight, smr.From, smr.To)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) slabsDeleteHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var sdr SlabsDeleteRequest
	if err := json.NewDecoder(req.Body).Decode(&sdr); err != nil {
		paramError(w, sdr, err)
		return
	}
	err := s.sm.DeleteSlabs(req.Context(), sdr.Slabs, sdr.Contracts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) objectsKeyHandlerGET(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	if strings.HasSuffix(ps.ByName("key"), "/") {
		writeJSON(w, s.os.List(ps.ByName("key")))
		return
	}
	o, err := s.os.Get(ps.ByName("key"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, o)
}

func (s *server) objectsKeyHandlerPUT(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	var o object.Object
	if err := json.NewDecoder(req.Body).Decode(&o); err != nil {
		paramError(w, o, err)
		return
	}
	if err := s.os.Put(ps.ByName("key"), o); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *server) objectsKeyHandlerDELETE(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	if err := s.os.Delete(ps.ByName("key")); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
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
	mux.GET("/rhp/registry/:host/:key", srv.rhpRegistryHandlerGET)
	mux.PUT("/rhp/registry/:host/:key", srv.rhpRegistryHandlerPUT)

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
	mux.GET("/rhp/registry/:host/:key", srv.rhpRegistryHandlerGET)
	mux.PUT("/rhp/registry/:host/:key", srv.rhpRegistryHandlerPUT)

	mux.POST("/slabs/upload", srv.slabsUploadHandler)
	mux.POST("/slabs/download", srv.slabsDownloadHandler)
	mux.POST("/slabs/migrate", srv.slabsMigrateHandler)
	mux.POST("/slabs/delete", srv.slabsDeleteHandler)

	return mux
}
