package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/jape"
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
		Split(cs consensus.State, outputs int, amount, feePerByte types.Currency, pool []types.Transaction) (types.Transaction, []types.OutputID, error)
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

	// A HostSetStore stores host sets.
	HostSetStore interface {
		HostSets() []string
		HostSet(name string) []consensus.PublicKey
		SetHostSet(name string, hosts []consensus.PublicKey) error
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

type server struct {
	s   Syncer
	cm  ChainManager
	tp  TransactionPool
	w   Wallet
	hdb HostDB
	rhp RHP
	cs  ContractStore
	hss HostSetStore
	sm  SlabMover
	os  ObjectStore
}

func (s *server) syncerPeersHandler(jc jape.Context) {
	jc.Encode(s.s.Peers())
}

func (s *server) syncerConnectHandler(jc jape.Context) {
	var addr string
	if jc.Decode(&addr) == nil {
		jc.Check("couldn't connect to peer", s.s.Connect(addr))
	}
}

func (s *server) consensusTipHandler(jc jape.Context) {
	jc.Encode(s.cm.TipState().Index)
}

func (s *server) txpoolTransactionsHandler(jc jape.Context) {
	jc.Encode(s.tp.Transactions())
}

func (s *server) txpoolBroadcastHandler(jc jape.Context) {
	var txnSet []types.Transaction
	if jc.Decode(&txnSet) == nil {
		jc.Check("couldn't broadcast transaction set", s.tp.AddTransactionSet(txnSet))
	}
}

func (s *server) walletBalanceHandler(jc jape.Context) {
	jc.Encode(s.w.Balance())
}

func (s *server) walletAddressHandler(jc jape.Context) {
	jc.Encode(s.w.Address())
}

func (s *server) walletTransactionsHandler(jc jape.Context) {
	var since time.Time
	max := -1
	if jc.DecodeForm("since", (*paramTime)(&since)) != nil || jc.DecodeForm("max", &max) != nil {
		return
	}
	txns, err := s.w.Transactions(since, max)
	if jc.Check("couldn't load transactions", err) == nil {
		jc.Encode(txns)
	}
}

func (s *server) walletOutputsHandler(jc jape.Context) {
	utxos, err := s.w.UnspentOutputs()
	if jc.Check("couldn't load outputs", err) == nil {
		jc.Encode(utxos)
	}
}

func (s *server) walletFundHandler(jc jape.Context) {
	var wfr WalletFundRequest
	if jc.Decode(&wfr) != nil {
		return
	}
	txn := wfr.Transaction
	fee := s.tp.RecommendedFee().Mul64(uint64(len(encoding.Marshal(txn))))
	txn.MinerFees = []types.Currency{fee}
	toSign, err := s.w.FundTransaction(s.cm.TipState(), &txn, wfr.Amount.Add(txn.MinerFees[0]), s.tp.Transactions())
	if jc.Check("couldn't fund transaction", err) != nil {
		return
	}
	parents, err := s.tp.UnconfirmedParents(txn)
	if jc.Check("couldn't load transaction dependencies", err) != nil {
		s.w.ReleaseInputs(txn)
		return
	}
	jc.Encode(WalletFundResponse{
		Transaction: txn,
		ToSign:      toSign,
		DependsOn:   parents,
	})
}

func (s *server) walletSignHandler(jc jape.Context) {
	var wsr WalletSignRequest
	if jc.Decode(&wsr) != nil {
		return
	}
	err := s.w.SignTransaction(s.cm.TipState(), &wsr.Transaction, wsr.ToSign, wsr.CoveredFields)
	if jc.Check("couldn't sign transaction", err) == nil {
		jc.Encode(wsr.Transaction)
	}
}

func (s *server) walletSplitHandler(jc jape.Context) {
	var wfr WalletSplitRequest
	if jc.Decode(&wfr) != nil {
		return
	}
	if wfr.Amount.Cmp(types.SiacoinPrecision) < 0 {
		jc.Error(errors.New("'amount' has to be at least 1SC"), http.StatusBadRequest)
		return
	}
	if wfr.Outputs == 0 {
		jc.Error(errors.New("'outputs' has to be greater than zero"), http.StatusBadRequest)
		return
	}

	txn, toSign, err := s.w.Split(s.cm.TipState(), wfr.Outputs, wfr.Amount, s.tp.RecommendedFee(), s.tp.Transactions())
	if jc.Check("couldn't split the wallet", err) != nil {
		return
	}

	jc.Encode(WalletSplitResponse{
		Transaction:   txn,
		ToSign:        toSign,
		CoveredFields: wallet.ExplicitCoveredFields(txn),
	})
}

func (s *server) walletDiscardHandler(jc jape.Context) {
	var txn types.Transaction
	if jc.Decode(&txn) == nil {
		s.w.ReleaseInputs(txn)
	}
}

func (s *server) walletPrepareFormHandler(jc jape.Context) {
	var wpfr WalletPrepareFormRequest
	if jc.Decode(&wpfr) != nil {
		return
	}
	fc := rhpv2.PrepareContractFormation(wpfr.RenterKey, wpfr.HostKey, wpfr.RenterFunds, wpfr.HostCollateral, wpfr.EndHeight, wpfr.HostSettings, wpfr.RenterAddress)
	cost := rhpv2.ContractFormationCost(fc, wpfr.HostSettings.ContractPrice)
	txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}
	txn.MinerFees = []types.Currency{s.tp.RecommendedFee().Mul64(uint64(len(encoding.Marshal(txn))))}
	toSign, err := s.w.FundTransaction(s.cm.TipState(), &txn, cost.Add(txn.MinerFees[0]), s.tp.Transactions())
	if jc.Check("couldn't fund transaction", err) == nil {
		return
	}
	cf := wallet.ExplicitCoveredFields(txn)
	err = s.w.SignTransaction(s.cm.TipState(), &txn, toSign, cf)
	if jc.Check("couldn't sign transaction", err) != nil {
		s.w.ReleaseInputs(txn)
		return
	}
	parents, err := s.tp.UnconfirmedParents(txn)
	if jc.Check("couldn't load transaction dependencies", err) != nil {
		s.w.ReleaseInputs(txn)
		return
	}
	jc.Encode(append(parents, txn))
}

func (s *server) walletPrepareRenewHandler(jc jape.Context) {
	var wprr WalletPrepareRenewRequest
	if jc.Decode(&wprr) != nil {
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
	if jc.Check("couldn't fund transaction", err) == nil {
		return
	}
	cf := wallet.ExplicitCoveredFields(txn)
	err = s.w.SignTransaction(s.cm.TipState(), &txn, toSign, cf)
	if jc.Check("couldn't sign transaction", err) != nil {
		s.w.ReleaseInputs(txn)
		return
	}
	parents, err := s.tp.UnconfirmedParents(txn)
	if jc.Check("couldn't load transaction dependencies", err) != nil {
		s.w.ReleaseInputs(txn)
		return
	}
	jc.Encode(WalletPrepareRenewResponse{
		TransactionSet: append(parents, txn),
		FinalPayment:   finalPayment,
	})
}

func (s *server) walletPendingHandler(jc jape.Context) {
	isRelevant := func(txn types.Transaction) bool {
		addr := s.w.Address()
		for _, sci := range txn.SiacoinInputs {
			if sci.UnlockConditions.UnlockHash() == addr {
				return true
			}
		}
		for _, sco := range txn.SiacoinOutputs {
			if sco.UnlockHash == addr {
				return true
			}
		}
		return false
	}

	txns := s.tp.Transactions()
	relevant := txns[:0]
	for _, txn := range txns {
		if isRelevant(txn) {
			relevant = append(relevant, txn)
		}
	}
	jc.Encode(relevant)
}

func (s *server) hostsHandler(jc jape.Context) {
	// TODO: support filtering via query params
	hosts, err := s.hdb.SelectHosts(-1, func(hostdb.Host) bool { return true })
	if jc.Check("couldn't load hosts", err) == nil {
		jc.Encode(hosts)
	}
}

func (s *server) hostsPubkeyHandler(jc jape.Context) {
	var pk PublicKey
	if jc.DecodeParam("pubkey", &pk) != nil {
		return
	}
	host, err := s.hdb.Host(pk)
	if jc.Check("couldn't load host", err) == nil {
		jc.Encode(host)
	}
}

func (s *server) hostsScoreHandler(jc jape.Context) {
	var score float64
	var pk PublicKey
	if jc.Decode(&score) == nil && jc.DecodeParam("pubkey", &pk) == nil {
		jc.Check("couldn't set score", s.hdb.SetScore(pk, score))
	}
}

func (s *server) hostsInteractionHandler(jc jape.Context) {
	var hi hostdb.Interaction
	var pk PublicKey
	if jc.Decode(&hi) == nil && jc.DecodeParam("pubkey", &pk) == nil {
		jc.Check("couldn't record interaction", s.hdb.RecordInteraction(pk, hi))
	}
}

func (s *server) rhpPrepareFormHandler(jc jape.Context) {
	var rpfr RHPPrepareFormRequest
	if jc.Decode(&rpfr) != nil {
		return
	}
	fc := rhpv2.PrepareContractFormation(rpfr.RenterKey, rpfr.HostKey, rpfr.RenterFunds, rpfr.HostCollateral, rpfr.EndHeight, rpfr.HostSettings, rpfr.RenterAddress)
	cost := rhpv2.ContractFormationCost(fc, rpfr.HostSettings.ContractPrice)
	jc.Encode(RHPPrepareFormResponse{
		Contract: fc,
		Cost:     cost,
	})
}

func (s *server) rhpPrepareRenewHandler(jc jape.Context) {
	var rprr RHPPrepareRenewRequest
	if jc.Decode(&rprr) != nil {
		return
	}
	fc := rhpv2.PrepareContractRenewal(rprr.Contract, rprr.RenterKey, rprr.HostKey, rprr.RenterFunds, rprr.HostCollateral, rprr.EndHeight, rprr.HostSettings, rprr.RenterAddress)
	cost := rhpv2.ContractRenewalCost(fc, rprr.HostSettings.ContractPrice)
	finalPayment := rprr.HostSettings.BaseRPCPrice
	if finalPayment.Cmp(rprr.Contract.ValidRenterPayout()) > 0 {
		finalPayment = rprr.Contract.ValidRenterPayout()
	}
	jc.Encode(RHPPrepareRenewResponse{
		Contract:     fc,
		Cost:         cost,
		FinalPayment: finalPayment,
	})
}

func (s *server) rhpPreparePaymentHandler(jc jape.Context) {
	var rppr RHPPreparePaymentRequest
	if jc.Decode(&rppr) == nil {
		jc.Encode(rhpv3.PayByEphemeralAccount(rppr.Account, rppr.Amount, rppr.Expiry, rppr.AccountKey))
	}
}

func (s *server) rhpScanHandler(jc jape.Context) {
	var rsr RHPScanRequest
	if jc.Decode(&rsr) != nil {
		return
	}
	settings, err := s.rhp.Settings(jc.Request.Context(), rsr.HostIP, rsr.HostKey)
	if jc.Check("couldn't scan host", err) == nil {
		jc.Encode(settings)
	}
}

func (s *server) rhpFormHandler(jc jape.Context) {
	var rfr RHPFormRequest
	if jc.Decode(&rfr) != nil {
		return
	}
	var cs consensus.State
	cs.Index.Height = uint64(rfr.TransactionSet[len(rfr.TransactionSet)-1].FileContracts[0].WindowStart)
	contract, txnSet, err := s.rhp.FormContract(jc.Request.Context(), cs, rfr.HostIP, rfr.HostKey, rfr.RenterKey, rfr.TransactionSet)
	if jc.Check("couldn't form contract", err) != nil {
		return
	}
	jc.Encode(RHPFormResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (s *server) rhpRenewHandler(jc jape.Context) {
	var rrr RHPRenewRequest
	if jc.Decode(&rrr) != nil {
		return
	}
	var cs consensus.State
	cs.Index.Height = uint64(rrr.TransactionSet[len(rrr.TransactionSet)-1].FileContracts[0].WindowStart)
	contract, txnSet, err := s.rhp.RenewContract(jc.Request.Context(), cs, rrr.HostIP, rrr.HostKey, rrr.RenterKey, rrr.ContractID, rrr.TransactionSet, rrr.FinalPayment)
	if jc.Check("couldn't renew contract", err) != nil {
		return
	}
	jc.Encode(RHPRenewResponse{
		ContractID:     contract.ID(),
		Contract:       contract,
		TransactionSet: txnSet,
	})
}

func (s *server) rhpFundHandler(jc jape.Context) {
	var rfr RHPFundRequest
	if jc.Decode(&rfr) != nil {
		return
	}
	_, err := s.rhp.FundAccount(jc.Request.Context(), rfr.HostIP, rfr.HostKey, rfr.Contract, rfr.RenterKey, rfr.Account, rfr.Amount)
	jc.Check("couldn't fund account", err)
}

func (s *server) rhpRegistryReadHandler(jc jape.Context) {
	var rrrr RHPRegistryReadRequest
	if jc.Decode(&rrrr) != nil {
		return
	}
	value, err := s.rhp.ReadRegistry(jc.Request.Context(), rrrr.HostIP, rrrr.HostKey, &rrrr.Payment, rrrr.RegistryKey)
	if jc.Check("couldn't read registry", err) == nil {
		jc.Encode(value)
	}
}

func (s *server) rhpRegistryUpdateHandler(jc jape.Context) {
	var rrur RHPRegistryUpdateRequest
	if jc.Decode(&rrur) != nil {
		return
	}
	err := s.rhp.UpdateRegistry(jc.Request.Context(), rrur.HostIP, rrur.HostKey, &rrur.Payment, rrur.RegistryKey, rrur.RegistryValue)
	jc.Check("couldn't update registry", err)
}

func (s *server) contractsHandler(jc jape.Context) {
	cs, err := s.cs.Contracts()
	if jc.Check("couldn't load contracts", err) == nil {
		jc.Encode(cs)
	}
}

func (s *server) contractsIDHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	c, err := s.cs.Contract(id)
	if jc.Check("couldn't load contract", err) == nil {
		jc.Encode(c)
	}
}

func (s *server) contractsIDHandlerPUT(jc jape.Context) {
	var id types.FileContractID
	var c rhpv2.Contract
	if jc.DecodeParam("id", &id) != nil || jc.Decode(&c) != nil {
		return
	}
	if c.ID() != id {
		http.Error(jc.ResponseWriter, "contract ID mismatch", http.StatusBadRequest)
		return
	}
	jc.Check("couldn't store contract", s.cs.AddContract(c))
}

func (s *server) contractsIDHandlerDELETE(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	jc.Check("couldn't remove contract", s.cs.RemoveContract(id))
}

func (s *server) hostsetsHandler(jc jape.Context) {
	jc.Encode(s.hss.HostSets())
}

func (s *server) hostsetsNameHandlerGET(jc jape.Context) {
	jc.Encode(s.hss.HostSet(jc.PathParam("name")))
}

func (s *server) hostsetsNameHandlerPUT(jc jape.Context) {
	var hosts []consensus.PublicKey
	if jc.Decode(&hosts) != nil {
		return
	}
	jc.Check("couldn't store host set", s.hss.SetHostSet(jc.PathParam("name"), hosts))
}

func (s *server) hostsetsContractsHandler(jc jape.Context) {
	hosts := s.hss.HostSet(jc.PathParam("name"))
	latest := make(map[consensus.PublicKey]rhpv2.Contract)
	all, err := s.cs.Contracts()
	if jc.Check("couldn't load contracts", err) != nil {
		return
	}
	for _, c := range all {
		if old, ok := latest[c.HostKey()]; !ok || c.EndHeight() > old.EndHeight() {
			latest[c.HostKey()] = c
		}
	}
	contracts := make([]*rhpv2.Contract, len(hosts))
	for i, host := range hosts {
		if c, ok := latest[host]; ok {
			contracts[i] = &c
		}
	}
	jc.Encode(contracts)
}

func (s *server) hostsetsResolveHandler(jc jape.Context) {
	hosts := s.hss.HostSet(jc.PathParam("name"))
	ips := make([]string, len(hosts))
	for i, hostKey := range hosts {
		hi, _ := s.hdb.Host(hostKey)
		ips[i] = hi.NetAddress()
	}
	jc.Encode(ips)
}

func (s *server) slabsUploadHandler(jc jape.Context) {
	jc.Custom((*[]byte)(nil), []slab.Slab{})

	var sur SlabsUploadRequest
	if err := json.NewDecoder(strings.NewReader(jc.Request.PostFormValue("meta"))).Decode(&sur); err != nil {
		http.Error(jc.ResponseWriter, err.Error(), http.StatusBadRequest)
		return
	}
	f, _, err := jc.Request.FormFile("data")
	if err != nil {
		http.Error(jc.ResponseWriter, err.Error(), http.StatusBadRequest)
		return
	}
	slabs, err := s.sm.UploadSlabs(jc.Request.Context(), f, sur.MinShards, sur.TotalShards, sur.CurrentHeight, sur.Contracts)
	if jc.Check("couldn't upload slabs", err) == nil {
		jc.Encode(slabs)
	}
}

func (s *server) slabsDownloadHandler(jc jape.Context) {
	jc.Custom(&SlabsDownloadRequest{}, []byte{})

	var sdr SlabsDownloadRequest
	if jc.Decode(&sdr) != nil {
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
	err := s.sm.DownloadSlabs(jc.Request.Context(), jc.ResponseWriter, sdr.Slabs, sdr.Offset, sdr.Length, sdr.Contracts)
	jc.Check("couldn't download slabs", err)
}

func (s *server) slabsMigrateHandler(jc jape.Context) {
	var smr SlabsMigrateRequest
	if jc.Decode(&smr) == nil {
		err := s.sm.MigrateSlabs(jc.Request.Context(), smr.Slabs, smr.CurrentHeight, smr.From, smr.To)
		jc.Check("couldn't migrate slabs", err)
	}
}

func (s *server) slabsDeleteHandler(jc jape.Context) {
	var sdr SlabsDeleteRequest
	if jc.Decode(&sdr) == nil {
		err := s.sm.DeleteSlabs(jc.Request.Context(), sdr.Slabs, sdr.Contracts)
		jc.Check("couldn't delete slabs", err)
	}
}

func (s *server) objectsKeyHandlerGET(jc jape.Context) {
	if strings.HasSuffix(jc.PathParam("key"), "/") {
		jc.Encode(ObjectsResponse{Entries: s.os.List(jc.PathParam("key"))})
		return
	}
	o, err := s.os.Get(jc.PathParam("key"))
	if jc.Check("couldn't load object", err) == nil {
		jc.Encode(ObjectsResponse{Object: &o})
	}
}

func (s *server) objectsKeyHandlerPUT(jc jape.Context) {
	var o object.Object
	if jc.Decode(&o) == nil {
		jc.Check("couldn't store object", s.os.Put(jc.PathParam("key"), o))
	}
}

func (s *server) objectsKeyHandlerDELETE(jc jape.Context) {
	jc.Check("couldn't delete object", s.os.Delete(jc.PathParam("key")))
}

// NewServer returns an HTTP handler that serves the renterd API.
func NewServer(s Syncer, cm ChainManager, tp TransactionPool, w Wallet, hdb HostDB, rhp RHP, cs ContractStore, sm SlabMover, os ObjectStore) http.Handler {
	srv := server{
		s:   s,
		cm:  cm,
		tp:  tp,
		w:   w,
		hdb: hdb,
		rhp: rhp,
		cs:  cs,
		sm:  sm,
		os:  os,
	}
	return jape.Mux(map[string]jape.Handler{
		"GET    /syncer/peers":   srv.syncerPeersHandler,
		"POST   /syncer/connect": srv.syncerConnectHandler,

		"GET    /consensus/tip": srv.consensusTipHandler,

		"GET    /txpool/transactions": srv.txpoolTransactionsHandler,
		"POST   /txpool/broadcast":    srv.txpoolBroadcastHandler,

		"GET    /wallet/balance":       srv.walletBalanceHandler,
		"GET    /wallet/address":       srv.walletAddressHandler,
		"GET    /wallet/transactions":  srv.walletTransactionsHandler,
		"GET    /wallet/outputs":       srv.walletOutputsHandler,
		"POST   /wallet/fund":          srv.walletFundHandler,
		"POST   /wallet/sign":          srv.walletSignHandler,
		"POST   /wallet/split":         srv.walletSplitHandler,
		"POST   /wallet/discard":       srv.walletDiscardHandler,
		"POST   /wallet/prepare/form":  srv.walletPrepareFormHandler,
		"POST   /wallet/prepare/renew": srv.walletPrepareRenewHandler,
		"GET    /wallet/pending":       srv.walletPendingHandler,

		"GET    /hosts":                     srv.hostsHandler,
		"GET    /hosts/:pubkey":             srv.hostsPubkeyHandler,
		"PUT    /hosts/:pubkey/score":       srv.hostsScoreHandler,
		"POST   /hosts/:pubkey/interaction": srv.hostsInteractionHandler,

		"POST   /rhp/prepare/form":    srv.rhpPrepareFormHandler,
		"POST   /rhp/prepare/renew":   srv.rhpPrepareRenewHandler,
		"POST   /rhp/prepare/payment": srv.rhpPreparePaymentHandler,
		"POST   /rhp/scan":            srv.rhpScanHandler,
		"POST   /rhp/form":            srv.rhpFormHandler,
		"POST   /rhp/renew":           srv.rhpRenewHandler,
		"POST   /rhp/fund":            srv.rhpFundHandler,
		"POST   /rhp/registry/read":   srv.rhpRegistryReadHandler,
		"POST   /rhp/registry/update": srv.rhpRegistryUpdateHandler,

		"GET    /contracts":     srv.contractsHandler,
		"GET    /contracts/:id": srv.contractsIDHandlerGET,
		"PUT    /contracts/:id": srv.contractsIDHandlerPUT,
		"DELETE /contracts/:id": srv.contractsIDHandlerDELETE,

		"GET    /hostsets":                 srv.hostsetsHandler,
		"GET    /hostsets/:name":           srv.hostsetsNameHandlerGET,
		"PUT    /hostsets/:name":           srv.hostsetsNameHandlerPUT,
		"GET    /hostsets/:name/contracts": srv.hostsetsContractsHandler,
		"GET    /hostsets/:name/resolve":   srv.hostsetsResolveHandler,

		"POST   /slabs/upload":   srv.slabsUploadHandler,
		"POST   /slabs/download": srv.slabsDownloadHandler,
		"POST   /slabs/migrate":  srv.slabsMigrateHandler,
		"POST   /slabs/delete":   srv.slabsDeleteHandler,

		"GET    /objects/*key": srv.objectsKeyHandlerGET,
		"PUT    /objects/*key": srv.objectsKeyHandlerPUT,
		"DELETE /objects/*key": srv.objectsKeyHandlerDELETE,
	})
}

// NewStatelessServer returns an HTTP handler that serves the stateless renterd API.
func NewStatelessServer(rhp RHP, sm SlabMover) http.Handler {
	srv := server{
		rhp: rhp,
		sm:  sm,
	}

	return jape.Mux(map[string]jape.Handler{
		"POST   /rhp/prepare/form":    srv.rhpPrepareFormHandler,
		"POST   /rhp/prepare/renew":   srv.rhpPrepareRenewHandler,
		"POST   /rhp/prepare/payment": srv.rhpPreparePaymentHandler,
		"POST   /rhp/scan":            srv.rhpScanHandler,
		"POST   /rhp/form":            srv.rhpFormHandler,
		"POST   /rhp/renew":           srv.rhpRenewHandler,
		"POST   /rhp/fund":            srv.rhpFundHandler,
		"POST   /rhp/registry/read":   srv.rhpRegistryReadHandler,
		"POST   /rhp/registry/update": srv.rhpRegistryUpdateHandler,

		"POST   /slabs/upload":   srv.slabsUploadHandler,
		"POST   /slabs/download": srv.slabsDownloadHandler,
		"POST   /slabs/migrate":  srv.slabsMigrateHandler,
		"POST   /slabs/delete":   srv.slabsDeleteHandler,
	})
}
