package bus

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/jape"
	"go.sia.tech/renterd"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/siad/types"
)

type (
	// A ChainManager manages blockchain state.
	ChainManager interface {
		AcceptBlock(types.Block) error
		Synced() bool
		TipState() consensus.State
	}

	// A Syncer can connect to other peers and synchronize the blockchain.
	Syncer interface {
		SyncerAddress() (string, error)
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
		Address() types.UnlockHash
		Balance() types.Currency
		FundTransaction(cs consensus.State, txn *types.Transaction, amount types.Currency, pool []types.Transaction) ([]types.OutputID, error)
		Redistribute(cs consensus.State, outputs int, amount, feePerByte types.Currency, pool []types.Transaction) (types.Transaction, []types.OutputID, error)
		ReleaseInputs(txn types.Transaction)
		SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.OutputID, cf types.CoveredFields) error
		Transactions(since time.Time, max int) ([]wallet.Transaction, error)
		UnspentOutputs() ([]wallet.SiacoinElement, error)
	}

	// A HostDB stores information about hosts.
	HostDB interface {
		Hosts(notSince time.Time, max int) ([]hostdb.Host, error)
		Host(hostKey consensus.PublicKey) (hostdb.Host, error)
		RecordInteraction(hostKey consensus.PublicKey, hi hostdb.Interaction) error
	}

	// A ContractStore stores contracts.
	ContractStore interface {
		AcquireContract(fcid types.FileContractID, duration time.Duration) (types.FileContractRevision, bool, error)
		ReleaseContract(fcid types.FileContractID) error
		Contracts() ([]renterd.Contract, error)
		Contract(id types.FileContractID) (renterd.Contract, error)
		AddContract(c rhpv2.ContractRevision, totalCost types.Currency) error
		AddRenewedContract(c rhpv2.ContractRevision, totalCost types.Currency, renewedFrom types.FileContractID) error
		RemoveContract(id types.FileContractID) error
	}

	// A ContractSetStore stores contract sets.
	ContractSetStore interface {
		ContractSets() ([]string, error)
		ContractSet(name string) ([]types.FileContractID, error)
		SetContractSet(name string, contracts []types.FileContractID) error
	}

	// An ObjectStore stores objects.
	ObjectStore interface {
		Get(key string) (object.Object, error)
		List(key string) ([]string, error)
		MarkSlabsMigrationFailure(slabIDs []SlabID) (int, error)
		Put(key string, o object.Object, usedContracts map[consensus.PublicKey]types.FileContractID) error
		Delete(key string) error
		SlabsForMigration(n int, failureCutoff time.Time, goodContracts []types.FileContractID) ([]SlabID, error)
		SlabForMigration(slabID SlabID) (object.Slab, []renterd.SlabLocation, error)
	}
)

type bus struct {
	s   Syncer
	cm  ChainManager
	tp  TransactionPool
	w   Wallet
	hdb HostDB
	cs  ContractStore
	css ContractSetStore
	os  ObjectStore
}

func (b *bus) consensusAcceptBlock(jc jape.Context) {
	var block types.Block
	if jc.Decode(&block) != nil {
		return
	}
	if jc.Check("failed to accept block", b.cm.AcceptBlock(block)) != nil {
		return
	}
}

func (b *bus) syncerAddrHandler(jc jape.Context) {
	addr, err := b.s.SyncerAddress()
	if jc.Check("failed to fetch syncer's address", err) != nil {
		return
	}
	jc.Encode(addr)
}

func (b *bus) syncerPeersHandler(jc jape.Context) {
	jc.Encode(b.s.Peers())
}

func (b *bus) syncerConnectHandler(jc jape.Context) {
	var addr string
	if jc.Decode(&addr) == nil {
		jc.Check("couldn't connect to peer", b.s.Connect(addr))
	}
}

func (b *bus) consensusStateHandler(jc jape.Context) {
	jc.Encode(ConsensusState{
		BlockHeight: b.cm.TipState().Index.Height,
		Synced:      b.cm.Synced(),
	})
}

func (b *bus) txpoolFeeHandler(jc jape.Context) {
	fee := b.tp.RecommendedFee()
	jc.Encode(fee)
}

func (b *bus) txpoolTransactionsHandler(jc jape.Context) {
	jc.Encode(b.tp.Transactions())
}

func (b *bus) txpoolBroadcastHandler(jc jape.Context) {
	var txnSet []types.Transaction
	if jc.Decode(&txnSet) == nil {
		jc.Check("couldn't broadcast transaction set", b.tp.AddTransactionSet(txnSet))
	}
}

func (b *bus) walletBalanceHandler(jc jape.Context) {
	jc.Encode(b.w.Balance())
}

func (b *bus) walletAddressHandler(jc jape.Context) {
	jc.Encode(b.w.Address())
}

func (b *bus) walletTransactionsHandler(jc jape.Context) {
	var since time.Time
	max := -1
	if jc.DecodeForm("since", (*paramTime)(&since)) != nil || jc.DecodeForm("max", &max) != nil {
		return
	}
	txns, err := b.w.Transactions(since, max)
	if jc.Check("couldn't load transactions", err) == nil {
		jc.Encode(txns)
	}
}

func (b *bus) walletOutputsHandler(jc jape.Context) {
	utxos, err := b.w.UnspentOutputs()
	if jc.Check("couldn't load outputs", err) == nil {
		jc.Encode(utxos)
	}
}

func (b *bus) walletFundHandler(jc jape.Context) {
	var wfr WalletFundRequest
	if jc.Decode(&wfr) != nil {
		return
	}
	txn := wfr.Transaction
	fee := b.tp.RecommendedFee().Mul64(uint64(len(encoding.Marshal(txn))))
	txn.MinerFees = []types.Currency{fee}
	toSign, err := b.w.FundTransaction(b.cm.TipState(), &txn, wfr.Amount.Add(txn.MinerFees[0]), b.tp.Transactions())
	if jc.Check("couldn't fund transaction", err) != nil {
		return
	}
	parents, err := b.tp.UnconfirmedParents(txn)
	if jc.Check("couldn't load transaction dependencies", err) != nil {
		b.w.ReleaseInputs(txn)
		return
	}
	jc.Encode(WalletFundResponse{
		Transaction: txn,
		ToSign:      toSign,
		DependsOn:   parents,
	})
}

func (b *bus) walletSignHandler(jc jape.Context) {
	var wsr WalletSignRequest
	if jc.Decode(&wsr) != nil {
		return
	}
	err := b.w.SignTransaction(b.cm.TipState(), &wsr.Transaction, wsr.ToSign, wsr.CoveredFields)
	if jc.Check("couldn't sign transaction", err) == nil {
		jc.Encode(wsr.Transaction)
	}
}

func (b *bus) walletRedistributeHandler(jc jape.Context) {
	var wfr WalletRedistributeRequest
	if jc.Decode(&wfr) != nil {
		return
	}
	if wfr.Outputs == 0 {
		jc.Error(errors.New("'outputs' has to be greater than zero"), http.StatusBadRequest)
		return
	}

	cs := b.cm.TipState()
	txn, toSign, err := b.w.Redistribute(cs, wfr.Outputs, wfr.Amount, b.tp.RecommendedFee(), b.tp.Transactions())
	if jc.Check("couldn't redistribute money in the wallet into the desired outputs", err) != nil {
		return
	}

	err = b.w.SignTransaction(cs, &txn, toSign, types.FullCoveredFields)
	if jc.Check("couldn't sign the transaction", err) != nil {
		return
	}

	jc.Encode(txn)
}

func (b *bus) walletDiscardHandler(jc jape.Context) {
	var txn types.Transaction
	if jc.Decode(&txn) == nil {
		b.w.ReleaseInputs(txn)
	}
}

func (b *bus) walletPrepareFormHandler(jc jape.Context) {
	var wpfr WalletPrepareFormRequest
	if jc.Decode(&wpfr) != nil {
		return
	}
	fc := rhpv2.PrepareContractFormation(wpfr.RenterKey, wpfr.HostKey, wpfr.RenterFunds, wpfr.HostCollateral, wpfr.EndHeight, wpfr.HostSettings, wpfr.RenterAddress)
	cost := rhpv2.ContractFormationCost(fc, wpfr.HostSettings.ContractPrice)
	txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}
	txn.MinerFees = []types.Currency{b.tp.RecommendedFee().Mul64(uint64(len(encoding.Marshal(txn))))}
	toSign, err := b.w.FundTransaction(b.cm.TipState(), &txn, cost.Add(txn.MinerFees[0]), b.tp.Transactions())
	if jc.Check("couldn't fund transaction", err) != nil {
		return
	}
	cf := wallet.ExplicitCoveredFields(txn)
	err = b.w.SignTransaction(b.cm.TipState(), &txn, toSign, cf)
	if jc.Check("couldn't sign transaction", err) != nil {
		b.w.ReleaseInputs(txn)
		return
	}
	parents, err := b.tp.UnconfirmedParents(txn)
	if jc.Check("couldn't load transaction dependencies", err) != nil {
		b.w.ReleaseInputs(txn)
		return
	}
	jc.Encode(append(parents, txn))
}

func (b *bus) walletPrepareRenewHandler(jc jape.Context) {
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
	txn.MinerFees = []types.Currency{b.tp.RecommendedFee().Mul64(uint64(len(encoding.Marshal(txn))))}
	toSign, err := b.w.FundTransaction(b.cm.TipState(), &txn, cost.Add(txn.MinerFees[0]), b.tp.Transactions())
	if jc.Check("couldn't fund transaction", err) != nil {
		return
	}
	cf := wallet.ExplicitCoveredFields(txn)
	err = b.w.SignTransaction(b.cm.TipState(), &txn, toSign, cf)
	if jc.Check("couldn't sign transaction", err) != nil {
		b.w.ReleaseInputs(txn)
		return
	}
	parents, err := b.tp.UnconfirmedParents(txn)
	if jc.Check("couldn't load transaction dependencies", err) != nil {
		b.w.ReleaseInputs(txn)
		return
	}
	jc.Encode(WalletPrepareRenewResponse{
		TransactionSet: append(parents, txn),
		FinalPayment:   finalPayment,
	})
}

func (b *bus) walletPendingHandler(jc jape.Context) {
	isRelevant := func(txn types.Transaction) bool {
		addr := b.w.Address()
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

	txns := b.tp.Transactions()
	relevant := txns[:0]
	for _, txn := range txns {
		if isRelevant(txn) {
			relevant = append(relevant, txn)
		}
	}
	jc.Encode(relevant)
}

func (b *bus) hostsHandler(jc jape.Context) {
	var notSince time.Time
	max := -1
	if jc.DecodeForm("notSince", (*paramTime)(&notSince)) != nil || jc.DecodeForm("max", &max) != nil {
		return
	}
	hosts, err := b.hdb.Hosts(notSince, max)
	if jc.Check("couldn't load hosts", err) == nil {
		jc.Encode(hosts)
	}
}

func (b *bus) hostsPubkeyHandlerGET(jc jape.Context) {
	var hostKey PublicKey
	if jc.DecodeParam("hostkey", &hostKey) != nil {
		return
	}
	host, err := b.hdb.Host(hostKey)
	if jc.Check("couldn't load host", err) == nil {
		jc.Encode(host)
	}
}

func (b *bus) hostsPubkeyHandlerPOST(jc jape.Context) {
	var hi hostdb.Interaction
	var hostKey PublicKey
	if jc.Decode(&hi) == nil && jc.DecodeParam("hostkey", &hostKey) == nil {
		jc.Check("couldn't record interaction", b.hdb.RecordInteraction(hostKey, hi))
	}
}

func (b *bus) contractsHandler(jc jape.Context) {
	cs, err := b.cs.Contracts()
	if jc.Check("couldn't load contracts", err) == nil {
		jc.Encode(cs)
	}
}

func (b *bus) contractsAcquireHandler(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req ContractAcquireRequest
	if jc.Decode(&req) != nil {
		return
	}
	rev, locked, err := b.cs.AcquireContract(id, req.Duration)
	if jc.Check("failed to acquire contract", err) != nil {
		return
	}
	jc.Encode(ContractAcquireResponse{
		Locked:   locked,
		Revision: rev,
	})
}

func (b *bus) contractsReleaseHandler(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	if jc.Check("failed to release contract", b.cs.ReleaseContract(id)) != nil {
		return
	}
}

func (b *bus) contractsIDHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	c, err := b.cs.Contract(id)
	if jc.Check("couldn't load contract", err) == nil {
		jc.Encode(c)
	}
}

func (b *bus) contractsIDNewHandlerPUT(jc jape.Context) {
	var id types.FileContractID
	var req ContractsIDAddRequest
	if jc.DecodeParam("id", &id) != nil || jc.Decode(&req) != nil {
		return
	}
	if req.Contract.ID() != id {
		http.Error(jc.ResponseWriter, "contract ID mismatch", http.StatusBadRequest)
		return
	}
	jc.Check("couldn't store contract", b.cs.AddContract(req.Contract, req.TotalCost))
}

func (b *bus) contractsIDRenewedHandlerPUT(jc jape.Context) {
	var id types.FileContractID
	var req ContractsIDRenewedRequest
	if jc.DecodeParam("id", &id) != nil || jc.Decode(&req) != nil {
		return
	}
	if req.Contract.ID() != id {
		http.Error(jc.ResponseWriter, "contract ID mismatch", http.StatusBadRequest)
		return
	}
	jc.Check("couldn't store contract", b.cs.AddRenewedContract(req.Contract, req.TotalCost, req.RenewedFrom))
}

func (b *bus) contractsIDHandlerDELETE(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	jc.Check("couldn't remove contract", b.cs.RemoveContract(id))
}

func (b *bus) contractSetHandler(jc jape.Context) {
	contractSets, err := b.css.ContractSets()
	if jc.Check("couldn't load contract sets", err) != nil {
		return
	}
	jc.Encode(contractSets)
}

func (b *bus) contractSetsNameHandlerGET(jc jape.Context) {
	hostSet, err := b.css.ContractSet(jc.PathParam("name"))
	if jc.Check("couldn't load host set", err) != nil {
		return
	}
	jc.Encode(hostSet)
}

func (b *bus) contractSetsNameHandlerPUT(jc jape.Context) {
	var contracts []types.FileContractID
	if jc.Decode(&contracts) != nil {
		return
	}
	jc.Check("couldn't store host set", b.css.SetContractSet(jc.PathParam("name"), contracts))
}

func (b *bus) contractSetContractsHandler(jc jape.Context) {
	setContracts, err := b.css.ContractSet(jc.PathParam("name"))
	if jc.Check("couldn't load host set", err) != nil {
		return
	}
	// TODO: This could be a b.cs.ContractSetContracts method which does a
	// smart query.
	all, err := b.cs.Contracts()
	if jc.Check("couldn't load contracts", err) != nil {
		return
	}
	allMap := make(map[types.FileContractID]renterd.Contract)
	for _, c := range all {
		allMap[c.ID()] = c
	}
	var contracts []renterd.Contract
	for _, fcid := range setContracts {
		c, exists := allMap[fcid]
		if exists {
			contracts = append(contracts, c)
		}
	}
	jc.Encode(contracts)
}

func (b *bus) objectsKeyHandlerGET(jc jape.Context) {
	if strings.HasSuffix(jc.PathParam("key"), "/") {
		keys, err := b.os.List(jc.PathParam("key"))
		if jc.Check("couldn't list objects", err) == nil {
			jc.Encode(ObjectsResponse{Entries: keys})
		}
		return
	}
	o, err := b.os.Get(jc.PathParam("key"))
	if jc.Check("couldn't load object", err) == nil {
		jc.Encode(ObjectsResponse{Object: &o})
	}
}

func (b *bus) objectsKeyHandlerPUT(jc jape.Context) {
	var aor AddObjectRequest
	if jc.Decode(&aor) == nil {
		jc.Check("couldn't store object", b.os.Put(jc.PathParam("key"), aor.Object, aor.UsedContracts))
	}
}

func (b *bus) objectsKeyHandlerDELETE(jc jape.Context) {
	jc.Check("couldn't delete object", b.os.Delete(jc.PathParam("key")))
}

func (b *bus) objectsMigrationSlabsHandlerGET(jc jape.Context) {
	var cutoff time.Time
	var limit int
	var goodContracts []types.FileContractID
	if jc.DecodeForm("cutoff", (*paramTime)(&cutoff)) != nil {
		return
	}
	if jc.DecodeForm("limit", &limit) != nil {
		return
	}
	if jc.DecodeForm("goodContracts", &goodContracts) != nil {
		return
	}
	slabIDs, err := b.os.SlabsForMigration(limit, time.Time(cutoff), goodContracts)
	if jc.Check("couldn't fetch slabs for migration", err) != nil {
		return
	}
	jc.Encode(ObjectsMigrateSlabsResponse{
		SlabIDs: slabIDs,
	})
}

func (b *bus) objectsMigrationSlabHandlerGET(jc jape.Context) {
	var slabID SlabID
	if jc.DecodeParam("id", &slabID) != nil {
		return
	}
	slab, locations, err := b.os.SlabForMigration(slabID)
	if jc.Check("couldn't fetch slab for migration", err) != nil {
		return
	}
	jc.Encode(ObjectsMigrateSlabResponse{
		Locations: locations,
		Slab:      slab,
	})
}

func (b *bus) objectsMarkSlabMigrationFailureHandlerPOST(jc jape.Context) {
	var req ObjectsMarkSlabMigrationFailureRequest
	if jc.Decode(&req) != nil {
		return
	}
	updates, err := b.os.MarkSlabsMigrationFailure(req.SlabIDs)
	if jc.Check("couldn't mark slab migration failure", err) == nil {
		jc.Encode(ObjectsMarkSlabMigrationFailureResponse{
			Updates: updates,
		})
	}
}

// New returns a new Bus.
func New(s Syncer, cm ChainManager, tp TransactionPool, w Wallet, hdb HostDB, cs ContractStore, css ContractSetStore, os ObjectStore) http.Handler {
	b := &bus{
		s:   s,
		cm:  cm,
		tp:  tp,
		w:   w,
		hdb: hdb,
		cs:  cs,
		css: css,
		os:  os,
	}
	return jape.Mux(map[string]jape.Handler{
		"GET    /syncer/address": b.syncerAddrHandler,
		"GET    /syncer/peers":   b.syncerPeersHandler,
		"POST   /syncer/connect": b.syncerConnectHandler,

		"POST   /consensus/acceptblock": b.consensusAcceptBlock,
		"GET    /consensus/state":       b.consensusStateHandler,

		"GET    /txpool/recommendedfee": b.txpoolFeeHandler,
		"GET    /txpool/transactions":   b.txpoolTransactionsHandler,
		"POST   /txpool/broadcast":      b.txpoolBroadcastHandler,

		"GET    /wallet/balance":       b.walletBalanceHandler,
		"GET    /wallet/address":       b.walletAddressHandler,
		"GET    /wallet/transactions":  b.walletTransactionsHandler,
		"GET    /wallet/outputs":       b.walletOutputsHandler,
		"POST   /wallet/fund":          b.walletFundHandler,
		"POST   /wallet/sign":          b.walletSignHandler,
		"POST   /wallet/redistribute":  b.walletRedistributeHandler,
		"POST   /wallet/discard":       b.walletDiscardHandler,
		"POST   /wallet/prepare/form":  b.walletPrepareFormHandler,
		"POST   /wallet/prepare/renew": b.walletPrepareRenewHandler,
		"GET    /wallet/pending":       b.walletPendingHandler,

		"GET    /hosts":          b.hostsHandler,
		"GET    /hosts/:hostkey": b.hostsPubkeyHandlerGET,
		"POST   /hosts/:hostkey": b.hostsPubkeyHandlerPOST,

		"GET    /contracts":             b.contractsHandler,
		"GET    /contracts/:id":         b.contractsIDHandlerGET,
		"PUT    /contracts/:id/new":     b.contractsIDNewHandlerPUT,
		"PUT    /contracts/:id/renewed": b.contractsIDRenewedHandlerPUT,
		"DELETE /contracts/:id":         b.contractsIDHandlerDELETE,
		"POST   /contracts/:id/acquire": b.contractsAcquireHandler,
		"POST   /contracts/:id/release": b.contractsReleaseHandler,

		"GET    /contractsets":                 b.contractSetHandler,
		"GET    /contractsets/:name":           b.contractSetsNameHandlerGET,
		"GET    /contractsets/:name/contracts": b.contractSetContractsHandler,
		"PUT    /contractsets/:name":           b.contractSetsNameHandlerPUT,

		"GET    /objects/*key":       b.objectsKeyHandlerGET,
		"PUT    /objects/*key":       b.objectsKeyHandlerPUT,
		"DELETE /objects/*key":       b.objectsKeyHandlerDELETE,
		"GET    /migration/slabs":    b.objectsMigrationSlabsHandlerGET,
		"GET    /migration/slab/:id": b.objectsMigrationSlabHandlerGET,
		"POST   /migration/failed":   b.objectsMarkSlabMigrationFailureHandlerPOST,
	})
}
