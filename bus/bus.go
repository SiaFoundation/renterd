package bus

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/siad/types"
)

const (
	SettingGouging    = "gouging"
	SettingRedundancy = "redundancy"
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
		Host(hostKey consensus.PublicKey) (hostdb.Host, error)
		Hosts(offset, limit int) ([]hostdb.Host, error)
		RecordInteractions(interactions []hostdb.Interaction) error

		HostBlocklist() ([]string, error)
		AddHostBlocklistEntry(entry string) error
		RemoveHostBlocklistEntry(entry string) error
	}

	// A ContractStore stores contracts.
	ContractStore interface {
		AcquireContract(fcid types.FileContractID, duration time.Duration) (bool, error)
		AddContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (api.ContractMetadata, error)
		AddRenewedContract(c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (api.ContractMetadata, error)
		ActiveContracts() ([]api.ContractMetadata, error)
		AncestorContracts(fcid types.FileContractID, minStartHeight uint64) ([]api.ArchivedContract, error)
		Contract(id types.FileContractID) (api.ContractMetadata, error)
		Contracts(set string) ([]api.ContractMetadata, error)
		ReleaseContract(fcid types.FileContractID) error
		RemoveContract(id types.FileContractID) error
		SetContractSet(set string, contracts []types.FileContractID) error
	}

	// An ObjectStore stores objects.
	ObjectStore interface {
		Get(key string) (object.Object, error)
		List(key string) ([]string, error)
		Put(key string, o object.Object, usedContracts map[consensus.PublicKey]types.FileContractID) error
		Delete(key string) error
		SlabsForMigration(n int, failureCutoff time.Time, goodContracts []types.FileContractID) ([]object.Slab, error)
	}

	// A SettingStore stores settings.
	SettingStore interface {
		Settings() ([]string, error)
		Setting(key string) (string, error)
		UpdateSetting(key, value string) error
	}
)

type bus struct {
	s   Syncer
	cm  ChainManager
	tp  TransactionPool
	w   Wallet
	hdb HostDB
	cs  ContractStore
	os  ObjectStore
	ss  SettingStore

	interactionsMu            sync.Mutex
	interactions              []hostdb.Interaction
	interactionsFlushInterval time.Duration
	interactionsFlush         *interactionsFlush
}

type interactionsFlush struct {
	c      chan struct{}
	result error
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
	jc.Encode(api.ConsensusState{
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
	if jc.DecodeForm("since", (*api.ParamTime)(&since)) != nil || jc.DecodeForm("max", &max) != nil {
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
	var wfr api.WalletFundRequest
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
	jc.Encode(api.WalletFundResponse{
		Transaction: txn,
		ToSign:      toSign,
		DependsOn:   parents,
	})
}

func (b *bus) walletSignHandler(jc jape.Context) {
	var wsr api.WalletSignRequest
	if jc.Decode(&wsr) != nil {
		return
	}
	err := b.w.SignTransaction(b.cm.TipState(), &wsr.Transaction, wsr.ToSign, wsr.CoveredFields)
	if jc.Check("couldn't sign transaction", err) == nil {
		jc.Encode(wsr.Transaction)
	}
}

func (b *bus) walletRedistributeHandler(jc jape.Context) {
	var wfr api.WalletRedistributeRequest
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
	var wpfr api.WalletPrepareFormRequest
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
	var wprr api.WalletPrepareRenewRequest
	if jc.Decode(&wprr) != nil {
		return
	}
	fc := rhpv2.PrepareContractRenewal(wprr.Contract, wprr.RenterKey, wprr.HostKey, wprr.RenterFunds, wprr.EndHeight, wprr.HostSettings, wprr.RenterAddress)
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
	jc.Encode(api.WalletPrepareRenewResponse{
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

func (b *bus) hostsHandlerGET(jc jape.Context) {
	if offset := 0; jc.DecodeForm("offset", &offset) == nil {
		if limit := -1; jc.DecodeForm("limit", &limit) == nil {
			if hosts, err := b.hdb.Hosts(offset, limit); jc.Check(fmt.Sprintf("couldn't fetch hosts %d-%d", offset, offset+limit), err) == nil {
				jc.Encode(hosts)
			}
		}
	}
}

func (b *bus) hostsPubkeyHandlerGET(jc jape.Context) {
	var hostKey consensus.PublicKey
	if jc.DecodeParam("hostkey", &hostKey) != nil {
		return
	}
	host, err := b.hdb.Host(hostKey)
	if jc.Check("couldn't load host", err) == nil {
		jc.Encode(host)
	}
}

func (b *bus) hostsPubkeyHandlerPOST(jc jape.Context) {
	var interactions []hostdb.Interaction
	if jc.Decode(&interactions) == nil {
		jc.Check("couldn't record interaction", b.recordInteractions(interactions))
	}
}

func (b *bus) hostsBlocklistHandlerGET(jc jape.Context) {
	blocklist, err := b.hdb.HostBlocklist()
	if jc.Check("couldn't load blocklist", err) == nil {
		jc.Encode(blocklist)
	}
}

func (b *bus) hostsBlocklistHandlerPUT(jc jape.Context) {
	var req api.UpdateBlocklistRequest
	if jc.Decode(&req) == nil {
		for _, entry := range req.Add {
			if jc.Check(fmt.Sprintf("couldn't add blocklist entry '%s'", entry), b.hdb.AddHostBlocklistEntry(entry)) != nil {
				return
			}
		}
		for _, entry := range req.Remove {
			if jc.Check(fmt.Sprintf("couldn't remove blocklist entry '%s'", entry), b.hdb.RemoveHostBlocklistEntry(entry)) != nil {
				return
			}
		}
	}
}

func (b *bus) contractsActiveHandlerGET(jc jape.Context) {
	cs, err := b.cs.ActiveContracts()
	if jc.Check("couldn't load contracts", err) == nil {
		jc.Encode(cs)
	}
}

func (b *bus) contractsSetHandlerGET(jc jape.Context) {
	cs, err := b.cs.Contracts(jc.PathParam("set"))
	if jc.Check("couldn't load contracts", err) == nil {
		jc.Encode(cs)
	}
}

func (b *bus) contractsSetHandlerPUT(jc jape.Context) {
	var contractIds []types.FileContractID
	if set := jc.PathParam("set"); set == "" {
		jc.Error(errors.New("param 'set' can not be empty"), http.StatusBadRequest)
	} else if jc.Decode(&contractIds) == nil {
		jc.Check("could not add contracts to set", b.cs.SetContractSet(set, contractIds))
	}
}

func (b *bus) contractAcquireHandlerPOST(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.ContractAcquireRequest
	if jc.Decode(&req) != nil {
		return
	}
	locked, err := b.cs.AcquireContract(id, req.Duration)
	if jc.Check("failed to acquire contract", err) != nil {
		return
	}
	jc.Encode(api.ContractAcquireResponse{
		Locked: locked,
	})
}

func (b *bus) contractReleaseHandlerPOST(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	if jc.Check("failed to release contract", b.cs.ReleaseContract(id)) != nil {
		return
	}
}

func (b *bus) contractIDHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	c, err := b.cs.Contract(id)
	if jc.Check("couldn't load contract", err) == nil {
		jc.Encode(c)
	}
}

func (b *bus) contractIDHandlerPOST(jc jape.Context) {
	var id types.FileContractID
	var req api.ContractsIDAddRequest
	if jc.DecodeParam("id", &id) != nil || jc.Decode(&req) != nil {
		return
	}
	if req.Contract.ID() != id {
		http.Error(jc.ResponseWriter, "contract ID mismatch", http.StatusBadRequest)
		return
	}

	a, err := b.cs.AddContract(req.Contract, req.TotalCost, req.StartHeight)
	if jc.Check("couldn't store contract", err) == nil {
		jc.Encode(a)
	}
}

func (b *bus) contractIDRenewedHandlerPOST(jc jape.Context) {
	var id types.FileContractID
	var req api.ContractsIDRenewedRequest
	if jc.DecodeParam("id", &id) != nil || jc.Decode(&req) != nil {
		return
	}
	if req.Contract.ID() != id {
		http.Error(jc.ResponseWriter, "contract ID mismatch", http.StatusBadRequest)
		return
	}

	r, err := b.cs.AddRenewedContract(req.Contract, req.TotalCost, req.StartHeight, req.RenewedFrom)
	if jc.Check("couldn't store contract", err) == nil {
		jc.Encode(r)
	}
}

func (b *bus) contractIDHandlerDELETE(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	jc.Check("couldn't remove contract", b.cs.RemoveContract(id))
}

func (b *bus) objectsKeyHandlerGET(jc jape.Context) {
	if strings.HasSuffix(jc.PathParam("key"), "/") {
		keys, err := b.os.List(jc.PathParam("key"))
		if jc.Check("couldn't list objects", err) == nil {
			jc.Encode(api.ObjectsResponse{Entries: keys})
		}
		return
	}
	o, err := b.os.Get(jc.PathParam("key"))
	if jc.Check("couldn't load object", err) == nil {
		jc.Encode(api.ObjectsResponse{Object: &o})
	}
}

func (b *bus) objectsKeyHandlerPUT(jc jape.Context) {
	var aor api.AddObjectRequest
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
	if jc.DecodeForm("cutoff", (*api.ParamTime)(&cutoff)) != nil {
		return
	}
	if jc.DecodeForm("limit", &limit) != nil {
		return
	}
	if jc.DecodeForm("goodContracts", &goodContracts) != nil {
		return
	}
	slabs, err := b.os.SlabsForMigration(limit, time.Time(cutoff), goodContracts)
	if jc.Check("couldn't fetch slabs for migration", err) != nil {
		return
	}
	jc.Encode(slabs)
}

func (b *bus) settingsHandlerGET(jc jape.Context) {
	if settings, err := b.ss.Settings(); jc.Check("couldn't load settings", err) == nil {
		jc.Encode(settings)
	}
}

func (b *bus) settingKeyHandlerGET(jc jape.Context) {
	if key := jc.PathParam("key"); key == "" {
		jc.Error(errors.New("param 'key' can not be empty"), http.StatusBadRequest)
	} else if setting, err := b.ss.Setting(jc.PathParam("key")); err == api.ErrSettingNotFound {
		jc.Error(err, http.StatusNotFound)
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
	} else {
		jc.Encode(setting)
	}
}

func (b *bus) settingKeyHandlerPOST(jc jape.Context) {
	if key := jc.PathParam("key"); key == "" {
		jc.Error(errors.New("param 'key' can not be empty"), http.StatusBadRequest)
	} else if value := jc.PathParam("value"); value == "" {
		jc.Error(errors.New("param 'value' can not be empty"), http.StatusBadRequest)
	} else if value, err := url.QueryUnescape(value); err != nil {
		jc.Error(errors.New("could not unescape 'value'"), http.StatusBadRequest)
	} else {
		jc.Check("could not update setting", b.ss.UpdateSetting(key, value))
	}
}

func (b *bus) setGougingSettings(gs api.GougingSettings) error {
	if js, err := json.Marshal(gs); err != nil {
		panic(err)
	} else {
		return b.ss.UpdateSetting(SettingGouging, string(js))
	}
}

func (b *bus) setRedundancySettings(rs api.RedundancySettings) error {
	if js, err := json.Marshal(rs); err != nil {
		panic(err)
	} else if rs.MinShards == 0 || rs.MinShards >= rs.TotalShards {
		return errors.New("invalid redundancy settings: MinShards has to be greater than zero and smaller than TotalShards")
	} else {
		return b.ss.UpdateSetting(SettingRedundancy, string(js))
	}
}

func (b *bus) contractIDAncestorsHandler(jc jape.Context) {
	var fcid types.FileContractID
	if jc.DecodeParam("id", &fcid) != nil {
		return
	}
	var minStartHeight uint64
	if jc.DecodeForm("startHeight", &minStartHeight) != nil {
		return
	}
	ancestors, err := b.cs.AncestorContracts(fcid, minStartHeight)
	if jc.Check("failed to fetch ancestor contracts", err) != nil {
		return
	}
	jc.Encode(ancestors)
}

func (b *bus) paramsHandlerDownloadGET(jc jape.Context) {
	gp, err := b.gougingParams()
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	jc.Encode(api.DownloadParams{
		ContractSet:   "autopilot", // TODO
		GougingParams: gp,
	})
}

func (b *bus) paramsHandlerUploadGET(jc jape.Context) {
	gp, err := b.gougingParams()
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	jc.Encode(api.UploadParams{
		ContractSet:   "autopilot", // TODO
		CurrentHeight: b.cm.TipState().Index.Height,
		GougingParams: gp,
	})
}

func (b *bus) paramsHandlerMigrateGET(jc jape.Context) {
	gp, err := b.gougingParams()
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	jc.Encode(api.MigrateParams{
		CurrentHeight: b.cm.TipState().Index.Height,
		FromContracts: "", // TODO
		ToContracts:   "", // TODO
		GougingParams: gp,
	})
}

func (b *bus) paramsHandlerGougingGET(jc jape.Context) {
	gp, err := b.gougingParams()
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	jc.Encode(gp)
}

func (b *bus) gougingParams() (api.GougingParams, error) {
	var gs api.GougingSettings
	if gss, err := b.ss.Setting(SettingGouging); err != nil {
		return api.GougingParams{}, err
	} else if err := json.Unmarshal([]byte(gss), &gs); err != nil {
		panic(err)
	}

	var rs api.RedundancySettings
	if rss, err := b.ss.Setting(SettingRedundancy); err != nil {
		return api.GougingParams{}, err
	} else if err := json.Unmarshal([]byte(rss), &rs); err != nil {
		panic(err)
	}

	return api.GougingParams{
		GougingSettings:    gs,
		RedundancySettings: rs,
	}, nil
}

// New returns a new Bus.
func New(s Syncer, cm ChainManager, tp TransactionPool, w Wallet, hdb HostDB, cs ContractStore, os ObjectStore, ss SettingStore, gs api.GougingSettings, rs api.RedundancySettings, interactionsFlushInterval time.Duration) (http.Handler, error) {
	b := &bus{
		s:                         s,
		cm:                        cm,
		tp:                        tp,
		w:                         w,
		hdb:                       hdb,
		cs:                        cs,
		os:                        os,
		ss:                        ss,
		interactionsFlushInterval: interactionsFlushInterval,
	}

	if err := b.setGougingSettings(gs); err != nil {
		return nil, err
	}

	if err := b.setRedundancySettings(rs); err != nil {
		return nil, err
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

		"GET    /hosts":              b.hostsHandlerGET,
		"GET    /host/:hostkey":      b.hostsPubkeyHandlerGET,
		"POST   /hosts/interactions": b.hostsPubkeyHandlerPOST,
		"GET    /hosts/blocklist":    b.hostsBlocklistHandlerGET,
		"PUT    /hosts/blocklist":    b.hostsBlocklistHandlerPUT,

		"GET    /contracts/active":       b.contractsActiveHandlerGET,
		"GET    /contracts/set/:set":     b.contractsSetHandlerGET,
		"PUT    /contracts/set/:set":     b.contractsSetHandlerPUT,
		"GET    /contract/:id":           b.contractIDHandlerGET,
		"POST   /contract/:id":           b.contractIDHandlerPOST,
		"GET    /contract/:id/ancestors": b.contractIDAncestorsHandler,
		"POST   /contract/:id/renewed":   b.contractIDRenewedHandlerPOST,
		"DELETE /contract/:id":           b.contractIDHandlerDELETE,
		"POST   /contract/:id/acquire":   b.contractAcquireHandlerPOST,
		"POST   /contract/:id/release":   b.contractReleaseHandlerPOST,

		"GET    /objects/*key":    b.objectsKeyHandlerGET,
		"PUT    /objects/*key":    b.objectsKeyHandlerPUT,
		"DELETE /objects/*key":    b.objectsKeyHandlerDELETE,
		"GET    /migration/slabs": b.objectsMigrationSlabsHandlerGET,

		"GET    /settings":            b.settingsHandlerGET,
		"GET    /setting/:key":        b.settingKeyHandlerGET,
		"POST   /setting/:key/:value": b.settingKeyHandlerPOST,

		"GET    /params/download": b.paramsHandlerDownloadGET,
		"GET    /params/upload":   b.paramsHandlerUploadGET,
		"GET    /params/migrate":  b.paramsHandlerMigrateGET,
		"GET    /params/gouging":  b.paramsHandlerGougingGET,
	}), nil
}

func (b *bus) recordInteractions(interactions []hostdb.Interaction) error {
	// Check whether someone else is flushing the interactions or if this
	// thread needs to.
	var shouldFlush bool

	// No interactionsFlush exists so this thread needs to flush.
	b.interactionsMu.Lock()
	f := b.interactionsFlush
	if f == nil {
		f = &interactionsFlush{
			c: make(chan struct{}),
		}
		b.interactionsFlush = f
		shouldFlush = true
	}
	b.interactions = append(b.interactions, interactions...)
	b.interactionsMu.Unlock()

	// If we don't need to flush, we block on the flush channel and then
	// return the result.
	if !shouldFlush {
		<-f.c // TODO: handle shutdown
		return f.result
	}

	// Otherwise we wait for the interval and apply the interactions.
	time.Sleep(b.interactionsFlushInterval) // TODO: handle shutdown

	b.interactionsMu.Lock()
	defer b.interactionsMu.Unlock()

	f.result = b.hdb.RecordInteractions(b.interactions)
	close(f.c)
	b.interactions = nil
	b.interactionsFlush = nil
	return f.result
}
