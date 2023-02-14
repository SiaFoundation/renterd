package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/tracing"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/wallet"
	"go.uber.org/zap"
)

const (
	SettingContractSet = "contract_set"
	SettingGouging     = "gouging"
	SettingRedundancy  = "redundancy"
)

type (
	// A ChainManager manages blockchain state.
	ChainManager interface {
		AcceptBlock(context.Context, types.Block) error
		Synced(ctx context.Context) bool
		TipState(ctx context.Context) consensus.State
	}

	// A Syncer can connect to other peers and synchronize the blockchain.
	Syncer interface {
		SyncerAddress(ctx context.Context) (string, error)
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
		Address() types.Address
		Balance() types.Currency
		FundTransaction(cs consensus.State, txn *types.Transaction, amount types.Currency, pool []types.Transaction) ([]types.Hash256, error)
		Redistribute(cs consensus.State, outputs int, amount, feePerByte types.Currency, pool []types.Transaction) (types.Transaction, []types.Hash256, error)
		ReleaseInputs(txn types.Transaction)
		SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error
		Transactions(since time.Time, max int) ([]wallet.Transaction, error)
		UnspentOutputs() ([]wallet.SiacoinElement, error)
	}

	// A HostDB stores information about hosts.
	HostDB interface {
		Host(ctx context.Context, hostKey types.PublicKey) (hostdb.Host, error)
		Hosts(ctx context.Context, offset, limit int) ([]hostdb.Host, error)
		HostsForScanning(ctx context.Context, maxLastScan time.Time, offset, limit int) ([]hostdb.HostAddress, error)
		RecordInteractions(ctx context.Context, interactions []hostdb.Interaction) error
		RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (uint64, error)

		HostBlocklist(ctx context.Context) ([]string, error)
		AddHostBlocklistEntry(ctx context.Context, entry string) error
		RemoveHostBlocklistEntry(ctx context.Context, entry string) error
	}

	// A MetadataStore stores information about contracts and objects.
	MetadataStore interface {
		AddContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (api.ContractMetadata, error)
		AddRenewedContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (api.ContractMetadata, error)
		ActiveContracts(ctx context.Context) ([]api.ContractMetadata, error)
		AncestorContracts(ctx context.Context, fcid types.FileContractID, minStartHeight uint64) ([]api.ArchivedContract, error)
		Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
		Contracts(ctx context.Context, set string) ([]api.ContractMetadata, error)
		RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error
		RemoveContract(ctx context.Context, id types.FileContractID) error
		SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) error

		Object(ctx context.Context, key string) (object.Object, error)
		Objects(ctx context.Context, key string) ([]string, error)
		UpdateObject(ctx context.Context, key string, o object.Object, usedContracts map[types.PublicKey]types.FileContractID) error
		RemoveObject(ctx context.Context, key string) error

		UnhealthySlabs(ctx context.Context, set string, limit int) ([]object.Slab, error)
		UpdateSlab(ctx context.Context, s object.Slab, usedContracts map[types.PublicKey]types.FileContractID) error
	}

	// A SettingStore stores settings.
	SettingStore interface {
		Setting(ctx context.Context, key string) (string, error)
		Settings(ctx context.Context) ([]string, error)
		UpdateSetting(ctx context.Context, key, value string) error
		UpdateSettings(ctx context.Context, settings map[string]string) error
	}

	// EphemeralAccountStore persists information about accounts. Since
	// accounts are rapidly updated and can be recovered, they are only
	// loaded upon startup and persisted upon shutdown.
	EphemeralAccountStore interface {
		Accounts(context.Context) ([]api.Account, error)
		SaveAccounts(context.Context, []api.Account) error
	}
)

type bus struct {
	s   Syncer
	cm  ChainManager
	tp  TransactionPool
	w   Wallet
	hdb HostDB
	ms  MetadataStore
	ss  SettingStore

	logger        *zap.SugaredLogger
	accounts      *accounts
	contractLocks *contractLocks
}

func (b *bus) consensusAcceptBlock(jc jape.Context) {
	var block types.Block
	if jc.Decode(&block) != nil {
		return
	}
	if jc.Check("failed to accept block", b.cm.AcceptBlock(jc.Request.Context(), block)) != nil {
		return
	}
}

func (b *bus) syncerAddrHandler(jc jape.Context) {
	addr, err := b.s.SyncerAddress(jc.Request.Context())
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
		BlockHeight: b.cm.TipState(jc.Request.Context()).Index.Height,
		Synced:      b.cm.Synced(jc.Request.Context()),
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
	toSign, err := b.w.FundTransaction(b.cm.TipState(jc.Request.Context()), &txn, wfr.Amount.Add(txn.MinerFees[0]), b.tp.Transactions())
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
	err := b.w.SignTransaction(b.cm.TipState(jc.Request.Context()), &wsr.Transaction, wsr.ToSign, wsr.CoveredFields)
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

	cs := b.cm.TipState(jc.Request.Context())
	txn, toSign, err := b.w.Redistribute(cs, wfr.Outputs, wfr.Amount, b.tp.RecommendedFee(), b.tp.Transactions())
	if jc.Check("couldn't redistribute money in the wallet into the desired outputs", err) != nil {
		return
	}

	err = b.w.SignTransaction(cs, &txn, toSign, types.CoveredFields{WholeTransaction: true})
	if jc.Check("couldn't sign the transaction", err) != nil {
		return
	}

	if jc.Check("couldn't broadcast the transaction", b.tp.AddTransactionSet([]types.Transaction{txn})) != nil {
		b.w.ReleaseInputs(txn)
		return
	}

	jc.Encode(txn.ID())
}

func (b *bus) walletDiscardHandler(jc jape.Context) {
	var txn types.Transaction
	if jc.Decode(&txn) == nil {
		b.w.ReleaseInputs(txn)
	}
}

func (b *bus) walletPrepareFormHandler(jc jape.Context) {
	ctx := jc.Request.Context()
	var wpfr api.WalletPrepareFormRequest
	if jc.Decode(&wpfr) != nil {
		return
	}
	if wpfr.HostKey == (types.PublicKey{}) {
		jc.Error(errors.New("no host key provided"), http.StatusBadRequest)
		return
	}
	if wpfr.RenterKey == nil {
		jc.Error(errors.New("no renter key provided"), http.StatusBadRequest)
		return
	}

	fc := rhpv2.PrepareContractFormation(wpfr.RenterKey, wpfr.HostKey, wpfr.RenterFunds, wpfr.HostCollateral, wpfr.EndHeight, wpfr.HostSettings, wpfr.RenterAddress)
	cost := rhpv2.ContractFormationCost(fc, wpfr.HostSettings.ContractPrice)
	txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}
	txn.MinerFees = []types.Currency{b.tp.RecommendedFee().Mul64(uint64(len(encoding.Marshal(txn))))}
	toSign, err := b.w.FundTransaction(b.cm.TipState(ctx), &txn, cost.Add(txn.MinerFees[0]), b.tp.Transactions())
	if jc.Check("couldn't fund transaction", err) != nil {
		return
	}
	cf := wallet.ExplicitCoveredFields(txn)
	err = b.w.SignTransaction(b.cm.TipState(ctx), &txn, toSign, cf)
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
	if wprr.HostKey == (types.PublicKey{}) {
		jc.Error(errors.New("no host key provided"), http.StatusBadRequest)
		return
	}
	if wprr.RenterKey == nil {
		jc.Error(errors.New("no renter key provided"), http.StatusBadRequest)
		return
	}

	fc := rhpv2.PrepareContractRenewal(wprr.Contract, wprr.RenterAddress, wprr.RenterKey, wprr.RenterFunds, wprr.NewCollateral, wprr.HostKey, wprr.HostSettings, wprr.EndHeight)
	cost := rhpv2.ContractRenewalCost(fc, wprr.HostSettings.ContractPrice)
	finalPayment := wprr.HostSettings.BaseRPCPrice
	if finalPayment.Cmp(wprr.Contract.ValidRenterPayout()) > 0 {
		finalPayment = wprr.Contract.ValidRenterPayout()
	}
	txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}
	txn.MinerFees = []types.Currency{b.tp.RecommendedFee().Mul64(uint64(len(encoding.Marshal(txn))))}
	toSign, err := b.w.FundTransaction(b.cm.TipState(jc.Request.Context()), &txn, cost.Add(txn.MinerFees[0]), b.tp.Transactions())
	if jc.Check("couldn't fund transaction", err) != nil {
		return
	}
	cf := wallet.ExplicitCoveredFields(txn)
	err = b.w.SignTransaction(b.cm.TipState(jc.Request.Context()), &txn, toSign, cf)
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
			if sco.Address == addr {
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
	offset := 0
	limit := -1
	if jc.DecodeForm("offset", &offset) != nil || jc.DecodeForm("limit", &limit) != nil {
		return
	}
	hosts, err := b.hdb.Hosts(jc.Request.Context(), offset, limit)
	if jc.Check(fmt.Sprintf("couldn't fetch hosts %d-%d", offset, offset+limit), err) != nil {
		return
	}
	jc.Encode(hosts)
}

func (b *bus) hostsRemoveHandlerPOST(jc jape.Context) {
	var hrr api.HostsRemoveRequest
	if jc.Decode(&hrr) != nil {
		return
	}
	if hrr.MaxDowntimeHours == 0 {
		jc.Error(errors.New("maxDowntime must be non-zero"), http.StatusBadRequest)
		return
	}
	removed, err := b.hdb.RemoveOfflineHosts(jc.Request.Context(), hrr.MinRecentScanFailures, time.Duration(hrr.MaxDowntimeHours))
	if jc.Check("couldn't remove offline hosts", err) != nil {
		return
	}
	jc.Encode(removed)
}

func (b *bus) hostsScanningHandlerGET(jc jape.Context) {
	offset := 0
	limit := -1
	maxLastScan := time.Now()
	if jc.DecodeForm("offset", &offset) != nil || jc.DecodeForm("limit", &limit) != nil || jc.DecodeForm("lastScan", (*api.ParamTime)(&maxLastScan)) != nil {
		return
	}
	hosts, err := b.hdb.HostsForScanning(jc.Request.Context(), maxLastScan, offset, limit)
	if jc.Check(fmt.Sprintf("couldn't fetch hosts %d-%d", offset, offset+limit), err) != nil {
		return
	}
	jc.Encode(hosts)
}

func (b *bus) hostsPubkeyHandlerGET(jc jape.Context) {
	var hostKey types.PublicKey
	if jc.DecodeParam("hostkey", &hostKey) != nil {
		return
	}
	host, err := b.hdb.Host(jc.Request.Context(), hostKey)
	if jc.Check("couldn't load host", err) == nil {
		jc.Encode(host)
	}
}

func (b *bus) hostsPubkeyHandlerPOST(jc jape.Context) {
	var interactions []hostdb.Interaction
	if jc.Decode(&interactions) != nil {
		return
	}
	if jc.Check("failed to record interactions", b.hdb.RecordInteractions(jc.Request.Context(), interactions)) != nil {
		return
	}
}

func (b *bus) contractsSpendingHandlerPOST(jc jape.Context) {
	var records []api.ContractSpendingRecord
	if jc.Decode(&records) != nil {
		return
	}
	if jc.Check("failed to record spending metrics for contract", b.ms.RecordContractSpending(jc.Request.Context(), records)) != nil {
		return
	}
}

func (b *bus) hostsBlocklistHandlerGET(jc jape.Context) {
	blocklist, err := b.hdb.HostBlocklist(jc.Request.Context())
	if jc.Check("couldn't load blocklist", err) == nil {
		jc.Encode(blocklist)
	}
}

func (b *bus) hostsBlocklistHandlerPUT(jc jape.Context) {
	ctx := jc.Request.Context()
	var req api.UpdateBlocklistRequest
	if jc.Decode(&req) == nil {
		for _, entry := range req.Add {
			if jc.Check(fmt.Sprintf("couldn't add blocklist entry '%s'", entry), b.hdb.AddHostBlocklistEntry(ctx, entry)) != nil {
				return
			}
		}
		for _, entry := range req.Remove {
			if jc.Check(fmt.Sprintf("couldn't remove blocklist entry '%s'", entry), b.hdb.RemoveHostBlocklistEntry(ctx, entry)) != nil {
				return
			}
		}
	}
}

func (b *bus) contractsActiveHandlerGET(jc jape.Context) {
	cs, err := b.ms.ActiveContracts(jc.Request.Context())
	if jc.Check("couldn't load contracts", err) == nil {
		jc.Encode(cs)
	}
}

func (b *bus) contractsSetHandlerGET(jc jape.Context) {
	cs, err := b.ms.Contracts(jc.Request.Context(), jc.PathParam("set"))
	if jc.Check("couldn't load contracts", err) == nil {
		jc.Encode(cs)
	}
}

func (b *bus) contractsSetHandlerPUT(jc jape.Context) {
	var contractIds []types.FileContractID
	if set := jc.PathParam("set"); set == "" {
		jc.Error(errors.New("param 'set' can not be empty"), http.StatusBadRequest)
	} else if jc.Decode(&contractIds) == nil {
		jc.Check("could not add contracts to set", b.ms.SetContractSet(jc.Request.Context(), set, contractIds))
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

	lockID, err := b.contractLocks.Acquire(jc.Request.Context(), req.Priority, id, time.Duration(req.Duration))
	if jc.Check("failed to acquire contract", err) != nil {
		return
	}
	jc.Encode(api.ContractAcquireResponse{
		LockID: lockID,
	})
}

func (b *bus) contractReleaseHandlerPOST(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.ContractReleaseRequest
	if jc.Decode(&req) != nil {
		return
	}
	if jc.Check("failed to release contract", b.contractLocks.Release(id, req.LockID)) != nil {
		return
	}
}

func (b *bus) contractIDHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	c, err := b.ms.Contract(jc.Request.Context(), id)
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

	a, err := b.ms.AddContract(jc.Request.Context(), req.Contract, req.TotalCost, req.StartHeight)
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

	r, err := b.ms.AddRenewedContract(jc.Request.Context(), req.Contract, req.TotalCost, req.StartHeight, req.RenewedFrom)
	if jc.Check("couldn't store contract", err) == nil {
		jc.Encode(r)
	}
}

func (b *bus) contractIDHandlerDELETE(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	jc.Check("couldn't remove contract", b.ms.RemoveContract(jc.Request.Context(), id))
}

func (b *bus) objectsKeyHandlerGET(jc jape.Context) {
	ctx := jc.Request.Context()
	if strings.HasSuffix(jc.PathParam("key"), "/") {
		keys, err := b.ms.Objects(ctx, jc.PathParam("key"))
		if jc.Check("couldn't list objects", err) == nil {
			jc.Encode(api.ObjectsResponse{Entries: keys})
		}
		return
	}
	o, err := b.ms.Object(ctx, jc.PathParam("key"))
	if jc.Check("couldn't load object", err) == nil {
		jc.Encode(api.ObjectsResponse{Object: &o})
	}
}

func (b *bus) objectsKeyHandlerPUT(jc jape.Context) {
	var aor api.AddObjectRequest
	if jc.Decode(&aor) == nil {
		jc.Check("couldn't store object", b.ms.UpdateObject(jc.Request.Context(), jc.PathParam("key"), aor.Object, aor.UsedContracts))
	}
}

func (b *bus) objectsKeyHandlerDELETE(jc jape.Context) {
	jc.Check("couldn't delete object", b.ms.RemoveObject(jc.Request.Context(), jc.PathParam("key")))
}

func (b *bus) slabHandlerPUT(jc jape.Context) {
	var usr api.UpdateSlabRequest
	if jc.Decode(&usr) == nil {
		jc.Check("couldn't update slab", b.ms.UpdateSlab(jc.Request.Context(), usr.Slab, usr.UsedContracts))
	}
}

func (b *bus) slabsMigrationHandlerPOST(jc jape.Context) {
	var msr api.MigrationSlabsRequest
	if jc.Decode(&msr) == nil {
		if slabs, err := b.ms.UnhealthySlabs(jc.Request.Context(), msr.ContractSet, msr.Limit); jc.Check("couldn't fetch slabs for migration", err) == nil {
			jc.Encode(slabs)
		}
	}
}

func (b *bus) settingsHandlerGET(jc jape.Context) {
	if settings, err := b.ss.Settings(jc.Request.Context()); jc.Check("couldn't load settings", err) == nil {
		jc.Encode(settings)
	}
}

func (b *bus) settingsHandlerPUT(jc jape.Context) {
	var settings map[string]string
	if jc.Decode(&settings) == nil {
		jc.Check("couldn't update settings", b.ss.UpdateSettings(jc.Request.Context(), settings))
	}
}

func (b *bus) settingKeyHandlerGET(jc jape.Context) {
	if key := jc.PathParam("key"); key == "" {
		jc.Error(errors.New("param 'key' can not be empty"), http.StatusBadRequest)
	} else if setting, err := b.ss.Setting(jc.Request.Context(), jc.PathParam("key")); errors.Is(err, api.ErrSettingNotFound) {
		jc.Error(err, http.StatusNotFound)
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
	} else {
		jc.Encode(setting)
	}
}

func (b *bus) settingKeyHandlerPUT(jc jape.Context) {
	var value string
	if key := jc.PathParam("key"); key == "" {
		jc.Error(errors.New("param 'key' can not be empty"), http.StatusBadRequest)
	} else if jc.Decode(&value) == nil {
		jc.Check("could not update setting", b.ss.UpdateSetting(jc.Request.Context(), key, value))
	}
}

func (b *bus) setGougingSettings(ctx context.Context, gs api.GougingSettings) error {
	if js, err := json.Marshal(gs); err != nil {
		panic(err)
	} else {
		return b.ss.UpdateSetting(ctx, SettingGouging, string(js))
	}
}

func (b *bus) setRedundancySettings(ctx context.Context, rs api.RedundancySettings) error {
	if err := rs.Validate(); err != nil {
		return err
	} else if js, err := json.Marshal(rs); err != nil {
		panic(err)
	} else {
		return b.ss.UpdateSetting(ctx, SettingRedundancy, string(js))
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
	ancestors, err := b.ms.AncestorContracts(jc.Request.Context(), fcid, minStartHeight)
	if jc.Check("failed to fetch ancestor contracts", err) != nil {
		return
	}
	jc.Encode(ancestors)
}

func (b *bus) paramsHandlerDownloadGET(jc jape.Context) {
	gp, err := b.gougingParams(jc.Request.Context())
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	cs, err := b.ss.Setting(jc.Request.Context(), SettingContractSet)
	if jc.Check("could not get contract set setting", err) != nil {
		return
	}

	jc.Encode(api.DownloadParams{
		ContractSet:   cs,
		GougingParams: gp,
	})
}

func (b *bus) paramsHandlerUploadGET(jc jape.Context) {
	gp, err := b.gougingParams(jc.Request.Context())
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	cs, err := b.ss.Setting(jc.Request.Context(), SettingContractSet)
	if jc.Check("could not get contract set setting", err) != nil {
		return
	}

	jc.Encode(api.UploadParams{
		ContractSet:   cs,
		CurrentHeight: b.cm.TipState(jc.Request.Context()).Index.Height,
		GougingParams: gp,
	})
}

func (b *bus) paramsHandlerGougingGET(jc jape.Context) {
	gp, err := b.gougingParams(jc.Request.Context())
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	jc.Encode(gp)
}

func (b *bus) gougingParams(ctx context.Context) (api.GougingParams, error) {
	var gs api.GougingSettings
	if gss, err := b.ss.Setting(ctx, SettingGouging); err != nil {
		return api.GougingParams{}, err
	} else if err := json.Unmarshal([]byte(gss), &gs); err != nil {
		panic(err)
	}

	var rs api.RedundancySettings
	if rss, err := b.ss.Setting(ctx, SettingRedundancy); err != nil {
		return api.GougingParams{}, err
	} else if err := json.Unmarshal([]byte(rss), &rs); err != nil {
		panic(err)
	}

	return api.GougingParams{
		GougingSettings:    gs,
		RedundancySettings: rs,
	}, nil
}

func (b *bus) accountsOwnerHandlerGET(jc jape.Context) {
	var owner api.ParamString
	if jc.DecodeParam("owner", &owner) != nil {
		return
	}
	jc.Encode(b.accounts.Accounts(owner.String()))
}

func (b *bus) accountsAddHandlerPOST(jc jape.Context) {
	var id rhpv3.Account
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.AccountsAddBalanceRequest
	if jc.Decode(&req) != nil {
		return
	}
	if id == (rhpv3.Account{}) {
		jc.Error(errors.New("account id needs to be set"), http.StatusBadRequest)
		return
	}
	if req.Owner == "" {
		jc.Error(errors.New("owner needs to be set"), http.StatusBadRequest)
		return
	}
	if req.Host == (types.PublicKey{}) {
		jc.Error(errors.New("host needs to be set"), http.StatusBadRequest)
		return
	}
	b.accounts.AddAmount(id, string(req.Owner), req.Host, req.Amount)
}

func (b *bus) accountsUpdateHandlerPOST(jc jape.Context) {
	var id rhpv3.Account
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.AccountsUpdateBalanceRequest
	if jc.Decode(&req) != nil {
		return
	}
	if id == (rhpv3.Account{}) {
		jc.Error(errors.New("account id needs to be set"), http.StatusBadRequest)
		return
	}
	if req.Owner == "" {
		jc.Error(errors.New("owner needs to be set"), http.StatusBadRequest)
		return
	}
	if req.Host == (types.PublicKey{}) {
		jc.Error(errors.New("host needs to be set"), http.StatusBadRequest)
		return
	}
	b.accounts.SetBalance(id, string(req.Owner), req.Host, req.Amount)
}

// New returns a new Bus.
func New(s Syncer, cm ChainManager, tp TransactionPool, w Wallet, hdb HostDB, ms MetadataStore, ss SettingStore, eas EphemeralAccountStore, gs api.GougingSettings, rs api.RedundancySettings, l *zap.Logger) (http.Handler, func() error, error) {
	b := &bus{
		s:             s,
		cm:            cm,
		tp:            tp,
		w:             w,
		hdb:           hdb,
		ms:            ms,
		ss:            ss,
		contractLocks: newContractLocks(),
		logger:        l.Sugar().Named("bus"),
	}
	ctx, span := tracing.Tracer.Start(context.Background(), "bus.New")
	defer span.End()
	if err := b.setGougingSettings(ctx, gs); err != nil {
		return nil, nil, err
	}

	if err := b.setRedundancySettings(ctx, rs); err != nil {
		return nil, nil, err
	}

	// Load the ephemeral accounts into memory and save them again on
	// cleanup.
	accounts, err := eas.Accounts(ctx)
	if err != nil {
		return nil, nil, err
	}
	b.accounts = newAccounts(accounts)
	cleanup := func() error {
		return eas.SaveAccounts(ctx, b.accounts.ToPersist())
	}

	return jape.Mux(tracing.TracedRoutes("bus", map[string]jape.Handler{
		"GET    /accounts/:owner":     b.accountsOwnerHandlerGET,
		"POST   /accounts/:id/add":    b.accountsAddHandlerPOST,
		"POST   /accounts/:id/update": b.accountsUpdateHandlerPOST,

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
		"POST   /hosts/remove":       b.hostsRemoveHandlerPOST,
		"GET    /hosts/blocklist":    b.hostsBlocklistHandlerGET,
		"PUT    /hosts/blocklist":    b.hostsBlocklistHandlerPUT,
		"GET    /hosts/scanning":     b.hostsScanningHandlerGET,

		"GET    /contracts/active":       b.contractsActiveHandlerGET,
		"GET    /contracts/set/:set":     b.contractsSetHandlerGET,
		"PUT    /contracts/set/:set":     b.contractsSetHandlerPUT,
		"POST   /contracts/spending":     b.contractsSpendingHandlerPOST,
		"GET    /contract/:id":           b.contractIDHandlerGET,
		"POST   /contract/:id":           b.contractIDHandlerPOST,
		"GET    /contract/:id/ancestors": b.contractIDAncestorsHandler,
		"POST   /contract/:id/renewed":   b.contractIDRenewedHandlerPOST,
		"DELETE /contract/:id":           b.contractIDHandlerDELETE,
		"POST   /contract/:id/acquire":   b.contractAcquireHandlerPOST,
		"POST   /contract/:id/release":   b.contractReleaseHandlerPOST,

		"GET    /objects/*key": b.objectsKeyHandlerGET,
		"PUT    /objects/*key": b.objectsKeyHandlerPUT,
		"DELETE /objects/*key": b.objectsKeyHandlerDELETE,

		"POST   /slabs/migration": b.slabsMigrationHandlerPOST,
		"PUT    /slab":            b.slabHandlerPUT,

		"GET    /settings":     b.settingsHandlerGET,
		"PUT    /settings":     b.settingsHandlerPUT,
		"GET    /setting/:key": b.settingKeyHandlerGET,
		"PUT    /setting/:key": b.settingKeyHandlerPUT,

		"GET    /params/download": b.paramsHandlerDownloadGET,
		"GET    /params/upload":   b.paramsHandlerUploadGET,
		"GET    /params/gouging":  b.paramsHandlerGougingGET,
	})), cleanup, nil
}
