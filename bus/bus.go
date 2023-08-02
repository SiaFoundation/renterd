package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"go.sia.tech/renterd/wallet"
	"go.uber.org/zap"
)

type (
	// A ChainManager manages blockchain state.
	ChainManager interface {
		AcceptBlock(context.Context, types.Block) error
		LastBlockTime() time.Time
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
		Balance() (types.Currency, error)
		FundTransaction(cs consensus.State, txn *types.Transaction, amount types.Currency, pool []types.Transaction) ([]types.Hash256, error)
		Redistribute(cs consensus.State, outputs int, amount, feePerByte types.Currency, pool []types.Transaction) (types.Transaction, []types.Hash256, error)
		ReleaseInputs(txn types.Transaction)
		SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error
		Transactions(before, since time.Time, offset, limit int) ([]wallet.Transaction, error)
		UnspentOutputs() ([]wallet.SiacoinElement, error)
	}

	// A HostDB stores information about hosts.
	HostDB interface {
		Host(ctx context.Context, hostKey types.PublicKey) (hostdb.HostInfo, error)
		Hosts(ctx context.Context, offset, limit int) ([]hostdb.Host, error)
		SearchHosts(ctx context.Context, filterMode, addressContains string, keyIn []types.PublicKey, offset, limit int) ([]hostdb.Host, error)
		HostsForScanning(ctx context.Context, maxLastScan time.Time, offset, limit int) ([]hostdb.HostAddress, error)
		RecordInteractions(ctx context.Context, interactions []hostdb.Interaction) error
		RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (uint64, error)

		HostAllowlist(ctx context.Context) ([]types.PublicKey, error)
		HostBlocklist(ctx context.Context) ([]string, error)
		UpdateHostAllowlistEntries(ctx context.Context, add, remove []types.PublicKey, clear bool) error
		UpdateHostBlocklistEntries(ctx context.Context, add, remove []string, clear bool) error
	}

	// A MetadataStore stores information about contracts and objects.
	MetadataStore interface {
		AddContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (api.ContractMetadata, error)
		AddRenewedContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (api.ContractMetadata, error)
		AncestorContracts(ctx context.Context, fcid types.FileContractID, minStartHeight uint64) ([]api.ArchivedContract, error)
		ArchiveContract(ctx context.Context, id types.FileContractID, reason string) error
		ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) error
		ArchiveAllContracts(ctx context.Context, reason string) error
		Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
		Contracts(ctx context.Context) ([]api.ContractMetadata, error)
		ContractSetContracts(ctx context.Context, set string) ([]api.ContractMetadata, error)
		ContractSets(ctx context.Context) ([]string, error)
		RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error
		RemoveContractSet(ctx context.Context, name string) error
		RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error)
		SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) error

		Object(ctx context.Context, path string) (object.Object, error)
		ObjectEntries(ctx context.Context, path, prefix string, offset, limit int) ([]api.ObjectMetadata, error)
		SearchObjects(ctx context.Context, substring string, offset, limit int) ([]api.ObjectMetadata, error)
		UpdateObject(ctx context.Context, path, contractSet string, o object.Object, ps *object.PartialSlab, usedContracts map[types.PublicKey]types.FileContractID) error
		RemoveObject(ctx context.Context, path string) error
		RemoveObjects(ctx context.Context, prefix string) error
		RenameObject(ctx context.Context, from, to string) error
		RenameObjects(ctx context.Context, from, to string) error

		ObjectsStats(ctx context.Context) (api.ObjectsStats, error)

		Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error)
		RefreshHealth(ctx context.Context) error
		UnhealthySlabs(ctx context.Context, healthCutoff float64, set string, limit int) ([]api.UnhealthySlab, error)
		UpdateSlab(ctx context.Context, s object.Slab, contractSet string, usedContracts map[types.PublicKey]types.FileContractID) error
	}

	// An AutopilotStore stores autopilots.
	AutopilotStore interface {
		Autopilots(ctx context.Context) ([]api.Autopilot, error)
		Autopilot(ctx context.Context, id string) (api.Autopilot, error)
		UpdateAutopilot(ctx context.Context, ap api.Autopilot) error
	}

	// A SettingStore stores settings.
	SettingStore interface {
		DeleteSetting(ctx context.Context, key string) error
		Setting(ctx context.Context, key string) (string, error)
		Settings(ctx context.Context) ([]string, error)
		UpdateSetting(ctx context.Context, key, value string) error
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
	alerts *alerts.Manager
	s      Syncer
	cm     ChainManager
	tp     TransactionPool
	w      Wallet
	hdb    HostDB
	as     AutopilotStore
	ms     MetadataStore
	ss     SettingStore

	eas EphemeralAccountStore

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
	jc.Encode(b.consensusState(jc.Request.Context()))
}

func (b *bus) consensusNetworkHandler(jc jape.Context) {
	jc.Encode(api.ConsensusNetwork{
		Name: b.cm.TipState(jc.Request.Context()).Network.Name,
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
	balance, err := b.w.Balance()
	if jc.Check("couldn't fetch wallet balance", err) != nil {
		return
	}
	jc.Encode(balance)
}

func (b *bus) walletAddressHandler(jc jape.Context) {
	jc.Encode(b.w.Address())
}

func (b *bus) walletTransactionsHandler(jc jape.Context) {
	var before, since time.Time
	offset := 0
	limit := -1
	if jc.DecodeForm("before", (*api.ParamTime)(&before)) != nil ||
		jc.DecodeForm("since", (*api.ParamTime)(&since)) != nil ||
		jc.DecodeForm("offset", &offset) != nil ||
		jc.DecodeForm("limit", &limit) != nil {
		return
	}
	txns, err := b.w.Transactions(before, since, offset, limit)
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
	fee := b.tp.RecommendedFee().Mul64(uint64(types.EncodedLen(txn)))
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
	if wpfr.RenterKey == (types.PublicKey{}) {
		jc.Error(errors.New("no renter key provided"), http.StatusBadRequest)
		return
	}
	cs := b.cm.TipState(ctx)

	fc := rhpv2.PrepareContractFormation(wpfr.RenterKey, wpfr.HostKey, wpfr.RenterFunds, wpfr.HostCollateral, wpfr.EndHeight, wpfr.HostSettings, wpfr.RenterAddress)
	cost := rhpv2.ContractFormationCost(cs, fc, wpfr.HostSettings.ContractPrice)
	txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}
	txn.MinerFees = []types.Currency{b.tp.RecommendedFee().Mul64(uint64(types.EncodedLen(txn)))}
	toSign, err := b.w.FundTransaction(cs, &txn, cost.Add(txn.MinerFees[0]), b.tp.Transactions())
	if jc.Check("couldn't fund transaction", err) != nil {
		return
	}
	cf := wallet.ExplicitCoveredFields(txn)
	err = b.w.SignTransaction(cs, &txn, toSign, cf)
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
	cs := b.cm.TipState(jc.Request.Context())

	// Create the final revision from the provided revision.
	finalRevision := wprr.Revision
	finalRevision.MissedProofOutputs = finalRevision.ValidProofOutputs
	finalRevision.Filesize = 0
	finalRevision.FileMerkleRoot = types.Hash256{}
	finalRevision.RevisionNumber = math.MaxUint64

	// Prepare the new contract.
	fc, basePrice := rhpv3.PrepareContractRenewal(wprr.Revision, wprr.HostAddress, wprr.RenterAddress, wprr.RenterFunds, wprr.NewCollateral, wprr.HostKey, wprr.PriceTable, wprr.EndHeight)

	// Create the transaction containing both the final revision and new
	// contract.
	txn := types.Transaction{
		FileContracts:         []types.FileContract{fc},
		FileContractRevisions: []types.FileContractRevision{finalRevision},
		MinerFees:             []types.Currency{wprr.PriceTable.TxnFeeMaxRecommended.Mul64(4096)},
	}

	// Compute how much renter funds to put into the new contract.
	cost := rhpv3.ContractRenewalCost(cs, wprr.PriceTable, fc, txn.MinerFees[0], basePrice)

	// Fund the txn. We are not signing it yet since it's not complete. The host
	// still needs to complete it and the revision + contract are signed with
	// the renter key by the worker.
	toSign, err := b.w.FundTransaction(cs, &txn, cost, b.tp.Transactions())
	if jc.Check("couldn't fund transaction", err) != nil {
		return
	}

	// Add any required parents.
	parents, err := b.tp.UnconfirmedParents(txn)
	if jc.Check("couldn't load transaction dependencies", err) != nil {
		b.w.ReleaseInputs(txn)
		return
	}
	jc.Encode(api.WalletPrepareRenewResponse{
		ToSign:         toSign,
		TransactionSet: append(parents, txn),
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

func (b *bus) searchHostsHandlerPOST(jc jape.Context) {
	var req api.SearchHostsRequest
	if jc.Decode(&req) != nil {
		return
	}
	hosts, err := b.hdb.SearchHosts(jc.Request.Context(), req.FilterMode, req.AddressContains, req.KeyIn, req.Offset, req.Limit)
	if jc.Check(fmt.Sprintf("couldn't fetch hosts %d-%d", req.Offset, req.Offset+req.Limit), err) != nil {
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

func (b *bus) hostsAllowlistHandlerGET(jc jape.Context) {
	allowlist, err := b.hdb.HostAllowlist(jc.Request.Context())
	if jc.Check("couldn't load allowlist", err) == nil {
		jc.Encode(allowlist)
	}
}

func (b *bus) hostsAllowlistHandlerPUT(jc jape.Context) {
	ctx := jc.Request.Context()
	var req api.UpdateAllowlistRequest
	if jc.Decode(&req) == nil {
		if len(req.Add)+len(req.Remove) > 0 && req.Clear {
			jc.Error(errors.New("cannot add or remove entries while clearing the allowlist"), http.StatusBadRequest)
			return
		} else if jc.Check("couldn't update allowlist entries", b.hdb.UpdateHostAllowlistEntries(ctx, req.Add, req.Remove, req.Clear)) != nil {
			return
		}
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
		if len(req.Add)+len(req.Remove) > 0 && req.Clear {
			jc.Error(errors.New("cannot add or remove entries while clearing the blocklist"), http.StatusBadRequest)
			return
		} else if jc.Check("couldn't update blocklist entries", b.hdb.UpdateHostBlocklistEntries(ctx, req.Add, req.Remove, req.Clear)) != nil {
			return
		}
	}
}

func (b *bus) contractsHandlerGET(jc jape.Context) {
	cs, err := b.ms.Contracts(jc.Request.Context())
	if jc.Check("couldn't load contracts", err) == nil {
		jc.Encode(cs)
	}
}

func (b *bus) contractsRenewedIDHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	md, err := b.ms.RenewedContract(jc.Request.Context(), id)
	if jc.Check("faild to fetch renewed contract", err) == nil {
		jc.Encode(md)
	}
}

func (b *bus) contractsArchiveHandlerPOST(jc jape.Context) {
	var toArchive api.ArchiveContractsRequest
	if jc.Decode(&toArchive) != nil {
		return
	}

	jc.Check("failed to archive contracts", b.ms.ArchiveContracts(jc.Request.Context(), toArchive))
}

func (b *bus) contractsSetHandlerGET(jc jape.Context) {
	cs, err := b.ms.ContractSetContracts(jc.Request.Context(), jc.PathParam("set"))
	if jc.Check("couldn't load contracts", err) == nil {
		jc.Encode(cs)
	}
}

func (b *bus) contractsSetsHandlerGET(jc jape.Context) {
	sets, err := b.ms.ContractSets(jc.Request.Context())
	if jc.Check("couldn't fetch contract sets", err) == nil {
		jc.Encode(sets)
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

func (b *bus) contractsSetHandlerDELETE(jc jape.Context) {
	if set := jc.PathParam("set"); set != "" {
		jc.Check("could not remove contract set", b.ms.RemoveContractSet(jc.Request.Context(), set))
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

func (b *bus) contractKeepaliveHandlerPOST(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.ContractKeepaliveRequest
	if jc.Decode(&req) != nil {
		return
	}

	err := b.contractLocks.KeepAlive(id, req.LockID, time.Duration(req.Duration))
	if jc.Check("failed to extend lock duration", err) != nil {
		return
	}
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
	jc.Check("couldn't remove contract", b.ms.ArchiveContract(jc.Request.Context(), id, api.ContractArchivalReasonRemoved))
}

func (b *bus) contractsAllHandlerDELETE(jc jape.Context) {
	jc.Check("couldn't remove contracts", b.ms.ArchiveAllContracts(jc.Request.Context(), api.ContractArchivalReasonRemoved))
}

func (b *bus) searchObjectsHandlerGET(jc jape.Context) {
	offset := 0
	limit := -1
	var key string
	if jc.DecodeForm("offset", &offset) != nil || jc.DecodeForm("limit", &limit) != nil || jc.DecodeForm("key", &key) != nil {
		return
	}
	keys, err := b.ms.SearchObjects(jc.Request.Context(), key, offset, limit)
	if jc.Check("couldn't list objects", err) != nil {
		return
	}
	jc.Encode(keys)
}

func (b *bus) objectsHandlerGET(jc jape.Context) {
	path := jc.PathParam("path")
	if strings.HasSuffix(path, "/") {
		b.objectEntriesHandlerGET(jc, path)
		return
	}

	o, err := b.ms.Object(jc.Request.Context(), path)
	if errors.Is(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	if jc.Check("couldn't load object", err) != nil {
		return
	}
	jc.Encode(api.ObjectsResponse{Object: &o})
}

func (b *bus) objectEntriesHandlerGET(jc jape.Context, path string) {
	var offset int
	if jc.DecodeForm("offset", &offset) != nil {
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

	// look for object entries
	entries, err := b.ms.ObjectEntries(jc.Request.Context(), path, prefix, offset, limit)
	if jc.Check("couldn't list object entries", err) != nil {
		return
	}

	jc.Encode(api.ObjectsResponse{Entries: entries})
}

func (b *bus) objectsHandlerPUT(jc jape.Context) {
	var aor api.AddObjectRequest
	if jc.Decode(&aor) == nil {
		jc.Check("couldn't store object", b.ms.UpdateObject(jc.Request.Context(), jc.PathParam("path"), aor.ContractSet, aor.Object, nil, aor.UsedContracts)) // TODO
	}
}

func (b *bus) objectsRenameHandlerPOST(jc jape.Context) {
	var orr api.ObjectsRenameRequest
	if jc.Decode(&orr) != nil {
		return
	}
	if orr.Mode == api.ObjectsRenameModeSingle {
		// Single object rename.
		if strings.HasSuffix(orr.From, "/") || strings.HasSuffix(orr.To, "/") {
			jc.Error(fmt.Errorf("can't rename dirs with mode %v", orr.Mode), http.StatusBadRequest)
			return
		}
		jc.Check("couldn't rename object", b.ms.RenameObject(jc.Request.Context(), orr.From, orr.To))
		return
	} else if orr.Mode == api.ObjectsRenameModeMulti {
		// Multi object rename.
		if !strings.HasSuffix(orr.From, "/") || !strings.HasSuffix(orr.To, "/") {
			jc.Error(fmt.Errorf("can't rename file with mode %v", orr.Mode), http.StatusBadRequest)
			return
		}
		jc.Check("couldn't rename objects", b.ms.RenameObjects(jc.Request.Context(), orr.From, orr.To))
		return
	} else {
		// Invalid mode.
		jc.Error(fmt.Errorf("invalid mode: %v", orr.Mode), http.StatusBadRequest)
		return
	}
}

func (b *bus) objectsHandlerDELETE(jc jape.Context) {
	var batch bool
	if jc.DecodeForm("batch", &batch) != nil {
		return
	}
	var err error
	if batch {
		err = b.ms.RemoveObjects(jc.Request.Context(), jc.PathParam("path"))
	} else {
		err = b.ms.RemoveObject(jc.Request.Context(), jc.PathParam("path"))
	}
	if errors.Is(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	jc.Check("couldn't delete object", err)
}

func (b *bus) objectsStatshandlerGET(jc jape.Context) {
	info, err := b.ms.ObjectsStats(jc.Request.Context())
	if jc.Check("couldn't get objects stats", err) != nil {
		return
	}
	jc.Encode(info)
}

func (b *bus) slabHandlerGET(jc jape.Context) {
	var key object.EncryptionKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	slab, err := b.ms.Slab(jc.Request.Context(), key)
	if errors.Is(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(slab)
}

func (b *bus) slabHandlerPUT(jc jape.Context) {
	var usr api.UpdateSlabRequest
	if jc.Decode(&usr) == nil {
		jc.Check("couldn't update slab", b.ms.UpdateSlab(jc.Request.Context(), usr.Slab, usr.ContractSet, usr.UsedContracts))
	}
}

func (b *bus) slabsRefreshHealthHandlerPOST(jc jape.Context) {
	jc.Check("failed to recompute health", b.ms.RefreshHealth(jc.Request.Context()))
}

func (b *bus) slabsMigrationHandlerPOST(jc jape.Context) {
	var msr api.MigrationSlabsRequest
	if jc.Decode(&msr) == nil {
		if slabs, err := b.ms.UnhealthySlabs(jc.Request.Context(), msr.HealthCutoff, msr.ContractSet, msr.Limit); jc.Check("couldn't fetch slabs for migration", err) == nil {
			jc.Encode(api.UnhealthySlabsResponse{
				Slabs: slabs,
			})
		}
	}
}

func (b *bus) settingsHandlerGET(jc jape.Context) {
	if settings, err := b.ss.Settings(jc.Request.Context()); jc.Check("couldn't load settings", err) == nil {
		jc.Encode(settings)
	}
}

func (b *bus) settingKeyHandlerGET(jc jape.Context) {
	key := jc.PathParam("key")
	if key == "" {
		jc.Error(errors.New("param 'key' can not be empty"), http.StatusBadRequest)
		return
	}

	setting, err := b.ss.Setting(jc.Request.Context(), jc.PathParam("key"))
	if errors.Is(err, api.ErrSettingNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	var resp interface{}
	err = json.Unmarshal([]byte(setting), &resp)
	if err != nil {
		jc.Error(fmt.Errorf("couldn't unmarshal the setting, error: %v", err), http.StatusInternalServerError)
		return
	}

	jc.Encode(resp)
}

func (b *bus) settingKeyHandlerPUT(jc jape.Context) {
	key := jc.PathParam("key")
	if key == "" {
		jc.Error(errors.New("param 'key' can not be empty"), http.StatusBadRequest)
		return
	}

	var value interface{}
	if jc.Decode(&value) != nil {
		return
	}

	data, err := json.Marshal(value)
	if err != nil {
		jc.Error(fmt.Errorf("couldn't marshal the given value, error: %v", err), http.StatusBadRequest)
		return
	}

	switch key {
	case api.SettingGouging:
		var gs api.GougingSettings
		if err := json.Unmarshal(data, &gs); err != nil {
			jc.Error(fmt.Errorf("couldn't update gouging settings, invalid request body, %t", value), http.StatusBadRequest)
			return
		} else if err := gs.Validate(); err != nil {
			jc.Error(fmt.Errorf("couldn't update gouging settings, error: %v", err), http.StatusBadRequest)
			return
		}
	case api.SettingRedundancy:
		var rs api.RedundancySettings
		if err := json.Unmarshal(data, &rs); err != nil {
			jc.Error(fmt.Errorf("couldn't update redundancy settings, invalid request body"), http.StatusBadRequest)
			return
		} else if err := rs.Validate(); err != nil {
			jc.Error(fmt.Errorf("couldn't update redundancy settings, error: %v", err), http.StatusBadRequest)
			return
		}
	}

	jc.Check("could not update setting", b.ss.UpdateSetting(jc.Request.Context(), key, string(data)))
}

func (b *bus) settingKeyHandlerDELETE(jc jape.Context) {
	key := jc.PathParam("key")
	if key == "" {
		jc.Error(errors.New("param 'key' can not be empty"), http.StatusBadRequest)
		return
	}
	jc.Check("could not delete setting", b.ss.DeleteSetting(jc.Request.Context(), key))
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

func (b *bus) paramsHandlerUploadGET(jc jape.Context) {
	gp, err := b.gougingParams(jc.Request.Context())
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}

	val, err := b.ss.Setting(jc.Request.Context(), api.SettingContractSet)
	if err != nil && errors.Is(err, api.ErrSettingNotFound) {
		// return the upload params without a contract set, if the user is
		// specifying a contract set through the query string that's fine
		jc.Encode(api.UploadParams{
			ContractSet:   "",
			CurrentHeight: b.cm.TipState(jc.Request.Context()).Index.Height,
			GougingParams: gp,
		})
		return
	} else if err != nil {
		jc.Error(fmt.Errorf("could not get contract set settings: %w", err), http.StatusInternalServerError)
		return
	}

	var css api.ContractSetSetting
	if err := json.Unmarshal([]byte(val), &css); err != nil {
		b.logger.Panicf("failed to unmarshal contract set settings '%s': %v", val, err)
	}

	jc.Encode(api.UploadParams{
		ContractSet:   css.Default,
		CurrentHeight: b.cm.TipState(jc.Request.Context()).Index.Height,
		GougingParams: gp,
	})
}

func (b *bus) consensusState(ctx context.Context) api.ConsensusState {
	return api.ConsensusState{
		BlockHeight:   b.cm.TipState(ctx).Index.Height,
		LastBlockTime: b.cm.LastBlockTime(),
		Synced:        b.cm.Synced(ctx),
	}
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
	if gss, err := b.ss.Setting(ctx, api.SettingGouging); err != nil {
		return api.GougingParams{}, err
	} else if err := json.Unmarshal([]byte(gss), &gs); err != nil {
		b.logger.Panicf("failed to unmarshal gouging settings '%s': %v", gss, err)
	}

	var rs api.RedundancySettings
	if rss, err := b.ss.Setting(ctx, api.SettingRedundancy); err != nil {
		return api.GougingParams{}, err
	} else if err := json.Unmarshal([]byte(rss), &rs); err != nil {
		b.logger.Panicf("failed to unmarshal redundancy settings '%s': %v", rss, err)
	}

	cs := b.consensusState(ctx)

	return api.GougingParams{
		ConsensusState:     cs,
		GougingSettings:    gs,
		RedundancySettings: rs,
		TransactionFee:     b.tp.RecommendedFee(),
	}, nil
}

func (b *bus) handleGETAlerts(c jape.Context) {
	c.Encode(b.alerts.Active())
}

func (b *bus) handlePOSTAlertsDismiss(c jape.Context) {
	var ids []types.Hash256
	if c.Decode(&ids) != nil {
		return
	} else if len(ids) == 0 {
		c.Error(errors.New("no alerts to dismiss"), http.StatusBadRequest)
		return
	}
	b.alerts.Dismiss(ids...)
}

func (b *bus) accountsHandlerGET(jc jape.Context) {
	jc.Encode(b.accounts.Accounts())
}

func (b *bus) accountHandlerGET(jc jape.Context) {
	var id rhpv3.Account
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.AccountHandlerPOST
	if jc.Decode(&req) != nil {
		return
	}
	acc, err := b.accounts.Account(id, req.HostKey)
	if jc.Check("failed to fetch account", err) != nil {
		return
	}
	jc.Encode(acc)
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
	if req.HostKey == (types.PublicKey{}) {
		jc.Error(errors.New("host needs to be set"), http.StatusBadRequest)
		return
	}
	b.accounts.AddAmount(id, req.HostKey, req.Amount)
}

func (b *bus) accountsResetDriftHandlerPOST(jc jape.Context) {
	var id rhpv3.Account
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	err := b.accounts.ResetDrift(id)
	if errors.Is(err, errAccountsNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	if jc.Check("failed to reset drift", err) != nil {
		return
	}
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
	if req.HostKey == (types.PublicKey{}) {
		jc.Error(errors.New("host needs to be set"), http.StatusBadRequest)
		return
	}
	b.accounts.SetBalance(id, req.HostKey, req.Amount)
}

func (b *bus) accountsRequiresSyncHandlerPOST(jc jape.Context) {
	var id rhpv3.Account
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.AccountsRequiresSyncRequest
	if jc.Decode(&req) != nil {
		return
	}
	if id == (rhpv3.Account{}) {
		jc.Error(errors.New("account id needs to be set"), http.StatusBadRequest)
		return
	}
	if req.HostKey == (types.PublicKey{}) {
		jc.Error(errors.New("host needs to be set"), http.StatusBadRequest)
		return
	}
	err := b.accounts.ScheduleSync(id, req.HostKey)
	if errors.Is(err, errAccountsNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	if jc.Check("failed to set requiresSync flag on account", err) != nil {
		return
	}
}

func (b *bus) accountsLockHandlerPOST(jc jape.Context) {
	var id rhpv3.Account
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.AccountsLockHandlerRequest
	if jc.Decode(&req) != nil {
		return
	}

	acc, lockID := b.accounts.LockAccount(jc.Request.Context(), id, req.HostKey, req.Exclusive, time.Duration(req.Duration))
	jc.Encode(api.AccountsLockHandlerResponse{
		Account: acc,
		LockID:  lockID,
	})
}

func (b *bus) accountsUnlockHandlerPOST(jc jape.Context) {
	var id rhpv3.Account
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.AccountsUnlockHandlerRequest
	if jc.Decode(&req) != nil {
		return
	}

	err := b.accounts.UnlockAccount(id, req.LockID)
	if jc.Check("failed to unlock account", err) != nil {
		return
	}
}

func (b *bus) autopilotsListHandlerGET(jc jape.Context) {
	if autopilots, err := b.as.Autopilots(jc.Request.Context()); jc.Check("failed to fetch autopilots", err) == nil {
		jc.Encode(autopilots)
	}
}

func (b *bus) autopilotsHandlerGET(jc jape.Context) {
	var id string
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	ap, err := b.as.Autopilot(jc.Request.Context(), id)
	if errors.Is(err, api.ErrAutopilotNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	if jc.Check("couldn't load object", err) != nil {
		return
	}

	jc.Encode(ap)
}

func (b *bus) autopilotsHandlerPUT(jc jape.Context) {
	var id string
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	var ap api.Autopilot
	if jc.Decode(&ap) != nil {
		return
	}

	if ap.ID != id {
		jc.Error(errors.New("id in path and body don't match"), http.StatusBadRequest)
		return
	}

	jc.Check("failed to update autopilot", b.as.UpdateAutopilot(jc.Request.Context(), ap))
}

func (b *bus) contractTaxHandlerGET(jc jape.Context) {
	var payout types.Currency
	if jc.DecodeParam("payout", (*api.ParamCurrency)(&payout)) != nil {
		return
	}
	cs := b.cm.TipState(jc.Request.Context())
	jc.Encode(cs.FileContractTax(types.FileContract{Payout: payout}))
}

// New returns a new Bus.
func New(s Syncer, cm ChainManager, tp TransactionPool, w Wallet, hdb HostDB, as AutopilotStore, ms MetadataStore, ss SettingStore, eas EphemeralAccountStore, l *zap.Logger) (*bus, error) {
	b := &bus{
		alerts:        alerts.NewManager(),
		s:             s,
		cm:            cm,
		tp:            tp,
		w:             w,
		hdb:           hdb,
		as:            as,
		ms:            ms,
		ss:            ss,
		eas:           eas,
		contractLocks: newContractLocks(),
		logger:        l.Sugar().Named("bus"),
	}
	ctx, span := tracing.Tracer.Start(context.Background(), "bus.New")
	defer span.End()

	// Load default settings if the setting is not already set.
	for key, value := range map[string]interface{}{
		api.SettingGouging:    build.DefaultGougingSettings,
		api.SettingRedundancy: build.DefaultRedundancySettings,
	} {
		if _, err := b.ss.Setting(ctx, key); errors.Is(err, api.ErrSettingNotFound) {
			if bytes, err := json.Marshal(value); err != nil {
				panic("failed to marshal default settings") // should never happen
			} else if err := b.ss.UpdateSetting(ctx, key, string(bytes)); err != nil {
				return nil, err
			}
		}
	}

	// Check redundancy settings for validity
	var rs api.RedundancySettings
	if rss, err := b.ss.Setting(ctx, api.SettingRedundancy); err != nil {
		return nil, err
	} else if err := json.Unmarshal([]byte(rss), &rs); err != nil {
		return nil, err
	} else if err := rs.Validate(); err != nil {
		l.Warn(fmt.Sprintf("invalid redundancy setting found '%v', overwriting the redundancy settings with the default settings", rss))
		bytes, _ := json.Marshal(build.DefaultRedundancySettings)
		if err := b.ss.UpdateSetting(ctx, api.SettingRedundancy, string(bytes)); err != nil {
			return nil, err
		}
	}

	// Check gouging settings for validity
	var gs api.GougingSettings
	if gss, err := b.ss.Setting(ctx, api.SettingGouging); err != nil {
		return nil, err
	} else if err := json.Unmarshal([]byte(gss), &gs); err != nil {
		return nil, err
	} else if err := gs.Validate(); err != nil {
		// compat: apply default EA gouging settings
		gs.MinMaxEphemeralAccountBalance = build.DefaultGougingSettings.MinMaxEphemeralAccountBalance
		gs.MinPriceTableValidity = build.DefaultGougingSettings.MinPriceTableValidity
		gs.MinAccountExpiry = build.DefaultGougingSettings.MinAccountExpiry
		if err := gs.Validate(); err == nil {
			l.Info(fmt.Sprintf("updating gouging settings with default EA settings: %+v", gs))
			bytes, _ := json.Marshal(gs)
			if err := b.ss.UpdateSetting(ctx, api.SettingGouging, string(bytes)); err != nil {
				return nil, err
			}
		} else {
			// compat: apply default host block leeway settings
			gs.HostBlockHeightLeeway = build.DefaultGougingSettings.HostBlockHeightLeeway
			if err := gs.Validate(); err == nil {
				l.Info(fmt.Sprintf("updating gouging settings with default HostBlockHeightLeeway settings: %v", gs))
				bytes, _ := json.Marshal(gs)
				if err := b.ss.UpdateSetting(ctx, api.SettingGouging, string(bytes)); err != nil {
					return nil, err
				}
			} else {
				l.Warn(fmt.Sprintf("invalid gouging setting found '%v', overwriting the gouging settings with the default settings", gss))
				bytes, _ := json.Marshal(build.DefaultGougingSettings)
				if err := b.ss.UpdateSetting(ctx, api.SettingGouging, string(bytes)); err != nil {
					return nil, err
				}
			}
		}
	}

	// Load the accounts into memory. They're saved when the bus is stopped.
	accounts, err := eas.Accounts(ctx)
	if err != nil {
		return nil, err
	}
	b.accounts = newAccounts(accounts, b.logger)
	return b, nil
}

// Handler returns an HTTP handler that serves the bus API.
func (b *bus) Handler() http.Handler {
	return jape.Mux(tracing.TracedRoutes("bus", map[string]jape.Handler{
		"GET    /alerts":                    b.handleGETAlerts,
		"POST   /alerts/dismiss":            b.handlePOSTAlertsDismiss,
		"GET    /accounts":                  b.accountsHandlerGET,
		"POST   /accounts/:id":              b.accountHandlerGET,
		"POST   /accounts/:id/lock":         b.accountsLockHandlerPOST,
		"POST   /accounts/:id/unlock":       b.accountsUnlockHandlerPOST,
		"POST   /accounts/:id/add":          b.accountsAddHandlerPOST,
		"POST   /accounts/:id/update":       b.accountsUpdateHandlerPOST,
		"POST   /accounts/:id/requiressync": b.accountsRequiresSyncHandlerPOST,
		"POST   /accounts/:id/resetdrift":   b.accountsResetDriftHandlerPOST,

		"GET    /autopilots":     b.autopilotsListHandlerGET,
		"GET    /autopilots/:id": b.autopilotsHandlerGET,
		"PUT    /autopilots/:id": b.autopilotsHandlerPUT,

		"GET    /syncer/address": b.syncerAddrHandler,
		"GET    /syncer/peers":   b.syncerPeersHandler,
		"POST   /syncer/connect": b.syncerConnectHandler,

		"POST   /consensus/acceptblock":        b.consensusAcceptBlock,
		"GET    /consensus/state":              b.consensusStateHandler,
		"GET    /consensus/network":            b.consensusNetworkHandler,
		"GET    /consensus/siafundfee/:payout": b.contractTaxHandlerGET,

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
		"GET    /hosts/allowlist":    b.hostsAllowlistHandlerGET,
		"PUT    /hosts/allowlist":    b.hostsAllowlistHandlerPUT,
		"GET    /hosts/blocklist":    b.hostsBlocklistHandlerGET,
		"PUT    /hosts/blocklist":    b.hostsBlocklistHandlerPUT,
		"GET    /hosts/scanning":     b.hostsScanningHandlerGET,

		"GET    /contracts":              b.contractsHandlerGET,
		"DELETE /contracts/all":          b.contractsAllHandlerDELETE,
		"POST   /contracts/archive":      b.contractsArchiveHandlerPOST,
		"GET    /contracts/renewed/:id":  b.contractsRenewedIDHandlerGET,
		"GET    /contracts/sets":         b.contractsSetsHandlerGET,
		"GET    /contracts/set/:set":     b.contractsSetHandlerGET,
		"PUT    /contracts/set/:set":     b.contractsSetHandlerPUT,
		"DELETE /contracts/set/:set":     b.contractsSetHandlerDELETE,
		"POST   /contracts/spending":     b.contractsSpendingHandlerPOST,
		"GET    /contract/:id":           b.contractIDHandlerGET,
		"POST   /contract/:id":           b.contractIDHandlerPOST,
		"GET    /contract/:id/ancestors": b.contractIDAncestorsHandler,
		"POST   /contract/:id/renewed":   b.contractIDRenewedHandlerPOST,
		"POST   /contract/:id/acquire":   b.contractAcquireHandlerPOST,
		"POST   /contract/:id/keepalive": b.contractKeepaliveHandlerPOST,
		"POST   /contract/:id/release":   b.contractReleaseHandlerPOST,
		"DELETE /contract/:id":           b.contractIDHandlerDELETE,

		"POST /search/hosts":   b.searchHostsHandlerPOST,
		"GET  /search/objects": b.searchObjectsHandlerGET,

		"GET    /stats/objects": b.objectsStatshandlerGET,

		"GET    /objects/*path":  b.objectsHandlerGET,
		"PUT    /objects/*path":  b.objectsHandlerPUT,
		"DELETE /objects/*path":  b.objectsHandlerDELETE,
		"POST   /objects/rename": b.objectsRenameHandlerPOST,

		"POST   /slabs/migration":     b.slabsMigrationHandlerPOST,
		"POST   /slabs/refreshhealth": b.slabsRefreshHealthHandlerPOST,
		"GET    /slab/:key":           b.slabHandlerGET,
		"PUT    /slab":                b.slabHandlerPUT,

		"GET    /settings":     b.settingsHandlerGET,
		"GET    /setting/:key": b.settingKeyHandlerGET,
		"PUT    /setting/:key": b.settingKeyHandlerPUT,
		"DELETE /setting/:key": b.settingKeyHandlerDELETE,

		"GET    /params/upload":  b.paramsHandlerUploadGET,
		"GET    /params/gouging": b.paramsHandlerGougingGET,
	}))
}

// Shutdown shuts down the bus.
func (b *bus) Shutdown(ctx context.Context) error {
	return b.eas.SaveAccounts(ctx, b.accounts.ToPersist())
}
