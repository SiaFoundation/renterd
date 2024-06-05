package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/gofakes3"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/bus/client"
	ibus "go.sia.tech/renterd/internal/bus"
	"go.sia.tech/renterd/internal/chain"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/siad/modules"
	"go.uber.org/zap"
)

// Client re-exports the client from the client package.
type Client struct {
	*client.Client
}

// NewClient returns a new bus client.
func NewClient(addr, password string) *Client {
	return &Client{
		client.New(
			addr,
			password,
		),
	}
}

type (
	// ChainManager tracks multiple blockchains and identifies the best valid
	// chain.
	ChainManager interface {
		AddBlocks(blocks []types.Block) error
		AddPoolTransactions(txns []types.Transaction) (bool, error)
		Block(id types.BlockID) (types.Block, bool)
		PoolTransaction(txid types.TransactionID) (types.Transaction, bool)
		PoolTransactions() []types.Transaction
		RecommendedFee() types.Currency
		TipState() consensus.State
		UnconfirmedParents(txn types.Transaction) []types.Transaction
	}

	// A TransactionPool can validate and relay unconfirmed transactions.
	TransactionPool interface {
		AcceptTransactionSet(txns []types.Transaction) error
		Close() error
		RecommendedFee() types.Currency
		Subscribe(subscriber modules.TransactionPoolSubscriber)
		Transactions() []types.Transaction
		UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error)
	}

	// A HostDB stores information about hosts.
	HostDB interface {
		Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error)
		HostAllowlist(ctx context.Context) ([]types.PublicKey, error)
		HostBlocklist(ctx context.Context) ([]string, error)
		HostsForScanning(ctx context.Context, maxLastScan time.Time, offset, limit int) ([]api.HostAddress, error)
		RecordHostScans(ctx context.Context, scans []api.HostScan) error
		RecordPriceTables(ctx context.Context, priceTableUpdate []api.HostPriceTableUpdate) error
		RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (uint64, error)
		ResetLostSectors(ctx context.Context, hk types.PublicKey) error
		SearchHosts(ctx context.Context, autopilotID, filterMode, usabilityMode, addressContains string, keyIn []types.PublicKey, offset, limit int) ([]api.Host, error)
		UpdateHostAllowlistEntries(ctx context.Context, add, remove []types.PublicKey, clear bool) error
		UpdateHostBlocklistEntries(ctx context.Context, add, remove []string, clear bool) error
		UpdateHostCheck(ctx context.Context, autopilotID string, hk types.PublicKey, check api.HostCheck) error
	}

	// A MetadataStore stores information about contracts and objects.
	MetadataStore interface {
		AddContract(ctx context.Context, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, state string) (api.ContractMetadata, error)
		AddRenewedContract(ctx context.Context, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID, state string) (api.ContractMetadata, error)
		AncestorContracts(ctx context.Context, fcid types.FileContractID, minStartHeight uint64) ([]api.ArchivedContract, error)
		ArchiveContract(ctx context.Context, id types.FileContractID, reason string) error
		ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) error
		ArchiveAllContracts(ctx context.Context, reason string) error
		Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
		Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error)
		ContractSets(ctx context.Context) ([]string, error)
		RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error
		RemoveContractSet(ctx context.Context, name string) error
		RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error)
		SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) error

		ContractRoots(ctx context.Context, id types.FileContractID) ([]types.Hash256, error)
		ContractSizes(ctx context.Context) (map[types.FileContractID]api.ContractSize, error)
		ContractSize(ctx context.Context, id types.FileContractID) (api.ContractSize, error)

		DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) (int, error)

		Bucket(_ context.Context, bucketName string) (api.Bucket, error)
		CreateBucket(_ context.Context, bucketName string, policy api.BucketPolicy) error
		DeleteBucket(_ context.Context, bucketName string) error
		ListBuckets(_ context.Context) ([]api.Bucket, error)
		UpdateBucketPolicy(ctx context.Context, bucketName string, policy api.BucketPolicy) error

		CopyObject(ctx context.Context, srcBucket, dstBucket, srcPath, dstPath, mimeType string, metadata api.ObjectUserMetadata) (api.ObjectMetadata, error)
		ListObjects(ctx context.Context, bucketName, prefix, sortBy, sortDir, marker string, limit int) (api.ObjectsListResponse, error)
		Object(ctx context.Context, bucketName, path string) (api.Object, error)
		ObjectMetadata(ctx context.Context, bucketName, path string) (api.Object, error)
		ObjectEntries(ctx context.Context, bucketName, path, prefix, sortBy, sortDir, marker string, offset, limit int) ([]api.ObjectMetadata, bool, error)
		ObjectsBySlabKey(ctx context.Context, bucketName string, slabKey object.EncryptionKey) ([]api.ObjectMetadata, error)
		ObjectsStats(ctx context.Context, opts api.ObjectsStatsOpts) (api.ObjectsStatsResponse, error)
		RemoveObject(ctx context.Context, bucketName, path string) error
		RemoveObjects(ctx context.Context, bucketName, prefix string) error
		RenameObject(ctx context.Context, bucketName, from, to string, force bool) error
		RenameObjects(ctx context.Context, bucketName, from, to string, force bool) error
		SearchObjects(ctx context.Context, bucketName, substring string, offset, limit int) ([]api.ObjectMetadata, error)
		UpdateObject(ctx context.Context, bucketName, path, contractSet, ETag, mimeType string, metadata api.ObjectUserMetadata, o object.Object) error

		AbortMultipartUpload(ctx context.Context, bucketName, path string, uploadID string) (err error)
		AddMultipartPart(ctx context.Context, bucketName, path, contractSet, eTag, uploadID string, partNumber int, slices []object.SlabSlice) (err error)
		CompleteMultipartUpload(ctx context.Context, bucketName, path, uploadID string, parts []api.MultipartCompletedPart, opts api.CompleteMultipartOptions) (_ api.MultipartCompleteResponse, err error)
		CreateMultipartUpload(ctx context.Context, bucketName, path string, ec object.EncryptionKey, mimeType string, metadata api.ObjectUserMetadata) (api.MultipartCreateResponse, error)
		MultipartUpload(ctx context.Context, uploadID string) (resp api.MultipartUpload, _ error)
		MultipartUploads(ctx context.Context, bucketName, prefix, keyMarker, uploadIDMarker string, maxUploads int) (resp api.MultipartListUploadsResponse, _ error)
		MultipartUploadParts(ctx context.Context, bucketName, object string, uploadID string, marker int, limit int64) (resp api.MultipartListPartsResponse, _ error)

		MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error
		PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) ([]api.PackedSlab, error)
		SlabBuffers(ctx context.Context) ([]api.SlabBuffer, error)

		AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet string) (slabs []object.SlabSlice, bufferSize int64, err error)
		FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error)
		Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error)
		RefreshHealth(ctx context.Context) error
		UnhealthySlabs(ctx context.Context, healthCutoff float64, set string, limit int) ([]api.UnhealthySlab, error)
		UpdateSlab(ctx context.Context, s object.Slab, contractSet string) error
	}

	// An AutopilotStore stores autopilots.
	AutopilotStore interface {
		Autopilot(ctx context.Context, id string) (api.Autopilot, error)
		Autopilots(ctx context.Context) ([]api.Autopilot, error)
		UpdateAutopilot(ctx context.Context, ap api.Autopilot) error
	}

	// A ChainStore stores chain information.
	ChainStore interface {
		ChainIndex(ctx context.Context) (types.ChainIndex, error)
	}

	// A SettingStore stores settings.
	SettingStore interface {
		DeleteSetting(ctx context.Context, key string) error
		Setting(ctx context.Context, key string) (string, error)
		Settings(ctx context.Context) ([]string, error)
		UpdateSetting(ctx context.Context, key, value string) error
	}

	// EphemeralAccountStore persists information about accounts. Since accounts
	// are rapidly updated and can be recovered, they are only loaded upon
	// startup and persisted upon shutdown.
	EphemeralAccountStore interface {
		Accounts(context.Context) ([]api.Account, error)
		SaveAccounts(context.Context, []api.Account) error
		SetUncleanShutdown(context.Context) error
	}

	MetricsStore interface {
		ContractSetMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetMetricsQueryOpts) ([]api.ContractSetMetric, error)

		ContractPruneMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractPruneMetricsQueryOpts) ([]api.ContractPruneMetric, error)
		RecordContractPruneMetric(ctx context.Context, metrics ...api.ContractPruneMetric) error

		ContractMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractMetricsQueryOpts) ([]api.ContractMetric, error)
		RecordContractMetric(ctx context.Context, metrics ...api.ContractMetric) error

		PruneMetrics(ctx context.Context, metric string, cutoff time.Time) error
		ContractSetChurnMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetChurnMetricsQueryOpts) ([]api.ContractSetChurnMetric, error)
		RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error

		WalletMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.WalletMetricsQueryOpts) ([]api.WalletMetric, error)
	}

	Syncer interface {
		Addr() string
		BroadcastHeader(h gateway.BlockHeader)
		BroadcastTransactionSet([]types.Transaction)
		Connect(ctx context.Context, addr string) (*syncer.Peer, error)
		Peers() []*syncer.Peer
	}

	Wallet interface {
		Address() types.Address
		Balance() (wallet.Balance, error)
		Close() error
		FundTransaction(txn *types.Transaction, amount types.Currency, useUnconfirmed bool) ([]types.Hash256, error)
		Redistribute(outputs int, amount, feePerByte types.Currency) (txns []types.Transaction, toSign []types.Hash256, err error)
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction)
		SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)
		SpendableOutputs() ([]types.SiacoinElement, error)
		Tip() (types.ChainIndex, error)
		UnconfirmedTransactions() ([]wallet.Event, error)
		Events(offset, limit int) ([]wallet.Event, error)
	}

	WebhookManager interface {
		webhooks.Broadcaster
		Close() error
		Delete(context.Context, webhooks.Webhook) error
		Info() ([]webhooks.Webhook, []webhooks.WebhookQueueInfo)
		Register(context.Context, webhooks.Webhook) error
	}
)

type bus struct {
	startTime time.Time

	alerts   alerts.Alerter
	alertMgr *alerts.Manager
	webhooks WebhookManager
	events   ibus.EventBroadcaster

	cm ChainManager
	s  Syncer
	w  Wallet

	as    AutopilotStore
	cs    ChainStore
	eas   EphemeralAccountStore
	hdb   HostDB
	ms    MetadataStore
	ss    SettingStore
	mtrcs MetricsStore

	accounts         *accounts
	contractLocks    *contractLocks
	uploadingSectors *uploadingSectorsCache

	logger *zap.SugaredLogger
}

// Handler returns an HTTP handler that serves the bus API.
func (b *bus) Handler() http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"GET    /accounts":                 b.accountsHandlerGET,
		"POST   /account/:id":              b.accountHandlerGET,
		"POST   /account/:id/add":          b.accountsAddHandlerPOST,
		"POST   /account/:id/lock":         b.accountsLockHandlerPOST,
		"POST   /account/:id/unlock":       b.accountsUnlockHandlerPOST,
		"POST   /account/:id/update":       b.accountsUpdateHandlerPOST,
		"POST   /account/:id/requiressync": b.accountsRequiresSyncHandlerPOST,
		"POST   /account/:id/resetdrift":   b.accountsResetDriftHandlerPOST,

		"GET    /alerts":          b.handleGETAlerts,
		"POST   /alerts/dismiss":  b.handlePOSTAlertsDismiss,
		"POST   /alerts/register": b.handlePOSTAlertsRegister,

		"GET    /autopilots":    b.autopilotsListHandlerGET,
		"GET    /autopilot/:id": b.autopilotsHandlerGET,
		"PUT    /autopilot/:id": b.autopilotsHandlerPUT,

		"PUT    /autopilot/:id/host/:hostkey/check": b.autopilotHostCheckHandlerPUT,

		"GET    /buckets":             b.bucketsHandlerGET,
		"POST   /buckets":             b.bucketsHandlerPOST,
		"PUT    /bucket/:name/policy": b.bucketsHandlerPolicyPUT,
		"DELETE /bucket/:name":        b.bucketHandlerDELETE,
		"GET    /bucket/:name":        b.bucketHandlerGET,

		"POST   /consensus/acceptblock":        b.consensusAcceptBlock,
		"GET    /consensus/network":            b.consensusNetworkHandler,
		"GET    /consensus/siafundfee/:payout": b.contractTaxHandlerGET,
		"GET    /consensus/state":              b.consensusStateHandler,

		"GET    /contracts":              b.contractsHandlerGET,
		"DELETE /contracts/all":          b.contractsAllHandlerDELETE,
		"POST   /contracts/archive":      b.contractsArchiveHandlerPOST,
		"GET    /contracts/prunable":     b.contractsPrunableDataHandlerGET,
		"GET    /contracts/renewed/:id":  b.contractsRenewedIDHandlerGET,
		"GET    /contracts/sets":         b.contractsSetsHandlerGET,
		"PUT    /contracts/set/:set":     b.contractsSetHandlerPUT,
		"DELETE /contracts/set/:set":     b.contractsSetHandlerDELETE,
		"POST   /contracts/spending":     b.contractsSpendingHandlerPOST,
		"GET    /contract/:id":           b.contractIDHandlerGET,
		"POST   /contract/:id":           b.contractIDHandlerPOST,
		"DELETE /contract/:id":           b.contractIDHandlerDELETE,
		"POST   /contract/:id/acquire":   b.contractAcquireHandlerPOST,
		"GET    /contract/:id/ancestors": b.contractIDAncestorsHandler,
		"POST   /contract/:id/keepalive": b.contractKeepaliveHandlerPOST,
		"POST   /contract/:id/renewed":   b.contractIDRenewedHandlerPOST,
		"POST   /contract/:id/release":   b.contractReleaseHandlerPOST,
		"GET    /contract/:id/roots":     b.contractIDRootsHandlerGET,
		"GET    /contract/:id/size":      b.contractSizeHandlerGET,

		"GET    /hosts":                          b.hostsHandlerGETDeprecated,
		"GET    /hosts/allowlist":                b.hostsAllowlistHandlerGET,
		"PUT    /hosts/allowlist":                b.hostsAllowlistHandlerPUT,
		"GET    /hosts/blocklist":                b.hostsBlocklistHandlerGET,
		"PUT    /hosts/blocklist":                b.hostsBlocklistHandlerPUT,
		"POST   /hosts/pricetables":              b.hostsPricetableHandlerPOST,
		"POST   /hosts/remove":                   b.hostsRemoveHandlerPOST,
		"POST   /hosts/scans":                    b.hostsScanHandlerPOST,
		"GET    /hosts/scanning":                 b.hostsScanningHandlerGET,
		"GET    /host/:hostkey":                  b.hostsPubkeyHandlerGET,
		"POST   /host/:hostkey/resetlostsectors": b.hostsResetLostSectorsPOST,

		"PUT    /metric/:key": b.metricsHandlerPUT,
		"GET    /metric/:key": b.metricsHandlerGET,
		"DELETE /metric/:key": b.metricsHandlerDELETE,

		"POST   /multipart/create":      b.multipartHandlerCreatePOST,
		"POST   /multipart/abort":       b.multipartHandlerAbortPOST,
		"POST   /multipart/complete":    b.multipartHandlerCompletePOST,
		"PUT    /multipart/part":        b.multipartHandlerUploadPartPUT,
		"GET    /multipart/upload/:id":  b.multipartHandlerUploadGET,
		"POST   /multipart/listuploads": b.multipartHandlerListUploadsPOST,
		"POST   /multipart/listparts":   b.multipartHandlerListPartsPOST,

		"GET    /objects/*path":  b.objectsHandlerGET,
		"PUT    /objects/*path":  b.objectsHandlerPUT,
		"DELETE /objects/*path":  b.objectsHandlerDELETE,
		"POST   /objects/copy":   b.objectsCopyHandlerPOST,
		"POST   /objects/rename": b.objectsRenameHandlerPOST,
		"POST   /objects/list":   b.objectsListHandlerPOST,

		"GET    /params/gouging": b.paramsHandlerGougingGET,
		"GET    /params/upload":  b.paramsHandlerUploadGET,

		"GET    /slabbuffers":      b.slabbuffersHandlerGET,
		"POST   /slabbuffer/done":  b.packedSlabsHandlerDonePOST,
		"POST   /slabbuffer/fetch": b.packedSlabsHandlerFetchPOST,

		"POST   /search/hosts":   b.searchHostsHandlerPOST,
		"GET    /search/objects": b.searchObjectsHandlerGET,

		"DELETE /sectors/:hk/:root": b.sectorsHostRootHandlerDELETE,

		"GET    /settings":     b.settingsHandlerGET,
		"GET    /setting/:key": b.settingKeyHandlerGET,
		"PUT    /setting/:key": b.settingKeyHandlerPUT,
		"DELETE /setting/:key": b.settingKeyHandlerDELETE,

		"POST   /slabs/migration":     b.slabsMigrationHandlerPOST,
		"GET    /slabs/partial/:key":  b.slabsPartialHandlerGET,
		"POST   /slabs/partial":       b.slabsPartialHandlerPOST,
		"POST   /slabs/refreshhealth": b.slabsRefreshHealthHandlerPOST,
		"GET    /slab/:key":           b.slabHandlerGET,
		"GET    /slab/:key/objects":   b.slabObjectsHandlerGET,
		"PUT    /slab":                b.slabHandlerPUT,

		"GET    /state":         b.stateHandlerGET,
		"GET    /stats/objects": b.objectsStatshandlerGET,

		"GET    /syncer/address": b.syncerAddrHandler,
		"POST   /syncer/connect": b.syncerConnectHandler,
		"GET    /syncer/peers":   b.syncerPeersHandler,

		"GET    /txpool/recommendedfee": b.txpoolFeeHandler,
		"GET    /txpool/transactions":   b.txpoolTransactionsHandler,
		"POST   /txpool/broadcast":      b.txpoolBroadcastHandler,

		"POST   /upload/:id":        b.uploadTrackHandlerPOST,
		"DELETE /upload/:id":        b.uploadFinishedHandlerDELETE,
		"POST   /upload/:id/sector": b.uploadAddSectorHandlerPOST,

		"GET    /wallet":               b.walletHandler,
		"POST   /wallet/discard":       b.walletDiscardHandler,
		"POST   /wallet/fund":          b.walletFundHandler,
		"GET    /wallet/outputs":       b.walletOutputsHandler,
		"GET    /wallet/pending":       b.walletPendingHandler,
		"POST   /wallet/prepare/form":  b.walletPrepareFormHandler,
		"POST   /wallet/prepare/renew": b.walletPrepareRenewHandler,
		"POST   /wallet/redistribute":  b.walletRedistributeHandler,
		"POST   /wallet/sign":          b.walletSignHandler,
		"GET    /wallet/transactions":  b.walletTransactionsHandler,

		"GET    /webhooks":        b.webhookHandlerGet,
		"POST   /webhooks":        b.webhookHandlerPost,
		"POST   /webhooks/action": b.webhookActionHandlerPost,
		"POST   /webhook/delete":  b.webhookHandlerDelete,
	})
}

// Shutdown shuts down the bus.
func (b *bus) Shutdown(ctx context.Context) error {
	b.webhooks.Close()
	accounts := b.accounts.ToPersist()
	err := b.eas.SaveAccounts(ctx, accounts)
	if err != nil {
		b.logger.Errorf("failed to save %v accounts: %v", len(accounts), err)
	} else {
		b.logger.Infof("successfully saved %v accounts", len(accounts))
	}
	return err
}

func (b *bus) fetchSetting(ctx context.Context, key string, value interface{}) error {
	if val, err := b.ss.Setting(ctx, key); err != nil {
		return fmt.Errorf("could not get contract set settings: %w", err)
	} else if err := json.Unmarshal([]byte(val), &value); err != nil {
		b.logger.Panicf("failed to unmarshal %v settings '%s': %v", key, val, err)
	}
	return nil
}

func (b *bus) consensusAcceptBlock(jc jape.Context) {
	var block types.Block
	if jc.Decode(&block) != nil {
		return
	}

	if jc.Check("failed to accept block", b.cm.AddBlocks([]types.Block{block})) != nil {
		return
	}

	if block.V2 == nil {
		b.s.BroadcastHeader(gateway.BlockHeader{
			ParentID:   block.ParentID,
			Nonce:      block.Nonce,
			Timestamp:  block.Timestamp,
			MerkleRoot: block.MerkleRoot(),
		})
	}
}

func (b *bus) syncerAddrHandler(jc jape.Context) {
	jc.Encode(b.s.Addr())
}

func (b *bus) syncerPeersHandler(jc jape.Context) {
	var peers []string
	for _, p := range b.s.Peers() {
		peers = append(peers, p.String())
	}
	jc.Encode(peers)
}

func (b *bus) syncerConnectHandler(jc jape.Context) {
	var addr string
	if jc.Decode(&addr) == nil {
		_, err := b.s.Connect(jc.Request.Context(), addr)
		jc.Check("couldn't connect to peer", err)
	}
}

func (b *bus) consensusStateHandler(jc jape.Context) {
	cs, err := b.consensusState(jc.Request.Context())
	if jc.Check("couldn't fetch consensus state", err) != nil {
		return
	}
	jc.Encode(cs)
}

func (b *bus) consensusNetworkHandler(jc jape.Context) {
	jc.Encode(api.ConsensusNetwork{
		Name: b.cm.TipState().Network.Name,
	})
}

func (b *bus) txpoolFeeHandler(jc jape.Context) {
	jc.Encode(b.cm.RecommendedFee())
}

func (b *bus) txpoolTransactionsHandler(jc jape.Context) {
	jc.Encode(b.cm.PoolTransactions())
}

func (b *bus) txpoolBroadcastHandler(jc jape.Context) {
	var txnSet []types.Transaction
	if jc.Decode(&txnSet) != nil {
		return
	}

	_, err := b.cm.AddPoolTransactions(txnSet)
	if jc.Check("couldn't broadcast transaction set", err) != nil {
		return
	}

	b.s.BroadcastTransactionSet(txnSet)
}

func (b *bus) bucketsHandlerGET(jc jape.Context) {
	resp, err := b.ms.ListBuckets(jc.Request.Context())
	if jc.Check("couldn't list buckets", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *bus) bucketsHandlerPOST(jc jape.Context) {
	var bucket api.BucketCreateRequest
	if jc.Decode(&bucket) != nil {
		return
	} else if bucket.Name == "" {
		jc.Error(errors.New("no name provided"), http.StatusBadRequest)
		return
	} else if jc.Check("failed to create bucket", b.ms.CreateBucket(jc.Request.Context(), bucket.Name, bucket.Policy)) != nil {
		return
	}
}

func (b *bus) bucketsHandlerPolicyPUT(jc jape.Context) {
	var req api.BucketUpdatePolicyRequest
	if jc.Decode(&req) != nil {
		return
	} else if bucket := jc.PathParam("name"); bucket == "" {
		jc.Error(errors.New("no bucket name provided"), http.StatusBadRequest)
		return
	} else if jc.Check("failed to create bucket", b.ms.UpdateBucketPolicy(jc.Request.Context(), bucket, req.Policy)) != nil {
		return
	}
}

func (b *bus) bucketHandlerDELETE(jc jape.Context) {
	var name string
	if jc.DecodeParam("name", &name) != nil {
		return
	} else if name == "" {
		jc.Error(errors.New("no name provided"), http.StatusBadRequest)
		return
	} else if jc.Check("failed to delete bucket", b.ms.DeleteBucket(jc.Request.Context(), name)) != nil {
		return
	}
}

func (b *bus) bucketHandlerGET(jc jape.Context) {
	var name string
	if jc.DecodeParam("name", &name) != nil {
		return
	} else if name == "" {
		jc.Error(errors.New("parameter 'name' is required"), http.StatusBadRequest)
		return
	}
	bucket, err := b.ms.Bucket(jc.Request.Context(), name)
	if errors.Is(err, api.ErrBucketNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to fetch bucket", err) != nil {
		return
	}
	jc.Encode(bucket)
}

func (b *bus) walletHandler(jc jape.Context) {
	address := b.w.Address()
	balance, err := b.w.Balance()
	if jc.Check("couldn't fetch wallet balance", err) != nil {
		return
	}

	tip, err := b.w.Tip()
	if jc.Check("couldn't fetch wallet scan height", err) != nil {
		return
	}

	jc.Encode(api.WalletResponse{
		ScanHeight:  tip.Height,
		Address:     address,
		Confirmed:   balance.Confirmed,
		Spendable:   balance.Spendable,
		Unconfirmed: balance.Unconfirmed,
		Immature:    balance.Immature,
	})
}

func (b *bus) walletTransactionsHandler(jc jape.Context) {
	offset := 0
	limit := -1
	if jc.DecodeForm("offset", &offset) != nil ||
		jc.DecodeForm("limit", &limit) != nil {
		return
	}

	// TODO: deprecate these parameters when moving to v2.0.0
	var before, since time.Time
	if jc.DecodeForm("before", (*api.TimeRFC3339)(&before)) != nil ||
		jc.DecodeForm("since", (*api.TimeRFC3339)(&since)) != nil {
		return
	}

	// convertToTransaction converts wallet event data to a Transaction.
	convertToTransaction := func(kind string, data wallet.EventData) (txn types.Transaction, ok bool) {
		ok = true
		switch kind {
		case wallet.EventTypeV1Transaction:
			v1Txn, _ := data.(wallet.EventV1Transaction)
			txn = types.Transaction(v1Txn)
		case wallet.EventTypeFoundationSubsidy:
			subsidy, _ := data.(wallet.EventFoundationSubsidy)
			txn = types.Transaction{SiacoinOutputs: []types.SiacoinOutput{subsidy.SiacoinElement.SiacoinOutput}}
		case wallet.EventTypeV1Contract:
			payout, _ := data.(wallet.EventV1ContractPayout)
			txn = types.Transaction{
				FileContracts:  []types.FileContract{payout.FileContract.FileContract},
				SiacoinOutputs: []types.SiacoinOutput{payout.SiacoinElement.SiacoinOutput},
			}
		case wallet.EventTypeMinerPayout:
			payout, _ := data.(wallet.EventMinerPayout)
			txn = types.Transaction{SiacoinOutputs: []types.SiacoinOutput{payout.SiacoinElement.SiacoinOutput}}
		default:
			ok = false
		}
		return
	}

	// convertToTransactions converts wallet events to API transactions.
	convertToTransactions := func(events []wallet.Event) []api.Transaction {
		var transactions []api.Transaction
		for _, e := range events {
			if txn, ok := convertToTransaction(e.Type, e.Data); ok {
				transactions = append(transactions, api.Transaction{
					Raw:       txn,
					Index:     e.Index,
					ID:        types.TransactionID(e.ID),
					Inflow:    e.Inflow,
					Outflow:   e.Outflow,
					Timestamp: e.Timestamp,
				})
			}
		}
		return transactions
	}

	if before.IsZero() && since.IsZero() {
		events, err := b.w.Events(offset, limit)
		if jc.Check("couldn't load transactions", err) == nil {
			jc.Encode(convertToTransactions(events))
		}
		return
	}

	// TODO: remove this when 'before' and 'since' are deprecated, until then we
	// fetch all transactions and paginate manually if either is specified
	events, err := b.w.Events(0, -1)
	if jc.Check("couldn't load transactions", err) != nil {
		return
	}
	filtered := events[:0]
	for _, txn := range events {
		if (before.IsZero() || txn.Timestamp.Before(before)) &&
			(since.IsZero() || txn.Timestamp.After(since)) {
			filtered = append(filtered, txn)
		}
	}
	events = filtered
	if limit == 0 || limit == -1 {
		jc.Encode(convertToTransactions(events[offset:]))
	} else {
		jc.Encode(convertToTransactions(events[offset : offset+limit]))
	}
	return
}

func (b *bus) walletOutputsHandler(jc jape.Context) {
	utxos, err := b.w.SpendableOutputs()
	if jc.Check("couldn't load outputs", err) == nil {
		// convert to siacoin elements
		elements := make([]api.SiacoinElement, len(utxos))
		for i, sce := range utxos {
			elements[i] = api.SiacoinElement{
				ID: sce.StateElement.ID,
				SiacoinOutput: types.SiacoinOutput{
					Value:   sce.SiacoinOutput.Value,
					Address: sce.SiacoinOutput.Address,
				},
				MaturityHeight: sce.MaturityHeight,
			}
		}
		jc.Encode(elements)
	}
}

func (b *bus) walletFundHandler(jc jape.Context) {
	var wfr api.WalletFundRequest
	if jc.Decode(&wfr) != nil {
		return
	}
	txn := wfr.Transaction

	if len(txn.MinerFees) == 0 {
		// if no fees are specified, we add some
		fee := b.cm.RecommendedFee().Mul64(b.cm.TipState().TransactionWeight(txn))
		txn.MinerFees = []types.Currency{fee}
	}

	toSign, err := b.w.FundTransaction(&txn, wfr.Amount.Add(txn.MinerFees[0]), wfr.UseUnconfirmedTxns)
	if jc.Check("couldn't fund transaction", err) != nil {
		return
	}

	jc.Encode(api.WalletFundResponse{
		Transaction: txn,
		ToSign:      toSign,
		DependsOn:   b.cm.UnconfirmedParents(txn),
	})
}

func (b *bus) walletSignHandler(jc jape.Context) {
	var wsr api.WalletSignRequest
	if jc.Decode(&wsr) != nil {
		return
	}
	b.w.SignTransaction(&wsr.Transaction, wsr.ToSign, wsr.CoveredFields)
	jc.Encode(wsr.Transaction)
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

	txns, toSign, err := b.w.Redistribute(wfr.Outputs, wfr.Amount, b.cm.RecommendedFee())
	if jc.Check("couldn't redistribute money in the wallet into the desired outputs", err) != nil {
		return
	}

	var ids []types.TransactionID
	if len(txns) == 0 {
		jc.Encode(ids)
		return
	}

	for i := 0; i < len(txns); i++ {
		b.w.SignTransaction(&txns[i], toSign, types.CoveredFields{WholeTransaction: true})
		ids = append(ids, txns[i].ID())
	}

	_, err = b.cm.AddPoolTransactions(txns)
	if jc.Check("couldn't broadcast the transaction", err) != nil {
		b.w.ReleaseInputs(txns, nil)
		return
	}

	jc.Encode(ids)
}

func (b *bus) walletDiscardHandler(jc jape.Context) {
	var txn types.Transaction
	if jc.Decode(&txn) == nil {
		b.w.ReleaseInputs([]types.Transaction{txn}, nil)
	}
}

func (b *bus) walletPrepareFormHandler(jc jape.Context) {
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
	cs := b.cm.TipState()

	fc := rhpv2.PrepareContractFormation(wpfr.RenterKey, wpfr.HostKey, wpfr.RenterFunds, wpfr.HostCollateral, wpfr.EndHeight, wpfr.HostSettings, wpfr.RenterAddress)
	cost := rhpv2.ContractFormationCost(cs, fc, wpfr.HostSettings.ContractPrice)
	txn := types.Transaction{
		FileContracts: []types.FileContract{fc},
	}
	txn.MinerFees = []types.Currency{b.cm.RecommendedFee().Mul64(cs.TransactionWeight(txn))}
	toSign, err := b.w.FundTransaction(&txn, cost.Add(txn.MinerFees[0]), true)
	if jc.Check("couldn't fund transaction", err) != nil {
		return
	}

	b.w.SignTransaction(&txn, toSign, wallet.ExplicitCoveredFields(txn))

	jc.Encode(append(b.cm.UnconfirmedParents(txn), txn))
}

func (b *bus) walletPrepareRenewHandler(jc jape.Context) {
	var wprr api.WalletPrepareRenewRequest
	if jc.Decode(&wprr) != nil {
		return
	}
	if wprr.RenterKey == nil {
		jc.Error(errors.New("no renter key provided"), http.StatusBadRequest)
		return
	}
	cs := b.cm.TipState()

	// Create the final revision from the provided revision.
	finalRevision := wprr.Revision
	finalRevision.MissedProofOutputs = finalRevision.ValidProofOutputs
	finalRevision.Filesize = 0
	finalRevision.FileMerkleRoot = types.Hash256{}
	finalRevision.RevisionNumber = math.MaxUint64

	// Prepare the new contract.
	fc, basePrice, err := rhpv3.PrepareContractRenewal(wprr.Revision, wprr.HostAddress, wprr.RenterAddress, wprr.RenterFunds, wprr.MinNewCollateral, wprr.PriceTable, wprr.ExpectedNewStorage, wprr.EndHeight)
	if jc.Check("couldn't prepare contract renewal", err) != nil {
		return
	}

	// Create the transaction containing both the final revision and new
	// contract.
	txn := types.Transaction{
		FileContracts:         []types.FileContract{fc},
		FileContractRevisions: []types.FileContractRevision{finalRevision},
		MinerFees:             []types.Currency{wprr.PriceTable.TxnFeeMaxRecommended.Mul64(4096)},
	}

	// Compute how much renter funds to put into the new contract.
	cost := rhpv3.ContractRenewalCost(cs, wprr.PriceTable, fc, txn.MinerFees[0], basePrice)

	// Make sure we don't exceed the max fund amount.
	// TODO: remove the IsZero check for the v2 change
	if /*!wprr.MaxFundAmount.IsZero() &&*/ wprr.MaxFundAmount.Cmp(cost) < 0 {
		jc.Error(fmt.Errorf("%w: %v > %v", api.ErrMaxFundAmountExceeded, cost, wprr.MaxFundAmount), http.StatusBadRequest)
		return
	}

	// Fund the txn. We are not signing it yet since it's not complete. The host
	// still needs to complete it and the revision + contract are signed with
	// the renter key by the worker.
	toSign, err := b.w.FundTransaction(&txn, cost, true)
	if jc.Check("couldn't fund transaction", err) != nil {
		return
	}

	jc.Encode(api.WalletPrepareRenewResponse{
		FundAmount:     cost,
		ToSign:         toSign,
		TransactionSet: append(b.cm.UnconfirmedParents(txn), txn),
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

	txns := b.cm.PoolTransactions()
	relevant := txns[:0]
	for _, txn := range txns {
		if isRelevant(txn) {
			relevant = append(relevant, txn)
		}
	}
	jc.Encode(relevant)
}

func (b *bus) hostsHandlerGETDeprecated(jc jape.Context) {
	offset := 0
	limit := -1
	if jc.DecodeForm("offset", &offset) != nil || jc.DecodeForm("limit", &limit) != nil {
		return
	}

	// fetch hosts
	hosts, err := b.hdb.SearchHosts(jc.Request.Context(), "", api.HostFilterModeAllowed, api.UsabilityFilterModeAll, "", nil, offset, limit)
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

	// TODO: on the next major release:
	// - properly default search params (currently no defaults are set)
	// - properly validate and return 400 (currently validation is done in autopilot and the store)

	hosts, err := b.hdb.SearchHosts(jc.Request.Context(), req.AutopilotID, req.FilterMode, req.UsabilityMode, req.AddressContains, req.KeyIn, req.Offset, req.Limit)
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
	if hrr.MinRecentScanFailures == 0 {
		jc.Error(errors.New("minRecentScanFailures must be non-zero"), http.StatusBadRequest)
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
	if jc.DecodeForm("offset", &offset) != nil || jc.DecodeForm("limit", &limit) != nil || jc.DecodeForm("lastScan", (*api.TimeRFC3339)(&maxLastScan)) != nil {
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

func (b *bus) hostsResetLostSectorsPOST(jc jape.Context) {
	var hostKey types.PublicKey
	if jc.DecodeParam("hostkey", &hostKey) != nil {
		return
	}
	err := b.hdb.ResetLostSectors(jc.Request.Context(), hostKey)
	if jc.Check("couldn't reset lost sectors", err) != nil {
		return
	}
}

func (b *bus) hostsScanHandlerPOST(jc jape.Context) {
	var req api.HostsScanRequest
	if jc.Decode(&req) != nil {
		return
	}
	if jc.Check("failed to record scans", b.hdb.RecordHostScans(jc.Request.Context(), req.Scans)) != nil {
		return
	}
}

func (b *bus) hostsPricetableHandlerPOST(jc jape.Context) {
	var req api.HostsPriceTablesRequest
	if jc.Decode(&req) != nil {
		return
	}
	if jc.Check("failed to record interactions", b.hdb.RecordPriceTables(jc.Request.Context(), req.PriceTableUpdates)) != nil {
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
	var cs string
	if jc.DecodeForm("contractset", &cs) != nil {
		return
	}
	contracts, err := b.ms.Contracts(jc.Request.Context(), api.ContractsOpts{
		ContractSet: cs,
	})
	if jc.Check("couldn't load contracts", err) == nil {
		jc.Encode(contracts)
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
	var toArchive api.ContractsArchiveRequest
	if jc.Decode(&toArchive) != nil {
		return
	}

	if jc.Check("failed to archive contracts", b.ms.ArchiveContracts(jc.Request.Context(), toArchive)) == nil {
		for fcid, reason := range toArchive {
			b.events.BroadcastEvent(api.EventContractArchive{
				ContractID: fcid,
				Reason:     reason,
				Timestamp:  time.Now().UTC(),
			})
		}
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
		jc.Error(errors.New("path parameter 'set' can not be empty"), http.StatusBadRequest)
		return
	} else if jc.Decode(&contractIds) != nil {
		return
	} else if jc.Check("could not add contracts to set", b.ms.SetContractSet(jc.Request.Context(), set, contractIds)) != nil {
		return
	} else {
		b.events.BroadcastEvent(api.EventContractSetUpdate{
			Name:        set,
			ContractIDs: contractIds,
			Timestamp:   time.Now().UTC(),
		})
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

func (b *bus) contractsPrunableDataHandlerGET(jc jape.Context) {
	sizes, err := b.ms.ContractSizes(jc.Request.Context())
	if jc.Check("failed to fetch contract sizes", err) != nil {
		return
	}

	// prepare the response
	var contracts []api.ContractPrunableData
	var totalPrunable, totalSize uint64

	// build the response
	for fcid, size := range sizes {
		// adjust the amount of prunable data with the pending uploads, due to
		// how we record contract spending a contract's size might already
		// include pending sectors
		pending := b.uploadingSectors.Pending(fcid)
		if pending > size.Prunable {
			size.Prunable = 0
		} else {
			size.Prunable -= pending
		}

		contracts = append(contracts, api.ContractPrunableData{
			ID:           fcid,
			ContractSize: size,
		})
		totalPrunable += size.Prunable
		totalSize += size.Size
	}

	// sort contracts by the amount of prunable data
	sort.Slice(contracts, func(i, j int) bool {
		if contracts[i].Prunable == contracts[j].Prunable {
			return contracts[i].Size > contracts[j].Size
		}
		return contracts[i].Prunable > contracts[j].Prunable
	})

	jc.Encode(api.ContractsPrunableDataResponse{
		Contracts:     contracts,
		TotalPrunable: totalPrunable,
		TotalSize:     totalSize,
	})
}

func (b *bus) contractSizeHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	size, err := b.ms.ContractSize(jc.Request.Context(), id)
	if errors.Is(err, api.ErrContractNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to fetch contract size", err) != nil {
		return
	}

	// adjust the amount of prunable data with the pending uploads, due to how
	// we record contract spending a contract's size might already include
	// pending sectors
	pending := b.uploadingSectors.Pending(id)
	if pending > size.Prunable {
		size.Prunable = 0
	} else {
		size.Prunable -= pending
	}

	jc.Encode(size)
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
	var req api.ContractAddRequest
	if jc.DecodeParam("id", &id) != nil || jc.Decode(&req) != nil {
		return
	}
	if req.Contract.ID() != id {
		http.Error(jc.ResponseWriter, "contract ID mismatch", http.StatusBadRequest)
		return
	}
	if req.TotalCost.IsZero() {
		http.Error(jc.ResponseWriter, "TotalCost can not be zero", http.StatusBadRequest)
		return
	}

	a, err := b.ms.AddContract(jc.Request.Context(), req.Contract, req.ContractPrice, req.TotalCost, req.StartHeight, req.State)
	if jc.Check("couldn't store contract", err) == nil {
		jc.Encode(a)
	}
}

func (b *bus) contractIDRenewedHandlerPOST(jc jape.Context) {
	var id types.FileContractID
	var req api.ContractRenewedRequest
	if jc.DecodeParam("id", &id) != nil || jc.Decode(&req) != nil {
		return
	}
	if req.Contract.ID() != id {
		http.Error(jc.ResponseWriter, "contract ID mismatch", http.StatusBadRequest)
		return
	}
	if req.TotalCost.IsZero() {
		http.Error(jc.ResponseWriter, "TotalCost can not be zero", http.StatusBadRequest)
		return
	}
	if req.State == "" {
		req.State = api.ContractStatePending
	}
	r, err := b.ms.AddRenewedContract(jc.Request.Context(), req.Contract, req.ContractPrice, req.TotalCost, req.StartHeight, req.RenewedFrom, req.State)
	if jc.Check("couldn't store contract", err) != nil {
		return
	}

	b.uploadingSectors.HandleRenewal(req.Contract.ID(), req.RenewedFrom)
	b.events.BroadcastEvent(api.EventContractRenew{
		ContractID:    req.Contract.ID(),
		RenewedFromID: req.RenewedFrom,
		Timestamp:     time.Now().UTC(),
	})

	jc.Encode(r)
}

func (b *bus) contractIDRootsHandlerGET(jc jape.Context) {
	var id types.FileContractID
	if jc.DecodeParam("id", &id) != nil {
		return
	}

	roots, err := b.ms.ContractRoots(jc.Request.Context(), id)
	if jc.Check("couldn't fetch contract sectors", err) == nil {
		jc.Encode(api.ContractRootsResponse{
			Roots:     roots,
			Uploading: b.uploadingSectors.Sectors(id),
		})
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
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	keys, err := b.ms.SearchObjects(jc.Request.Context(), bucket, key, offset, limit)
	if jc.Check("couldn't list objects", err) != nil {
		return
	}
	jc.Encode(keys)
}

func (b *bus) objectsHandlerGET(jc jape.Context) {
	var ignoreDelim bool
	if jc.DecodeForm("ignoreDelim", &ignoreDelim) != nil {
		return
	}
	path := jc.PathParam("path")
	if strings.HasSuffix(path, "/") && !ignoreDelim {
		b.objectEntriesHandlerGET(jc, path)
		return
	}
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	var onlymetadata bool
	if jc.DecodeForm("onlymetadata", &onlymetadata) != nil {
		return
	}

	var o api.Object
	var err error
	if onlymetadata {
		o, err = b.ms.ObjectMetadata(jc.Request.Context(), bucket, path)
	} else {
		o, err = b.ms.Object(jc.Request.Context(), bucket, path)
	}
	if errors.Is(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("couldn't load object", err) != nil {
		return
	}
	jc.Encode(api.ObjectsResponse{Object: &o})
}

func (b *bus) objectEntriesHandlerGET(jc jape.Context, path string) {
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}

	var prefix string
	if jc.DecodeForm("prefix", &prefix) != nil {
		return
	}

	var sortBy string
	if jc.DecodeForm("sortBy", &sortBy) != nil {
		return
	}

	var sortDir string
	if jc.DecodeForm("sortDir", &sortDir) != nil {
		return
	}

	var marker string
	if jc.DecodeForm("marker", &marker) != nil {
		return
	}

	var offset int
	if jc.DecodeForm("offset", &offset) != nil {
		return
	}
	limit := -1
	if jc.DecodeForm("limit", &limit) != nil {
		return
	}

	// look for object entries
	entries, hasMore, err := b.ms.ObjectEntries(jc.Request.Context(), bucket, path, prefix, sortBy, sortDir, marker, offset, limit)
	if jc.Check("couldn't list object entries", err) != nil {
		return
	}

	jc.Encode(api.ObjectsResponse{Entries: entries, HasMore: hasMore})
}

func (b *bus) objectsHandlerPUT(jc jape.Context) {
	var aor api.AddObjectRequest
	if jc.Decode(&aor) != nil {
		return
	} else if aor.Bucket == "" {
		aor.Bucket = api.DefaultBucketName
	}
	jc.Check("couldn't store object", b.ms.UpdateObject(jc.Request.Context(), aor.Bucket, jc.PathParam("path"), aor.ContractSet, aor.ETag, aor.MimeType, aor.Metadata, aor.Object))
}

func (b *bus) objectsCopyHandlerPOST(jc jape.Context) {
	var orr api.CopyObjectsRequest
	if jc.Decode(&orr) != nil {
		return
	}
	om, err := b.ms.CopyObject(jc.Request.Context(), orr.SourceBucket, orr.DestinationBucket, orr.SourcePath, orr.DestinationPath, orr.MimeType, orr.Metadata)
	if jc.Check("couldn't copy object", err) != nil {
		return
	}

	jc.ResponseWriter.Header().Set("Last-Modified", om.ModTime.Std().Format(http.TimeFormat))
	jc.ResponseWriter.Header().Set("ETag", api.FormatETag(om.ETag))
	jc.Encode(om)
}

func (b *bus) objectsListHandlerPOST(jc jape.Context) {
	var req api.ObjectsListRequest
	if jc.Decode(&req) != nil {
		return
	}
	if req.Bucket == "" {
		req.Bucket = api.DefaultBucketName
	}
	resp, err := b.ms.ListObjects(jc.Request.Context(), req.Bucket, req.Prefix, req.SortBy, req.SortDir, req.Marker, req.Limit)
	if jc.Check("couldn't list objects", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *bus) objectsRenameHandlerPOST(jc jape.Context) {
	var orr api.ObjectsRenameRequest
	if jc.Decode(&orr) != nil {
		return
	} else if orr.Bucket == "" {
		orr.Bucket = api.DefaultBucketName
	}
	if orr.Mode == api.ObjectsRenameModeSingle {
		// Single object rename.
		if strings.HasSuffix(orr.From, "/") || strings.HasSuffix(orr.To, "/") {
			jc.Error(fmt.Errorf("can't rename dirs with mode %v", orr.Mode), http.StatusBadRequest)
			return
		}
		jc.Check("couldn't rename object", b.ms.RenameObject(jc.Request.Context(), orr.Bucket, orr.From, orr.To, orr.Force))
		return
	} else if orr.Mode == api.ObjectsRenameModeMulti {
		// Multi object rename.
		if !strings.HasSuffix(orr.From, "/") || !strings.HasSuffix(orr.To, "/") {
			jc.Error(fmt.Errorf("can't rename file with mode %v", orr.Mode), http.StatusBadRequest)
			return
		}
		jc.Check("couldn't rename objects", b.ms.RenameObjects(jc.Request.Context(), orr.Bucket, orr.From, orr.To, orr.Force))
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
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	var err error
	if batch {
		err = b.ms.RemoveObjects(jc.Request.Context(), bucket, jc.PathParam("path"))
	} else {
		err = b.ms.RemoveObject(jc.Request.Context(), bucket, jc.PathParam("path"))
	}
	if errors.Is(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	jc.Check("couldn't delete object", err)
}

func (b *bus) slabbuffersHandlerGET(jc jape.Context) {
	buffers, err := b.ms.SlabBuffers(jc.Request.Context())
	if jc.Check("couldn't get slab buffers info", err) != nil {
		return
	}
	jc.Encode(buffers)
}

func (b *bus) objectsStatshandlerGET(jc jape.Context) {
	opts := api.ObjectsStatsOpts{}
	if jc.DecodeForm("bucket", &opts.Bucket) != nil {
		return
	}
	info, err := b.ms.ObjectsStats(jc.Request.Context(), opts)
	if jc.Check("couldn't get objects stats", err) != nil {
		return
	}
	jc.Encode(info)
}

func (b *bus) packedSlabsHandlerFetchPOST(jc jape.Context) {
	var psrg api.PackedSlabsRequestGET
	if jc.Decode(&psrg) != nil {
		return
	}
	if psrg.MinShards == 0 || psrg.TotalShards == 0 {
		jc.Error(fmt.Errorf("min_shards and total_shards must be non-zero"), http.StatusBadRequest)
		return
	}
	if psrg.LockingDuration == 0 {
		jc.Error(fmt.Errorf("locking_duration must be non-zero"), http.StatusBadRequest)
		return
	}
	if psrg.ContractSet == "" {
		jc.Error(fmt.Errorf("contract_set must be non-empty"), http.StatusBadRequest)
		return
	}
	slabs, err := b.ms.PackedSlabsForUpload(jc.Request.Context(), time.Duration(psrg.LockingDuration), psrg.MinShards, psrg.TotalShards, psrg.ContractSet, psrg.Limit)
	if jc.Check("couldn't get packed slabs", err) != nil {
		return
	}
	jc.Encode(slabs)
}

func (b *bus) packedSlabsHandlerDonePOST(jc jape.Context) {
	var psrp api.PackedSlabsRequestPOST
	if jc.Decode(&psrp) != nil {
		return
	}
	jc.Check("failed to mark packed slab(s) as uploaded", b.ms.MarkPackedSlabsUploaded(jc.Request.Context(), psrp.Slabs))
}

func (b *bus) sectorsHostRootHandlerDELETE(jc jape.Context) {
	var hk types.PublicKey
	var root types.Hash256
	if jc.DecodeParam("hk", &hk) != nil {
		return
	} else if jc.DecodeParam("root", &root) != nil {
		return
	}
	n, err := b.ms.DeleteHostSector(jc.Request.Context(), hk, root)
	if jc.Check("failed to mark sector as lost", err) != nil {
		return
	} else if n > 0 {
		b.logger.Infow("successfully marked sector as lost", "hk", hk, "root", root)
	}
}

func (b *bus) slabObjectsHandlerGET(jc jape.Context) {
	var key object.EncryptionKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	objects, err := b.ms.ObjectsBySlabKey(jc.Request.Context(), bucket, key)
	if jc.Check("failed to retrieve objects by slab", err) != nil {
		return
	}
	jc.Encode(objects)
}

func (b *bus) slabHandlerGET(jc jape.Context) {
	var key object.EncryptionKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	slab, err := b.ms.Slab(jc.Request.Context(), key)
	if errors.Is(err, api.ErrSlabNotFound) {
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
		jc.Check("couldn't update slab", b.ms.UpdateSlab(jc.Request.Context(), usr.Slab, usr.ContractSet))
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

func (b *bus) slabsPartialHandlerGET(jc jape.Context) {
	jc.Custom(nil, []byte{})

	var key object.EncryptionKey
	if jc.DecodeParam("key", &key) != nil {
		return
	}
	var offset int
	if jc.DecodeForm("offset", &offset) != nil {
		return
	}
	var length int
	if jc.DecodeForm("length", &length) != nil {
		return
	}
	if length <= 0 || offset < 0 {
		jc.Error(fmt.Errorf("length must be positive and offset must be non-negative"), http.StatusBadRequest)
		return
	}
	data, err := b.ms.FetchPartialSlab(jc.Request.Context(), key, uint32(offset), uint32(length))
	if errors.Is(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.ResponseWriter.Write(data)
}

func (b *bus) slabsPartialHandlerPOST(jc jape.Context) {
	var minShards int
	if jc.DecodeForm("minShards", &minShards) != nil {
		return
	}
	var totalShards int
	if jc.DecodeForm("totalShards", &totalShards) != nil {
		return
	}
	var contractSet string
	if jc.DecodeForm("contractSet", &contractSet) != nil {
		return
	}
	if minShards <= 0 || totalShards <= minShards {
		jc.Error(errors.New("minShards must be positive and totalShards must be greater than minShards"), http.StatusBadRequest)
		return
	}
	if totalShards > math.MaxUint8 {
		jc.Error(fmt.Errorf("totalShards must be less than or equal to %d", math.MaxUint8), http.StatusBadRequest)
		return
	}
	if contractSet == "" {
		jc.Error(errors.New("parameter 'contractSet' is required"), http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(jc.Request.Body)
	if jc.Check("failed to read request body", err) != nil {
		return
	}
	slabs, bufferSize, err := b.ms.AddPartialSlab(jc.Request.Context(), data, uint8(minShards), uint8(totalShards), contractSet)
	if jc.Check("failed to add partial slab", err) != nil {
		return
	}
	var pus api.UploadPackingSettings
	if err := b.fetchSetting(jc.Request.Context(), api.SettingUploadPacking, &pus); err != nil && !errors.Is(err, api.ErrSettingNotFound) {
		jc.Error(fmt.Errorf("could not get upload packing settings: %w", err), http.StatusInternalServerError)
		return
	}
	jc.Encode(api.AddPartialSlabResponse{
		Slabs:                        slabs,
		SlabBufferMaxSizeSoftReached: bufferSize >= pus.SlabBufferMaxSizeSoft,
	})
}

func (b *bus) settingsHandlerGET(jc jape.Context) {
	if settings, err := b.ss.Settings(jc.Request.Context()); jc.Check("couldn't load settings", err) == nil {
		jc.Encode(settings)
	}
}

func (b *bus) settingKeyHandlerGET(jc jape.Context) {
	key := jc.PathParam("key")
	if key == "" {
		jc.Error(errors.New("path parameter 'key' can not be empty"), http.StatusBadRequest)
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
		jc.Error(errors.New("path parameter 'key' can not be empty"), http.StatusBadRequest)
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
	case api.SettingS3Authentication:
		var s3as api.S3AuthenticationSettings
		if err := json.Unmarshal(data, &s3as); err != nil {
			jc.Error(fmt.Errorf("couldn't update s3 authentication settings, invalid request body"), http.StatusBadRequest)
			return
		} else if err := s3as.Validate(); err != nil {
			jc.Error(fmt.Errorf("couldn't update s3 authentication settings, error: %v", err), http.StatusBadRequest)
			return
		}
	}

	if jc.Check("could not update setting", b.ss.UpdateSetting(jc.Request.Context(), key, string(data))) == nil {
		b.events.BroadcastEvent(api.EventSettingUpdate{
			Key:       key,
			Update:    value,
			Timestamp: time.Now().UTC(),
		})
	}
}

func (b *bus) settingKeyHandlerDELETE(jc jape.Context) {
	key := jc.PathParam("key")
	if key == "" {
		jc.Error(errors.New("path parameter 'key' can not be empty"), http.StatusBadRequest)
		return
	}

	if jc.Check("could not delete setting", b.ss.DeleteSetting(jc.Request.Context(), key)) == nil {
		b.events.BroadcastEvent(api.EventSettingDelete{
			Key:       key,
			Timestamp: time.Now().UTC(),
		})
	}
}

func (b *bus) contractIDAncestorsHandler(jc jape.Context) {
	var fcid types.FileContractID
	if jc.DecodeParam("id", &fcid) != nil {
		return
	}
	var minStartHeight uint64
	if jc.DecodeForm("minStartHeight", &minStartHeight) != nil {
		return
	}
	ancestors, err := b.ms.AncestorContracts(jc.Request.Context(), fcid, uint64(minStartHeight))
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

	var contractSet string
	var css api.ContractSetSetting
	if err := b.fetchSetting(jc.Request.Context(), api.SettingContractSet, &css); err != nil && !errors.Is(err, api.ErrSettingNotFound) {
		jc.Error(fmt.Errorf("could not get contract set settings: %w", err), http.StatusInternalServerError)
		return
	} else if err == nil {
		contractSet = css.Default
	}

	var uploadPacking bool
	var pus api.UploadPackingSettings
	if err := b.fetchSetting(jc.Request.Context(), api.SettingUploadPacking, &pus); err != nil && !errors.Is(err, api.ErrSettingNotFound) {
		jc.Error(fmt.Errorf("could not get upload packing settings: %w", err), http.StatusInternalServerError)
		return
	} else if err == nil {
		uploadPacking = pus.Enabled
	}

	jc.Encode(api.UploadParams{
		ContractSet:   contractSet,
		CurrentHeight: b.cm.TipState().Index.Height,
		GougingParams: gp,
		UploadPacking: uploadPacking,
	})
}

func (b *bus) consensusState(ctx context.Context) (api.ConsensusState, error) {
	index, err := b.cs.ChainIndex(ctx)
	if err != nil {
		return api.ConsensusState{}, err
	}

	var synced bool
	block, found := b.cm.Block(index.ID)
	if found {
		synced = chain.IsSynced(block)
	}

	return api.ConsensusState{
		BlockHeight:   index.Height,
		LastBlockTime: api.TimeRFC3339(block.Timestamp),
		Synced:        synced,
	}, nil
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

	cs, err := b.consensusState(ctx)
	if err != nil {
		return api.GougingParams{}, err
	}

	return api.GougingParams{
		ConsensusState:     cs,
		GougingSettings:    gs,
		RedundancySettings: rs,
		TransactionFee:     b.cm.RecommendedFee(),
	}, nil
}

func (b *bus) handleGETAlertsDeprecated(jc jape.Context) {
	ar, err := b.alertMgr.Alerts(jc.Request.Context(), alerts.AlertsOpts{Offset: 0, Limit: -1})
	if jc.Check("failed to fetch alerts", err) != nil {
		return
	}
	jc.Encode(ar.Alerts)
}

func (b *bus) handleGETAlerts(jc jape.Context) {
	if jc.Request.FormValue("offset") == "" && jc.Request.FormValue("limit") == "" {
		b.handleGETAlertsDeprecated(jc)
		return
	}
	offset, limit := 0, -1
	var severity alerts.Severity
	if jc.DecodeForm("offset", &offset) != nil {
		return
	} else if jc.DecodeForm("limit", &limit) != nil {
		return
	} else if offset < 0 {
		jc.Error(errors.New("offset must be non-negative"), http.StatusBadRequest)
		return
	} else if jc.DecodeForm("severity", &severity) != nil {
		return
	}
	ar, err := b.alertMgr.Alerts(jc.Request.Context(), alerts.AlertsOpts{
		Offset:   offset,
		Limit:    limit,
		Severity: severity,
	})
	if jc.Check("failed to fetch alerts", err) != nil {
		return
	}
	jc.Encode(ar)
}

func (b *bus) handlePOSTAlertsDismiss(jc jape.Context) {
	var ids []types.Hash256
	if jc.Decode(&ids) != nil {
		return
	}
	jc.Check("failed to dismiss alerts", b.alertMgr.DismissAlerts(jc.Request.Context(), ids...))
}

func (b *bus) handlePOSTAlertsRegister(jc jape.Context) {
	var alert alerts.Alert
	if jc.Decode(&alert) != nil {
		return
	}
	jc.Check("failed to register alert", b.alertMgr.RegisterAlert(jc.Request.Context(), alert))
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

func (b *bus) autopilotHostCheckHandlerPUT(jc jape.Context) {
	var id string
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var hk types.PublicKey
	if jc.DecodeParam("hostkey", &hk) != nil {
		return
	}
	var hc api.HostCheck
	if jc.Check("failed to decode host check", jc.Decode(&hc)) != nil {
		return
	}

	err := b.hdb.UpdateHostCheck(jc.Request.Context(), id, hk, hc)
	if errors.Is(err, api.ErrAutopilotNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if jc.Check("failed to update host", err) != nil {
		return
	}
}

func (b *bus) contractTaxHandlerGET(jc jape.Context) {
	var payout types.Currency
	if jc.DecodeParam("payout", (*api.ParamCurrency)(&payout)) != nil {
		return
	}
	cs := b.cm.TipState()
	jc.Encode(cs.FileContractTax(types.FileContract{Payout: payout}))
}

func (b *bus) stateHandlerGET(jc jape.Context) {
	jc.Encode(api.BusStateResponse{
		StartTime: api.TimeRFC3339(b.startTime),
		BuildState: api.BuildState{
			Network:   build.NetworkName(),
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: api.TimeRFC3339(build.BuildTime()),
		},
	})
}

func (b *bus) uploadTrackHandlerPOST(jc jape.Context) {
	var id api.UploadID
	if jc.DecodeParam("id", &id) == nil {
		jc.Check("failed to track upload", b.uploadingSectors.StartUpload(id))
	}
}

func (b *bus) uploadAddSectorHandlerPOST(jc jape.Context) {
	var id api.UploadID
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	var req api.UploadSectorRequest
	if jc.Decode(&req) != nil {
		return
	}
	jc.Check("failed to add sector", b.uploadingSectors.AddSector(id, req.ContractID, req.Root))
}

func (b *bus) uploadFinishedHandlerDELETE(jc jape.Context) {
	var id api.UploadID
	if jc.DecodeParam("id", &id) == nil {
		b.uploadingSectors.FinishUpload(id)
	}
}

func (b *bus) webhookActionHandlerPost(jc jape.Context) {
	var action webhooks.Event
	if jc.Check("failed to decode action", jc.Decode(&action)) != nil {
		return
	}
	b.webhooks.BroadcastAction(jc.Request.Context(), action)
}

func (b *bus) webhookHandlerDelete(jc jape.Context) {
	var wh webhooks.Webhook
	if jc.Decode(&wh) != nil {
		return
	}
	err := b.webhooks.Delete(jc.Request.Context(), wh)
	if errors.Is(err, webhooks.ErrWebhookNotFound) {
		jc.Error(fmt.Errorf("webhook for URL %v and event %v.%v not found", wh.URL, wh.Module, wh.Event), http.StatusNotFound)
		return
	} else if jc.Check("failed to delete webhook", err) != nil {
		return
	}
}

func (b *bus) webhookHandlerGet(jc jape.Context) {
	webhooks, queueInfos := b.webhooks.Info()
	jc.Encode(api.WebHookResponse{
		Queues:   queueInfos,
		Webhooks: webhooks,
	})
}

func (b *bus) webhookHandlerPost(jc jape.Context) {
	var req webhooks.Webhook
	if jc.Decode(&req) != nil {
		return
	}
	err := b.webhooks.Register(jc.Request.Context(), webhooks.Webhook{
		Event:  req.Event,
		Module: req.Module,
		URL:    req.URL,
	})
	if err != nil {
		jc.Error(fmt.Errorf("failed to add Webhook: %w", err), http.StatusInternalServerError)
		return
	}
}

func (b *bus) metricsHandlerDELETE(jc jape.Context) {
	metric := jc.PathParam("key")
	if metric == "" {
		jc.Error(errors.New("parameter 'metric' is required"), http.StatusBadRequest)
		return
	}

	var cutoff time.Time
	if jc.DecodeForm("cutoff", (*api.TimeRFC3339)(&cutoff)) != nil {
		return
	} else if cutoff.IsZero() {
		jc.Error(errors.New("parameter 'cutoff' is required"), http.StatusBadRequest)
		return
	}

	err := b.mtrcs.PruneMetrics(jc.Request.Context(), metric, cutoff)
	if jc.Check("failed to prune metrics", err) != nil {
		return
	}
}

func (b *bus) metricsHandlerPUT(jc jape.Context) {
	jc.Custom((*interface{})(nil), nil)

	key := jc.PathParam("key")
	switch key {
	case api.MetricContractPrune:
		// TODO: jape hack - remove once jape can handle decoding multiple different request types
		var req api.ContractPruneMetricRequestPUT
		if err := json.NewDecoder(jc.Request.Body).Decode(&req); err != nil {
			jc.Error(fmt.Errorf("couldn't decode request type (%T): %w", req, err), http.StatusBadRequest)
			return
		} else if jc.Check("failed to record contract prune metric", b.mtrcs.RecordContractPruneMetric(jc.Request.Context(), req.Metrics...)) != nil {
			return
		}
	case api.MetricContractSetChurn:
		// TODO: jape hack - remove once jape can handle decoding multiple different request types
		var req api.ContractSetChurnMetricRequestPUT
		if err := json.NewDecoder(jc.Request.Body).Decode(&req); err != nil {
			jc.Error(fmt.Errorf("couldn't decode request type (%T): %w", req, err), http.StatusBadRequest)
			return
		} else if jc.Check("failed to record contract churn metric", b.mtrcs.RecordContractSetChurnMetric(jc.Request.Context(), req.Metrics...)) != nil {
			return
		}
	default:
		jc.Error(fmt.Errorf("unknown metric key '%s'", key), http.StatusBadRequest)
		return
	}
}

func (b *bus) metricsHandlerGET(jc jape.Context) {
	// parse mandatory query parameters
	var start time.Time
	if jc.DecodeForm("start", (*api.TimeRFC3339)(&start)) != nil {
		return
	} else if start.IsZero() {
		jc.Error(errors.New("parameter 'start' is required"), http.StatusBadRequest)
		return
	}

	var n uint64
	if jc.DecodeForm("n", &n) != nil {
		return
	} else if n == 0 {
		if jc.Request.FormValue("n") == "" {
			jc.Error(errors.New("parameter 'n' is required"), http.StatusBadRequest)
		} else {
			jc.Error(errors.New("'n' has to be greater than zero"), http.StatusBadRequest)
		}
		return
	}

	var interval time.Duration
	if jc.DecodeForm("interval", (*api.DurationMS)(&interval)) != nil {
		return
	} else if interval == 0 {
		jc.Error(errors.New("parameter 'interval' is required"), http.StatusBadRequest)
		return
	}

	// parse optional query parameters
	var metrics interface{}
	var err error
	key := jc.PathParam("key")
	switch key {
	case api.MetricContract:
		var opts api.ContractMetricsQueryOpts
		if jc.DecodeForm("contractID", &opts.ContractID) != nil {
			return
		} else if jc.DecodeForm("hostKey", &opts.HostKey) != nil {
			return
		}
		metrics, err = b.metrics(jc.Request.Context(), key, start, n, interval, opts)
	case api.MetricContractPrune:
		var opts api.ContractPruneMetricsQueryOpts
		if jc.DecodeForm("contractID", &opts.ContractID) != nil {
			return
		} else if jc.DecodeForm("hostKey", &opts.HostKey) != nil {
			return
		} else if jc.DecodeForm("hostVersion", &opts.HostVersion) != nil {
			return
		}
		metrics, err = b.metrics(jc.Request.Context(), key, start, n, interval, opts)
	case api.MetricContractSet:
		var opts api.ContractSetMetricsQueryOpts
		if jc.DecodeForm("name", &opts.Name) != nil {
			return
		}
		metrics, err = b.metrics(jc.Request.Context(), key, start, n, interval, opts)
	case api.MetricContractSetChurn:
		var opts api.ContractSetChurnMetricsQueryOpts
		if jc.DecodeForm("name", &opts.Name) != nil {
			return
		} else if jc.DecodeForm("direction", &opts.Direction) != nil {
			return
		} else if jc.DecodeForm("reason", &opts.Reason) != nil {
			return
		}
		metrics, err = b.metrics(jc.Request.Context(), key, start, n, interval, opts)
	case api.MetricWallet:
		var opts api.WalletMetricsQueryOpts
		metrics, err = b.metrics(jc.Request.Context(), key, start, n, interval, opts)
	default:
		jc.Error(fmt.Errorf("unknown metric '%s'", key), http.StatusBadRequest)
		return
	}
	if errors.Is(err, api.ErrMaxIntervalsExceeded) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if jc.Check(fmt.Sprintf("failed to fetch '%s' metrics", key), err) != nil {
		return
	}
	jc.Encode(metrics)
}

func (b *bus) metrics(ctx context.Context, key string, start time.Time, n uint64, interval time.Duration, opts interface{}) (interface{}, error) {
	switch key {
	case api.MetricContract:
		return b.mtrcs.ContractMetrics(ctx, start, n, interval, opts.(api.ContractMetricsQueryOpts))
	case api.MetricContractPrune:
		return b.mtrcs.ContractPruneMetrics(ctx, start, n, interval, opts.(api.ContractPruneMetricsQueryOpts))
	case api.MetricContractSet:
		return b.mtrcs.ContractSetMetrics(ctx, start, n, interval, opts.(api.ContractSetMetricsQueryOpts))
	case api.MetricContractSetChurn:
		return b.mtrcs.ContractSetChurnMetrics(ctx, start, n, interval, opts.(api.ContractSetChurnMetricsQueryOpts))
	case api.MetricWallet:
		return b.mtrcs.WalletMetrics(ctx, start, n, interval, opts.(api.WalletMetricsQueryOpts))
	}
	return nil, fmt.Errorf("unknown metric '%s'", key)
}

func (b *bus) multipartHandlerCreatePOST(jc jape.Context) {
	var req api.MultipartCreateRequest
	if jc.Decode(&req) != nil {
		return
	}

	var key object.EncryptionKey
	if req.GenerateKey {
		key = object.GenerateEncryptionKey()
	} else if req.Key == nil {
		key = object.NoOpKey
	} else {
		key = *req.Key
	}

	resp, err := b.ms.CreateMultipartUpload(jc.Request.Context(), req.Bucket, req.Path, key, req.MimeType, req.Metadata)
	if jc.Check("failed to create multipart upload", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *bus) multipartHandlerAbortPOST(jc jape.Context) {
	var req api.MultipartAbortRequest
	if jc.Decode(&req) != nil {
		return
	}
	err := b.ms.AbortMultipartUpload(jc.Request.Context(), req.Bucket, req.Path, req.UploadID)
	if jc.Check("failed to abort multipart upload", err) != nil {
		return
	}
}

func (b *bus) multipartHandlerCompletePOST(jc jape.Context) {
	var req api.MultipartCompleteRequest
	if jc.Decode(&req) != nil {
		return
	}
	resp, err := b.ms.CompleteMultipartUpload(jc.Request.Context(), req.Bucket, req.Path, req.UploadID, req.Parts, api.CompleteMultipartOptions{
		Metadata: req.Metadata,
	})
	if jc.Check("failed to complete multipart upload", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *bus) multipartHandlerUploadPartPUT(jc jape.Context) {
	var req api.MultipartAddPartRequest
	if jc.Decode(&req) != nil {
		return
	}
	if req.Bucket == "" {
		req.Bucket = api.DefaultBucketName
	} else if req.ContractSet == "" {
		jc.Error(errors.New("contract_set must be non-empty"), http.StatusBadRequest)
		return
	} else if req.ETag == "" {
		jc.Error(errors.New("etag must be non-empty"), http.StatusBadRequest)
		return
	} else if req.PartNumber <= 0 || req.PartNumber > gofakes3.MaxUploadPartNumber {
		jc.Error(fmt.Errorf("part_number must be between 1 and %d", gofakes3.MaxUploadPartNumber), http.StatusBadRequest)
		return
	} else if req.UploadID == "" {
		jc.Error(errors.New("upload_id must be non-empty"), http.StatusBadRequest)
		return
	}
	err := b.ms.AddMultipartPart(jc.Request.Context(), req.Bucket, req.Path, req.ContractSet, req.ETag, req.UploadID, req.PartNumber, req.Slices)
	if jc.Check("failed to upload part", err) != nil {
		return
	}
}

func (b *bus) multipartHandlerUploadGET(jc jape.Context) {
	resp, err := b.ms.MultipartUpload(jc.Request.Context(), jc.PathParam("id"))
	if jc.Check("failed to get multipart upload", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *bus) multipartHandlerListUploadsPOST(jc jape.Context) {
	var req api.MultipartListUploadsRequest
	if jc.Decode(&req) != nil {
		return
	}
	resp, err := b.ms.MultipartUploads(jc.Request.Context(), req.Bucket, req.Prefix, req.PathMarker, req.UploadIDMarker, req.Limit)
	if jc.Check("failed to list multipart uploads", err) != nil {
		return
	}
	jc.Encode(resp)
}

func (b *bus) multipartHandlerListPartsPOST(jc jape.Context) {
	var req api.MultipartListPartsRequest
	if jc.Decode(&req) != nil {
		return
	}
	resp, err := b.ms.MultipartUploadParts(jc.Request.Context(), req.Bucket, req.Path, req.UploadID, req.PartNumberMarker, int64(req.Limit))
	if jc.Check("failed to list multipart upload parts", err) != nil {
		return
	}
	jc.Encode(resp)
}

// New returns a new Bus.
func New(am *alerts.Manager, hm WebhookManager, events ibus.EventBroadcaster, cm ChainManager, cs ChainStore, s Syncer, w Wallet, hdb HostDB, as AutopilotStore, ms MetadataStore, ss SettingStore, eas EphemeralAccountStore, mtrcs MetricsStore, l *zap.Logger) (*bus, error) {
	b := &bus{
		alerts:           alerts.WithOrigin(am, "bus"),
		alertMgr:         am,
		webhooks:         hm,
		events:           events,
		cm:               cm,
		cs:               cs,
		s:                s,
		w:                w,
		hdb:              hdb,
		as:               as,
		ms:               ms,
		mtrcs:            mtrcs,
		ss:               ss,
		eas:              eas,
		contractLocks:    newContractLocks(),
		uploadingSectors: newUploadingSectorsCache(),
		logger:           l.Sugar().Named("bus"),

		startTime: time.Now(),
	}

	// ensure we don't hang indefinitely
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// load default settings if the setting is not already set
	for key, value := range map[string]interface{}{
		api.SettingGouging:       build.DefaultGougingSettings,
		api.SettingRedundancy:    build.DefaultRedundancySettings,
		api.SettingUploadPacking: build.DefaultUploadPackingSettings,
	} {
		if _, err := b.ss.Setting(ctx, key); errors.Is(err, api.ErrSettingNotFound) {
			if bytes, err := json.Marshal(value); err != nil {
				panic("failed to marshal default settings") // should never happen
			} else if err := b.ss.UpdateSetting(ctx, key, string(bytes)); err != nil {
				return nil, err
			}
		}
	}

	// check redundancy settings for validity
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

	// check gouging settings for validity
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

	// load the accounts into memory, they're saved when the bus is stopped
	accounts, err := eas.Accounts(ctx)
	if err != nil {
		return nil, err
	}
	b.accounts = newAccounts(accounts, b.logger)

	// mark the shutdown as unclean, this will be overwritten when/if the
	// accounts are saved on shutdown
	if err := eas.SetUncleanShutdown(ctx); err != nil {
		return nil, fmt.Errorf("failed to mark account shutdown as unclean: %w", err)
	}

	return b, nil
}
