package bus

// TODOs:
// - add UPNP support

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"strings"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus/client"
	ibus "go.sia.tech/renterd/internal/bus"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/rhp"
	rhp2 "go.sia.tech/renterd/internal/rhp/v2"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
	"golang.org/x/crypto/blake2b"
)

const (
	defaultWalletRecordMetricInterval = 5 * time.Minute
	defaultPinUpdateInterval          = 5 * time.Minute
	defaultPinRateWindow              = 6 * time.Hour
	lockingPriorityFunding            = 40
	lockingPriorityRenew              = 80
	stdTxnSize                        = 1200 // bytes
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
	AlertManager interface {
		alerts.Alerter
		RegisterWebhookBroadcaster(b webhooks.Broadcaster)
	}

	ChainManager interface {
		AddBlocks(blocks []types.Block) error
		AddPoolTransactions(txns []types.Transaction) (bool, error)
		AddV2PoolTransactions(basis types.ChainIndex, txns []types.V2Transaction) (known bool, err error)
		Block(id types.BlockID) (types.Block, bool)
		OnReorg(fn func(types.ChainIndex)) (cancel func())
		PoolTransaction(txid types.TransactionID) (types.Transaction, bool)
		PoolTransactions() []types.Transaction
		V2PoolTransactions() []types.V2Transaction
		RecommendedFee() types.Currency
		Tip() types.ChainIndex
		TipState() consensus.State
		UnconfirmedParents(txn types.Transaction) []types.Transaction
		UpdatesSince(index types.ChainIndex, max int) (rus []chain.RevertUpdate, aus []chain.ApplyUpdate, err error)
		V2UnconfirmedParents(txn types.V2Transaction) []types.V2Transaction
	}

	ContractLocker interface {
		Acquire(ctx context.Context, priority int, id types.FileContractID, d time.Duration) (uint64, error)
		KeepAlive(id types.FileContractID, lockID uint64, d time.Duration) error
		Release(id types.FileContractID, lockID uint64) error
	}

	ChainSubscriber interface {
		ChainIndex(context.Context) (types.ChainIndex, error)
		Shutdown(context.Context) error
	}

	// A TransactionPool can validate and relay unconfirmed transactions.
	TransactionPool interface {
		AcceptTransactionSet(txns []types.Transaction) error
		Close() error
		RecommendedFee() types.Currency
		Transactions() []types.Transaction
		UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error)
	}

	UploadingSectorsCache interface {
		AddSector(uID api.UploadID, fcid types.FileContractID, root types.Hash256) error
		FinishUpload(uID api.UploadID)
		HandleRenewal(fcid, renewedFrom types.FileContractID)
		Pending(fcid types.FileContractID) (size uint64)
		Sectors(fcid types.FileContractID) (roots []types.Hash256)
		StartUpload(uID api.UploadID) error
	}

	PinManager interface {
		Shutdown(context.Context) error
		TriggerUpdate()
	}

	Syncer interface {
		Addr() string
		BroadcastHeader(h gateway.BlockHeader)
		BroadcastV2BlockOutline(bo gateway.V2BlockOutline)
		BroadcastTransactionSet([]types.Transaction)
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction)
		Connect(ctx context.Context, addr string) (*syncer.Peer, error)
		Peers() []*syncer.Peer
	}

	Wallet interface {
		Address() types.Address
		Balance() (wallet.Balance, error)
		Close() error
		FundTransaction(txn *types.Transaction, amount types.Currency, useUnconfirmed bool) ([]types.Hash256, error)
		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (consensus.State, []int, error)
		Redistribute(outputs int, amount, feePerByte types.Currency) (txns []types.Transaction, toSign []types.Hash256, err error)
		RedistributeV2(outputs int, amount, feePerByte types.Currency) (txns []types.V2Transaction, toSign [][]int, err error)
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction)
		SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)
		SignV2Inputs(state consensus.State, txn *types.V2Transaction, toSign []int)
		SpendableOutputs() ([]types.SiacoinElement, error)
		Tip() (types.ChainIndex, error)
		UnconfirmedEvents() ([]wallet.Event, error)
		UpdateChainState(tx wallet.UpdateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error
		Events(offset, limit int) ([]wallet.Event, error)
	}

	WebhooksManager interface {
		webhooks.Broadcaster
		Delete(context.Context, webhooks.Webhook) error
		Info() ([]webhooks.Webhook, []webhooks.WebhookQueueInfo)
		Register(context.Context, webhooks.Webhook) error
		Shutdown(context.Context) error
	}

	// Store is a collection of stores used by the bus.
	Store interface {
		AccountStore
		AutopilotStore
		ChainStore
		HostStore
		MetadataStore
		MetricsStore
		SettingStore
	}

	// AccountStore persists information about accounts. Since accounts
	// are rapidly updated and can be recovered, they are only loaded upon
	// startup and persisted upon shutdown.
	AccountStore interface {
		Accounts(context.Context, string) ([]api.Account, error)
		SaveAccounts(context.Context, []api.Account) error
	}

	// An AutopilotStore stores autopilots.
	AutopilotStore interface {
		Autopilot(ctx context.Context, id string) (api.Autopilot, error)
		Autopilots(ctx context.Context) ([]api.Autopilot, error)
		UpdateAutopilot(ctx context.Context, ap api.Autopilot) error
	}

	// A ChainStore stores information about the chain.
	ChainStore interface {
		ChainIndex(ctx context.Context) (types.ChainIndex, error)
		ProcessChainUpdate(ctx context.Context, applyFn func(sql.ChainUpdateTx) error) error
	}

	// A HostStore stores information about hosts.
	HostStore interface {
		Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error)
		HostAllowlist(ctx context.Context) ([]types.PublicKey, error)
		HostBlocklist(ctx context.Context) ([]string, error)
		HostsForScanning(ctx context.Context, maxLastScan time.Time, offset, limit int) ([]api.HostAddress, error)
		RecordHostScans(ctx context.Context, scans []api.HostScan) error
		RecordPriceTables(ctx context.Context, priceTableUpdate []api.HostPriceTableUpdate) error
		RemoveOfflineHosts(ctx context.Context, maxConsecutiveScanFailures uint64, maxDowntime time.Duration) (uint64, error)
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

	// A MetricsStore stores metrics.
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
		RecordWalletMetric(ctx context.Context, metrics ...api.WalletMetric) error
	}

	// A SettingStore stores settings.
	SettingStore interface {
		DeleteSetting(ctx context.Context, key string) error
		Setting(ctx context.Context, key string) (string, error)
		Settings(ctx context.Context) ([]string, error)
		UpdateSetting(ctx context.Context, key, value string) error
	}

	WalletMetricsRecorder interface {
		Shutdown(context.Context) error
	}
)

type Bus struct {
	startTime time.Time
	masterKey utils.MasterKey

	alerts      alerts.Alerter
	alertMgr    AlertManager
	pinMgr      PinManager
	webhooksMgr WebhooksManager
	cm          ChainManager
	cs          ChainSubscriber
	s           Syncer
	w           Wallet

	accounts AccountStore
	as       AutopilotStore
	hs       HostStore
	ms       MetadataStore
	mtrcs    MetricsStore
	ss       SettingStore

	rhp2 *rhp2.Client
	rhp3 *rhp3.Client

	contractLocker        ContractLocker
	sectors               UploadingSectorsCache
	walletMetricsRecorder WalletMetricsRecorder

	logger *zap.SugaredLogger
}

// New returns a new Bus
func New(ctx context.Context, masterKey [32]byte, am AlertManager, wm WebhooksManager, cm ChainManager, s Syncer, w Wallet, store Store, announcementMaxAge time.Duration, l *zap.Logger) (_ *Bus, err error) {
	l = l.Named("bus")

	b := &Bus{
		startTime: time.Now(),
		masterKey: masterKey,

		accounts: store,
		s:        s,
		cm:       cm,
		w:        w,
		hs:       store,
		as:       store,
		ms:       store,
		mtrcs:    store,
		ss:       store,

		alerts:      alerts.WithOrigin(am, "bus"),
		alertMgr:    am,
		webhooksMgr: wm,
		logger:      l.Sugar(),

		rhp2: rhp2.New(rhp.NewFallbackDialer(store, net.Dialer{}, l), l),
		rhp3: rhp3.New(rhp.NewFallbackDialer(store, net.Dialer{}, l), l),
	}

	// init settings
	if err := b.initSettings(ctx); err != nil {
		return nil, err
	}

	// create contract locker
	b.contractLocker = ibus.NewContractLocker()

	// create sectors cache
	b.sectors = ibus.NewSectorsCache()

	// create pin manager
	b.pinMgr = ibus.NewPinManager(b.alerts, wm, store, defaultPinUpdateInterval, defaultPinRateWindow, l)

	// create chain subscriber
	b.cs = ibus.NewChainSubscriber(wm, cm, store, w, announcementMaxAge, l)

	// create wallet metrics recorder
	b.walletMetricsRecorder = ibus.NewWalletMetricRecorder(store, w, defaultWalletRecordMetricInterval, l)

	return b, nil
}

// Handler returns an HTTP handler that serves the bus API.
func (b *Bus) Handler() http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"GET    /accounts":      b.accountsHandlerGET,
		"POST   /accounts":      b.accountsHandlerPOST,
		"POST   /accounts/fund": b.accountsFundHandler,

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

		"POST   /contracts":              b.contractsFormHandler,
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
		"POST   /contract/:id/renew":     b.contractIDRenewHandlerPOST,
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

		"GET    /wallet":              b.walletHandler,
		"POST   /wallet/discard":      b.walletDiscardHandler,
		"POST   /wallet/fund":         b.walletFundHandler,
		"GET    /wallet/outputs":      b.walletOutputsHandler,
		"GET    /wallet/pending":      b.walletPendingHandler,
		"POST   /wallet/redistribute": b.walletRedistributeHandler,
		"POST   /wallet/send":         b.walletSendSiacoinsHandler,
		"POST   /wallet/sign":         b.walletSignHandler,
		"GET    /wallet/transactions": b.walletTransactionsHandler,

		"GET    /webhooks":        b.webhookHandlerGet,
		"POST   /webhooks":        b.webhookHandlerPost,
		"POST   /webhooks/action": b.webhookActionHandlerPost,
		"POST   /webhook/delete":  b.webhookHandlerDelete,
	})
}

// Shutdown shuts down the bus.
func (b *Bus) Shutdown(ctx context.Context) error {
	return errors.Join(
		b.walletMetricsRecorder.Shutdown(ctx),
		b.webhooksMgr.Shutdown(ctx),
		b.pinMgr.Shutdown(ctx),
		b.cs.Shutdown(ctx),
	)
}

func (b *Bus) addContract(ctx context.Context, rev rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, state string) (api.ContractMetadata, error) {
	c, err := b.ms.AddContract(ctx, rev, contractPrice, totalCost, startHeight, state)
	if err != nil {
		return api.ContractMetadata{}, err
	}

	b.broadcastAction(webhooks.Event{
		Module: api.ModuleContract,
		Event:  api.EventAdd,
		Payload: api.EventContractAdd{
			Added:     c,
			Timestamp: time.Now().UTC(),
		},
	})
	return c, nil
}

func (b *Bus) addRenewedContract(ctx context.Context, renewedFrom types.FileContractID, rev rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, state string) (api.ContractMetadata, error) {
	r, err := b.ms.AddRenewedContract(ctx, rev, contractPrice, totalCost, startHeight, renewedFrom, state)
	if err != nil {
		return api.ContractMetadata{}, err
	}

	b.sectors.HandleRenewal(r.ID, r.RenewedFrom)
	b.broadcastAction(webhooks.Event{
		Module: api.ModuleContract,
		Event:  api.EventRenew,
		Payload: api.EventContractRenew{
			Renewal:   r,
			Timestamp: time.Now().UTC(),
		},
	})
	return r, nil
}

func (b *Bus) formContract(ctx context.Context, hostSettings rhpv2.HostSettings, renterAddress types.Address, renterFunds, hostCollateral types.Currency, hostKey types.PublicKey, hostIP string, endHeight uint64) (rhpv2.ContractRevision, error) {
	// derive the renter key
	renterKey := b.deriveRenterKey(hostKey)

	// prepare the transaction
	cs := b.cm.TipState()
	fc := rhpv2.PrepareContractFormation(renterKey.PublicKey(), hostKey, renterFunds, hostCollateral, endHeight, hostSettings, renterAddress)
	txn := types.Transaction{FileContracts: []types.FileContract{fc}}

	// calculate the miner fee
	fee := b.cm.RecommendedFee().Mul64(cs.TransactionWeight(txn))
	txn.MinerFees = []types.Currency{fee}

	// fund the transaction
	cost := rhpv2.ContractFormationCost(cs, fc, hostSettings.ContractPrice).Add(fee)
	toSign, err := b.w.FundTransaction(&txn, cost, true)
	if err != nil {
		return rhpv2.ContractRevision{}, fmt.Errorf("couldn't fund transaction: %w", err)
	}

	// sign the transaction
	b.w.SignTransaction(&txn, toSign, wallet.ExplicitCoveredFields(txn))

	// form the contract
	contract, txnSet, err := b.rhp2.FormContract(ctx, hostKey, hostIP, renterKey, append(b.cm.UnconfirmedParents(txn), txn))
	if err != nil {
		b.w.ReleaseInputs([]types.Transaction{txn}, nil)
		return rhpv2.ContractRevision{}, err
	}

	// add transaction set to the pool
	_, err = b.cm.AddPoolTransactions(txnSet)
	if err != nil {
		b.w.ReleaseInputs([]types.Transaction{txn}, nil)
		return rhpv2.ContractRevision{}, fmt.Errorf("couldn't add transaction set to the pool: %w", err)
	}

	// broadcast the transaction set
	go b.s.BroadcastTransactionSet(txnSet)

	return contract, nil
}

// initSettings loads the default settings if the setting is not already set and
// ensures the settings are valid
func (b *Bus) initSettings(ctx context.Context) error {
	// testnets have different redundancy settings
	defaultRedundancySettings := api.DefaultRedundancySettings
	if mn, _ := chain.Mainnet(); mn.Name != b.cm.TipState().Network.Name {
		defaultRedundancySettings = api.DefaultRedundancySettingsTestnet
	}

	// load default settings if the setting is not already set
	for key, value := range map[string]interface{}{
		api.SettingGouging:       api.DefaultGougingSettings,
		api.SettingPricePinning:  api.DefaultPricePinSettings,
		api.SettingRedundancy:    defaultRedundancySettings,
		api.SettingUploadPacking: api.DefaultUploadPackingSettings,
	} {
		if _, err := b.ss.Setting(ctx, key); errors.Is(err, api.ErrSettingNotFound) {
			if bytes, err := json.Marshal(value); err != nil {
				panic("failed to marshal default settings") // should never happen
			} else if err := b.ss.UpdateSetting(ctx, key, string(bytes)); err != nil {
				return err
			}
		}
	}

	// check redundancy settings for validity
	var rs api.RedundancySettings
	if rss, err := b.ss.Setting(ctx, api.SettingRedundancy); err != nil {
		return err
	} else if err := json.Unmarshal([]byte(rss), &rs); err != nil {
		return err
	} else if err := rs.Validate(); err != nil {
		b.logger.Warn(fmt.Sprintf("invalid redundancy setting found '%v', overwriting the redundancy settings with the default settings", rss))
		bytes, _ := json.Marshal(defaultRedundancySettings)
		if err := b.ss.UpdateSetting(ctx, api.SettingRedundancy, string(bytes)); err != nil {
			return err
		}
	}

	// check gouging settings for validity
	var gs api.GougingSettings
	if gss, err := b.ss.Setting(ctx, api.SettingGouging); err != nil {
		return err
	} else if err := json.Unmarshal([]byte(gss), &gs); err != nil {
		return err
	} else if err := gs.Validate(); err != nil {
		// compat: apply default EA gouging settings
		gs.MinMaxEphemeralAccountBalance = api.DefaultGougingSettings.MinMaxEphemeralAccountBalance
		gs.MinPriceTableValidity = api.DefaultGougingSettings.MinPriceTableValidity
		gs.MinAccountExpiry = api.DefaultGougingSettings.MinAccountExpiry
		if err := gs.Validate(); err == nil {
			b.logger.Info(fmt.Sprintf("updating gouging settings with default EA settings: %+v", gs))
			bytes, _ := json.Marshal(gs)
			if err := b.ss.UpdateSetting(ctx, api.SettingGouging, string(bytes)); err != nil {
				return err
			}
		} else {
			// compat: apply default host block leeway settings
			gs.HostBlockHeightLeeway = api.DefaultGougingSettings.HostBlockHeightLeeway
			if err := gs.Validate(); err == nil {
				b.logger.Info(fmt.Sprintf("updating gouging settings with default HostBlockHeightLeeway settings: %v", gs))
				bytes, _ := json.Marshal(gs)
				if err := b.ss.UpdateSetting(ctx, api.SettingGouging, string(bytes)); err != nil {
					return err
				}
			} else {
				b.logger.Warn(fmt.Sprintf("invalid gouging setting found '%v', overwriting the gouging settings with the default settings", gss))
				bytes, _ := json.Marshal(api.DefaultGougingSettings)
				if err := b.ss.UpdateSetting(ctx, api.SettingGouging, string(bytes)); err != nil {
					return err
				}
			}
		}
	}

	// compat: default price pin settings
	var pps api.PricePinSettings
	if pss, err := b.ss.Setting(ctx, api.SettingPricePinning); err != nil {
		return err
	} else if err := json.Unmarshal([]byte(pss), &pps); err != nil {
		return err
	} else if err := pps.Validate(); err != nil {
		// overwrite values with defaults
		var updates []string
		if pps.ForexEndpointURL == "" {
			pps.ForexEndpointURL = api.DefaultPricePinSettings.ForexEndpointURL
			updates = append(updates, fmt.Sprintf("set PricePinSettings.ForexEndpointURL to %v", pps.ForexEndpointURL))
		}
		if pps.Currency == "" {
			pps.Currency = api.DefaultPricePinSettings.Currency
			updates = append(updates, fmt.Sprintf("set PricePinSettings.Currency to %v", pps.Currency))
		}
		if pps.Threshold == 0 {
			pps.Threshold = api.DefaultPricePinSettings.Threshold
			updates = append(updates, fmt.Sprintf("set PricePinSettings.Threshold to %v", pps.Threshold))
		}

		var updated []byte
		if err := pps.Validate(); err == nil {
			b.logger.Info(fmt.Sprintf("updating price pinning settings with default values: %v", strings.Join(updates, ", ")))
			updated, _ = json.Marshal(pps)
		} else {
			b.logger.Warn(fmt.Sprintf("updated price pinning settings are invalid (%v), they have been overwritten with the default settings", err))
			updated, _ = json.Marshal(api.DefaultPricePinSettings)
		}

		if err := b.ss.UpdateSetting(ctx, api.SettingPricePinning, string(updated)); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bus) isPassedV2AllowHeight() bool {
	cs := b.cm.TipState()
	return cs.Index.Height >= cs.Network.HardforkV2.AllowHeight
}

func (b *Bus) deriveRenterKey(hostKey types.PublicKey) types.PrivateKey {
	seed := blake2b.Sum256(append(b.deriveSubKey("renterkey"), hostKey[:]...))
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

func (b *Bus) deriveSubKey(purpose string) types.PrivateKey {
	seed := blake2b.Sum256(append(b.masterKey[:], []byte(purpose)...))
	pk := types.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

func (b *Bus) prepareRenew(cs consensus.State, revision types.FileContractRevision, hostAddress, renterAddress types.Address, renterFunds, minNewCollateral, maxFundAmount types.Currency, endHeight, expectedStorage uint64) rhp3.PrepareRenewFn {
	return func(pt rhpv3.HostPriceTable) ([]types.Hash256, []types.Transaction, types.Currency, rhp3.DiscardTxnFn, error) {
		// create the final revision from the provided revision
		finalRevision := revision
		finalRevision.MissedProofOutputs = finalRevision.ValidProofOutputs
		finalRevision.Filesize = 0
		finalRevision.FileMerkleRoot = types.Hash256{}
		finalRevision.RevisionNumber = math.MaxUint64

		// prepare the new contract
		fc, basePrice, err := rhpv3.PrepareContractRenewal(revision, hostAddress, renterAddress, renterFunds, minNewCollateral, pt, expectedStorage, endHeight)
		if err != nil {
			return nil, nil, types.ZeroCurrency, nil, fmt.Errorf("couldn't prepare contract renewal: %w", err)
		}

		// prepare the transaction
		txn := types.Transaction{
			FileContracts:         []types.FileContract{fc},
			FileContractRevisions: []types.FileContractRevision{finalRevision},
			MinerFees:             []types.Currency{pt.TxnFeeMaxRecommended.Mul64(4096)},
		}

		// compute how much renter funds to put into the new contract
		fundAmount := rhpv3.ContractRenewalCost(cs, pt, fc, txn.MinerFees[0], basePrice)

		// make sure we don't exceed the max fund amount.
		if maxFundAmount.Cmp(fundAmount) < 0 {
			return nil, nil, types.ZeroCurrency, nil, fmt.Errorf("%w: %v > %v", api.ErrMaxFundAmountExceeded, fundAmount, maxFundAmount)
		}

		// fund the transaction, we are not signing it yet since it's not
		// complete. The host still needs to complete it and the revision +
		// contract are signed with the renter key by the worker.
		toSign, err := b.w.FundTransaction(&txn, fundAmount, true)
		if err != nil {
			return nil, nil, types.ZeroCurrency, nil, fmt.Errorf("couldn't fund transaction: %w", err)
		}

		return toSign, append(b.cm.UnconfirmedParents(txn), txn), fundAmount, func(err *error) {
			if *err == nil {
				return
			}
			b.w.ReleaseInputs([]types.Transaction{txn}, nil)
		}, nil
	}
}

func (b *Bus) renewContract(ctx context.Context, cs consensus.State, gp api.GougingParams, c api.ContractMetadata, hs rhpv2.HostSettings, renterFunds, minNewCollateral, maxFundAmount types.Currency, endHeight, expectedNewStorage uint64) (rhpv2.ContractRevision, types.Currency, types.Currency, error) {
	// acquire contract lock indefinitely and defer the release
	lockID, err := b.contractLocker.Acquire(ctx, lockingPriorityRenew, c.ID, time.Duration(math.MaxInt64))
	if err != nil {
		return rhpv2.ContractRevision{}, types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("couldn't acquire contract lock; %w", err)
	}
	defer func() {
		if err := b.contractLocker.Release(c.ID, lockID); err != nil {
			b.logger.Error("failed to release contract lock", zap.Error(err))
		}
	}()

	// fetch the revision
	rev, err := b.rhp3.Revision(ctx, c.ID, c.HostKey, c.SiamuxAddr)
	if err != nil {
		return rhpv2.ContractRevision{}, types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("couldn't fetch revision; %w", err)
	}

	// renew contract
	gc := gouging.NewChecker(gp.GougingSettings, gp.ConsensusState, gp.TransactionFee, nil, nil)
	renterKey := b.deriveRenterKey(c.HostKey)
	prepareRenew := b.prepareRenew(cs, rev, hs.Address, b.w.Address(), renterFunds, minNewCollateral, maxFundAmount, endHeight, expectedNewStorage)
	newRevision, txnSet, contractPrice, fundAmount, err := b.rhp3.Renew(ctx, gc, rev, renterKey, c.HostKey, c.SiamuxAddr, prepareRenew, b.w.SignTransaction)
	if err != nil {
		return rhpv2.ContractRevision{}, types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("couldn't renew contract; %w", err)
	}

	// broadcast the transaction set
	b.s.BroadcastTransactionSet(txnSet)

	return newRevision, contractPrice, fundAmount, nil
}
