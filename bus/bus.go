package bus

// TODOs:
// - add UPNP support

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
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
	"go.sia.tech/renterd/config"
	ibus "go.sia.tech/renterd/internal/bus"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/rhp"
	rhp2 "go.sia.tech/renterd/internal/rhp/v2"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	rhp4 "go.sia.tech/renterd/internal/rhp/v4"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
)

const (
	defaultWalletRecordMetricInterval = 5 * time.Minute
	defaultPinUpdateInterval          = 5 * time.Minute
	defaultPinRateWindow              = 6 * time.Hour

	lockingPriorityPruning   = 20
	lockingPriorityFunding   = 40
	lockingPriorityRenew     = 80
	lockingPriorityBroadcast = 100

	stdTxnSize = 1200 // bytes
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
		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
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
		AddSectors(uID api.UploadID, roots ...types.Hash256) error
		FinishUpload(uID api.UploadID)
		Sectors() (sectors []types.Hash256)
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
		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		Redistribute(outputs int, amount, feePerByte types.Currency) (txns []types.Transaction, toSign []types.Hash256, err error)
		RedistributeV2(outputs int, amount, feePerByte types.Currency) (txns []types.V2Transaction, toSign [][]int, err error)
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction)
		SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
		SpendableOutputs() ([]types.SiacoinElement, error)
		Tip() types.ChainIndex
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
		BackupStore
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

	// BackupStore is the interface of a store that can be backed up.
	BackupStore interface {
		Backup(ctx context.Context, dbID, dst string) error
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
		Hosts(ctx context.Context, opts api.HostOptions) ([]api.Host, error)
		RecordHostScans(ctx context.Context, scans []api.HostScan) error
		RecordPriceTables(ctx context.Context, priceTableUpdate []api.HostPriceTableUpdate) error
		RemoveOfflineHosts(ctx context.Context, maxConsecutiveScanFailures uint64, maxDowntime time.Duration) (uint64, error)
		ResetLostSectors(ctx context.Context, hk types.PublicKey) error
		UpdateHostAllowlistEntries(ctx context.Context, add, remove []types.PublicKey, clear bool) error
		UpdateHostBlocklistEntries(ctx context.Context, add, remove []string, clear bool) error
		UpdateHostCheck(ctx context.Context, autopilotID string, hk types.PublicKey, check api.HostCheck) error
		UsableHosts(ctx context.Context) ([]sql.HostInfo, error)
	}

	// A MetadataStore stores information about contracts and objects.
	MetadataStore interface {
		AddRenewal(ctx context.Context, c api.ContractMetadata) error
		AncestorContracts(ctx context.Context, fcid types.FileContractID, minStartHeight uint64) ([]api.ContractMetadata, error)
		ArchiveContract(ctx context.Context, id types.FileContractID, reason string) error
		ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) error
		ArchiveAllContracts(ctx context.Context, reason string) error
		Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
		Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error)
		ContractSets(ctx context.Context) ([]string, error)
		RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error
		RemoveContractSet(ctx context.Context, name string) error
		PutContract(ctx context.Context, c api.ContractMetadata) error
		RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error)
		UpdateContractSet(ctx context.Context, set string, toAdd, toRemove []types.FileContractID) error

		ContractRoots(ctx context.Context, id types.FileContractID) ([]types.Hash256, error)
		ContractSizes(ctx context.Context) (map[types.FileContractID]api.ContractSize, error)
		ContractSize(ctx context.Context, id types.FileContractID) (api.ContractSize, error)
		PrunableContractRoots(ctx context.Context, id types.FileContractID, roots []types.Hash256) ([]uint64, error)

		DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) (int, error)

		Bucket(_ context.Context, bucketName string) (api.Bucket, error)
		Buckets(_ context.Context) ([]api.Bucket, error)
		CreateBucket(_ context.Context, bucketName string, policy api.BucketPolicy) error
		DeleteBucket(_ context.Context, bucketName string) error
		UpdateBucketPolicy(ctx context.Context, bucketName string, policy api.BucketPolicy) error

		CopyObject(ctx context.Context, srcBucket, dstBucket, srcKey, dstKey, mimeType string, metadata api.ObjectUserMetadata) (api.ObjectMetadata, error)
		Object(ctx context.Context, bucketName, key string) (api.Object, error)
		Objects(ctx context.Context, bucketName, prefix, substring, delim, sortBy, sortDir, marker string, limit int, slabEncryptionKey object.EncryptionKey) (api.ObjectsResponse, error)
		ObjectMetadata(ctx context.Context, bucketName, key string) (api.Object, error)
		ObjectsStats(ctx context.Context, opts api.ObjectsStatsOpts) (api.ObjectsStatsResponse, error)
		RemoveObject(ctx context.Context, bucketName, key string) error
		RemoveObjects(ctx context.Context, bucketName, prefix string) error
		RenameObject(ctx context.Context, bucketName, from, to string, force bool) error
		RenameObjects(ctx context.Context, bucketName, from, to string, force bool) error
		UpdateObject(ctx context.Context, bucketName, key, contractSet, ETag, mimeType string, metadata api.ObjectUserMetadata, o object.Object) error

		AbortMultipartUpload(ctx context.Context, bucketName, key string, uploadID string) (err error)
		AddMultipartPart(ctx context.Context, bucketName, key, contractSet, eTag, uploadID string, partNumber int, slices []object.SlabSlice) (err error)
		CompleteMultipartUpload(ctx context.Context, bucketName, key, uploadID string, parts []api.MultipartCompletedPart, opts api.CompleteMultipartOptions) (_ api.MultipartCompleteResponse, err error)
		CreateMultipartUpload(ctx context.Context, bucketName, key string, ec object.EncryptionKey, mimeType string, metadata api.ObjectUserMetadata) (api.MultipartCreateResponse, error)
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
		UpdateSlab(ctx context.Context, key object.EncryptionKey, sectors []api.UploadedSector) error
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
		GougingSettings(ctx context.Context) (api.GougingSettings, error)
		UpdateGougingSettings(ctx context.Context, gs api.GougingSettings) error

		PinnedSettings(ctx context.Context) (api.PinnedSettings, error)
		UpdatePinnedSettings(ctx context.Context, ps api.PinnedSettings) error

		UploadSettings(ctx context.Context) (api.UploadSettings, error)
		UpdateUploadSettings(ctx context.Context, us api.UploadSettings) error

		S3Settings(ctx context.Context) (api.S3Settings, error)
		UpdateS3Settings(ctx context.Context, s3as api.S3Settings) error
	}

	WalletMetricsRecorder interface {
		Shutdown(context.Context) error
	}
)

type Bus struct {
	allowPrivateIPs bool
	startTime       time.Time
	masterKey       utils.MasterKey

	alerts      alerts.Alerter
	alertMgr    AlertManager
	pinMgr      PinManager
	webhooksMgr WebhooksManager
	cm          ChainManager
	cs          ChainSubscriber
	s           Syncer
	w           Wallet
	store       Store

	rhp2Client *rhp2.Client
	rhp3Client *rhp3.Client
	rhp4Client *rhp4.Client

	contractLocker        ContractLocker
	explorer              *ibus.Explorer
	sectors               UploadingSectorsCache
	walletMetricsRecorder WalletMetricsRecorder

	logger *zap.SugaredLogger
}

// New returns a new Bus
func New(ctx context.Context, cfg config.Bus, masterKey [32]byte, am AlertManager, wm WebhooksManager, cm ChainManager, s Syncer, w Wallet, store Store, explorerURL string, l *zap.Logger) (_ *Bus, err error) {
	l = l.Named("bus")
	dialer := rhp.NewFallbackDialer(store, net.Dialer{}, l)

	b := &Bus{
		allowPrivateIPs: cfg.AllowPrivateIPs,
		startTime:       time.Now(),
		masterKey:       masterKey,

		s:        s,
		cm:       cm,
		w:        w,
		explorer: ibus.NewExplorer(explorerURL),
		store:    store,

		alerts:      alerts.WithOrigin(am, "bus"),
		alertMgr:    am,
		webhooksMgr: wm,
		logger:      l.Sugar(),

		rhp2Client: rhp2.New(dialer, l),
		rhp3Client: rhp3.New(dialer, l),
		rhp4Client: rhp4.New(dialer),
	}

	// create contract locker
	b.contractLocker = ibus.NewContractLocker()

	// create sectors cache
	b.sectors = ibus.NewSectorsCache()

	// create pin manager
	b.pinMgr = ibus.NewPinManager(b.alerts, wm, b.explorer, store, defaultPinUpdateInterval, defaultPinRateWindow, l)

	// create chain subscriber
	announcementMaxAge := time.Duration(cfg.AnnouncementMaxAgeHours) * time.Hour
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

		"PUT    /contracts":              b.contractsHandlerPUT,
		"GET    /contracts":              b.contractsHandlerGET,
		"DELETE /contracts/all":          b.contractsAllHandlerDELETE,
		"POST   /contracts/archive":      b.contractsArchiveHandlerPOST,
		"POST   /contracts/form":         b.contractsFormHandler,
		"GET    /contracts/prunable":     b.contractsPrunableDataHandlerGET,
		"GET    /contracts/renewed/:id":  b.contractsRenewedIDHandlerGET,
		"GET    /contracts/sets":         b.contractsSetsHandlerGET,
		"POST   /contracts/set/:set":     b.contractsSetHandlerPUT,
		"DELETE /contracts/set/:set":     b.contractsSetHandlerDELETE,
		"POST   /contracts/spending":     b.contractsSpendingHandlerPOST,
		"GET    /contract/:id":           b.contractIDHandlerGET,
		"DELETE /contract/:id":           b.contractIDHandlerDELETE,
		"POST   /contract/:id/acquire":   b.contractAcquireHandlerPOST,
		"GET    /contract/:id/ancestors": b.contractIDAncestorsHandler,
		"POST   /contract/:id/broadcast": b.contractIDBroadcastHandler,
		"POST   /contract/:id/keepalive": b.contractKeepaliveHandlerPOST,
		"GET    /contract/:id/revision":  b.contractLatestRevisionHandlerGET,
		"POST   /contract/:id/prune":     b.contractPruneHandlerPOST,
		"POST   /contract/:id/renew":     b.contractIDRenewHandlerPOST,
		"POST   /contract/:id/release":   b.contractReleaseHandlerPOST,
		"GET    /contract/:id/roots":     b.contractIDRootsHandlerGET,
		"GET    /contract/:id/size":      b.contractSizeHandlerGET,

		"GET    /hosts":                          b.hostsHandlerGET,
		"POST   /hosts":                          b.hostsHandlerPOST,
		"GET    /hosts/allowlist":                b.hostsAllowlistHandlerGET,
		"PUT    /hosts/allowlist":                b.hostsAllowlistHandlerPUT,
		"GET    /hosts/blocklist":                b.hostsBlocklistHandlerGET,
		"PUT    /hosts/blocklist":                b.hostsBlocklistHandlerPUT,
		"POST   /hosts/remove":                   b.hostsRemoveHandlerPOST,
		"GET    /host/:hostkey":                  b.hostsPubkeyHandlerGET,
		"POST   /host/:hostkey/resetlostsectors": b.hostsResetLostSectorsPOST,
		"POST   /host/:hostkey/scan":             b.hostsScanHandlerPOST,

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

		"GET    /object/*key":     b.objectHandlerGET,
		"PUT    /object/*key":     b.objectHandlerPUT,
		"DELETE /object/*key":     b.objectHandlerDELETE,
		"GET    /objects/*prefix": b.objectsHandlerGET,
		"POST   /objects/copy":    b.objectsCopyHandlerPOST,
		"POST   /objects/remove":  b.objectsRemoveHandlerPOST,
		"POST   /objects/rename":  b.objectsRenameHandlerPOST,

		"GET    /params/gouging": b.paramsHandlerGougingGET,
		"GET    /params/upload":  b.paramsHandlerUploadGET,

		"GET    /slabbuffers":      b.slabbuffersHandlerGET,
		"POST   /slabbuffer/done":  b.packedSlabsHandlerDonePOST,
		"POST   /slabbuffer/fetch": b.packedSlabsHandlerFetchPOST,

		"DELETE /sectors/:hk/:root": b.sectorsHostRootHandlerDELETE,

		"GET    /settings/gouging": b.settingsGougingHandlerGET,
		"PUT    /settings/gouging": b.settingsGougingHandlerPUT,
		"GET    /settings/pinned":  b.settingsPinnedHandlerGET,
		"PUT    /settings/pinned":  b.settingsPinnedHandlerPUT,
		"GET    /settings/s3":      b.settingsS3HandlerGET,
		"PUT    /settings/s3":      b.settingsS3HandlerPUT,
		"GET    /settings/upload":  b.settingsUploadHandlerGET,
		"PUT    /settings/upload":  b.settingsUploadHandlerPUT,

		"POST   /slabs/migration":     b.slabsMigrationHandlerPOST,
		"GET    /slabs/partial/:key":  b.slabsPartialHandlerGET,
		"POST   /slabs/partial":       b.slabsPartialHandlerPOST,
		"POST   /slabs/refreshhealth": b.slabsRefreshHealthHandlerPOST,
		"GET    /slab/:key":           b.slabHandlerGET,
		"PUT    /slab/:key":           b.slabHandlerPUT,

		"GET    /state":         b.stateHandlerGET,
		"GET    /stats/objects": b.objectsStatshandlerGET,

		"GET    /syncer/address": b.syncerAddrHandler,
		"POST   /syncer/connect": b.syncerConnectHandler,
		"GET    /syncer/peers":   b.syncerPeersHandler,

		"POST /system/sqlite3/backup": b.postSystemSQLite3BackupHandler,

		"GET    /txpool/recommendedfee": b.txpoolFeeHandler,
		"GET    /txpool/transactions":   b.txpoolTransactionsHandler,
		"POST   /txpool/broadcast":      b.txpoolBroadcastHandler,

		"POST   /upload/:id":        b.uploadTrackHandlerPOST,
		"DELETE /upload/:id":        b.uploadFinishedHandlerDELETE,
		"POST   /upload/:id/sector": b.uploadAddSectorHandlerPOST,

		"GET  /wallet":              b.walletHandler,
		"GET  /wallet/events":       b.walletEventsHandler,
		"GET  /wallet/pending":      b.walletPendingHandler,
		"POST /wallet/redistribute": b.walletRedistributeHandler,
		"POST /wallet/send":         b.walletSendSiacoinsHandler,

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

func (b *Bus) addContract(ctx context.Context, rev rhpv2.ContractRevision, contractPrice, initialRenterFunds types.Currency, startHeight uint64, state string) (api.ContractMetadata, error) {
	if err := b.store.PutContract(ctx, api.ContractMetadata{
		ID:                 rev.ID(),
		HostKey:            rev.HostKey(),
		StartHeight:        startHeight,
		State:              state,
		Usability:          api.ContractUsabilityGood,
		WindowStart:        rev.Revision.WindowStart,
		WindowEnd:          rev.Revision.WindowEnd,
		ContractPrice:      contractPrice,
		InitialRenterFunds: initialRenterFunds,
	}); err != nil {
		return api.ContractMetadata{}, err
	}

	added, err := b.store.Contract(ctx, rev.ID())
	if err != nil {
		return api.ContractMetadata{}, err
	}

	b.broadcastAction(webhooks.Event{
		Module: api.ModuleContract,
		Event:  api.EventAdd,
		Payload: api.EventContractAdd{
			Added:     added,
			Timestamp: time.Now().UTC(),
		},
	})

	return added, err
}

func (b *Bus) addRenewal(ctx context.Context, renewedFrom types.FileContractID, rev rhpv2.ContractRevision, contractPrice, initialRenterFunds types.Currency, startHeight uint64, state string) (api.ContractMetadata, error) {
	if err := b.store.AddRenewal(ctx, api.ContractMetadata{
		ID:                 rev.ID(),
		HostKey:            rev.HostKey(),
		RenewedFrom:        renewedFrom,
		StartHeight:        startHeight,
		State:              state,
		Usability:          api.ContractUsabilityGood,
		WindowStart:        rev.Revision.WindowStart,
		WindowEnd:          rev.Revision.WindowEnd,
		ContractPrice:      contractPrice,
		InitialRenterFunds: initialRenterFunds,
	}); err != nil {
		return api.ContractMetadata{}, fmt.Errorf("couldn't add renewal: %w", err)
	}

	renewal, err := b.store.Contract(ctx, rev.ID())
	if err != nil {
		return api.ContractMetadata{}, err
	}

	b.broadcastAction(webhooks.Event{
		Module: api.ModuleContract,
		Event:  api.EventRenew,
		Payload: api.EventContractRenew{
			Renewal:   renewal,
			Timestamp: time.Now().UTC(),
		},
	})

	return renewal, err
}

func (b *Bus) broadcastContract(ctx context.Context, fcid types.FileContractID) (txnID types.TransactionID, _ error) {
	// acquire contract lock indefinitely and defer the release
	lockID, err := b.contractLocker.Acquire(ctx, lockingPriorityRenew, fcid, time.Duration(math.MaxInt64))
	if err != nil {
		return types.TransactionID{}, fmt.Errorf("couldn't acquire contract lock; %w", err)
	}
	defer func() {
		if err := b.contractLocker.Release(fcid, lockID); err != nil {
			b.logger.Error("failed to release contract lock", zap.Error(err))
		}
	}()

	// fetch contract
	c, err := b.store.Contract(ctx, fcid)
	if err != nil {
		return types.TransactionID{}, fmt.Errorf("couldn't fetch contract; %w", err)
	}

	// derive the renter key
	renterKey := b.masterKey.DeriveContractKey(c.HostKey)

	// fetch revision
	rev, err := b.rhp2Client.SignedRevision(ctx, c.HostIP, c.HostKey, renterKey, fcid, time.Minute)
	if err != nil {
		return types.TransactionID{}, fmt.Errorf("couldn't fetch revision; %w", err)
	}

	// send V2 transaction if we're passed the V2 hardfork allow height
	if b.isPassedV2AllowHeight() {
		panic("not implemented")
	} else {
		// create the transaction
		txn := types.Transaction{
			FileContractRevisions: []types.FileContractRevision{rev.Revision},
			Signatures:            rev.Signatures[:],
		}

		// fund the transaction (only the fee)
		toSign, err := b.w.FundTransaction(&txn, types.ZeroCurrency, true)
		if err != nil {
			return types.TransactionID{}, fmt.Errorf("couldn't fund transaction; %w", err)
		}
		// sign the transaction
		b.w.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})

		// verify the transaction and add it to the transaction pool
		txnset := append(b.cm.UnconfirmedParents(txn), txn)
		_, err = b.cm.AddPoolTransactions(txnset)
		if err != nil {
			b.w.ReleaseInputs([]types.Transaction{txn}, nil)
			return types.TransactionID{}, fmt.Errorf("couldn't add transaction set to the pool; %w", err)
		}

		// broadcast the transaction
		b.s.BroadcastTransactionSet(txnset)
		txnID = txn.ID()
	}

	return
}

func (b *Bus) formContract(ctx context.Context, hostSettings rhpv2.HostSettings, renterAddress types.Address, renterFunds, hostCollateral types.Currency, hostKey types.PublicKey, hostIP string, endHeight uint64) (rhpv2.ContractRevision, error) {
	// derive the renter key
	renterKey := b.masterKey.DeriveContractKey(hostKey)

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
	contract, txnSet, err := b.rhp2Client.FormContract(ctx, hostKey, hostIP, renterKey, append(b.cm.UnconfirmedParents(txn), txn))
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

func (b *Bus) isPassedV2AllowHeight() bool {
	cs := b.cm.TipState()
	return cs.Index.Height >= cs.Network.HardforkV2.AllowHeight
}

func (b *Bus) prepareRenew(cs consensus.State, revision types.FileContractRevision, hostAddress, renterAddress types.Address, renterFunds, minNewCollateral types.Currency, endHeight, expectedStorage uint64) rhp3.PrepareRenewFn {
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

func (b *Bus) renewContract(ctx context.Context, cs consensus.State, gp api.GougingParams, c api.ContractMetadata, hs rhpv2.HostSettings, renterFunds, minNewCollateral types.Currency, endHeight, expectedNewStorage uint64) (rhpv2.ContractRevision, types.Currency, types.Currency, error) {
	// derive the renter key
	renterKey := b.masterKey.DeriveContractKey(c.HostKey)

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
	rev, err := b.rhp3Client.Revision(ctx, c.ID, c.HostKey, c.SiamuxAddr)
	if err != nil {
		return rhpv2.ContractRevision{}, types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("couldn't fetch revision; %w", err)
	}

	// renew contract
	gc := gouging.NewChecker(gp.GougingSettings, gp.ConsensusState, nil, nil)
	prepareRenew := b.prepareRenew(cs, rev, hs.Address, b.w.Address(), renterFunds, minNewCollateral, endHeight, expectedNewStorage)
	newRevision, txnSet, contractPrice, fundAmount, err := b.rhp3Client.Renew(ctx, gc, rev, renterKey, c.HostKey, c.SiamuxAddr, prepareRenew, b.w.SignTransaction)
	if err != nil {
		return rhpv2.ContractRevision{}, types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("couldn't renew contract; %w", err)
	}

	// broadcast the transaction set
	b.s.BroadcastTransactionSet(txnSet)

	return newRevision, contractPrice, fundAmount, nil
}

func (b *Bus) scanHost(ctx context.Context, timeout time.Duration, hostKey types.PublicKey, hostIP string) (rhpv2.HostSettings, rhpv3.HostPriceTable, time.Duration, error) {
	logger := b.logger.With("host", hostKey).With("hostIP", hostIP).With("timeout", timeout)

	// prepare a helper to create a context for scanning
	timeoutCtx := func() (context.Context, context.CancelFunc) {
		if timeout > 0 {
			return context.WithTimeout(ctx, timeout)
		}
		return ctx, func() {}
	}

	// prepare a helper for scanning
	scan := func() (rhpv2.HostSettings, rhpv3.HostPriceTable, time.Duration, error) {
		// fetch the host settings
		start := time.Now()
		scanCtx, cancel := timeoutCtx()
		settings, err := b.rhp2Client.Settings(scanCtx, hostKey, hostIP)
		cancel()
		if err != nil {
			return settings, rhpv3.HostPriceTable{}, time.Since(start), err
		}

		// fetch the host pricetable
		scanCtx, cancel = timeoutCtx()
		pt, err := b.rhp3Client.PriceTableUnpaid(scanCtx, hostKey, settings.SiamuxAddr())
		cancel()
		if err != nil {
			return settings, rhpv3.HostPriceTable{}, time.Since(start), err
		}
		return settings, pt.HostPriceTable, time.Since(start), nil
	}

	// resolve host ip, don't scan if the host is on a private network or if it
	// resolves to more than two addresses of the same type, if it fails for
	// another reason the host scan won't have subnets
	resolvedAddresses, private, err := utils.ResolveHostIP(ctx, hostIP)
	if errors.Is(err, utils.ErrHostTooManyAddresses) {
		return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, err
	} else if private && !b.allowPrivateIPs {
		return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, api.ErrHostOnPrivateNetwork
	}

	// scan: first try
	settings, pt, duration, err := scan()
	if err != nil {
		logger = logger.With(zap.Error(err))

		// scan: second try
		select {
		case <-ctx.Done():
			return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, context.Cause(ctx)
		case <-time.After(time.Second):
		}
		settings, pt, duration, err = scan()

		logger = logger.With("elapsed", duration).With(zap.Error(err))
		if err == nil {
			logger.Info("successfully scanned host on second try")
		} else if !isErrHostUnreachable(err) {
			logger.Infow("failed to scan host")
		}
	}

	// check if the scan failed due to a shutdown - shouldn't be necessary but
	// just in case since recording a failed scan might have serious
	// repercussions
	select {
	case <-ctx.Done():
		return rhpv2.HostSettings{}, rhpv3.HostPriceTable{}, 0, context.Cause(ctx)
	default:
	}

	// record host scan - make sure this is interrupted by the request ctx and
	// not the context with the timeout used to time out the scan itself.
	// Otherwise scans that time out won't be recorded.
	scanErr := b.store.RecordHostScans(ctx, []api.HostScan{
		{
			HostKey:           hostKey,
			PriceTable:        pt,
			ResolvedAddresses: resolvedAddresses,

			// NOTE: A scan is considered successful if both fetching the price
			// table and the settings succeeded. Right now scanning can't fail
			// due to a reason that is our fault unless we are offline. If that
			// changes, we should adjust this code to account for that.
			Success:   err == nil,
			Settings:  settings,
			Timestamp: time.Now(),
		},
	})
	if scanErr != nil {
		logger.Errorw("failed to record host scan", zap.Error(scanErr))
	}
	logger.With(zap.Error(err)).Debugw("scanned host", "success", err == nil)
	return settings, pt, duration, err
}

func isErrHostUnreachable(err error) bool {
	return utils.IsErr(err, os.ErrDeadlineExceeded) ||
		utils.IsErr(err, context.DeadlineExceeded) ||
		utils.IsErr(err, api.ErrHostOnPrivateNetwork) ||
		utils.IsErr(err, utils.ErrNoRouteToHost) ||
		utils.IsErr(err, utils.ErrNoSuchHost) ||
		utils.IsErr(err, utils.ErrConnectionRefused) ||
		utils.IsErr(err, errors.New("unknown port")) ||
		utils.IsErr(err, errors.New("cannot assign requested address"))
}
