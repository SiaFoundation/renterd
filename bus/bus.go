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
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	cRhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/v2/alerts"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/bus/client"
	"go.sia.tech/renterd/v2/config"
	ibus "go.sia.tech/renterd/v2/internal/bus"
	"go.sia.tech/renterd/v2/internal/gouging"
	"go.sia.tech/renterd/v2/internal/rhp"
	rhp4 "go.sia.tech/renterd/v2/internal/rhp/v4"
	"go.sia.tech/renterd/v2/internal/utils"
	"go.sia.tech/renterd/v2/object"
	"go.sia.tech/renterd/v2/stores/sql"
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
		BroadcastHeader(h types.BlockHeader) error
		BroadcastV2BlockOutline(bo gateway.V2BlockOutline) error
		Connect(ctx context.Context, addr string) (*syncer.Peer, error)
		Peers() []*syncer.Peer
	}

	Wallet interface {
		Address() types.Address
		Balance() (wallet.Balance, error)
		BroadcastTransactionSet([]types.Transaction) error
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) error
		Close() error
		FundTransaction(txn *types.Transaction, amount types.Currency, useUnconfirmed bool) ([]types.Hash256, error)
		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		RecommendedFee() types.Currency
		Redistribute(outputs int, amount, feePerByte types.Currency) (txns []types.Transaction, toSign [][]types.Hash256, err error)
		RedistributeV2(outputs int, amount, feePerByte types.Currency) (txns []types.V2Transaction, toSign [][]int, err error)
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction) error
		SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
		SpendableOutputs() ([]types.SiacoinElement, error)
		Tip() (types.ChainIndex, error)
		UnconfirmedEvents() ([]wallet.Event, error)
		UpdateChainState(tx wallet.UpdateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error
		Events(offset, limit int) ([]wallet.Event, error)
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

	// A AutopilotStore stores autopilot state.
	AutopilotStore interface {
		AutopilotConfig(ctx context.Context) (api.AutopilotConfig, error)
		InitAutopilotConfig(ctx context.Context) error
		UpdateAutopilotConfig(ctx context.Context, ap api.AutopilotConfig) error
	}

	// BackupStore is the interface of a store that can be backed up.
	BackupStore interface {
		Backup(ctx context.Context, dbID, dst string) error
	}

	// A ChainStore stores information about the chain.
	ChainStore interface {
		ChainIndex(ctx context.Context) (types.ChainIndex, error)
		FileContractElement(ctx context.Context, fcid types.FileContractID) (types.V2FileContractElement, error)
		ProcessChainUpdate(ctx context.Context, applyFn func(sql.ChainUpdateTx) error) error
		ResetChainState(ctx context.Context) error
	}

	// A HostStore stores information about hosts.
	HostStore interface {
		Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error)
		HostAllowlist(ctx context.Context) ([]types.PublicKey, error)
		HostBlocklist(ctx context.Context) ([]string, error)
		Hosts(ctx context.Context, opts api.HostOptions) ([]api.Host, error)
		RecordHostScans(ctx context.Context, scans []api.HostScan) error
		RemoveOfflineHosts(ctx context.Context, maxConsecutiveScanFailures uint64, maxDowntime time.Duration) (uint64, error)
		ResetLostSectors(ctx context.Context, hk types.PublicKey) error
		UpdateHostAllowlistEntries(ctx context.Context, add, remove []types.PublicKey, clear bool) error
		UpdateHostBlocklistEntries(ctx context.Context, add, remove []string, clear bool) error
		UpdateHostCheck(ctx context.Context, hk types.PublicKey, check api.HostChecks) error
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
		RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error
		PutContract(ctx context.Context, c api.ContractMetadata) error
		RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error)
		UpdateContractUsability(ctx context.Context, id types.FileContractID, usability string) error

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
		UpdateObject(ctx context.Context, bucketName, key, ETag, mimeType string, metadata api.ObjectUserMetadata, o object.Object) error

		AbortMultipartUpload(ctx context.Context, bucketName, key string, uploadID string) (err error)
		AddMultipartPart(ctx context.Context, bucketName, key, eTag, uploadID string, partNumber int, slices []object.SlabSlice) (err error)
		CompleteMultipartUpload(ctx context.Context, bucketName, key, uploadID string, parts []api.MultipartCompletedPart, opts api.CompleteMultipartOptions) (_ api.MultipartCompleteResponse, err error)
		CreateMultipartUpload(ctx context.Context, bucketName, key string, ec object.EncryptionKey, mimeType string, metadata api.ObjectUserMetadata) (api.MultipartCreateResponse, error)
		MultipartUpload(ctx context.Context, uploadID string) (resp api.MultipartUpload, _ error)
		MultipartUploads(ctx context.Context, bucketName, prefix, keyMarker, uploadIDMarker string, maxUploads int) (resp api.MultipartListUploadsResponse, _ error)
		MultipartUploadParts(ctx context.Context, bucketName, object string, uploadID string, marker int, limit int64) (resp api.MultipartListPartsResponse, _ error)

		MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error
		PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, limit int) ([]api.PackedSlab, error)
		SlabBuffers(ctx context.Context) ([]api.SlabBuffer, error)

		AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8) (slabs []object.SlabSlice, bufferSize int64, err error)
		FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error)
		Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error)
		SlabsForMigration(ctx context.Context, healthCutoff float64, limit int) ([]api.UnhealthySlab, error)
		RefreshHealth(ctx context.Context) error
		UpdateSlab(ctx context.Context, key object.EncryptionKey, sectors []api.UploadedSector) error
	}

	// A MetricsStore stores metrics.
	MetricsStore interface {
		ContractPruneMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractPruneMetricsQueryOpts) ([]api.ContractPruneMetric, error)
		RecordContractPruneMetric(ctx context.Context, metrics ...api.ContractPruneMetric) error

		ContractMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractMetricsQueryOpts) ([]api.ContractMetric, error)
		RecordContractMetric(ctx context.Context, metrics ...api.ContractMetric) error

		WalletMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.WalletMetricsQueryOpts) ([]api.WalletMetric, error)
		RecordWalletMetric(ctx context.Context, metrics ...api.WalletMetric) error

		PruneMetrics(ctx context.Context, metric string, cutoff time.Time) error
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

	alerts   alerts.Alerter
	alertMgr AlertManager
	pinMgr   PinManager
	cm       ChainManager
	cs       ChainSubscriber
	s        Syncer
	w        Wallet
	store    Store

	rhp4Client *rhp4.Client

	contractLocker        ContractLocker
	explorer              *ibus.Explorer
	sectors               UploadingSectorsCache
	walletMetricsRecorder WalletMetricsRecorder

	logger *zap.SugaredLogger
}

// New returns a new Bus
func New(cfg config.Bus, masterKey [32]byte, am AlertManager, cm ChainManager, s Syncer, w Wallet, store Store, explorerURL string, l *zap.Logger) (_ *Bus, err error) {
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

		alerts:   alerts.WithOrigin(am, "bus"),
		alertMgr: am,
		logger:   l.Sugar(),

		rhp4Client: rhp4.New(dialer),
	}

	// initialize autopilot config
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	err = store.InitAutopilotConfig(ctx)
	if err != nil {
		return nil, err
	}

	// create contract locker
	b.contractLocker = ibus.NewContractLocker()

	// create sectors cache
	b.sectors = ibus.NewSectorsCache()

	// create pin manager
	b.pinMgr = ibus.NewPinManager(b.alerts, b.explorer, store, defaultPinUpdateInterval, defaultPinRateWindow, l)

	// create chain subscriber
	announcementMaxAge := time.Duration(cfg.AnnouncementMaxAgeHours) * time.Hour
	b.cs = ibus.NewChainSubscriber(cm, store, w, announcementMaxAge, l)

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

		"GET    /autopilot": b.autopilotHandlerGET,
		"PUT    /autopilot": b.autopilotHandlerPUT,

		"GET    /buckets":             b.bucketsHandlerGET,
		"POST   /buckets":             b.bucketsHandlerPOST,
		"PUT    /bucket/:name/policy": b.bucketsHandlerPolicyPUT,
		"DELETE /bucket/:name":        b.bucketHandlerDELETE,
		"GET    /bucket/:name":        b.bucketHandlerGET,

		"POST   /consensus/acceptblock":        b.consensusAcceptBlock,
		"GET    /consensus/network":            b.consensusNetworkHandler,
		"GET    /consensus/siafundfee/:payout": b.consensusPayoutContractTaxHandlerGET,
		"GET    /consensus/state":              b.consensusStateHandler,

		"PUT    /contracts":             b.contractsHandlerPUT,
		"GET    /contracts":             b.contractsHandlerGET,
		"DELETE /contracts/all":         b.contractsAllHandlerDELETE,
		"POST   /contracts/archive":     b.contractsArchiveHandlerPOST,
		"POST   /contracts/form":        b.contractsFormHandler,
		"GET    /contracts/prunable":    b.contractsPrunableDataHandlerGET,
		"GET    /contracts/renewed/:id": b.contractsRenewedIDHandlerGET,
		"POST   /contracts/spending":    b.contractsSpendingHandlerPOST,

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
		"PUT    /contract/:id/usability": b.contractUsabilityHandlerPUT,

		"GET    /hosts":           b.hostsHandlerGET,
		"POST   /hosts":           b.hostsHandlerPOST,
		"GET    /hosts/allowlist": b.hostsAllowlistHandlerGET,
		"PUT    /hosts/allowlist": b.hostsAllowlistHandlerPUT,
		"GET    /hosts/blocklist": b.hostsBlocklistHandlerGET,
		"PUT    /hosts/blocklist": b.hostsBlocklistHandlerPUT,
		"POST   /hosts/remove":    b.hostsRemoveHandlerPOST,

		"GET    /host/:hostkey":                  b.hostsPubkeyHandlerGET,
		"PUT    /host/:hostkey/check":            b.hostsCheckHandlerPUT,
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

		"GET    /objects/*prefix": b.objectsHandlerGET,
		"POST   /objects/copy":    b.objectsCopyHandlerPOST,
		"POST   /objects/remove":  b.objectsRemoveHandlerPOST,
		"POST   /objects/rename":  b.objectsRenameHandlerPOST,

		"GET    /object/*key": b.objectHandlerGET,
		"PUT    /object/*key": b.objectHandlerPUT,
		"DELETE /object/*key": b.objectHandlerDELETE,

		"GET    /params/gouging": b.paramsHandlerGougingGET,
		"GET    /params/upload":  b.paramsHandlerUploadGET,

		"DELETE /sectors/:hostkey/:root": b.sectorsHostRootHandlerDELETE,

		"GET    /settings/gouging": b.settingsGougingHandlerGET,
		"PUT    /settings/gouging": b.settingsGougingHandlerPUT,
		"GET    /settings/pinned":  b.settingsPinnedHandlerGET,
		"PUT    /settings/pinned":  b.settingsPinnedHandlerPUT,
		"GET    /settings/s3":      b.settingsS3HandlerGET,
		"PUT    /settings/s3":      b.settingsS3HandlerPUT,
		"GET    /settings/upload":  b.settingsUploadHandlerGET,
		"PUT    /settings/upload":  b.settingsUploadHandlerPUT,

		"GET    /slabbuffers":      b.slabbuffersHandlerGET,
		"POST   /slabbuffer/done":  b.packedSlabsHandlerDonePOST,
		"POST   /slabbuffer/fetch": b.packedSlabsHandlerFetchPOST,

		"POST   /slabs/migration":     b.slabsMigrationHandlerPOST,
		"GET    /slabs/partial/:key":  b.slabsPartialHandlerGET,
		"POST   /slabs/partial":       b.slabsPartialHandlerPOST,
		"POST   /slabs/refreshhealth": b.slabsRefreshHealthHandlerPOST,
		"GET    /slab/:key":           b.slabHandlerGET,
		"PUT    /slab/:key":           b.slabHandlerPUT,

		"GET    /state": b.stateHandlerGET,

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
	})
}

// Shutdown shuts down the bus.
func (b *Bus) Shutdown(ctx context.Context) error {
	return errors.Join(
		b.walletMetricsRecorder.Shutdown(ctx),
		b.pinMgr.Shutdown(ctx),
		b.cs.Shutdown(ctx),
	)
}

func (b *Bus) addContract(ctx context.Context, contract api.ContractMetadata) (api.ContractMetadata, error) {
	if err := b.store.PutContract(ctx, contract); err != nil {
		return api.ContractMetadata{}, err
	}
	return b.store.Contract(ctx, contract.ID)
}

func (b *Bus) addRenewal(ctx context.Context, contract api.ContractMetadata) (api.ContractMetadata, error) {
	if err := b.store.AddRenewal(ctx, contract); err != nil {
		return api.ContractMetadata{}, fmt.Errorf("couldn't add renewal: %w", err)
	}
	return b.store.Contract(ctx, contract.ID)
}

func (b *Bus) broadcastContract(ctx context.Context, fcid types.FileContractID) (types.TransactionID, error) {
	// acquire contract lock indefinitely and defer the release
	lockID, err := b.contractLocker.Acquire(ctx, lockingPriorityRenew, fcid, time.Duration(math.MaxInt64))
	if err != nil {
		return types.TransactionID{}, fmt.Errorf("couldn't acquire contract lock; %w", err)
	}
	defer func() {
		if err := b.contractLocker.Release(fcid, lockID); err != nil {
			b.logger.Errorw("failed to release contract lock", zap.Error(err))
		}
	}()

	// fetch contract
	c, err := b.store.Contract(ctx, fcid)
	if err != nil {
		return types.TransactionID{}, fmt.Errorf("couldn't fetch contract; %w", err)
	}

	// fetch host
	host, err := b.store.Host(ctx, c.HostKey)
	if err != nil {
		return types.TransactionID{}, fmt.Errorf("couldn't fetch host; %w", err)
	}
	fee := b.w.RecommendedFee().Mul64(10e3)

	// fetch revision
	rev, err := b.rhp4Client.LatestRevision(ctx, c.HostKey, host.SiamuxAddr(), fcid)
	if err != nil {
		return types.TransactionID{}, fmt.Errorf("couldn't fetch revision; %w", err)
	}

	// fetch parent contract element
	fce, err := b.store.FileContractElement(ctx, fcid)
	if err != nil {
		return types.TransactionID{}, fmt.Errorf("couldn't fetch file contract element; %w", err)
	}

	// create the transaction
	txn := types.V2Transaction{
		MinerFee: fee,
		FileContractRevisions: []types.V2FileContractRevision{
			{
				Parent:   fce,
				Revision: rev,
			},
		},
	}

	// fund the transaction (only the fee)
	basis, toSign, err := b.w.FundV2Transaction(&txn, fee, true)
	if err != nil {
		return types.TransactionID{}, fmt.Errorf("couldn't fund transaction; %w", err)
	}
	// sign the transaction
	b.w.SignV2Inputs(&txn, toSign)

	// broadcast the transaction
	txnSet := []types.V2Transaction{txn}
	if err := b.w.BroadcastV2TransactionSet(basis, txnSet); err != nil {
		b.w.ReleaseInputs(nil, txnSet)
		return types.TransactionID{}, fmt.Errorf("couldn't broadcast transaction set; %w", err)
	}
	return txn.ID(), nil
}

func (b *Bus) formContract(ctx context.Context, hk types.PublicKey, hostIP string, hostAddr, renterAddr types.Address, prices rhpv4.HostPrices, renterFunds types.Currency, collateral types.Currency, endHeight uint64) (api.ContractMetadata, error) {
	cs := b.cm.TipState()
	key := b.masterKey.DeriveContractKey(hk)
	signer := ibus.NewFormContractSigner(b.w, key)

	// form the contract
	res, err := b.rhp4Client.FormContract(ctx, hk, hostIP, b.cm, signer, cs, prices, hostAddr, rhpv4.RPCFormContractParams{
		RenterPublicKey: key.PublicKey(),
		RenterAddress:   renterAddr,
		Allowance:       renterFunds,
		Collateral:      collateral,
		ProofHeight:     endHeight,
	})
	if err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to form contract: %w", err)
	} else if err = b.w.BroadcastV2TransactionSet(res.FormationSet.Basis, res.FormationSet.Transactions); err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to broadcast transaction set: %w", err)
	}

	contract := res.Contract
	return api.ContractMetadata{
		ID:                 contract.ID,
		HostKey:            contract.Revision.HostPublicKey,
		StartHeight:        cs.Index.Height,
		State:              api.ContractStatePending,
		WindowStart:        contract.Revision.ProofHeight,
		WindowEnd:          contract.Revision.ProofHeight + rhpv4.ProofWindow,
		ContractPrice:      res.Usage.RenterCost(),
		InitialRenterFunds: renterFunds,
		Usability:          api.ContractUsabilityGood,
	}, nil
}

func (b *Bus) refreshContract(ctx context.Context, cs consensus.State, h api.Host, gp api.GougingParams, c api.ContractMetadata, renterFunds, minNewCollateral types.Currency) (api.ContractMetadata, error) {
	// derive the renter key
	renterKey := b.masterKey.DeriveContractKey(c.HostKey)
	signer := ibus.NewFormContractSigner(b.w, renterKey)

	// fetch the revision
	rev, err := b.rhp4Client.LatestRevision(ctx, h.PublicKey, h.SiamuxAddr(), c.ID)
	if err != nil {
		return api.ContractMetadata{}, err
	}

	// fetch prices and check them
	settings, err := b.rhp4Client.Settings(ctx, h.PublicKey, h.SiamuxAddr())
	if err != nil {
		return api.ContractMetadata{}, err
	}
	gc := gouging.NewChecker(gp.GougingSettings, gp.ConsensusState)
	if gb := gc.Check(settings); gb.Gouging() {
		return api.ContractMetadata{}, errors.New(gb.String())
	}
	prices := settings.Prices

	// raise the renterFunds if they are too low to reach minNewCollateral
	if minRenterFunds := rhpv4.MinRenterAllowance(prices, minNewCollateral); renterFunds.Cmp(minRenterFunds) < 0 {
		renterFunds = minRenterFunds
	}

	// target the max collateral the host is willing to accept for the renterFunds
	collateral := rhpv4.MaxHostCollateral(prices, renterFunds)

	// cap the collateral at the per-contract MaxCollateral of the host
	totalCollateral := collateral.Add(rev.TotalCollateral)
	if totalCollateral.Cmp(settings.MaxCollateral) > 0 {
		excess := totalCollateral.Sub(settings.MaxCollateral)
		if excess.Cmp(collateral) > 0 {
			collateral = types.ZeroCurrency
		} else {
			collateral = collateral.Sub(excess)
		}
	}

	// make sure the new remaining collateral is at least the minimum
	if collateral.Cmp(minNewCollateral) < 0 {
		return api.ContractMetadata{}, fmt.Errorf("new collateral %v is less than minimum %v (max: %v)", collateral, minNewCollateral, settings.MaxCollateral)
	}

	var res cRhp4.RPCRefreshContractResult
	res, err = b.rhp4Client.RefreshContract(ctx, h.PublicKey, h.SiamuxAddr(), b.cm, signer, cs, settings.Prices, rev, rhpv4.RPCRefreshContractParams{
		ContractID: c.ID,
		Allowance:  renterFunds,
		Collateral: collateral,
	})
	if err != nil {
		return api.ContractMetadata{}, err
	} else if err = b.w.BroadcastV2TransactionSet(res.RenewalSet.Basis, res.RenewalSet.Transactions); err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to broadcast transaction set: %w", err)
	}
	contract := res.Contract

	return api.ContractMetadata{
		ID:                 contract.ID,
		HostKey:            h.PublicKey,
		RenewedFrom:        c.ID,
		StartHeight:        cs.Index.Height,
		State:              api.ContractStatePending,
		WindowStart:        contract.Revision.ProofHeight,
		WindowEnd:          contract.Revision.ExpirationHeight,
		ContractPrice:      settings.Prices.ContractPrice,
		InitialRenterFunds: contract.Revision.RenterOutput.Value,
		Usability:          api.ContractUsabilityGood,
	}, nil
}

func (b *Bus) renewContract(ctx context.Context, cs consensus.State, h api.Host, gp api.GougingParams, c api.ContractMetadata, renterFunds types.Currency, endHeight uint64) (api.ContractMetadata, error) {
	// derive the renter key
	renterKey := b.masterKey.DeriveContractKey(c.HostKey)
	signer := ibus.NewFormContractSigner(b.w, renterKey)

	// fetch the revision
	rev, err := b.rhp4Client.LatestRevision(ctx, h.PublicKey, h.SiamuxAddr(), c.ID)
	if err != nil {
		return api.ContractMetadata{}, err
	}

	// fetch prices and check them
	settings, err := b.rhp4Client.Settings(ctx, h.PublicKey, h.SiamuxAddr())
	if err != nil {
		return api.ContractMetadata{}, err
	}
	gc := gouging.NewChecker(gp.GougingSettings, gp.ConsensusState)
	if gb := gc.Check(settings); gb.Gouging() {
		return api.ContractMetadata{}, errors.New(gb.String())
	}
	prices := settings.Prices

	// target the max collateral the host is willing to accept for the renterFunds
	collateral := rhpv4.MaxHostCollateral(prices, renterFunds)

	// cap the collateral at the per-contract MaxCollateral of the host
	riskedCollateral := prices.Collateral.Mul64(rev.Filesize).Mul64(rev.ExpirationHeight - prices.TipHeight)
	totalCollateral := collateral.Add(riskedCollateral)
	if totalCollateral.Cmp(settings.MaxCollateral) > 0 {
		excess := totalCollateral.Sub(settings.MaxCollateral)
		if excess.Cmp(collateral) > 0 {
			collateral = types.ZeroCurrency
		} else {
			collateral = collateral.Sub(excess)
		}
	}

	var res cRhp4.RPCRenewContractResult
	res, err = b.rhp4Client.RenewContract(ctx, h.PublicKey, h.SiamuxAddr(), b.cm, signer, cs, settings.Prices, rev, rhpv4.RPCRenewContractParams{
		ContractID:  c.ID,
		Allowance:   renterFunds,
		Collateral:  collateral,
		ProofHeight: endHeight,
	})
	if err != nil {
		return api.ContractMetadata{}, err
	} else if err = b.w.BroadcastV2TransactionSet(res.RenewalSet.Basis, res.RenewalSet.Transactions); err != nil {
		return api.ContractMetadata{}, fmt.Errorf("failed to broadcast transaction set: %w", err)
	}
	contract := res.Contract

	return api.ContractMetadata{
		ID:                 contract.ID,
		HostKey:            h.PublicKey,
		RenewedFrom:        c.ID,
		StartHeight:        cs.Index.Height,
		State:              api.ContractStatePending,
		WindowStart:        contract.Revision.ProofHeight,
		WindowEnd:          contract.Revision.ExpirationHeight,
		ContractPrice:      settings.Prices.ContractPrice,
		InitialRenterFunds: contract.Revision.RenterOutput.Value,
		Usability:          api.ContractUsabilityGood,
	}, nil
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
