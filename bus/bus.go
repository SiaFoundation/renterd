package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/bus/client"
	ibus "go.sia.tech/renterd/internal/bus"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/stores/sql"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
)

const (
	defaultPinUpdateInterval = 5 * time.Minute
	defaultPinRateWindow     = 6 * time.Hour
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
	ChainManager interface {
		AddBlocks(blocks []types.Block) error
		AddPoolTransactions(txns []types.Transaction) (bool, error)
		Block(id types.BlockID) (types.Block, bool)
		OnReorg(fn func(types.ChainIndex)) (cancel func())
		PoolTransaction(txid types.TransactionID) (types.Transaction, bool)
		PoolTransactions() []types.Transaction
		RecommendedFee() types.Currency
		Tip() types.ChainIndex
		TipState() consensus.State
		UnconfirmedParents(txn types.Transaction) []types.Transaction
		UpdatesSince(index types.ChainIndex, max int) (rus []chain.RevertUpdate, aus []chain.ApplyUpdate, err error)
	}

	ChainSubscriber interface {
		ChainIndex(context.Context) (types.ChainIndex, error)
		Shutdown(context.Context) error
	}

	ChainStore interface {
		ChainIndex(ctx context.Context) (types.ChainIndex, error)
		ProcessChainUpdate(ctx context.Context, applyFn func(sql.ChainUpdateTx) error) error
	}

	// A TransactionPool can validate and relay unconfirmed transactions.
	TransactionPool interface {
		AcceptTransactionSet(txns []types.Transaction) error
		Close() error
		RecommendedFee() types.Currency
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

	PinManager interface {
		Shutdown(context.Context) error
		TriggerUpdate()
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
)

type bus struct {
	startTime time.Time

	alerts      alerts.Alerter
	alertMgr    *alerts.Manager
	pinMgr      PinManager
	webhooksMgr WebhooksManager

	cm ChainManager
	cs ChainSubscriber
	s  Syncer
	w  Wallet

	as    AutopilotStore
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

// New returns a new Bus
func New(ctx context.Context, am *alerts.Manager, wm WebhooksManager, cm ChainManager, s Syncer, w Wallet, hdb HostDB, as AutopilotStore, cs ChainStore, ms MetadataStore, ss SettingStore, eas EphemeralAccountStore, mtrcs MetricsStore, announcementMaxAge time.Duration, l *zap.Logger) (*bus, error) {
	l = l.Named("bus")
	b := &bus{
		s:                s,
		cm:               cm,
		w:                w,
		hdb:              hdb,
		as:               as,
		ms:               ms,
		mtrcs:            mtrcs,
		ss:               ss,
		eas:              eas,
		contractLocks:    newContractLocks(),
		uploadingSectors: newUploadingSectorsCache(),

		alerts:      alerts.WithOrigin(am, "bus"),
		alertMgr:    am,
		webhooksMgr: wm,
		logger:      l.Sugar(),

		startTime: time.Now(),
	}

	// init accounts
	if err := b.initAccounts(ctx); err != nil {
		return nil, err
	}

	// init settings
	if err := b.initSettings(ctx); err != nil {
		return nil, err
	}

	// create pin manager
	b.pinMgr = ibus.NewPinManager(b.alerts, wm, as, ss, defaultPinUpdateInterval, defaultPinRateWindow, l)

	// create chain subscriber
	b.cs = ibus.NewChainSubscriber(wm, cm, cs, w, announcementMaxAge, l)

	return b, nil
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
	return errors.Join(
		b.saveAccounts(ctx),
		b.webhooksMgr.Shutdown(ctx),
		b.pinMgr.Shutdown(ctx),
		b.cs.Shutdown(ctx),
	)
}

// initAccounts loads the accounts into memory
func (b *bus) initAccounts(ctx context.Context) error {
	accounts, err := b.eas.Accounts(ctx)
	if err != nil {
		return err
	}
	b.accounts = newAccounts(accounts, b.logger)

	// mark the shutdown as unclean, this will be overwritten when/if the
	// accounts are saved on shutdown
	if err := b.eas.SetUncleanShutdown(ctx); err != nil {
		return fmt.Errorf("failed to mark account shutdown as unclean: %w", err)
	}

	return nil
}

// initSettings loads the default settings if the setting is not already set and
// ensures the settings are valid
func (b *bus) initSettings(ctx context.Context) error {
	// load default settings if the setting is not already set
	for key, value := range map[string]interface{}{
		api.SettingGouging:       build.DefaultGougingSettings,
		api.SettingPricePinning:  build.DefaultPricePinSettings,
		api.SettingRedundancy:    build.DefaultRedundancySettings,
		api.SettingUploadPacking: build.DefaultUploadPackingSettings,
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
		bytes, _ := json.Marshal(build.DefaultRedundancySettings)
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
		gs.MinMaxEphemeralAccountBalance = build.DefaultGougingSettings.MinMaxEphemeralAccountBalance
		gs.MinPriceTableValidity = build.DefaultGougingSettings.MinPriceTableValidity
		gs.MinAccountExpiry = build.DefaultGougingSettings.MinAccountExpiry
		if err := gs.Validate(); err == nil {
			b.logger.Info(fmt.Sprintf("updating gouging settings with default EA settings: %+v", gs))
			bytes, _ := json.Marshal(gs)
			if err := b.ss.UpdateSetting(ctx, api.SettingGouging, string(bytes)); err != nil {
				return err
			}
		} else {
			// compat: apply default host block leeway settings
			gs.HostBlockHeightLeeway = build.DefaultGougingSettings.HostBlockHeightLeeway
			if err := gs.Validate(); err == nil {
				b.logger.Info(fmt.Sprintf("updating gouging settings with default HostBlockHeightLeeway settings: %v", gs))
				bytes, _ := json.Marshal(gs)
				if err := b.ss.UpdateSetting(ctx, api.SettingGouging, string(bytes)); err != nil {
					return err
				}
			} else {
				b.logger.Warn(fmt.Sprintf("invalid gouging setting found '%v', overwriting the gouging settings with the default settings", gss))
				bytes, _ := json.Marshal(build.DefaultGougingSettings)
				if err := b.ss.UpdateSetting(ctx, api.SettingGouging, string(bytes)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// saveAccounts saves the accounts to the db when the bus is stopped
func (b *bus) saveAccounts(ctx context.Context) error {
	accounts := b.accounts.ToPersist()
	if err := b.eas.SaveAccounts(ctx, accounts); err != nil {
		b.logger.Errorf("failed to save %v accounts: %v", len(accounts), err)
		return err
	}

	b.logger.Infof("successfully saved %v accounts", len(accounts))
	return nil
}
