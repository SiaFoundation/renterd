package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gotd/contrib/http_range"
	rhpv3 "go.sia.tech/core/rhp/v3"
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/prices"
	"go.sia.tech/renterd/internal/rhp"
	rhp2 "go.sia.tech/renterd/internal/rhp/v2"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	rhp4 "go.sia.tech/renterd/internal/rhp/v4"
	"go.sia.tech/renterd/internal/utils"
	iworker "go.sia.tech/renterd/internal/worker"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/renterd/worker/client"
	"go.uber.org/zap"
)

const (
	defaultRevisionFetchTimeout = 30 * time.Second

	lockingPrioritySyncing                = 30
	lockingPriorityActiveContractRevision = 100
)

var (
	ErrShuttingDown = errors.New("worker is shutting down")
)

// re-export the client
type Client struct {
	*client.Client
}

func NewClient(address, password string) *Client {
	return &Client{
		Client: client.New(address, password),
	}
}

type (
	Bus interface {
		alerts.Alerter
		gouging.ConsensusState
		webhooks.Broadcaster

		AccountFunder
		iworker.AccountStore

		ContractLocker
		ContractStore
		HostStore
		ObjectStore
		SettingStore
		WebhookStore

		Syncer
	}

	AccountFunder interface {
		FundAccount(ctx context.Context, account rhpv3.Account, fcid types.FileContractID, amount types.Currency) (types.Currency, error)
	}

	ContractStore interface {
		Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
		ContractSize(ctx context.Context, id types.FileContractID) (api.ContractSize, error)
		ContractRoots(ctx context.Context, id types.FileContractID) ([]types.Hash256, error)
		Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error)
		RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error)
	}

	HostStore interface {
		RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error

		Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error)
		UsableHosts(ctx context.Context) ([]api.HostInfo, error)
	}

	ObjectStore interface {
		// NOTE: used for download
		DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) error
		FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error)
		Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error)

		// NOTE: used for upload
		AddObject(ctx context.Context, bucket, key, contractSet string, o object.Object, opts api.AddObjectOptions) error
		AddMultipartPart(ctx context.Context, bucket, key, contractSet, ETag, uploadID string, partNumber int, slices []object.SlabSlice) (err error)
		AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet string) (slabs []object.SlabSlice, slabBufferMaxSizeSoftReached bool, err error)
		AddUploadingSectors(ctx context.Context, uID api.UploadID, root []types.Hash256) error
		FinishUpload(ctx context.Context, uID api.UploadID) error
		Objects(ctx context.Context, prefix string, opts api.ListObjectOptions) (resp api.ObjectsResponse, err error)
		MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error
		TrackUpload(ctx context.Context, uID api.UploadID) error
		UpdateSlab(ctx context.Context, key object.EncryptionKey, sectors []api.UploadedSector) error

		// NOTE: used by worker
		Bucket(_ context.Context, bucket string) (api.Bucket, error)
		Object(ctx context.Context, bucket, key string, opts api.GetObjectOptions) (api.Object, error)
		DeleteObject(ctx context.Context, bucket, key string) error
		MultipartUpload(ctx context.Context, uploadID string) (resp api.MultipartUpload, err error)
		PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) ([]api.PackedSlab, error)
		RemoveObjects(ctx context.Context, bucket, prefix string) error
	}

	SettingStore interface {
		GougingParams(ctx context.Context) (api.GougingParams, error)
		UploadParams(ctx context.Context) (api.UploadParams, error)
	}

	Syncer interface {
		BroadcastTransaction(ctx context.Context, txns []types.Transaction) error
		SyncerPeers(ctx context.Context) (resp []string, err error)
	}

	WebhookStore interface {
		RegisterWebhook(ctx context.Context, webhook webhooks.Webhook) error
		UnregisterWebhook(ctx context.Context, webhook webhooks.Webhook) error
	}
)

// TODO: deriving the renter key from the host key using the master key only
// works if we persist a hash of the renter's master key in the database and
// compare it on startup, otherwise there's no way of knowing the derived key is
// usuable
// NOTE: Instead of hashing the masterkey and comparing, we could use random
// bytes + the HMAC thereof as the salt. e.g. 32 bytes + 32 bytes HMAC. Then
// whenever we read a specific salt we can verify that is was created with a
// given key. That would eventually allow different masterkeys to coexist in the
// same bus.
//
// TODO: instead of deriving a renter key use a randomly generated salt so we're
// not limited to one key per host
func (w *Worker) deriveRenterKey(hostKey types.PublicKey) types.PrivateKey {
	return w.masterKey.DeriveContractKey(hostKey)
}

// A worker talks to Sia hosts to perform contract and storage operations within
// a renterd system.
type Worker struct {
	alerts alerts.Alerter

	rhp2Client *rhp2.Client
	rhp3Client *rhp3.Client
	rhp4Client *rhp4.Client

	id        string
	bus       Bus
	masterKey utils.MasterKey
	startTime time.Time

	downloadManager *downloadManager
	uploadManager   *uploadManager

	accounts    *iworker.AccountMgr
	dialer      *rhp.FallbackDialer
	cache       iworker.WorkerCache
	priceTables *prices.PriceTables
	pricesCache *prices.PricesCache

	uploadsMu            sync.Mutex
	uploadingPackedSlabs map[string]struct{}

	contractSpendingRecorder ContractSpendingRecorder

	shutdownCtx       context.Context
	shutdownCtxCancel context.CancelFunc

	logger *zap.SugaredLogger
}

func (w *Worker) isStopped() bool {
	select {
	case <-w.shutdownCtx.Done():
		return true
	default:
	}
	return false
}

func (w *Worker) withRevision(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, siamuxAddr string, fetchTimeout time.Duration, lockPriority int, fn func(rev types.FileContractRevision) error) error {
	return w.withContractLock(ctx, fcid, lockPriority, func() error {
		rev, err := w.Host(hk, fcid, siamuxAddr).FetchRevision(ctx, fetchTimeout)
		if err != nil {
			return err
		}
		return fn(rev)
	})
}

func (w *Worker) registerAlert(a alerts.Alert) {
	ctx, cancel := context.WithTimeout(w.shutdownCtx, time.Minute)
	if err := w.alerts.RegisterAlert(ctx, a); err != nil {
		w.logger.Errorf("failed to register alert, err: %v", err)
	}
	cancel()
}

func (w *Worker) slabMigrateHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode the slab
	var slab object.Slab
	if jc.Decode(&slab) != nil {
		return
	}

	// fetch the upload parameters
	up, err := w.bus.UploadParams(ctx)
	if jc.Check("couldn't fetch upload parameters from bus", err) != nil {
		return
	}

	// NOTE: migrations do not use the default contract set but instead require
	// the user to specify the contract set through the query string parameter,
	// this to avoid accidentally migration to the default set if the autopilot
	// configuration is missing a contract set
	up.ContractSet = ""

	// decode the contract set from the query string
	var contractset string
	if jc.DecodeForm("contractset", &contractset) != nil {
		return
	} else if contractset != "" {
		up.ContractSet = contractset
	}

	// cancel the migration if no contract set is specified
	if up.ContractSet == "" {
		jc.Error(fmt.Errorf("migrations require the contract set to be passed as a query string parameter; %w", api.ErrContractSetNotSpecified), http.StatusBadRequest)
		return
	}

	// cancel the upload if consensus is not synced
	if !up.ConsensusState.Synced {
		w.logger.Errorf("migration cancelled, err: %v", api.ErrConsensusNotSynced)
		jc.Error(api.ErrConsensusNotSynced, http.StatusServiceUnavailable)
		return
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// fetch hosts
	dlHosts, err := w.cache.UsableHosts(ctx)
	if jc.Check("couldn't fetch hosts from bus", err) != nil {
		return
	}

	// fetch contracts
	ulContracts, err := w.bus.Contracts(ctx, api.ContractsOpts{ContractSet: up.ContractSet})
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	// migrate the slab and handle alerts
	err = w.migrate(ctx, slab, up.ContractSet, dlHosts, ulContracts, up.CurrentHeight)
	if err != nil && !utils.IsErr(err, api.ErrSlabNotFound) {
		var objects []api.ObjectMetadata
		if res, err := w.bus.Objects(ctx, "", api.ListObjectOptions{SlabEncryptionKey: slab.EncryptionKey}); err != nil {
			w.logger.Errorf("failed to list objects for slab key; %v", err)
		} else {
			objects = res.Objects
		}
		w.alerts.RegisterAlert(ctx, newMigrationFailedAlert(slab.EncryptionKey, slab.Health, objects, err))
	} else if err == nil {
		w.alerts.DismissAlerts(jc.Request.Context(), alerts.IDForSlab(alertMigrationID, slab.EncryptionKey))
	}

	jc.Check("failed to migrate slab", err)
}

func (w *Worker) downloadsStatsHandlerGET(jc jape.Context) {
	stats := w.downloadManager.Stats()

	// prepare downloaders stats
	var healthy uint64
	var dss []api.DownloaderStats
	for hk, stat := range stats.downloaders {
		if stat.healthy {
			healthy++
		}
		dss = append(dss, api.DownloaderStats{
			HostKey:                    hk,
			AvgSectorDownloadSpeedMBPS: stat.avgSpeedMBPS,
			NumDownloads:               stat.numDownloads,
		})
	}
	sort.SliceStable(dss, func(i, j int) bool {
		return dss[i].AvgSectorDownloadSpeedMBPS > dss[j].AvgSectorDownloadSpeedMBPS
	})

	// encode response
	api.WriteResponse(jc, api.DownloadStatsResponse{
		AvgDownloadSpeedMBPS: math.Ceil(stats.avgDownloadSpeedMBPS*100) / 100,
		AvgOverdrivePct:      math.Floor(stats.avgOverdrivePct*100*100) / 100,
		HealthyDownloaders:   healthy,
		NumDownloaders:       uint64(len(stats.downloaders)),
		DownloadersStats:     dss,
	})
}

func (w *Worker) uploadsStatsHandlerGET(jc jape.Context) {
	stats := w.uploadManager.Stats()

	// prepare upload stats
	var uss []api.UploaderStats
	for hk, mbps := range stats.uploadSpeedsMBPS {
		uss = append(uss, api.UploaderStats{
			HostKey:                  hk,
			AvgSectorUploadSpeedMBPS: mbps,
		})
	}
	sort.SliceStable(uss, func(i, j int) bool {
		return uss[i].AvgSectorUploadSpeedMBPS > uss[j].AvgSectorUploadSpeedMBPS
	})

	// encode response
	api.WriteResponse(jc, api.UploadStatsResponse{
		AvgSlabUploadSpeedMBPS: math.Ceil(stats.avgSlabUploadSpeedMBPS*100) / 100,
		AvgOverdrivePct:        math.Floor(stats.avgOverdrivePct*100*100) / 100,
		HealthyUploaders:       stats.healthyUploaders,
		NumUploaders:           stats.numUploaders,
		UploadersStats:         uss,
	})
}

func (w *Worker) objectHandlerHEAD(jc jape.Context) {
	// parse bucket
	var bucket string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	} else if bucket == "" {
		jc.Error(api.ErrBucketMissing, http.StatusBadRequest)
		return
	}

	// parse range
	dr, err := api.ParseDownloadRange(jc.Request)
	if errors.Is(err, http_range.ErrInvalid) || errors.Is(err, api.ErrMultiRangeNotSupported) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if errors.Is(err, http_range.ErrNoOverlap) {
		jc.Error(err, http.StatusRequestedRangeNotSatisfiable)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	// parse key
	path := jc.PathParam("key")

	// fetch object metadata
	hor, err := w.HeadObject(jc.Request.Context(), bucket, path, api.HeadObjectOptions{
		Range: &dr,
	})
	if utils.IsErr(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if errors.Is(err, http_range.ErrInvalid) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if jc.Check("couldn't get object", err) != nil {
		return
	}

	// serve the content to ensure we're setting the exact same headers as we
	// would for a GET request
	serveContent(jc.ResponseWriter, jc.Request, path, bytes.NewReader(nil), *hor)
}

func (w *Worker) objectHandlerGET(jc jape.Context) {
	jc.Custom(nil, []api.ObjectMetadata{})

	ctx := jc.Request.Context()

	var bucket string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	} else if bucket == "" {
		jc.Error(api.ErrBucketMissing, http.StatusBadRequest)
		return
	}

	var prefix string
	if jc.DecodeForm("prefix", &prefix) != nil {
		return
	}
	var sortBy string
	if jc.DecodeForm("sortby", &sortBy) != nil {
		return
	}
	var sortDir string
	if jc.DecodeForm("sortdir", &sortDir) != nil {
		return
	}
	var marker string
	if jc.DecodeForm("marker", &marker) != nil {
		return
	}
	var ignoreDelim bool
	if jc.DecodeForm("ignoredelim", &ignoreDelim) != nil {
		return
	}

	key := jc.PathParam("key")
	if key == "" {
		jc.Error(errors.New("no path provided"), http.StatusBadRequest)
		return
	}

	dr, err := api.ParseDownloadRange(jc.Request)
	if errors.Is(err, http_range.ErrInvalid) || errors.Is(err, api.ErrMultiRangeNotSupported) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if errors.Is(err, http_range.ErrNoOverlap) {
		jc.Error(err, http.StatusRequestedRangeNotSatisfiable)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	gor, err := w.GetObject(ctx, bucket, key, api.DownloadObjectOptions{
		Range: &dr,
	})
	if utils.IsErr(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if errors.Is(err, http_range.ErrInvalid) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if jc.Check("couldn't get object", err) != nil {
		return
	}
	defer gor.Content.Close()

	// serve the content
	serveContent(jc.ResponseWriter, jc.Request, key, gor.Content, gor.HeadObjectResponse)
}

func (w *Worker) objectHandlerPUT(jc jape.Context) {
	jc.Custom((*[]byte)(nil), nil)
	ctx := jc.Request.Context()

	// grab the path
	path := jc.PathParam("key")

	// decode the contract set from the query string
	var contractset string
	if jc.DecodeForm("contractset", &contractset) != nil {
		return
	}

	// decode the mimetype from the query string
	var mimeType string
	if jc.DecodeForm("mimetype", &mimeType) != nil {
		return
	}

	// decode the bucket from the query string
	var bucket string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	} else if bucket == "" {
		jc.Error(api.ErrBucketMissing, http.StatusBadRequest)
		return
	}

	// allow overriding the redundancy settings
	var minShards, totalShards int
	if jc.DecodeForm("minshards", &minShards) != nil {
		return
	}
	if jc.DecodeForm("totalshards", &totalShards) != nil {
		return
	}

	// parse headers and extract object meta
	metadata := make(api.ObjectUserMetadata)
	for k, v := range jc.Request.Header {
		if strings.HasPrefix(strings.ToLower(k), strings.ToLower(api.ObjectMetadataPrefix)) && len(v) > 0 {
			metadata[k[len(api.ObjectMetadataPrefix):]] = v[0]
		}
	}

	// upload the object
	resp, err := w.UploadObject(ctx, jc.Request.Body, bucket, path, api.UploadObjectOptions{
		MinShards:     minShards,
		TotalShards:   totalShards,
		ContractSet:   contractset,
		ContentLength: jc.Request.ContentLength,
		MimeType:      mimeType,
		Metadata:      metadata,
	})
	if utils.IsErr(err, api.ErrInvalidRedundancySettings) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if utils.IsErr(err, api.ErrBucketNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if utils.IsErr(err, api.ErrContractSetNotSpecified) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if utils.IsErr(err, api.ErrConsensusNotSynced) {
		jc.Error(err, http.StatusServiceUnavailable)
		return
	} else if jc.Check("couldn't upload object", err) != nil {
		return
	}

	// set etag header
	jc.ResponseWriter.Header().Set("ETag", api.FormatETag(resp.ETag))
}

func (w *Worker) multipartUploadHandlerPUT(jc jape.Context) {
	jc.Custom((*[]byte)(nil), nil)
	ctx := jc.Request.Context()

	// grab the path
	path := jc.PathParam("key")

	// decode the contract set from the query string
	var contractset string
	if jc.DecodeForm("contractset", &contractset) != nil {
		return
	}

	// decode the bucket from the query string
	var bucket string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	} else if bucket == "" {
		jc.Error(api.ErrBucketMissing, http.StatusBadRequest)
		return
	}

	// decode the upload id
	var uploadID string
	if jc.DecodeForm("uploadid", &uploadID) != nil {
		return
	} else if uploadID == "" {
		jc.Error(errors.New("upload id not specified"), http.StatusBadRequest)
		return
	}

	// decode the part number
	var partNumber int
	if jc.DecodeForm("partnumber", &partNumber) != nil {
		return
	}

	// allow overriding the redundancy settings
	var minShards, totalShards int
	if jc.DecodeForm("minshards", &minShards) != nil {
		return
	}
	if jc.DecodeForm("totalshards", &totalShards) != nil {
		return
	}

	// prepare options
	opts := api.UploadMultipartUploadPartOptions{
		ContractSet:      contractset,
		MinShards:        minShards,
		TotalShards:      totalShards,
		EncryptionOffset: nil,
		ContentLength:    jc.Request.ContentLength,
	}

	// get the encryption offset
	var encryptionOffset int
	if jc.DecodeForm("encryptionoffset", &encryptionOffset) != nil {
		return
	} else if jc.Request.FormValue("encryptionoffset") != "" {
		opts.EncryptionOffset = &encryptionOffset
	}

	// upload the multipart
	resp, err := w.UploadMultipartUploadPart(ctx, jc.Request.Body, bucket, path, uploadID, partNumber, opts)
	if utils.IsErr(err, api.ErrInvalidRedundancySettings) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if utils.IsErr(err, api.ErrBucketNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if utils.IsErr(err, api.ErrContractSetNotSpecified) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if utils.IsErr(err, api.ErrConsensusNotSynced) {
		jc.Error(err, http.StatusServiceUnavailable)
		return
	} else if utils.IsErr(err, api.ErrMultipartUploadNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if utils.IsErr(err, api.ErrInvalidMultipartEncryptionSettings) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if jc.Check("couldn't upload multipart part", err) != nil {
		return
	}

	// set etag header
	jc.ResponseWriter.Header().Set("ETag", api.FormatETag(resp.ETag))
}

func (w *Worker) objectHandlerDELETE(jc jape.Context) {
	var bucket string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	err := w.bus.DeleteObject(jc.Request.Context(), bucket, jc.PathParam("key"))
	if utils.IsErr(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	jc.Check("couldn't delete object", err)
}

func (w *Worker) objectsRemoveHandlerPOST(jc jape.Context) {
	var orr api.ObjectsRemoveRequest
	if jc.Decode(&orr) != nil {
		return
	} else if orr.Bucket == "" {
		jc.Error(api.ErrBucketMissing, http.StatusBadRequest)
		return
	}

	if orr.Prefix == "" {
		jc.Error(errors.New("prefix cannot be empty"), http.StatusBadRequest)
		return
	}

	jc.Check("couldn't remove objects", w.bus.RemoveObjects(jc.Request.Context(), orr.Bucket, orr.Prefix))
}

func (w *Worker) idHandlerGET(jc jape.Context) {
	jc.Encode(w.id)
}

func (w *Worker) memoryGET(jc jape.Context) {
	api.WriteResponse(jc, api.MemoryResponse{
		Download: w.downloadManager.mm.Status(),
		Upload:   w.uploadManager.mm.Status(),
	})
}

func (w *Worker) accountHandlerGET(jc jape.Context) {
	var hostKey types.PublicKey
	if jc.DecodeParam("hostkey", &hostKey) != nil {
		return
	}
	jc.Encode(w.accounts.Account(hostKey))
}

func (w *Worker) accountsHandlerGET(jc jape.Context) {
	jc.Encode(w.accounts.Accounts())
}

func (w *Worker) accountsResetDriftHandlerPOST(jc jape.Context) {
	var id rhpv3.Account
	if jc.DecodeParam("id", &id) != nil {
		return
	}
	err := w.accounts.ResetDrift(id)
	if errors.Is(err, iworker.ErrAccountNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	if jc.Check("failed to reset drift", err) != nil {
		return
	}
}

func (w *Worker) stateHandlerGET(jc jape.Context) {
	jc.Encode(api.WorkerStateResponse{
		ID:        w.id,
		StartTime: api.TimeRFC3339(w.startTime),
		BuildState: api.BuildState{
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: api.TimeRFC3339(build.BuildTime()),
		},
	})
}

// New returns an HTTP handler that serves the worker API.
func New(cfg config.Worker, masterKey [32]byte, b Bus, l *zap.Logger) (*Worker, error) {
	if cfg.ID == "" {
		return nil, errors.New("worker ID cannot be empty")
	}

	l = l.Named("worker").Named(cfg.ID)

	if cfg.BusFlushInterval == 0 {
		return nil, errors.New("bus flush interval must be positive")
	}
	if cfg.DownloadOverdriveTimeout == 0 {
		return nil, errors.New("download overdrive timeout must be positive")
	}
	if cfg.UploadOverdriveTimeout == 0 {
		return nil, errors.New("upload overdrive timeout must be positive")
	}
	if cfg.DownloadMaxMemory == 0 {
		return nil, errors.New("downloadMaxMemory cannot be 0")
	}
	if cfg.UploadMaxMemory == 0 {
		return nil, errors.New("uploadMaxMemory cannot be 0")
	}

	a := alerts.WithOrigin(b, fmt.Sprintf("worker.%s", cfg.ID))
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	dialer := rhp.NewFallbackDialer(b, net.Dialer{}, l)
	w := &Worker{
		alerts:               a,
		cache:                iworker.NewCache(b, l),
		dialer:               dialer,
		id:                   cfg.ID,
		bus:                  b,
		masterKey:            masterKey,
		logger:               l.Sugar(),
		priceTables:          prices.NewPriceTables(),
		pricesCache:          prices.NewPricesCache(),
		rhp2Client:           rhp2.New(dialer, l),
		rhp3Client:           rhp3.New(dialer, l),
		rhp4Client:           rhp4.New(dialer),
		startTime:            time.Now(),
		uploadingPackedSlabs: make(map[string]struct{}),
		shutdownCtx:          shutdownCtx,
		shutdownCtxCancel:    shutdownCancel,
	}

	if err := w.initAccounts(cfg.AccountsRefillInterval); err != nil {
		return nil, fmt.Errorf("failed to initialize accounts; %w", err)
	}

	uploadKey := w.masterKey.DeriveUploadKey()
	w.initDownloadManager(&uploadKey, cfg.DownloadMaxMemory, cfg.DownloadMaxOverdrive, cfg.DownloadOverdriveTimeout, l)
	w.initUploadManager(&uploadKey, cfg.UploadMaxMemory, cfg.UploadMaxOverdrive, cfg.UploadOverdriveTimeout, l)

	w.initContractSpendingRecorder(cfg.BusFlushInterval)
	return w, nil
}

// Handler returns an HTTP handler that serves the worker API.
func (w *Worker) Handler() http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"GET    /accounts":               w.accountsHandlerGET,
		"GET    /account/:hostkey":       w.accountHandlerGET,
		"POST   /account/:id/resetdrift": w.accountsResetDriftHandlerPOST,
		"GET    /id":                     w.idHandlerGET,

		"GET    /memory": w.memoryGET,

		"GET    /stats/downloads": w.downloadsStatsHandlerGET,
		"GET    /stats/uploads":   w.uploadsStatsHandlerGET,
		"POST   /slab/migrate":    w.slabMigrateHandler,

		"HEAD   /object/*key":    w.objectHandlerHEAD,
		"GET    /object/*key":    w.objectHandlerGET,
		"PUT    /object/*key":    w.objectHandlerPUT,
		"DELETE /object/*key":    w.objectHandlerDELETE,
		"POST   /objects/remove": w.objectsRemoveHandlerPOST,

		"PUT    /multipart/*key": w.multipartUploadHandlerPUT,

		"GET    /state": w.stateHandlerGET,
	})
}

// Shutdown shuts down the worker.
func (w *Worker) Shutdown(ctx context.Context) error {
	// cancel shutdown context
	w.shutdownCtxCancel()

	// stop uploads and downloads
	w.downloadManager.Stop()
	w.uploadManager.Stop()

	// stop account manager
	w.accounts.Shutdown(ctx)

	// stop recorders
	w.contractSpendingRecorder.Stop(ctx)

	return nil
}

func (w *Worker) headObject(ctx context.Context, bucket, key string, onlyMetadata bool, opts api.HeadObjectOptions) (*api.HeadObjectResponse, api.Object, error) {
	// fetch object
	res, err := w.bus.Object(ctx, bucket, key, api.GetObjectOptions{
		OnlyMetadata: onlyMetadata,
	})
	if err != nil {
		return nil, api.Object{}, fmt.Errorf("couldn't fetch object: %w", err)
	}

	// adjust length
	if opts.Range == nil {
		opts.Range = &api.DownloadRange{Offset: 0, Length: -1}
	}
	if opts.Range.Length == -1 {
		opts.Range.Length = res.Size - opts.Range.Offset
	}

	// check size of object against range
	if opts.Range.Offset+opts.Range.Length > res.Size {
		return nil, api.Object{}, http_range.ErrInvalid
	}

	return &api.HeadObjectResponse{
		ContentType:  res.MimeType,
		Etag:         res.ETag,
		LastModified: res.ModTime,
		Range:        opts.Range.ContentRange(res.Size),
		Size:         res.Size,
		Metadata:     res.Metadata,
	}, res, nil
}

func (w *Worker) FundAccount(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, desired types.Currency) error {
	// calculate the deposit amount
	acc := w.accounts.ForHost(hk)
	return acc.WithDeposit(func(balance types.Currency) (types.Currency, error) {
		// return early if we have the desired balance
		if balance.Cmp(desired) >= 0 {
			return types.ZeroCurrency, nil
		}
		deposit := desired.Sub(balance)

		// fund the account
		var err error
		deposit, err = w.bus.FundAccount(ctx, acc.ID(), fcid, desired.Sub(balance))
		if err != nil {
			if rhp3.IsBalanceMaxExceeded(err) {
				acc.ScheduleSync()
			}
			return types.ZeroCurrency, fmt.Errorf("failed to fund account with %v; %w", deposit, err)
		}

		// log the account balance after funding
		w.logger.Debugw("fund account succeeded",
			"balance", balance.ExactString(),
			"deposit", deposit.ExactString(),
		)
		return deposit, nil
	})
}

func (w *Worker) GetObject(ctx context.Context, bucket, key string, opts api.DownloadObjectOptions) (*api.GetObjectResponse, error) {
	// head object
	hor, res, err := w.headObject(ctx, bucket, key, false, api.HeadObjectOptions{
		Range: opts.Range,
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't fetch object: %w", err)
	}
	obj := *res.Object

	// adjust range
	if opts.Range == nil {
		opts.Range = &api.DownloadRange{}
	}
	opts.Range.Offset = hor.Range.Offset
	opts.Range.Length = hor.Range.Length

	// fetch gouging params
	gp, err := w.bus.GougingParams(ctx)
	if err != nil {
		return nil, fmt.Errorf("couldn't fetch gouging parameters from bus: %w", err)
	}

	// fetch usable hosts
	hosts, err := w.cache.UsableHosts(ctx)
	if err != nil {
		return nil, fmt.Errorf("couldn't fetch contracts from bus: %w", err)
	}

	// prepare the content
	var content io.ReadCloser
	if opts.Range.Length == 0 || obj.TotalSize() == 0 {
		// if the object has no content or the requested range is 0, return an
		// empty reader
		content = io.NopCloser(bytes.NewReader(nil))
	} else {
		// otherwise return a pipe reader
		downloadFn := func(wr io.Writer, offset, length int64) error {
			ctx = WithGougingChecker(ctx, w.bus, gp)
			err = w.downloadManager.DownloadObject(ctx, wr, obj, uint64(offset), uint64(length), hosts)
			if err != nil {
				w.logger.Error(err)
				if !errors.Is(err, ErrShuttingDown) &&
					!errors.Is(err, errDownloadCancelled) &&
					!errors.Is(err, io.ErrClosedPipe) {
					w.registerAlert(newDownloadFailedAlert(bucket, key, offset, length, int64(len(hosts)), err))
				}
				return fmt.Errorf("failed to download object: %w", err)
			}
			return nil
		}
		pr, pw := io.Pipe()
		go func() {
			err := downloadFn(pw, opts.Range.Offset, opts.Range.Length)
			pw.CloseWithError(err)
		}()
		content = pr
	}

	return &api.GetObjectResponse{
		Content:            content,
		HeadObjectResponse: *hor,
	}, nil
}

func (w *Worker) HeadObject(ctx context.Context, bucket, key string, opts api.HeadObjectOptions) (*api.HeadObjectResponse, error) {
	res, _, err := w.headObject(ctx, bucket, key, true, opts)
	return res, err
}

func (w *Worker) SyncAccount(ctx context.Context, fcid types.FileContractID, host api.HostInfo) error {
	// handle v2 host
	if host.IsV2() {
		account := w.accounts.ForHost(host.PublicKey)
		return account.WithSync(func() (types.Currency, error) {
			return w.rhp4Client.AccountBalance(ctx, host.PublicKey, host.V2SiamuxAddr(), rhpv4.Account(account.ID()))
		})
	}

	// attach gouging checker
	gp, err := w.bus.GougingParams(ctx)
	if err != nil {
		return fmt.Errorf("couldn't get gouging parameters; %w", err)
	}
	ctx = WithGougingChecker(ctx, w.bus, gp)

	// sync the account
	h := w.Host(host.PublicKey, fcid, host.SiamuxAddr)
	err = w.withRevision(ctx, fcid, host.PublicKey, host.SiamuxAddr, defaultRevisionFetchTimeout, lockingPrioritySyncing, func(rev types.FileContractRevision) error {
		return h.SyncAccount(ctx, &rev)
	})
	if err != nil {
		return fmt.Errorf("failed to sync account; %w", err)
	}
	return nil
}

func (w *Worker) UploadObject(ctx context.Context, r io.Reader, bucket, key string, opts api.UploadObjectOptions) (*api.UploadObjectResponse, error) {
	// prepare upload params
	up, err := w.prepareUploadParams(ctx, bucket, opts.ContractSet, opts.MinShards, opts.TotalShards)
	if err != nil {
		return nil, err
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// fetch contracts
	contracts, err := w.bus.Contracts(ctx, api.ContractsOpts{ContractSet: up.ContractSet})
	if err != nil {
		return nil, fmt.Errorf("couldn't fetch contracts from bus: %w", err)
	}

	// upload
	eTag, err := w.upload(ctx, bucket, key, up.RedundancySettings, r, contracts,
		WithBlockHeight(up.CurrentHeight),
		WithContractSet(up.ContractSet),
		WithMimeType(opts.MimeType),
		WithPacking(up.UploadPacking),
		WithObjectUserMetadata(opts.Metadata),
	)
	if err != nil {
		w.logger.With(zap.Error(err)).With("key", key).With("bucket", bucket).Error("failed to upload object")
		if !errors.Is(err, ErrShuttingDown) && !errors.Is(err, errUploadInterrupted) && !errors.Is(err, context.Canceled) {
			w.registerAlert(newUploadFailedAlert(bucket, key, up.ContractSet, opts.MimeType, up.RedundancySettings.MinShards, up.RedundancySettings.TotalShards, len(contracts), up.UploadPacking, false, err))
		}
		return nil, fmt.Errorf("couldn't upload object: %w", err)
	}
	return &api.UploadObjectResponse{
		ETag: eTag,
	}, nil
}

func (w *Worker) UploadMultipartUploadPart(ctx context.Context, r io.Reader, bucket, path, uploadID string, partNumber int, opts api.UploadMultipartUploadPartOptions) (*api.UploadMultipartUploadPartResponse, error) {
	// prepare upload params
	up, err := w.prepareUploadParams(ctx, bucket, opts.ContractSet, opts.MinShards, opts.TotalShards)
	if err != nil {
		return nil, err
	}

	// fetch upload from bus
	upload, err := w.bus.MultipartUpload(ctx, uploadID)
	if err != nil {
		return nil, fmt.Errorf("couldn't fetch multipart upload: %w", err)
	}

	// attach gouging checker to the context
	ctx = WithGougingChecker(ctx, w.bus, up.GougingParams)

	// prepare opts
	uploadOpts := []UploadOption{
		WithBlockHeight(up.CurrentHeight),
		WithContractSet(up.ContractSet),
		WithPacking(up.UploadPacking),
		WithCustomKey(upload.EncryptionKey),
		WithPartNumber(partNumber),
		WithUploadID(uploadID),
	}

	// make sure only one of the following is set
	if encryptionEnabled := !upload.EncryptionKey.IsNoopKey(); encryptionEnabled && opts.EncryptionOffset == nil {
		return nil, fmt.Errorf("%w: if object encryption (pre-erasure coding) wasn't disabled by creating the multipart upload with the no-op key, the offset needs to be set", api.ErrInvalidMultipartEncryptionSettings)
	} else if opts.EncryptionOffset != nil && *opts.EncryptionOffset < 0 {
		return nil, fmt.Errorf("%w: encryption offset must be positive", api.ErrInvalidMultipartEncryptionSettings)
	} else if encryptionEnabled {
		uploadOpts = append(uploadOpts, WithCustomEncryptionOffset(uint64(*opts.EncryptionOffset)))
	}

	// fetch contracts
	contracts, err := w.bus.Contracts(ctx, api.ContractsOpts{ContractSet: up.ContractSet})
	if err != nil {
		return nil, fmt.Errorf("couldn't fetch contracts from bus: %w", err)
	}

	// upload
	eTag, err := w.upload(ctx, bucket, path, up.RedundancySettings, r, contracts, uploadOpts...)
	if err != nil {
		w.logger.With(zap.Error(err)).With("path", path).With("bucket", bucket).Error("failed to upload object")
		if !errors.Is(err, ErrShuttingDown) && !errors.Is(err, errUploadInterrupted) && !errors.Is(err, context.Canceled) {
			w.registerAlert(newUploadFailedAlert(bucket, path, up.ContractSet, "", up.RedundancySettings.MinShards, up.RedundancySettings.TotalShards, len(contracts), up.UploadPacking, false, err))
		}
		return nil, fmt.Errorf("couldn't upload object: %w", err)
	}
	return &api.UploadMultipartUploadPartResponse{
		ETag: eTag,
	}, nil
}

func (w *Worker) initAccounts(refillInterval time.Duration) (err error) {
	if w.accounts != nil {
		panic("priceTables already initialized") // developer error
	}
	w.accounts, err = iworker.NewAccountManager(w.masterKey.DeriveAccountsKey(w.id), w.id, w.bus, w, w, w.bus, w.bus, w.bus, w.bus, refillInterval, w.logger.Desugar())
	return err
}

func (w *Worker) prepareUploadParams(ctx context.Context, bucket string, contractSet string, minShards, totalShards int) (api.UploadParams, error) {
	// return early if the bucket does not exist
	_, err := w.bus.Bucket(ctx, bucket)
	if err != nil {
		return api.UploadParams{}, fmt.Errorf("bucket '%s' not found; %w", bucket, err)
	}

	// fetch the upload parameters
	up, err := w.bus.UploadParams(ctx)
	if err != nil {
		return api.UploadParams{}, fmt.Errorf("couldn't fetch upload parameters from bus: %w", err)
	} else if contractSet != "" {
		up.ContractSet = contractSet
	} else if up.ContractSet == "" {
		return api.UploadParams{}, api.ErrContractSetNotSpecified
	}

	// cancel the upload if consensus is not synced
	if !up.ConsensusState.Synced {
		return api.UploadParams{}, api.ErrConsensusNotSynced
	}

	// allow overriding the redundancy settings
	if minShards != 0 {
		up.RedundancySettings.MinShards = minShards
	}
	if totalShards != 0 {
		up.RedundancySettings.TotalShards = totalShards
	}
	err = api.RedundancySettings{MinShards: up.RedundancySettings.MinShards, TotalShards: up.RedundancySettings.TotalShards}.Validate()
	if err != nil {
		return api.UploadParams{}, err
	}
	return up, nil
}
