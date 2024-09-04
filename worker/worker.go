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
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gotd/contrib/http_range"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/config"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/rhp"
	rhp2 "go.sia.tech/renterd/internal/rhp/v2"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	"go.sia.tech/renterd/internal/utils"
	iworker "go.sia.tech/renterd/internal/worker"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/webhooks"
	"go.sia.tech/renterd/worker/client"
	"go.sia.tech/renterd/worker/s3"
	"go.uber.org/zap"
)

const (
	defaultRevisionFetchTimeout = 30 * time.Second

	lockingPrioritySyncing                = 30
	lockingPriorityActiveContractRevision = 100

	lockingPriorityBlockedUpload    = 15
	lockingPriorityUpload           = 10
	lockingPriorityBackgroundUpload = 5
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
		s3.Bus

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
		Wallet
	}

	AccountFunder interface {
		FundAccount(ctx context.Context, account rhpv3.Account, fcid types.FileContractID, amount types.Currency) (types.Currency, error)
	}

	ContractStore interface {
		Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
		ContractSize(ctx context.Context, id types.FileContractID) (api.ContractSize, error)
		ContractRoots(ctx context.Context, id types.FileContractID) ([]types.Hash256, []types.Hash256, error)
		Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error)
		RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error)
	}

	HostStore interface {
		RecordHostScans(ctx context.Context, scans []api.HostScan) error
		RecordPriceTables(ctx context.Context, priceTableUpdate []api.HostPriceTableUpdate) error
		RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error

		Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error)
	}

	ObjectStore interface {
		// NOTE: used for download
		DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) error
		FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error)
		Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error)

		// NOTE: used for upload
		AddObject(ctx context.Context, bucket, path, contractSet string, o object.Object, opts api.AddObjectOptions) error
		AddMultipartPart(ctx context.Context, bucket, path, contractSet, ETag, uploadID string, partNumber int, slices []object.SlabSlice) (err error)
		AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8, contractSet string) (slabs []object.SlabSlice, slabBufferMaxSizeSoftReached bool, err error)
		AddUploadingSector(ctx context.Context, uID api.UploadID, id types.FileContractID, root types.Hash256) error
		FinishUpload(ctx context.Context, uID api.UploadID) error
		MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error
		TrackUpload(ctx context.Context, uID api.UploadID) error
		UpdateSlab(ctx context.Context, s object.Slab, contractSet string) error

		// NOTE: used by worker
		Bucket(_ context.Context, bucket string) (api.Bucket, error)
		Object(ctx context.Context, bucket, path string, opts api.GetObjectOptions) (api.ObjectsResponse, error)
		DeleteObject(ctx context.Context, bucket, path string, opts api.DeleteObjectOptions) error
		MultipartUpload(ctx context.Context, uploadID string) (resp api.MultipartUpload, err error)
		PackedSlabsForUpload(ctx context.Context, lockingDuration time.Duration, minShards, totalShards uint8, set string, limit int) ([]api.PackedSlab, error)
	}

	SettingStore interface {
		GougingParams(ctx context.Context) (api.GougingParams, error)
		UploadParams(ctx context.Context) (api.UploadParams, error)
	}

	Syncer interface {
		BroadcastTransaction(ctx context.Context, txns []types.Transaction) error
		SyncerPeers(ctx context.Context) (resp []string, err error)
	}

	Wallet interface {
		WalletDiscard(ctx context.Context, txn types.Transaction) error
		WalletFund(ctx context.Context, txn *types.Transaction, amount types.Currency, useUnconfirmedTxns bool) ([]types.Hash256, []types.Transaction, error)
		WalletSign(ctx context.Context, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error
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

	allowPrivateIPs bool
	id              string
	bus             Bus
	masterKey       utils.MasterKey
	startTime       time.Time

	eventSubscriber iworker.EventSubscriber
	downloadManager *downloadManager
	uploadManager   *uploadManager

	accounts    *iworker.AccountMgr
	dialer      *rhp.FallbackDialer
	cache       iworker.WorkerCache
	priceTables *priceTables

	uploadsMu            sync.Mutex
	uploadingPackedSlabs map[string]struct{}

	contractSpendingRecorder ContractSpendingRecorder
	contractLockingDuration  time.Duration

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

func (w *Worker) withRevision(ctx context.Context, fetchTimeout time.Duration, fcid types.FileContractID, hk types.PublicKey, siamuxAddr string, lockPriority int, fn func(rev types.FileContractRevision) error) error {
	return w.withContractLock(ctx, fcid, lockPriority, func() error {
		h := w.Host(hk, fcid, siamuxAddr)
		rev, err := h.FetchRevision(ctx, fetchTimeout)
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

func (w *Worker) rhpScanHandler(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode the request
	var rsr api.RHPScanRequest
	if jc.Decode(&rsr) != nil {
		return
	}

	// only scan hosts if we are online
	peers, err := w.bus.SyncerPeers(ctx)
	if jc.Check("failed to fetch peers from bus", err) != nil {
		return
	}
	if len(peers) == 0 {
		jc.Error(errors.New("not connected to the internet"), http.StatusServiceUnavailable)
		return
	}

	// scan host
	var errStr string
	settings, priceTable, elapsed, err := w.scanHost(ctx, time.Duration(rsr.Timeout), rsr.HostKey, rsr.HostIP)
	if err != nil {
		errStr = err.Error()
	}

	jc.Encode(api.RHPScanResponse{
		Ping:       api.DurationMS(elapsed),
		PriceTable: priceTable,
		ScanError:  errStr,
		Settings:   settings,
	})
}

func (w *Worker) fetchContracts(ctx context.Context, metadatas []api.ContractMetadata, timeout time.Duration) (contracts []api.Contract, errs HostErrorSet) {
	errs = make(HostErrorSet)

	// create requests channel
	reqs := make(chan api.ContractMetadata)

	// create worker function
	var mu sync.Mutex
	worker := func() {
		for md := range reqs {
			var revision types.FileContractRevision
			err := w.withRevision(ctx, timeout, md.ID, md.HostKey, md.SiamuxAddr, lockingPriorityActiveContractRevision, func(rev types.FileContractRevision) error {
				revision = rev
				return nil
			})
			mu.Lock()
			if err != nil {
				errs[md.HostKey] = err
				contracts = append(contracts, api.Contract{
					ContractMetadata: md,
				})
			} else {
				contracts = append(contracts, api.Contract{
					ContractMetadata: md,
					Revision:         &revision,
				})
			}
			mu.Unlock()
		}
	}

	// launch all workers
	var wg sync.WaitGroup
	for t := 0; t < 20 && t < len(metadatas); t++ {
		wg.Add(1)
		go func() {
			worker()
			wg.Done()
		}()
	}

	// launch all requests
	for _, metadata := range metadatas {
		reqs <- metadata
	}
	close(reqs)

	// wait until they're done
	wg.Wait()
	return
}

func (w *Worker) rhpPriceTableHandler(jc jape.Context) {
	// decode the request
	var rptr api.RHPPriceTableRequest
	if jc.Decode(&rptr) != nil {
		return
	}

	// defer interaction recording before applying timeout to make sure we still
	// record the failed update if it timed out
	var err error
	var hpt api.HostPriceTable
	defer func() {
		if shouldRecordPriceTable(err) {
			w.bus.RecordPriceTables(jc.Request.Context(), []api.HostPriceTableUpdate{
				{
					HostKey:    rptr.HostKey,
					Success:    err == nil,
					Timestamp:  time.Now(),
					PriceTable: hpt,
				},
			})
		}
	}()

	// apply timeout
	ctx := jc.Request.Context()
	if rptr.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(rptr.Timeout))
		defer cancel()
	}

	hpt, err = w.Host(rptr.HostKey, types.FileContractID{}, rptr.SiamuxAddr).PriceTableUnpaid(ctx)
	if jc.Check("could not get price table", err) != nil {
		return
	}
	jc.Encode(hpt)
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

	// fetch all contracts
	dlContracts, err := w.bus.Contracts(ctx, api.ContractsOpts{})
	if jc.Check("couldn't fetch contracts from bus", err) != nil {
		return
	}

	// filter upload contracts
	var ulContracts []api.ContractMetadata
	for _, c := range dlContracts {
		if c.InSet(up.ContractSet) {
			ulContracts = append(ulContracts, c)
		}
	}

	// migrate the slab
	numShardsMigrated, surchargeApplied, err := w.migrate(ctx, slab, up.ContractSet, dlContracts, ulContracts, up.CurrentHeight)
	if err != nil {
		jc.Encode(api.MigrateSlabResponse{
			NumShardsMigrated: numShardsMigrated,
			SurchargeApplied:  surchargeApplied,
			Error:             err.Error(),
		})
		return
	}

	jc.Encode(api.MigrateSlabResponse{
		NumShardsMigrated: numShardsMigrated,
		SurchargeApplied:  surchargeApplied,
	})
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
	jc.Encode(api.DownloadStatsResponse{
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
	jc.Encode(api.UploadStatsResponse{
		AvgSlabUploadSpeedMBPS: math.Ceil(stats.avgSlabUploadSpeedMBPS*100) / 100,
		AvgOverdrivePct:        math.Floor(stats.avgOverdrivePct*100*100) / 100,
		HealthyUploaders:       stats.healthyUploaders,
		NumUploaders:           stats.numUploaders,
		UploadersStats:         uss,
	})
}

func (w *Worker) objectsHandlerHEAD(jc jape.Context) {
	// parse bucket
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	var ignoreDelim bool
	if jc.DecodeForm("ignoreDelim", &ignoreDelim) != nil {
		return
	}

	// parse path
	path := jc.PathParam("path")
	if !ignoreDelim && (path == "" || strings.HasSuffix(path, "/")) {
		jc.Error(errors.New("HEAD requests can only be performed on objects, not directories"), http.StatusBadRequest)
		return
	}

	var off int
	if jc.DecodeForm("offset", &off) != nil {
		return
	}
	limit := -1
	if jc.DecodeForm("limit", &limit) != nil {
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

	// fetch object metadata
	hor, err := w.HeadObject(jc.Request.Context(), bucket, path, api.HeadObjectOptions{
		IgnoreDelim: ignoreDelim,
		Range:       &dr,
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

func (w *Worker) objectsHandlerGET(jc jape.Context) {
	jc.Custom(nil, []api.ObjectMetadata{})

	ctx := jc.Request.Context()

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
	var off int
	if jc.DecodeForm("offset", &off) != nil {
		return
	}
	limit := -1
	if jc.DecodeForm("limit", &limit) != nil {
		return
	}
	var ignoreDelim bool
	if jc.DecodeForm("ignoreDelim", &ignoreDelim) != nil {
		return
	}

	opts := api.GetObjectOptions{
		Prefix:      prefix,
		Marker:      marker,
		Offset:      off,
		Limit:       limit,
		IgnoreDelim: ignoreDelim,
		SortBy:      sortBy,
		SortDir:     sortDir,
	}

	path := jc.PathParam("path")
	if path == "" || strings.HasSuffix(path, "/") {
		// list directory
		res, err := w.bus.Object(ctx, bucket, path, opts)
		if utils.IsErr(err, api.ErrObjectNotFound) {
			jc.Error(err, http.StatusNotFound)
			return
		} else if jc.Check("couldn't get object or entries", err) != nil {
			return
		}
		jc.Encode(res.Entries)
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

	gor, err := w.GetObject(ctx, bucket, path, api.DownloadObjectOptions{
		GetObjectOptions: opts,
		Range:            &dr,
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
	serveContent(jc.ResponseWriter, jc.Request, path, gor.Content, gor.HeadObjectResponse)
}

func (w *Worker) objectsHandlerPUT(jc jape.Context) {
	jc.Custom((*[]byte)(nil), nil)
	ctx := jc.Request.Context()

	// grab the path
	path := jc.PathParam("path")

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
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
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
	path := jc.PathParam("path")

	// decode the contract set from the query string
	var contractset string
	if jc.DecodeForm("contractset", &contractset) != nil {
		return
	}

	// decode the bucket from the query string
	bucket := api.DefaultBucketName
	if jc.DecodeForm("bucket", &bucket) != nil {
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

	// get the offset
	var offset int
	if jc.DecodeForm("offset", &offset) != nil {
		return
	} else if jc.Request.FormValue("offset") != "" {
		opts.EncryptionOffset = &offset
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

func (w *Worker) objectsHandlerDELETE(jc jape.Context) {
	var batch bool
	if jc.DecodeForm("batch", &batch) != nil {
		return
	}
	var bucket string
	if jc.DecodeForm("bucket", &bucket) != nil {
		return
	}
	err := w.bus.DeleteObject(jc.Request.Context(), bucket, jc.PathParam("path"), api.DeleteObjectOptions{Batch: batch})
	if utils.IsErr(err, api.ErrObjectNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	}
	jc.Check("couldn't delete object", err)
}

func (w *Worker) rhpContractsHandlerGET(jc jape.Context) {
	ctx := jc.Request.Context()

	// fetch contracts
	busContracts, err := w.bus.Contracts(ctx, api.ContractsOpts{})
	if jc.Check("failed to fetch contracts from bus", err) != nil {
		return
	}
	if len(busContracts) == 0 {
		jc.Encode(api.ContractsResponse{Contracts: nil})
		return
	}

	var hosttimeout time.Duration
	if jc.DecodeForm("hosttimeout", (*api.DurationMS)(&hosttimeout)) != nil {
		return
	}

	gp, err := w.bus.GougingParams(ctx)
	if jc.Check("could not get gouging parameters", err) != nil {
		return
	}
	ctx = WithGougingChecker(ctx, w.bus, gp)

	contracts, errs := w.fetchContracts(ctx, busContracts, hosttimeout)
	resp := api.ContractsResponse{Contracts: contracts}
	if errs != nil {
		resp.Error = errs.Error()
		resp.Errors = make(map[types.PublicKey]string)
		for pk, err := range errs {
			resp.Errors[pk] = err.Error()
		}
	}
	jc.Encode(resp)
}

func (w *Worker) idHandlerGET(jc jape.Context) {
	jc.Encode(w.id)
}

func (w *Worker) memoryGET(jc jape.Context) {
	jc.Encode(api.MemoryResponse{
		Download: w.downloadManager.mm.Status(),
		Upload:   w.uploadManager.mm.Status(),
	})
}

func (w *Worker) accountHandlerGET(jc jape.Context) {
	var hostKey types.PublicKey
	if jc.DecodeParam("hostkey", &hostKey) != nil {
		return
	}
	account := rhpv3.Account(w.accounts.ForHost(hostKey).ID())
	jc.Encode(account)
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

func (w *Worker) eventHandlerPOST(jc jape.Context) {
	var event webhooks.Event
	if jc.Decode(&event) != nil {
		return
	} else if event.Event == webhooks.WebhookEventPing {
		jc.ResponseWriter.WriteHeader(http.StatusOK)
	} else {
		w.eventSubscriber.ProcessEvent(event)
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

	if cfg.ContractLockTimeout == 0 {
		return nil, errors.New("contract lock duration must be positive")
	}
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
		alerts:                  a,
		allowPrivateIPs:         cfg.AllowPrivateIPs,
		contractLockingDuration: cfg.ContractLockTimeout,
		cache:                   iworker.NewCache(b, l),
		dialer:                  dialer,
		eventSubscriber:         iworker.NewEventSubscriber(a, b, l, 10*time.Second),
		id:                      cfg.ID,
		bus:                     b,
		masterKey:               masterKey,
		logger:                  l.Sugar(),
		rhp2Client:              rhp2.New(dialer, l),
		rhp3Client:              rhp3.New(dialer, l),
		startTime:               time.Now(),
		uploadingPackedSlabs:    make(map[string]struct{}),
		shutdownCtx:             shutdownCtx,
		shutdownCtxCancel:       shutdownCancel,
	}

	if err := w.initAccounts(cfg.AccountsRefillInterval); err != nil {
		return nil, fmt.Errorf("failed to initialize accounts; %w", err)
	}
	w.initPriceTables()

	w.initDownloadManager(cfg.DownloadMaxMemory, cfg.DownloadMaxOverdrive, cfg.DownloadOverdriveTimeout, l)
	w.initUploadManager(cfg.UploadMaxMemory, cfg.UploadMaxOverdrive, cfg.UploadOverdriveTimeout, l)

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

		"POST   /event": w.eventHandlerPOST,

		"GET /memory": w.memoryGET,

		"GET    /rhp/contracts":  w.rhpContractsHandlerGET,
		"POST   /rhp/scan":       w.rhpScanHandler,
		"POST   /rhp/pricetable": w.rhpPriceTableHandler,

		"GET    /stats/downloads": w.downloadsStatsHandlerGET,
		"GET    /stats/uploads":   w.uploadsStatsHandlerGET,
		"POST   /slab/migrate":    w.slabMigrateHandler,

		"HEAD   /objects/*path": w.objectsHandlerHEAD,
		"GET    /objects/*path": w.objectsHandlerGET,
		"PUT    /objects/*path": w.objectsHandlerPUT,
		"DELETE /objects/*path": w.objectsHandlerDELETE,

		"PUT    /multipart/*path": w.multipartUploadHandlerPUT,

		"GET    /state": w.stateHandlerGET,
	})
}

// Setup register event webhooks that enable the worker cache.
func (w *Worker) Setup(ctx context.Context, apiURL, apiPassword string) error {
	go func() {
		eventsURL := fmt.Sprintf("%s/event", apiURL)
		webhookOpts := []webhooks.HeaderOption{webhooks.WithBasicAuth("", apiPassword)}
		if err := w.eventSubscriber.Register(w.shutdownCtx, eventsURL, webhookOpts...); err != nil {
			w.logger.Errorw("failed to register webhooks", zap.Error(err))
		}
	}()

	return w.cache.Subscribe(w.eventSubscriber)
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

	// shutdown the subscriber
	return w.eventSubscriber.Shutdown(ctx)
}

func (w *Worker) scanHost(ctx context.Context, timeout time.Duration, hostKey types.PublicKey, hostIP string) (rhpv2.HostSettings, rhpv3.HostPriceTable, time.Duration, error) {
	logger := w.logger.With("host", hostKey).With("hostIP", hostIP).With("timeout", timeout)

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
		settings, err := w.rhp2Client.Settings(scanCtx, hostKey, hostIP)
		cancel()
		if err != nil {
			return settings, rhpv3.HostPriceTable{}, time.Since(start), err
		}

		// fetch the host pricetable
		scanCtx, cancel = timeoutCtx()
		pt, err := w.rhp3Client.PriceTableUnpaid(scanCtx, hostKey, settings.SiamuxAddr())
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
	} else if private && !w.allowPrivateIPs {
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
	scanErr := w.bus.RecordHostScans(ctx, []api.HostScan{
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
		utils.IsErr(err, errors.New("no route to host")) ||
		utils.IsErr(err, errors.New("no such host")) ||
		utils.IsErr(err, errors.New("connection refused")) ||
		utils.IsErr(err, errors.New("unknown port")) ||
		utils.IsErr(err, errors.New("cannot assign requested address"))
}

func (w *Worker) headObject(ctx context.Context, bucket, path string, onlyMetadata bool, opts api.HeadObjectOptions) (*api.HeadObjectResponse, api.ObjectsResponse, error) {
	// fetch object
	res, err := w.bus.Object(ctx, bucket, path, api.GetObjectOptions{
		IgnoreDelim:  opts.IgnoreDelim,
		OnlyMetadata: onlyMetadata,
	})
	if err != nil {
		return nil, api.ObjectsResponse{}, fmt.Errorf("couldn't fetch object: %w", err)
	} else if res.Object == nil {
		return nil, api.ObjectsResponse{}, errors.New("object is a directory")
	}

	// adjust length
	if opts.Range == nil {
		opts.Range = &api.DownloadRange{Offset: 0, Length: -1}
	}
	if opts.Range.Length == -1 {
		opts.Range.Length = res.Object.Size - opts.Range.Offset
	}

	// check size of object against range
	if opts.Range.Offset+opts.Range.Length > res.Object.Size {
		return nil, api.ObjectsResponse{}, http_range.ErrInvalid
	}

	return &api.HeadObjectResponse{
		ContentType:  res.Object.MimeType,
		Etag:         res.Object.ETag,
		LastModified: res.Object.ModTime,
		Range:        opts.Range.ContentRange(res.Object.Size),
		Size:         res.Object.Size,
		Metadata:     res.Object.Metadata,
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

func (w *Worker) GetObject(ctx context.Context, bucket, path string, opts api.DownloadObjectOptions) (*api.GetObjectResponse, error) {
	// head object
	hor, res, err := w.headObject(ctx, bucket, path, false, api.HeadObjectOptions{
		IgnoreDelim: opts.IgnoreDelim,
		Range:       opts.Range,
	})
	if err != nil {
		return nil, fmt.Errorf("couldn't fetch object: %w", err)
	}
	obj := *res.Object.Object

	// adjust range
	if opts.Range == nil {
		opts.Range = &api.DownloadRange{}
	}
	opts.Range.Offset = hor.Range.Offset
	opts.Range.Length = hor.Range.Length

	// fetch gouging params
	gp, err := w.cache.GougingParams(ctx)
	if err != nil {
		return nil, fmt.Errorf("couldn't fetch gouging parameters from bus: %w", err)
	}

	// fetch all contracts
	contracts, err := w.cache.DownloadContracts(ctx)
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
			err = w.downloadManager.DownloadObject(ctx, wr, obj, uint64(offset), uint64(length), contracts)
			if err != nil {
				w.logger.Error(err)
				if !errors.Is(err, ErrShuttingDown) &&
					!errors.Is(err, errDownloadCancelled) &&
					!errors.Is(err, io.ErrClosedPipe) {
					w.registerAlert(newDownloadFailedAlert(bucket, path, offset, length, int64(len(contracts)), err))
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

func (w *Worker) HeadObject(ctx context.Context, bucket, path string, opts api.HeadObjectOptions) (*api.HeadObjectResponse, error) {
	res, _, err := w.headObject(ctx, bucket, path, true, opts)
	return res, err
}

func (w *Worker) SyncAccount(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, siamuxAddr string) error {
	// attach gouging checker
	gp, err := w.cache.GougingParams(ctx)
	if err != nil {
		return fmt.Errorf("couldn't get gouging parameters; %w", err)
	}
	ctx = WithGougingChecker(ctx, w.bus, gp)

	// sync the account
	h := w.Host(hk, fcid, siamuxAddr)
	err = w.withRevision(ctx, defaultRevisionFetchTimeout, fcid, hk, siamuxAddr, lockingPrioritySyncing, func(rev types.FileContractRevision) error {
		return h.SyncAccount(ctx, &rev)
	})
	if err != nil {
		return fmt.Errorf("failed to sync account; %w", err)
	}
	return nil
}

func (w *Worker) UploadObject(ctx context.Context, r io.Reader, bucket, path string, opts api.UploadObjectOptions) (*api.UploadObjectResponse, error) {
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
	eTag, err := w.upload(ctx, bucket, path, up.RedundancySettings, r, contracts,
		WithBlockHeight(up.CurrentHeight),
		WithContractSet(up.ContractSet),
		WithMimeType(opts.MimeType),
		WithPacking(up.UploadPacking),
		WithObjectUserMetadata(opts.Metadata),
	)
	if err != nil {
		w.logger.With(zap.Error(err)).With("path", path).With("bucket", bucket).Error("failed to upload object")
		if !errors.Is(err, ErrShuttingDown) && !errors.Is(err, errUploadInterrupted) && !errors.Is(err, context.Canceled) {
			w.registerAlert(newUploadFailedAlert(bucket, path, up.ContractSet, opts.MimeType, up.RedundancySettings.MinShards, up.RedundancySettings.TotalShards, len(contracts), up.UploadPacking, false, err))
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
		WithCustomKey(upload.Key),
		WithPartNumber(partNumber),
		WithUploadID(uploadID),
	}

	// make sure only one of the following is set
	if encryptionEnabled := !upload.Key.IsNoopKey(); encryptionEnabled && opts.EncryptionOffset == nil {
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
	w.accounts, err = iworker.NewAccountManager(w.masterKey.DeriveAccountsKey(w.id), w.id, w.bus, w, w, w.bus, w.cache, w.bus, refillInterval, w.logger.Desugar())
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

// A HostErrorSet is a collection of errors from various hosts.
type HostErrorSet map[types.PublicKey]error

// NumGouging returns numbers of host that errored out due to price gouging.
func (hes HostErrorSet) NumGouging() (n int) {
	for _, he := range hes {
		if errors.Is(he, gouging.ErrPriceTableGouging) {
			n++
		}
	}
	return
}

// Error implements error.
func (hes HostErrorSet) Error() string {
	if len(hes) == 0 {
		return ""
	}

	var strs []string
	for hk, he := range hes {
		strs = append(strs, fmt.Sprintf("%x: %v", hk[:4], he.Error()))
	}

	// include a leading newline so that the first error isn't printed on the
	// same line as the error context
	return "\n" + strings.Join(strs, "\n")
}
