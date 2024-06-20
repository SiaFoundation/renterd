package sql

import (
	"context"
	"io"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/webhooks"
)

// The database interfaces define all methods that a SQL database must implement
// to be used by the SQLStore.
type (
	Database interface {
		io.Closer

		// Migrate runs all missing migrations on the database.
		Migrate(ctx context.Context) error

		// Transaction starts a new transaction.
		Transaction(ctx context.Context, fn func(DatabaseTx) error) error

		// Version returns the database version and name.
		Version(ctx context.Context) (string, string, error)
	}

	DatabaseTx interface {
		// AbortMultipartUpload aborts a multipart upload and deletes it from
		// the database.
		AbortMultipartUpload(ctx context.Context, bucket, path string, uploadID string) error

		// Accounts returns all accounts from the db.
		Accounts(ctx context.Context) ([]api.Account, error)

		// AddMultipartPart adds a part to an unfinished multipart upload.
		AddMultipartPart(ctx context.Context, bucket, path, contractSet, eTag, uploadID string, partNumber int, slices object.SlabSlices) error

		// AddWebhook adds a new webhook to the database. If the webhook already
		// exists, it is updated.
		AddWebhook(ctx context.Context, wh webhooks.Webhook) error

		// ArchiveContract moves a contract from the regular contracts to the
		// archived ones.
		ArchiveContract(ctx context.Context, fcid types.FileContractID, reason string) error

		// Autopilot returns the autopilot with the given ID. Returns
		// api.ErrAutopilotNotFound if the autopilot doesn't exist.
		Autopilot(ctx context.Context, id string) (api.Autopilot, error)

		// Autopilots returns all autopilots.
		Autopilots(ctx context.Context) ([]api.Autopilot, error)

		// Bucket returns the bucket with the given name. If the bucket doesn't
		// exist, it returns api.ErrBucketNotFound.
		Bucket(ctx context.Context, bucket string) (api.Bucket, error)

		// CompleteMultipartUpload completes a multipart upload by combining the
		// provided parts into an object in bucket 'bucket' with key 'key'. The
		// parts need to be provided in ascending partNumber order without
		// duplicates but can contain gaps.
		CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []api.MultipartCompletedPart, opts api.CompleteMultipartOptions) (string, error)

		// Contracts returns contract metadata for all active contracts. The
		// opts argument can be used to filter the result.
		Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error)

		// ContractSize returns the size of the contract with the given ID as
		// well as the estimated number of bytes that can be pruned from it.
		ContractSize(ctx context.Context, id types.FileContractID) (api.ContractSize, error)

		// CopyObject copies an object from one bucket and key to another. If
		// source and destination are the same, only the metadata and mimeType
		// are overwritten with the provided ones.
		CopyObject(ctx context.Context, srcBucket, dstBucket, srcKey, dstKey, mimeType string, metadata api.ObjectUserMetadata) (api.ObjectMetadata, error)

		// CreateBucket creates a new bucket with the given name and policy. If
		// the bucket already exists, api.ErrBucketExists is returned.
		CreateBucket(ctx context.Context, bucket string, policy api.BucketPolicy) error

		// DeleteHostSector deletes all contract sector links that a host has
		// with the given root incrementing the lost sector count in the
		// process. If another contract with a different host exists that
		// contains the root, latest_host is updated to that host.
		DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) (int, error)

		// DeleteWebhook deletes the webhook with the matching module, event and
		// URL of the provided webhook. If the webhook doesn't exist,
		// webhooks.ErrWebhookNotFound is returned.
		DeleteWebhook(ctx context.Context, wh webhooks.Webhook) error

		// InsertBufferedSlab inserts a buffered slab into the database. This
		// includes the creation of a buffered slab as well as the corresponding
		// regular slab it is linked to. It returns the ID of the buffered slab
		// that was created.
		InsertBufferedSlab(ctx context.Context, fileName string, contractSetID int64, ec object.EncryptionKey, minShards, totalShards uint8) (int64, error)

		// InsertMultipartUpload creates a new multipart upload and returns a
		// unique upload ID.
		InsertMultipartUpload(ctx context.Context, bucket, path string, ec object.EncryptionKey, mimeType string, metadata api.ObjectUserMetadata) (string, error)

		// InvalidateSlabHealthByFCID invalidates the health of all slabs that
		// are associated with any of the provided contracts.
		InvalidateSlabHealthByFCID(ctx context.Context, fcids []types.FileContractID, limit int64) (int64, error)

		// DeleteBucket deletes a bucket. If the bucket isn't empty, it returns
		// api.ErrBucketNotEmpty. If the bucket doesn't exist, it returns
		// api.ErrBucketNotFound.
		DeleteBucket(ctx context.Context, bucket string) error

		// DeleteObject deletes an object from the database and returns true if
		// the requested object was actually deleted.
		DeleteObject(ctx context.Context, bucket, key string) (bool, error)

		// DeleteObjects deletes a batch of objects starting with the given
		// prefix and returns 'true' if any object was deleted.
		DeleteObjects(ctx context.Context, bucket, prefix string, limit int64) (bool, error)

		// HostAllowlist returns the list of public keys of hosts on the
		// allowlist.
		HostAllowlist(ctx context.Context) ([]types.PublicKey, error)

		// HostBlocklist returns the list of host addresses on the blocklist.
		HostBlocklist(ctx context.Context) ([]string, error)

		// InsertObject inserts a new object into the database.
		InsertObject(ctx context.Context, bucket, key, contractSet string, dirID int64, o object.Object, mimeType, eTag string, md api.ObjectUserMetadata) error

		// HostsForScanning returns a list of hosts to scan which haven't been
		// scanned since at least maxLastScan.
		HostsForScanning(ctx context.Context, maxLastScan time.Time, offset, limit int) ([]api.HostAddress, error)

		// ListBuckets returns a list of all buckets in the database.
		ListBuckets(ctx context.Context) ([]api.Bucket, error)

		// MakeDirsForPath creates all directories for a given object's path.
		MakeDirsForPath(ctx context.Context, path string) (int64, error)

		// MultipartUpload returns the multipart upload with the given ID or
		// api.ErrMultipartUploadNotFound if the upload doesn't exist.
		MultipartUpload(ctx context.Context, uploadID string) (api.MultipartUpload, error)

		// MultipartUploadParts returns a list of all parts for a given
		// multipart upload
		MultipartUploadParts(ctx context.Context, bucket, key, uploadID string, marker int, limit int64) (api.MultipartListPartsResponse, error)

		// MultipartUploads returns a list of all multipart uploads.
		MultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, limit int) (api.MultipartListUploadsResponse, error)

		// ObjectsStats returns overall stats about stored objects
		ObjectsStats(ctx context.Context, opts api.ObjectsStatsOpts) (api.ObjectsStatsResponse, error)

		// PruneEmptydirs prunes any directories that are empty.
		PruneEmptydirs(ctx context.Context) error

		// PruneSlabs deletes slabs that are no longer referenced by any slice
		// or slab buffer.
		PruneSlabs(ctx context.Context, limit int64) (int64, error)

		// RecordHostScans records the results of host scans in the database
		// such as recording the settings and price table of a host in case of
		// success and updating the uptime and downtime of a host.
		// NOTE: The price table is only updated if the known price table is
		// expired since price tables from scans are not paid for and are
		// therefore only useful for gouging checks.
		RecordHostScans(ctx context.Context, scans []api.HostScan) error

		// RecordPriceTables records price tables for hosts in the database
		// increasing the successful/failed interactions accordingly.
		RecordPriceTables(ctx context.Context, priceTableUpdate []api.HostPriceTableUpdate) error

		// RemoveOfflineHosts removes all hosts that have been offline for
		// longer than maxDownTime and been scanned at least minRecentFailures
		// times. The contracts of those hosts are also removed.
		RemoveOfflineHosts(ctx context.Context, minRecentFailures uint64, maxDownTime time.Duration) (int64, error)

		// RenameObject renames an object in the database from keyOld to keyNew
		// and the new directory dirID. returns api.ErrObjectExists if the an
		// object already exists at the target location or api.ErrObjectNotFound
		// if the object at keyOld doesn't exist. If force is true, the instead
		// of returning api.ErrObjectExists, the existing object will be
		// deleted.
		RenameObject(ctx context.Context, bucket, keyOld, keyNew string, dirID int64, force bool) error

		// RenameObjects renames all objects in the database with the given
		// prefix to the new prefix. If 'force' is true, it will overwrite any
		// existing objects with the new prefix. If no object can be renamed,
		// `api.ErrOBjectNotFound` is returned. If 'force' is false and an
		// object already exists with the new prefix, `api.ErrObjectExists` is
		// returned.
		RenameObjects(ctx context.Context, bucket, prefixOld, prefixNew string, dirID int64, force bool) error

		// ResetLostSectors resets the lost sector count for the given host.
		ResetLostSectors(ctx context.Context, hk types.PublicKey) error

		// SaveAccounts saves the given accounts in the db, overwriting any
		// existing ones and setting the clean shutdown flag.
		SaveAccounts(ctx context.Context, accounts []api.Account) error

		// SearchHosts returns a list of hosts that match the provided filters
		SearchHosts(ctx context.Context, autopilotID, filterMode, usabilityMode, addressContains string, keyIn []types.PublicKey, offset, limit int) ([]api.Host, error)

		// SetUncleanShutdown sets the clean shutdown flag on the accounts to
		// 'false' and also marks them as requiring a resync.
		SetUncleanShutdown(ctx context.Context) error

		// SlabBuffers returns the filenames and associated contract sets of all
		// slab buffers.
		SlabBuffers(ctx context.Context) (map[string]string, error)

		// UpdateAutopilot updates the autopilot with the provided one or
		// creates a new one if it doesn't exist yet.
		UpdateAutopilot(ctx context.Context, ap api.Autopilot) error

		// UpdateBucketPolicy updates the policy of the bucket with the provided
		// one, fully overwriting the existing policy.
		UpdateBucketPolicy(ctx context.Context, bucket string, policy api.BucketPolicy) error

		// UpdateHostAllowlistEntries updates the allowlist in the database
		UpdateHostAllowlistEntries(ctx context.Context, add, remove []types.PublicKey, clear bool) error

		// UpdateHostBlocklistEntries updates the blocklist in the database
		UpdateHostBlocklistEntries(ctx context.Context, add, remove []string, clear bool) error

		// UpdateHostCheck updates the host check for the given host.
		UpdateHostCheck(ctx context.Context, autopilot string, hk types.PublicKey, hc api.HostCheck) error

		// UpdateSlab updates the slab in the database. That includes the following:
		// - Optimistically set health to 100%
		// - Invalidate health_valid_until
		// - Update LatestHost for every shard
		// The operation is not allowed to update the number of shards
		// associated with a slab or the root/slabIndex of any shard.
		UpdateSlab(ctx context.Context, s object.Slab, contractSet string, usedContracts []types.FileContractID) error

		// UpdateSlabHealth updates the health of up to 'limit' slab in the
		// database if their health is not valid anymore. A random interval
		// between 'minValidity' and 'maxValidity' is used to determine the time
		// the health of the updated slabs becomes invalid
		UpdateSlabHealth(ctx context.Context, limit int64, minValidity, maxValidity time.Duration) (int64, error)

		// Webhooks returns all registered webhooks.
		Webhooks(ctx context.Context) ([]webhooks.Webhook, error)
	}

	MetricsDatabase interface {
		io.Closer

		// Migrate runs all missing migrations on the database.
		Migrate(ctx context.Context) error

		// Transaction starts a new transaction.
		Transaction(ctx context.Context, fn func(MetricsDatabaseTx) error) error

		// Version returns the database version and name.
		Version(ctx context.Context) (string, string, error)
	}

	MetricsDatabaseTx interface {
		// ContractMetrics returns contract metrics  for the given time range
		// and options.
		ContractMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractMetricsQueryOpts) ([]api.ContractMetric, error)

		// ContractPruneMetrics returns the contract prune metrics for the given
		// time range and options.
		ContractPruneMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractPruneMetricsQueryOpts) ([]api.ContractPruneMetric, error)

		// ContractSetChurnMetrics returns the contract set churn metrics for
		// the given time range and options.
		ContractSetChurnMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetChurnMetricsQueryOpts) ([]api.ContractSetChurnMetric, error)

		// ContractSetMetrics returns the contract set metrics for the given
		// time range and options.
		ContractSetMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetMetricsQueryOpts) ([]api.ContractSetMetric, error)

		// PerformanceMetrics returns performance metrics for the given time range
		PerformanceMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.PerformanceMetricsQueryOpts) ([]api.PerformanceMetric, error)

		// RecordContractMetric records contract metrics.
		RecordContractMetric(ctx context.Context, metrics ...api.ContractMetric) error

		// RecordContractPruneMetric records contract prune metrics.
		RecordContractPruneMetric(ctx context.Context, metrics ...api.ContractPruneMetric) error

		// RecordContractSetChurnMetric records contract set churn metrics.
		RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error

		// RecordContractSetMetric records contract set metrics.
		RecordContractSetMetric(ctx context.Context, metrics ...api.ContractSetMetric) error

		// RecordPerformanceMetric records performance metrics.
		RecordPerformanceMetric(ctx context.Context, metrics ...api.PerformanceMetric) error

		// RecordWalletMetric records wallet metrics.
		RecordWalletMetric(ctx context.Context, metrics ...api.WalletMetric) error

		// WalletMetrics returns wallet metrics for the given time range
		WalletMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.WalletMetricsQueryOpts) ([]api.WalletMetric, error)
	}

	UsedContract struct {
		ID          int64
		FCID        FileContractID
		RenewedFrom FileContractID
	}
)
