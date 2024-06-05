package sql

import (
	"context"
	"io"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

// The database interfaces define all methods that a SQL database must implement
// to be used by the SQLStore.
type (
	Database interface {
		io.Closer

		// Transaction starts a new transaction.
		Transaction(ctx context.Context, fn func(DatabaseTx) error) error

		// Migrate runs all missing migrations on the database.
		Migrate(ctx context.Context) error

		// Version returns the database version and name.
		Version(ctx context.Context) (string, string, error)
	}

	DatabaseTx interface {
		// AbortMultipartUpload aborts a multipart upload and deletes it from
		// the database.
		AbortMultipartUpload(ctx context.Context, bucket, path string, uploadID string) error

		// AddMultipartPart adds a part to an unfinished multipart upload.
		AddMultipartPart(ctx context.Context, bucket, path, contractSet, eTag, uploadID string, partNumber int, slices object.SlabSlices) error

		// Bucket returns the bucket with the given name. If the bucket doesn't
		// exist, it returns api.ErrBucketNotFound.
		Bucket(ctx context.Context, bucket string) (api.Bucket, error)

		// Contracts returns contract metadata for all active contracts. The
		// opts argument can be used to filter the result.
		Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error)

		// CompleteMultipartUpload completes a multipart upload by combining the
		// provided parts into an object in bucket 'bucket' with key 'key'. The
		// parts need to be provided in ascending partNumber order without
		// duplicates but can contain gaps.
		CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []api.MultipartCompletedPart, opts api.CompleteMultipartOptions) (string, error)

		// CopyObject copies an object from one bucket and key to another. If
		// source and destination are the same, only the metadata and mimeType
		// are overwritten with the provided ones.
		CopyObject(ctx context.Context, srcBucket, dstBucket, srcKey, dstKey, mimeType string, metadata api.ObjectUserMetadata) (api.ObjectMetadata, error)

		// CreateBucket creates a new bucket with the given name and policy. If
		// the bucket already exists, api.ErrBucketExists is returned.
		CreateBucket(ctx context.Context, bucket string, policy api.BucketPolicy) error

		// InsertMultipartUpload creates a new multipart upload and returns a
		// unique upload ID.
		InsertMultipartUpload(ctx context.Context, bucket, path string, ec object.EncryptionKey, mimeType string, metadata api.ObjectUserMetadata) (string, error)

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

		// InsertObject inserts a new object into the database.
		InsertObject(ctx context.Context, bucket, key, contractSet string, dirID int64, o object.Object, mimeType, eTag string, md api.ObjectUserMetadata) error

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

		// SearchHosts returns a list of hosts that match the provided filters
		SearchHosts(ctx context.Context, autopilotID, filterMode, usabilityMode, addressContains string, keyIn []types.PublicKey, offset, limit int, hasAllowList, hasBlocklist bool) ([]api.Host, error)

		// UpdateBucketPolicy updates the policy of the bucket with the provided
		// one, fully overwriting the existing policy.
		UpdateBucketPolicy(ctx context.Context, bucket string, policy api.BucketPolicy) error

		// UpdateSlab updates the slab in the database. That includes the following:
		// - Optimistically set health to 100%
		// - Invalidate health_valid_until
		// - Update LatestHost for every shard
		// The operation is not allowed to update the number of shards
		// associated with a slab or the root/slabIndex of any shard.
		UpdateSlab(ctx context.Context, s object.Slab, contractSet string, usedContracts []types.FileContractID) error
	}

	MetricsDatabase interface {
		io.Closer
		Migrate(ctx context.Context) error
		Version(ctx context.Context) (string, string, error)
	}

	UsedContract struct {
		ID          int64
		FCID        FileContractID
		RenewedFrom FileContractID
	}
)
