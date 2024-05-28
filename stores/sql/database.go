package sql

import (
	"context"
	"io"

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
		// DeleteObject deletes an object from the database and returns true if
		// the requested object was actually deleted.
		DeleteObject(ctx context.Context, bucket, key string) (bool, error)

		// DeleteObjects deletes a batch of objects starting with the given
		// prefix and returns 'true' if any object was deleted.
		DeleteObjects(ctx context.Context, bucket, prefix string, limit int64) (bool, error)

		// InsertObject inserts a new object into the database.
		InsertObject(ctx context.Context, bucket, key, contractSet string, dirID int64, o object.Object, mimeType, eTag string, md api.ObjectUserMetadata) error

		// MakeDirsForPath creates all directories for a given object's path.
		MakeDirsForPath(ctx context.Context, path string) (int64, error)

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
