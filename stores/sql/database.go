package sql

import (
	"context"
	"io"
)

// The database interfaces define all methods that a SQL database must implement
// to be used by the SQLStore.
type (
	Database interface {
		io.Closer

		// Transaction starts a new transaction.
		Transaction(fn func(DatabaseTx) error) error

		// Migrate runs all missing migrations on the database.
		Migrate() error

		// Version returns the database version and name.
		Version(ctx context.Context) (string, string, error)
	}

	DatabaseTx interface {
		// DeleteObject deletes an object from the database and returns true if
		// the requested object was actually deleted.
		DeleteObject(bucket, key string) (bool, error)

		// MakeDirsForPath creates all directories for a given object's path.
		MakeDirsForPath(path string) (uint, error)

		// RenameObject renames an object in the database from keyOld to keyNew
		// and the new directory dirID. returns api.ErrObjectExists if the an
		// object already exists at the target location or api.ErrObjectNotFound
		// if the object at keyOld doesn't exist.
		RenameObject(bucket, keyOld, keyNew string, dirID uint) error
	}

	MetricsDatabase interface {
		io.Closer
		Migrate() error
		Version(ctx context.Context) (string, string, error)
	}
)
