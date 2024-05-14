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

		Version(ctx context.Context) (string, string, error)
	}

	MetricsDatabase interface {
		io.Closer
	}
)
