package sql

import (
	"context"
	"io"
)

// The backend interfaces define all methods that a SQL backend must
// implement to be used by the SQLStore.
type (
	Backend interface {
		io.Closer

		Version(ctx context.Context) (string, string, error)
	}

	MetricsBackend interface {
		io.Closer
	}
)
