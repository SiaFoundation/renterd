package sql

import (
	"context"
	"io"

	"go.sia.tech/renterd/stores/sql/mysql"
	"go.sia.tech/renterd/stores/sql/sqlite"
)

// All database implementations must satisfy the interfaces.
var _ Database = (*mysql.MainDatabase)(nil)
var _ Database = (*sqlite.MainDatabase)(nil)
var _ MetricsDatabase = (*mysql.MetricsDatabase)(nil)
var _ MetricsDatabase = (*sqlite.MetricsDatabase)(nil)

// The database interfaces define all methods that a SQL database must implement
// to be used by the SQLStore.
type (
	Database interface {
		io.Closer
		Version(ctx context.Context) (string, string, error)
	}

	MetricsDatabase interface {
		io.Closer
		Version(ctx context.Context) (string, string, error)
	}
)
