package sqlite

import (
	"context"
	"database/sql"
	"time"

	isql "go.sia.tech/renterd/internal/sql"

	"go.uber.org/zap"
)

type MetricsDatabase struct {
	db *isql.DB
}

// NewSQLiteDatabase creates a new SQLite backend.
func NewMetricsDatabase(db *sql.DB, log *zap.Logger, lqd, ltd time.Duration) *MetricsDatabase {
	store := isql.NewDB(db, log, "database is locked", lqd, ltd)
	return &MetricsDatabase{
		db: store,
	}
}

func (b *MetricsDatabase) Close() error {
	return b.db.Close()
}

func (b *MetricsDatabase) Version(_ context.Context) (string, string, error) {
	var version string
	if err := b.db.QueryRow("select sqlite_version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "SQLite", version, nil
}
