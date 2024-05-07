package sql

import (
	"context"
	"database/sql"
	"time"

	isql "go.sia.tech/renterd/internal/sql"

	"go.uber.org/zap"
)

type SQLiteBackend struct {
	db *isql.DB
}

// NewSQLiteBackend creates a new SQLite backend.
func NewSQLiteBackend(db *sql.DB, log *zap.SugaredLogger, lqd, ltd time.Duration) Backend {
	store := isql.NewDB(db, log.Desugar(), "database is locked", lqd, ltd)
	return &SQLiteBackend{
		db: store,
	}
}

func (b *SQLiteBackend) Close() error {
	return b.db.Close()
}

func (b *SQLiteBackend) Version(_ context.Context) (string, string, error) {
	var version string
	if err := b.db.QueryRow("select sqlite_version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "SQLite", version, nil
}
