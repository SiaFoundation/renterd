package sql

import (
	"context"
	"database/sql"
	"time"

	isql "go.sia.tech/renterd/internal/sql"

	"go.uber.org/zap"
)

type SQLiteDatabase struct {
	db *isql.DB
}

// NewSQLiteDatabase creates a new SQLite backend.
func NewSQLiteDatabase(db *sql.DB, log *zap.Logger, lqd, ltd time.Duration) Database {
	store := isql.NewDB(db, log, "database is locked", lqd, ltd)
	return &SQLiteDatabase{
		db: store,
	}
}

func (b *SQLiteDatabase) Close() error {
	return b.db.Close()
}

func (b *SQLiteDatabase) Version(_ context.Context) (string, string, error) {
	var version string
	if err := b.db.QueryRow("select sqlite_version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "SQLite", version, nil
}
