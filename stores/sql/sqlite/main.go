package sqlite

import (
	"context"
	"database/sql"
	"time"

	isql "go.sia.tech/renterd/internal/sql"

	"go.uber.org/zap"
)

type MainDatabase struct {
	db *isql.DB
}

// NewMainDatabase creates a new SQLite backend.
func NewMainDatabase(db *sql.DB, log *zap.Logger, lqd, ltd time.Duration) *MainDatabase {
	store := isql.NewDB(db, log, "database is locked", lqd, ltd)
	return &MainDatabase{
		db: store,
	}
}

func (b *MainDatabase) Close() error {
	return b.db.Close()
}

func (b *MainDatabase) Version(_ context.Context) (string, string, error) {
	var version string
	if err := b.db.QueryRow("select sqlite_version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "SQLite", version, nil
}
