package sql

import (
	"context"
	"database/sql"
	"time"

	isql "go.sia.tech/renterd/internal/sql"

	"go.uber.org/zap"
)

type MySQLDatabase struct {
	db *isql.DB
}

// NewMySQLDatabase creates a new MySQL backend.
func NewMySQLDatabase(db *sql.DB, log *zap.Logger, lqd, ltd time.Duration) Database {
	store := isql.NewDB(db, log, "Deadlock found when trying to get lock", lqd, ltd)
	return &MySQLDatabase{
		db: store,
	}
}

func (b *MySQLDatabase) Close() error {
	return b.db.Close()
}

func (b *MySQLDatabase) Version(_ context.Context) (string, string, error) {
	var version string
	if err := b.db.QueryRow("select version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "MySQL", version, nil
}
