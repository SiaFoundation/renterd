package sql

import (
	"context"
	"database/sql"
	"time"

	isql "go.sia.tech/renterd/internal/sql"

	"go.uber.org/zap"
)

type MySQLBackend struct {
	db *isql.DB
}

// NewMySQLBackend creates a new MySQL backend.
func NewMySQLBackend(db *sql.DB, log *zap.SugaredLogger, lqd, ltd time.Duration) Backend {
	store := isql.NewDB(db, log.Desugar(), "Deadlock found when trying to get lock", lqd, ltd)
	return &MySQLBackend{
		db: store,
	}
}

func (b *MySQLBackend) Close() error {
	return b.db.Close()
}

func (b *MySQLBackend) Version(_ context.Context) (string, string, error) {
	var version string
	if err := b.db.QueryRow("select version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "MySQL", version, nil
}
