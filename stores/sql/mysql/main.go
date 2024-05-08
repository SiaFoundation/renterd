package mysql

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

// NewMySQLDatabase creates a new MySQL backend.
func NewMySQLDatabase(db *sql.DB, log *zap.Logger, lqd, ltd time.Duration) *MainDatabase {
	store := isql.NewDB(db, log, "Deadlock found when trying to get lock", lqd, ltd)
	return &MainDatabase{
		db: store,
	}
}

func (b *MainDatabase) Close() error {
	return b.db.Close()
}

func (b *MainDatabase) Version(_ context.Context) (string, string, error) {
	var version string
	if err := b.db.QueryRow("select version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "MySQL", version, nil
}
