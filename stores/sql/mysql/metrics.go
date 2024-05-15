package mysql

import (
	"context"
	"time"

	dsql "database/sql"

	"go.sia.tech/renterd/internal/sql"

	"go.uber.org/zap"
)

type MetricsDatabase struct {
	log *zap.SugaredLogger
	db  *sql.DB
}

// NewMetricsDatabase creates a new MySQL backend.
func NewMetricsDatabase(db *dsql.DB, log *zap.SugaredLogger, lqd, ltd time.Duration) *MetricsDatabase {
	store := sql.NewDB(db, log.Desugar(), "Deadlock found when trying to get lock", lqd, ltd)
	return &MetricsDatabase{
		db:  store,
		log: log,
	}
}

func (b *MetricsDatabase) Close() error {
	return b.db.Close()
}

func (b *MetricsDatabase) Migrate() error {
	dbIdentifier := "metrics"
	return performMigrations(b.db, dbIdentifier, []migration{
		{
			ID:      "00001_init",
			Migrate: func(tx sql.Tx) error { return sql.ErrRunV072 },
		},
		{
			ID: "00001_idx_contracts_fcid_timestamp",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00001_idx_contracts_fcid_timestamp", b.log)
			},
		},
	}, b.log)
}

func (b *MetricsDatabase) Version(_ context.Context) (string, string, error) {
	return version(b.db)
}
