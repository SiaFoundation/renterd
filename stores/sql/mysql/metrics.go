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
	return performMigrations(b.db, "metrics", sql.MetricsMigrations(migrationsFs, b.log), b.log)
}

func (b *MetricsDatabase) Version(_ context.Context) (string, string, error) {
	return version(b.db)
}
