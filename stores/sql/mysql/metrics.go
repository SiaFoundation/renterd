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

func (b *MetricsDatabase) ApplyMigration(fn func(tx sql.Tx) (bool, error)) error {
	return applyMigration(b.db, fn)
}

func (b *MetricsDatabase) Close() error {
	return b.db.Close()
}

func (b *MetricsDatabase) DB() *sql.DB {
	return b.db
}

func (b *MetricsDatabase) CreateMigrationTable() error {
	return createMigrationTable(b.db)
}

func (b *MetricsDatabase) Migrate() error {
	return sql.PerformMigrations(b, migrationsFs, "metrics", sql.MetricsMigrations(migrationsFs, b.log))
}

func (b *MetricsDatabase) Version(_ context.Context) (string, string, error) {
	return version(b.db)
}
