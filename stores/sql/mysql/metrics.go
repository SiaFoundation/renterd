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
func NewMetricsDatabase(db *dsql.DB, log *zap.SugaredLogger, lqd, ltd time.Duration) (*MetricsDatabase, error) {
	store, err := sql.NewDB(db, log.Desugar(), deadlockMsgs, lqd, ltd)
	return &MetricsDatabase{
		db:  store,
		log: log,
	}, err
}

func (b *MetricsDatabase) ApplyMigration(ctx context.Context, fn func(tx sql.Tx) (bool, error)) error {
	return applyMigration(ctx, b.db, fn)
}

func (b *MetricsDatabase) Close() error {
	return b.db.Close()
}

func (b *MetricsDatabase) DB() *sql.DB {
	return b.db
}

func (b *MetricsDatabase) CreateMigrationTable(ctx context.Context) error {
	return createMigrationTable(ctx, b.db)
}

func (b *MetricsDatabase) Migrate(ctx context.Context) error {
	return sql.PerformMigrations(ctx, b, migrationsFs, "metrics", sql.MetricsMigrations(ctx, migrationsFs, b.log))
}

func (b *MetricsDatabase) Version(ctx context.Context) (string, string, error) {
	return version(ctx, b.db)
}
