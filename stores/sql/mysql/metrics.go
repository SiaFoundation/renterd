package mysql

import (
	"context"
	"encoding/hex"
	"time"

	dsql "database/sql"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/sql"
	ssql "go.sia.tech/renterd/stores/sql"
	"lukechampine.com/frand"

	"go.uber.org/zap"
)

type (
	MetricsDatabase struct {
		log *zap.SugaredLogger
		db  *sql.DB
	}

	MetricsDatabaseTx struct {
		sql.Tx
		log *zap.SugaredLogger
	}
)

var _ ssql.MetricsDatabaseTx = (*MetricsDatabaseTx)(nil)

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

func (b *MetricsDatabase) Transaction(ctx context.Context, fn func(tx ssql.MetricsDatabaseTx) error) error {
	return b.db.Transaction(ctx, func(tx sql.Tx) error {
		return fn(b.wrapTxn(tx))
	})
}

func (b *MetricsDatabase) Version(ctx context.Context) (string, string, error) {
	return version(ctx, b.db)
}

func (b *MetricsDatabase) wrapTxn(tx sql.Tx) *MetricsDatabaseTx {
	return &MetricsDatabaseTx{tx, b.log.Named(hex.EncodeToString(frand.Bytes(16)))}
}

func (tx *MetricsDatabaseTx) ContractPruneMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractPruneMetricsQueryOpts) ([]api.ContractPruneMetric, error) {
	return ssql.ContractPruneMetrics(ctx, tx, start, n, interval, opts)
}

func (tx *MetricsDatabaseTx) ContractSetMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetMetricsQueryOpts) (metrics []api.ContractSetMetric, _ error) {
	return ssql.ContractSetMetrics(ctx, tx, start, n, interval, opts)
}

func (tx *MetricsDatabaseTx) RecordContractPruneMetric(ctx context.Context, metrics ...api.ContractPruneMetric) error {
	return ssql.RecordContractPruneMetric(ctx, tx, metrics...)
}

func (tx *MetricsDatabaseTx) RecordContractSetMetric(ctx context.Context, metrics ...api.ContractSetMetric) error {
	return ssql.RecordContractSetMetric(ctx, tx, metrics...)
}
