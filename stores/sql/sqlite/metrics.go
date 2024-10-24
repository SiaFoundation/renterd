package sqlite

import (
	"context"
	dsql "database/sql"
	"encoding/hex"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/sql"
	ssql "go.sia.tech/renterd/stores/sql"
	"lukechampine.com/frand"

	"go.uber.org/zap"
)

type (
	MetricsDatabase struct {
		db  *sql.DB
		log *zap.SugaredLogger
	}

	MetricsDatabaseTx struct {
		sql.Tx
		log *zap.SugaredLogger
	}
)

var _ ssql.MetricsDatabaseTx = (*MetricsDatabaseTx)(nil)

// NewSQLiteDatabase creates a new SQLite backend.
func NewMetricsDatabase(db *dsql.DB, log *zap.Logger, lqd, ltd time.Duration) (*MetricsDatabase, error) {
	log = log.Named("metrics")
	store, err := sql.NewDB(db, log, deadlockMsgs, lqd, ltd)
	return &MetricsDatabase{
		db:  store,
		log: log.Sugar(),
	}, err
}

func (b *MetricsDatabase) ApplyMigration(ctx context.Context, fn func(tx sql.Tx) (bool, error)) error {
	return applyMigration(ctx, b.db, fn)
}

func (b *MetricsDatabase) Close() error {
	return closeDB(b.db, b.log)
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

func (tx *MetricsDatabaseTx) ContractMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractMetricsQueryOpts) ([]api.ContractMetric, error) {
	return ssql.ContractMetrics(ctx, tx, start, n, interval, ssql.ContractMetricsQueryOpts{ContractMetricsQueryOpts: opts})
}

func (tx *MetricsDatabaseTx) ContractPruneMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractPruneMetricsQueryOpts) ([]api.ContractPruneMetric, error) {
	return ssql.ContractPruneMetrics(ctx, tx, start, n, interval, opts)
}

func (tx *MetricsDatabaseTx) ContractSetChurnMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetChurnMetricsQueryOpts) ([]api.ContractSetChurnMetric, error) {
	return ssql.ContractSetChurnMetrics(ctx, tx, start, n, interval, opts)
}

func (tx *MetricsDatabaseTx) ContractSetMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetMetricsQueryOpts) (metrics []api.ContractSetMetric, _ error) {
	return ssql.ContractSetMetrics(ctx, tx, start, n, interval, opts)
}

func (tx *MetricsDatabaseTx) PruneMetrics(ctx context.Context, metric string, cutoff time.Time) error {
	return ssql.PruneMetrics(ctx, tx, metric, cutoff)
}

func (tx *MetricsDatabaseTx) RecordContractMetric(ctx context.Context, metrics ...api.ContractMetric) error {
	return ssql.RecordContractMetric(ctx, tx, metrics...)
}

func (tx *MetricsDatabaseTx) RecordContractPruneMetric(ctx context.Context, metrics ...api.ContractPruneMetric) error {
	return ssql.RecordContractPruneMetric(ctx, tx, metrics...)
}

func (tx *MetricsDatabaseTx) RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error {
	return ssql.RecordContractSetChurnMetric(ctx, tx, metrics...)
}

func (tx *MetricsDatabaseTx) RecordContractSetMetric(ctx context.Context, metrics ...api.ContractSetMetric) error {
	return ssql.RecordContractSetMetric(ctx, tx, metrics...)
}

func (tx *MetricsDatabaseTx) RecordWalletMetric(ctx context.Context, metrics ...api.WalletMetric) error {
	return ssql.RecordWalletMetric(ctx, tx, metrics...)
}

func (tx *MetricsDatabaseTx) WalletMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.WalletMetricsQueryOpts) ([]api.WalletMetric, error) {
	return ssql.WalletMetrics(ctx, tx, start, n, interval, opts)
}
