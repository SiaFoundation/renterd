package stores

import (
	"context"
	"time"

	"go.sia.tech/renterd/v2/api"
	sql "go.sia.tech/renterd/v2/stores/sql"
)

func (s *SQLStore) ContractMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractMetricsQueryOpts) (metrics []api.ContractMetric, err error) {
	err = s.dbMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) (txErr error) {
		metrics, txErr = tx.ContractMetrics(ctx, start, n, interval, opts)
		return
	})
	return
}

func (s *SQLStore) ContractPruneMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractPruneMetricsQueryOpts) (metrics []api.ContractPruneMetric, err error) {
	err = s.dbMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) (txErr error) {
		metrics, txErr = tx.ContractPruneMetrics(ctx, start, n, interval, opts)
		return
	})
	return
}

func (s *SQLStore) RecordContractMetric(ctx context.Context, metrics ...api.ContractMetric) error {
	return s.dbMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) error {
		return tx.RecordContractMetric(ctx, metrics...)
	})
}

func (s *SQLStore) RecordContractPruneMetric(ctx context.Context, metrics ...api.ContractPruneMetric) error {
	return s.dbMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) error {
		return tx.RecordContractPruneMetric(ctx, metrics...)
	})
}

func (s *SQLStore) RecordWalletMetric(ctx context.Context, metrics ...api.WalletMetric) error {
	return s.dbMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) error {
		return tx.RecordWalletMetric(ctx, metrics...)
	})
}

func (s *SQLStore) WalletMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.WalletMetricsQueryOpts) (metrics []api.WalletMetric, err error) {
	err = s.dbMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) (txErr error) {
		metrics, txErr = tx.WalletMetrics(ctx, start, n, interval, opts)
		return
	})
	return
}

func (s *SQLStore) PruneMetrics(ctx context.Context, metric string, cutoff time.Time) error {
	return s.dbMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) error {
		return tx.PruneMetrics(ctx, metric, cutoff)
	})
}
