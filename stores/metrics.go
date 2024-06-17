package stores

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type (
	// dbContractMetric tracks information about a contract's funds.  It is
	// supposed to be reported by a worker every time a contract is revised.
	dbContractMetric struct {
		Model

		Timestamp unixTimeMS `gorm:"index;NOT NULL"`

		FCID fileContractID `gorm:"index;size:32;NOT NULL;column:fcid"`
		Host publicKey      `gorm:"index;size:32;NOT NULL"`

		RemainingCollateralLo unsigned64 `gorm:"index:idx_remaining_collateral;NOT NULL"`
		RemainingCollateralHi unsigned64 `gorm:"index:idx_remaining_collateral;NOT NULL"`
		RemainingFundsLo      unsigned64 `gorm:"index:idx_remaining_funds;NOT NULL"`
		RemainingFundsHi      unsigned64 `gorm:"index:idx_remaining_funds;NOT NULL"`
		RevisionNumber        unsigned64 `gorm:"index;NOT NULL"`

		UploadSpendingLo      unsigned64 `gorm:"index:idx_upload_spending;NOT NULL"`
		UploadSpendingHi      unsigned64 `gorm:"index:idx_upload_spending;NOT NULL"`
		DownloadSpendingLo    unsigned64 `gorm:"index:idx_download_spending;NOT NULL"`
		DownloadSpendingHi    unsigned64 `gorm:"index:idx_download_spending;NOT NULL"`
		FundAccountSpendingLo unsigned64 `gorm:"index:idx_fund_account_spending;NOT NULL"`
		FundAccountSpendingHi unsigned64 `gorm:"index:idx_fund_account_spending;NOT NULL"`
		DeleteSpendingLo      unsigned64 `gorm:"index:idx_delete_spending;NOT NULL"`
		DeleteSpendingHi      unsigned64 `gorm:"index:idx_delete_spending;NOT NULL"`
		ListSpendingLo        unsigned64 `gorm:"index:idx_list_spending;NOT NULL"`
		ListSpendingHi        unsigned64 `gorm:"index:idx_list_spending;NOT NULL"`
	}

	// dbContractPruneMetric tracks information about contract pruning. Such as
	// the number of bytes pruned, how much data there is left to prune and how
	// long it took, along with potential errors that occurred while trying to
	// prune the contract.
	dbContractPruneMetric struct {
		Model

		Timestamp unixTimeMS `gorm:"index;NOT NULL"`

		FCID        fileContractID `gorm:"index;size:32;NOT NULL;column:fcid"`
		Host        publicKey      `gorm:"index;size:32;NOT NULL"`
		HostVersion string         `gorm:"index"`

		Pruned    unsigned64    `gorm:"index;NOT NULL"`
		Remaining unsigned64    `gorm:"index;NOT NULL"`
		Duration  time.Duration `gorm:"index;NOT NULL"`
	}

	// dbContractSetMetric tracks information about a specific contract set.
	// Such as the number of contracts it contains. Intended to be reported by
	// the bus every time the set is updated.
	dbContractSetMetric struct {
		Model
		Timestamp unixTimeMS `gorm:"index;NOT NULL"`

		Name      string `gorm:"index;NOT NULL"`
		Contracts int    `gorm:"index;NOT NULL"`
	}

	// dbContractSetChurnMetric contains information about contracts being added
	// to / removed from a contract set. Expected to be reported by the entity
	// updating the set. e.g. the autopilot.
	dbContractSetChurnMetric struct {
		Model
		Timestamp unixTimeMS `gorm:"index;NOT NULL"`

		Name      string         `gorm:"index;NOT NULL"`
		FCID      fileContractID `gorm:"index;size:32;NOT NULL"`
		Direction string         `gorm:"index;NOT NULL"` // "added" or "removed"
		Reason    string         `gorm:"index;NOT NULL"`
	}

	// dbPerformanceMetric is a generic metric used to track the performance of
	// an action. Such an action could be a ReadSector operation. Expected to be
	// reported by workers.
	dbPerformanceMetric struct {
		Model
		Timestamp unixTimeMS `gorm:"index;NOT NULL"`

		Action   string        `gorm:"index;NOT NULL"`
		Host     publicKey     `gorm:"index;size:32;NOT NULL"`
		Origin   string        `gorm:"index;NOT NULL"`
		Duration time.Duration `gorm:"index;NOT NULL"`
	}

	// dbWalletMetric tracks information about a specific wallet.
	dbWalletMetric struct {
		Model
		Timestamp unixTimeMS `gorm:"index;NOT NULL"`

		ConfirmedLo   unsigned64 `gorm:"index:idx_confirmed;NOT NULL"`
		ConfirmedHi   unsigned64 `gorm:"index:idx_confirmed;NOT NULL"`
		SpendableLo   unsigned64 `gorm:"index:idx_spendable;NOT NULL"`
		SpendableHi   unsigned64 `gorm:"index:idx_spendable;NOT NULL"`
		UnconfirmedLo unsigned64 `gorm:"index:idx_unconfirmed;NOT NULL"`
		UnconfirmedHi unsigned64 `gorm:"index:idx_unconfirmed;NOT NULL"`
	}
)

func (dbContractMetric) TableName() string         { return "contracts" }
func (dbContractPruneMetric) TableName() string    { return "contract_prunes" }
func (dbContractSetMetric) TableName() string      { return "contract_sets" }
func (dbContractSetChurnMetric) TableName() string { return "contract_sets_churn" }
func (dbPerformanceMetric) TableName() string      { return "performance" }
func (dbWalletMetric) TableName() string           { return "wallets" }

func (s *SQLStore) ContractMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractMetricsQueryOpts) (metrics []api.ContractMetric, err error) {
	err = s.bMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) (txErr error) {
		metrics, txErr = tx.ContractMetrics(ctx, start, n, interval, opts)
		return
	})
	return
}

func (s *SQLStore) ContractPruneMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractPruneMetricsQueryOpts) (metrics []api.ContractPruneMetric, err error) {
	err = s.bMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) (txErr error) {
		metrics, txErr = tx.ContractPruneMetrics(ctx, start, n, interval, opts)
		return
	})
	return
}

func (s *SQLStore) ContractSetChurnMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetChurnMetricsQueryOpts) (metrics []api.ContractSetChurnMetric, err error) {
	err = s.bMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) (txErr error) {
		metrics, txErr = tx.ContractSetChurnMetrics(ctx, start, n, interval, opts)
		return
	})
	return
}

func (s *SQLStore) ContractSetMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.ContractSetMetricsQueryOpts) (metrics []api.ContractSetMetric, err error) {
	err = s.bMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) (txErr error) {
		metrics, txErr = tx.ContractSetMetrics(ctx, start, n, interval, opts)
		return
	})
	return
}

func (s *SQLStore) PerformanceMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.PerformanceMetricsQueryOpts) ([]api.PerformanceMetric, error) {
	metrics, err := s.performanceMetrics(ctx, start, n, interval, opts)
	if err != nil {
		return nil, err
	}
	resp := make([]api.PerformanceMetric, len(metrics))
	for i := range resp {
		resp[i] = api.PerformanceMetric{
			Action:    metrics[i].Action,
			HostKey:   types.PublicKey(metrics[i].Host),
			Origin:    metrics[i].Origin,
			Duration:  metrics[i].Duration,
			Timestamp: api.TimeRFC3339(time.Time(metrics[i].Timestamp).UTC()),
		}
	}
	return resp, nil
}

func (s *SQLStore) RecordContractMetric(ctx context.Context, metrics ...api.ContractMetric) error {
	return s.bMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) error {
		return tx.RecordContractMetric(ctx, metrics...)
	})
}

func (s *SQLStore) RecordContractPruneMetric(ctx context.Context, metrics ...api.ContractPruneMetric) error {
	return s.bMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) error {
		return tx.RecordContractPruneMetric(ctx, metrics...)
	})
}

func (s *SQLStore) RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error {
	return s.bMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) error {
		return tx.RecordContractSetChurnMetric(ctx, metrics...)
	})
}

func (s *SQLStore) RecordContractSetMetric(ctx context.Context, metrics ...api.ContractSetMetric) error {
	return s.bMetrics.Transaction(ctx, func(tx sql.MetricsDatabaseTx) error {
		return tx.RecordContractSetMetric(ctx, metrics...)
	})
}

func (s *SQLStore) RecordWalletMetric(ctx context.Context, metrics ...api.WalletMetric) error {
	dbMetrics := make([]dbWalletMetric, len(metrics))
	for i, metric := range metrics {
		dbMetrics[i] = dbWalletMetric{
			Timestamp:     unixTimeMS(metric.Timestamp),
			ConfirmedLo:   unsigned64(metric.Confirmed.Lo),
			ConfirmedHi:   unsigned64(metric.Confirmed.Hi),
			SpendableLo:   unsigned64(metric.Spendable.Lo),
			SpendableHi:   unsigned64(metric.Spendable.Hi),
			UnconfirmedLo: unsigned64(metric.Unconfirmed.Lo),
			UnconfirmedHi: unsigned64(metric.Unconfirmed.Hi),
		}
	}
	return s.dbMetrics.Transaction(func(tx *gorm.DB) error {
		return tx.Create(&dbMetrics).Error
	})
}

func (s *SQLStore) RecordPerformanceMetric(ctx context.Context, metrics ...api.PerformanceMetric) error {
	dbMetrics := make([]dbPerformanceMetric, len(metrics))
	for i, metric := range metrics {
		dbMetrics[i] = dbPerformanceMetric{
			Action:    metric.Action,
			Duration:  metric.Duration,
			Host:      publicKey(metric.HostKey),
			Origin:    metric.Origin,
			Timestamp: unixTimeMS(metric.Timestamp),
		}
	}
	return s.dbMetrics.Transaction(func(tx *gorm.DB) error {
		return tx.Create(&dbMetrics).Error
	})
}

func (s *SQLStore) WalletMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.WalletMetricsQueryOpts) ([]api.WalletMetric, error) {
	metrics, err := s.walletMetrics(ctx, start, n, interval, opts)
	if err != nil {
		return nil, err
	}
	resp := make([]api.WalletMetric, len(metrics))
	toCurr := func(lo, hi unsigned64) types.Currency {
		return types.NewCurrency(uint64(lo), uint64(hi))
	}
	for i := range resp {
		resp[i] = api.WalletMetric{
			Timestamp:   api.TimeRFC3339(time.Time(metrics[i].Timestamp).UTC()),
			Confirmed:   toCurr(metrics[i].ConfirmedLo, metrics[i].ConfirmedHi),
			Spendable:   toCurr(metrics[i].SpendableLo, metrics[i].SpendableHi),
			Unconfirmed: toCurr(metrics[i].UnconfirmedLo, metrics[i].UnconfirmedHi),
		}
	}
	return resp, nil
}

func (s *SQLStore) PruneMetrics(ctx context.Context, metric string, cutoff time.Time) error {
	if metric == "" {
		return errors.New("metric must be set")
	} else if cutoff.IsZero() {
		return errors.New("cutoff time must be set")
	}
	var model interface{}
	switch metric {
	case api.MetricContractPrune:
		model = &dbContractPruneMetric{}
	case api.MetricContractSet:
		model = &dbContractSetMetric{}
	case api.MetricContractSetChurn:
		model = &dbContractSetChurnMetric{}
	case api.MetricContract:
		model = &dbContractMetric{}
	case api.MetricPerformance:
		model = &dbPerformanceMetric{}
	case api.MetricWallet:
		model = &dbWalletMetric{}
	default:
		return fmt.Errorf("unknown metric '%s'", metric)
	}
	return s.dbMetrics.Model(model).
		Where("timestamp < ?", unixTimeMS(cutoff)).
		Delete(model).
		Error
}

func normaliseTimestamp(start time.Time, interval time.Duration, t unixTimeMS) unixTimeMS {
	startMS := start.UnixMilli()
	toNormaliseMS := time.Time(t).UnixMilli()
	intervalMS := interval.Milliseconds()
	if startMS > toNormaliseMS {
		return unixTimeMS(start)
	}
	normalizedMS := (toNormaliseMS-startMS)/intervalMS*intervalMS + start.UnixMilli()
	return unixTimeMS(time.UnixMilli(normalizedMS))
}

// findPeriods is the core of all methods retrieving metrics. By using integer
// division rounding combined with a GROUP BY operation, all rows of a table are
// split into intervals and the row with the lowest timestamp for each interval
// is returned. The result is then joined with the original table to retrieve
// only the metrics we want.
func (s *SQLStore) findPeriods(ctx context.Context, table string, dst interface{}, start time.Time, n uint64, interval time.Duration, whereExpr clause.Expr) error {
	if n > api.MetricMaxIntervals {
		return api.ErrMaxIntervalsExceeded
	}
	end := start.Add(time.Duration(n) * interval)
	return s.dbMetrics.WithContext(ctx).Raw(fmt.Sprintf(`
		WITH RECURSIVE periods AS (
			SELECT ? AS period_start
			UNION ALL
			SELECT period_start + ?
			FROM periods
			WHERE period_start < ? - ?
		)
		SELECT %s.* FROM %s
		INNER JOIN (
		SELECT
			p.period_start as Period,
			MIN(obj.id) AS id
		FROM
			periods p
		INNER JOIN
			%s obj ON obj.timestamp >= p.period_start AND obj.timestamp < p.period_start + ?
		WHERE ?
		GROUP BY
			p.period_start
		) i ON %s.id = i.id ORDER BY Period ASC
	`, table, table, table, table),
		unixTimeMS(start),
		interval.Milliseconds(),
		unixTimeMS(end),
		interval.Milliseconds(),
		interval.Milliseconds(),
		whereExpr,
	).Scan(dst).
		Error
}

func (s *SQLStore) walletMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.WalletMetricsQueryOpts) (metrics []dbWalletMetric, err error) {
	err = s.findPeriods(ctx, dbWalletMetric{}.TableName(), &metrics, start, n, interval, gorm.Expr("TRUE"))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch wallet metrics: %w", err)
	}
	for i, m := range metrics {
		metrics[i].Timestamp = normaliseTimestamp(start, interval, m.Timestamp)
	}
	return
}

func (s *SQLStore) performanceMetrics(ctx context.Context, start time.Time, n uint64, interval time.Duration, opts api.PerformanceMetricsQueryOpts) ([]dbPerformanceMetric, error) {
	whereExpr := gorm.Expr("TRUE")
	if opts.Action != "" {
		whereExpr = gorm.Expr("? AND action = ?", whereExpr, opts.Action)
	}
	if opts.HostKey != (types.PublicKey{}) {
		whereExpr = gorm.Expr("? AND host = ?", whereExpr, publicKey(opts.HostKey))
	}
	if opts.Origin != "" {
		whereExpr = gorm.Expr("? AND origin = ?", whereExpr, opts.Origin)
	}

	var metrics []dbPerformanceMetric
	err := s.findPeriods(ctx, dbPerformanceMetric{}.TableName(), &metrics, start, n, interval, whereExpr)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch performance metrics: %w", err)
	}

	return metrics, nil
}
