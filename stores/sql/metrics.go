package sql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/sql"
)

const (
	contractMetricGranularity = 5 * time.Minute
)

type (
	ContractMetricsQueryOpts struct {
		api.ContractMetricsQueryOpts
		IndexHint string
	}
)

func ContractMetrics(ctx context.Context, tx sql.Tx, start time.Time, n uint64, interval time.Duration, opts ContractMetricsQueryOpts) ([]api.ContractMetric, error) {
	// define a helper function to scan a contract metric from a row.
	scanContractMetric := func(rows *sql.LoggedRows, aggregate bool) (cm api.ContractMetric, err error) {
		var placeHolder int64
		var placeHolderTime time.Time
		var placeHolderHK PublicKey
		var placeHolderFCID FileContractID
		var placeHolderRevisionNumber Unsigned64

		var timestamp UnixTimeMS
		err = rows.Scan(
			&placeHolder,
			&placeHolderTime,
			&timestamp,
			&placeHolderFCID,
			&placeHolderHK,
			(*Unsigned64)(&cm.RemainingCollateral.Lo), (*Unsigned64)(&cm.RemainingCollateral.Hi),
			(*Unsigned64)(&cm.RemainingFunds.Lo), (*Unsigned64)(&cm.RemainingFunds.Hi),
			&placeHolderRevisionNumber,
			(*Unsigned64)(&cm.UploadSpending.Lo), (*Unsigned64)(&cm.UploadSpending.Hi),
			(*Unsigned64)(&cm.DownloadSpending.Lo), (*Unsigned64)(&cm.DownloadSpending.Hi),
			(*Unsigned64)(&cm.FundAccountSpending.Lo), (*Unsigned64)(&cm.FundAccountSpending.Hi),
			(*Unsigned64)(&cm.DeleteSpending.Lo), (*Unsigned64)(&cm.DeleteSpending.Hi),
			(*Unsigned64)(&cm.ListSpending.Lo), (*Unsigned64)(&cm.ListSpending.Hi),
		)
		if err != nil {
			err = fmt.Errorf("failed to scan contract metric: %w", err)
			return
		}

		cm.Timestamp = api.TimeRFC3339(normaliseTimestamp(start, interval, timestamp))
		if !aggregate {
			cm.ContractID = types.FileContractID(placeHolderFCID)
			cm.HostKey = types.PublicKey(placeHolderHK)
			cm.RevisionNumber = uint64(placeHolderRevisionNumber)
		}
		return
	}

	// if a host filter is set, query periods
	if opts.ContractID != (types.FileContractID{}) || opts.HostKey != (types.PublicKey{}) {
		return queryPeriods(ctx, tx, start, n, interval, opts.ContractMetricsQueryOpts, func(rows *sql.LoggedRows) (api.ContractMetric, error) {
			return scanContractMetric(rows, false)
		})
	}

	// otherwise we return the aggregated metrics for each period
	return queryAggregatedPeriods(ctx, tx, start, n, interval, opts.IndexHint, func(rows *sql.LoggedRows) (api.ContractMetric, error) {
		return scanContractMetric(rows, true)
	})
}

func ContractPruneMetrics(ctx context.Context, tx sql.Tx, start time.Time, n uint64, interval time.Duration, opts api.ContractPruneMetricsQueryOpts) ([]api.ContractPruneMetric, error) {
	return queryPeriods(ctx, tx, start, n, interval, opts, func(rows *sql.LoggedRows) (m api.ContractPruneMetric, err error) {
		var placeHolder int64
		var placeHolderTime time.Time
		var timestamp UnixTimeMS
		err = rows.Scan(
			&placeHolder,
			&placeHolderTime,
			&timestamp,
			(*FileContractID)(&m.ContractID),
			(*PublicKey)(&m.HostKey),
			&m.HostVersion,
			(*Unsigned64)(&m.Pruned),
			(*Unsigned64)(&m.Remaining),
			(*DurationMS)(&m.Duration),
		)
		if err != nil {
			err = fmt.Errorf("failed to scan contract prune metric: %w", err)
			return
		}
		m.Timestamp = api.TimeRFC3339(normaliseTimestamp(start, interval, timestamp))
		return
	})
}

func ContractSetChurnMetrics(ctx context.Context, tx sql.Tx, start time.Time, n uint64, interval time.Duration, opts api.ContractSetChurnMetricsQueryOpts) ([]api.ContractSetChurnMetric, error) {
	return queryPeriods(ctx, tx, start, n, interval, opts, func(rows *sql.LoggedRows) (m api.ContractSetChurnMetric, err error) {
		var placeHolder int64
		var placeHolderTime time.Time
		var timestamp UnixTimeMS
		err = rows.Scan(
			&placeHolder,
			&placeHolderTime,
			&timestamp,
			&m.Name,
			(*FileContractID)(&m.ContractID),
			&m.Direction,
			&m.Reason,
		)
		if err != nil {
			err = fmt.Errorf("failed to scan contract set churn metric: %w", err)
			return
		}
		m.Timestamp = api.TimeRFC3339(normaliseTimestamp(start, interval, timestamp))
		return
	})
}

func ContractSetMetrics(ctx context.Context, tx sql.Tx, start time.Time, n uint64, interval time.Duration, opts api.ContractSetMetricsQueryOpts) ([]api.ContractSetMetric, error) {
	return queryPeriods(ctx, tx, start, n, interval, opts, func(rows *sql.LoggedRows) (m api.ContractSetMetric, err error) {
		var placeHolder int64
		var placeHolderTime time.Time
		var timestamp UnixTimeMS
		err = rows.Scan(
			&placeHolder,
			&placeHolderTime,
			&timestamp,
			&m.Name,
			&m.Contracts,
		)
		if err != nil {
			err = fmt.Errorf("failed to scan contract set metric: %w", err)
			return
		}
		m.Timestamp = api.TimeRFC3339(normaliseTimestamp(start, interval, timestamp))
		return
	})
}

func PruneMetrics(ctx context.Context, tx sql.Tx, metric string, cutoff time.Time) error {
	if metric == "" {
		return errors.New("metric must be set")
	} else if cutoff.IsZero() {
		return errors.New("cutoff time must be set")
	}

	var table string
	switch metric {
	case api.MetricContractPrune:
		table = "contract_prunes"
	case api.MetricContractSet:
		table = "contract_sets"
	case api.MetricContractSetChurn:
		table = "contract_sets_churn"
	case api.MetricContract:
		table = "contracts"
	case api.MetricPerformance:
		table = "performance"
	case api.MetricWallet:
		table = "wallets"
	default:
		return fmt.Errorf("unknown metric '%s'", metric)
	}
	_, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE timestamp < ?", table), UnixTimeMS(cutoff))
	return err
}

func RecordContractMetric(ctx context.Context, tx sql.Tx, metrics ...api.ContractMetric) error {
	insertStmt, err := tx.Prepare(ctx, "INSERT INTO contracts (created_at, timestamp, fcid, host, remaining_collateral_lo, remaining_collateral_hi, remaining_funds_lo, remaining_funds_hi, revision_number, upload_spending_lo, upload_spending_hi, download_spending_lo, download_spending_hi, fund_account_spending_lo, fund_account_spending_hi, delete_spending_lo, delete_spending_hi, list_spending_lo, list_spending_hi) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement to insert contract metric: %w", err)
	}
	defer insertStmt.Close()

	deleteStmt, err := tx.Prepare(ctx, "DELETE FROM contracts WHERE fcid = ? AND timestamp >= ? AND timestamp < ?")
	if err != nil {
		return fmt.Errorf("failed to prepare statement to delete contract metric: %w", err)
	}
	defer deleteStmt.Close()

	for _, metric := range metrics {
		// delete any existing metric for the same contract that has happened
		// within the same 5' window by diving the timestamp by 5' and use
		// integer division.
		intervalStart := metric.Timestamp.Std().Truncate(contractMetricGranularity)
		intervalEnd := intervalStart.Add(contractMetricGranularity)
		_, err := deleteStmt.Exec(ctx,
			FileContractID(metric.ContractID),
			UnixTimeMS(intervalStart),
			UnixTimeMS(intervalEnd),
		)

		res, err := insertStmt.Exec(ctx,
			time.Now().UTC(),
			UnixTimeMS(metric.Timestamp),
			FileContractID(metric.ContractID),
			PublicKey(metric.HostKey),
			Unsigned64(metric.RemainingCollateral.Lo),
			Unsigned64(metric.RemainingCollateral.Hi),
			Unsigned64(metric.RemainingFunds.Lo),
			Unsigned64(metric.RemainingFunds.Hi),
			Unsigned64(metric.RevisionNumber),
			Unsigned64(metric.UploadSpending.Lo),
			Unsigned64(metric.UploadSpending.Hi),
			Unsigned64(metric.DownloadSpending.Lo),
			Unsigned64(metric.DownloadSpending.Hi),
			Unsigned64(metric.FundAccountSpending.Lo),
			Unsigned64(metric.FundAccountSpending.Hi),
			Unsigned64(metric.DeleteSpending.Lo),
			Unsigned64(metric.DeleteSpending.Hi),
			Unsigned64(metric.ListSpending.Lo),
			Unsigned64(metric.ListSpending.Hi),
		)
		if err != nil {
			return fmt.Errorf("failed to insert contract metric: %w", err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n == 0 {
			return fmt.Errorf("failed to insert contract metric: no rows affected")
		}
	}

	return nil
}

func RecordContractPruneMetric(ctx context.Context, tx sql.Tx, metrics ...api.ContractPruneMetric) error {
	insertStmt, err := tx.Prepare(ctx, "INSERT INTO contract_prunes (created_at, timestamp, fcid, host, host_version, pruned, remaining, duration) VALUES (?, ?,?, ?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement to insert contract prune metric: %w", err)
	}
	defer insertStmt.Close()

	for _, metric := range metrics {
		res, err := insertStmt.Exec(ctx,
			time.Now().UTC(),
			UnixTimeMS(metric.Timestamp),
			FileContractID(metric.ContractID),
			PublicKey(metric.HostKey),
			metric.HostVersion,
			Unsigned64(metric.Pruned),
			Unsigned64(metric.Remaining),
			(DurationMS)(metric.Duration),
		)
		if err != nil {
			return fmt.Errorf("failed to insert contract prune metric: %w", err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n == 0 {
			return fmt.Errorf("failed to insert contract prune metric: no rows affected")
		}
	}

	return nil
}

func RecordContractSetChurnMetric(ctx context.Context, tx sql.Tx, metrics ...api.ContractSetChurnMetric) error {
	insertStmt, err := tx.Prepare(ctx, "INSERT INTO contract_sets_churn (created_at, timestamp, name, fc_id, direction, reason) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement to insert contract set churn metric: %w", err)
	}
	defer insertStmt.Close()

	for _, metric := range metrics {
		res, err := insertStmt.Exec(ctx,
			time.Now().UTC(),
			UnixTimeMS(metric.Timestamp),
			metric.Name,
			FileContractID(metric.ContractID),
			metric.Direction,
			metric.Reason,
		)
		if err != nil {
			return fmt.Errorf("failed to insert contract set churn metric: %w", err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n == 0 {
			return fmt.Errorf("failed to insert contract set churn metric: no rows affected")
		}
	}

	return nil
}

func RecordContractSetMetric(ctx context.Context, tx sql.Tx, metrics ...api.ContractSetMetric) error {
	insertStmt, err := tx.Prepare(ctx, "INSERT INTO contract_sets (created_at, timestamp, name, contracts) VALUES (?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement to insert contract set metric: %w", err)
	}
	defer insertStmt.Close()

	for _, metric := range metrics {
		res, err := insertStmt.Exec(ctx,
			time.Now().UTC(),
			UnixTimeMS(metric.Timestamp),
			metric.Name,
			metric.Contracts,
		)
		if err != nil {
			return fmt.Errorf("failed to insert contract set metric: %w", err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n == 0 {
			return fmt.Errorf("failed to insert contract set metric: no rows affected")
		}
	}

	return nil
}

func RecordWalletMetric(ctx context.Context, tx sql.Tx, metrics ...api.WalletMetric) error {
	insertStmt, err := tx.Prepare(ctx, "INSERT INTO wallets (created_at, timestamp, confirmed_lo, confirmed_hi, spendable_lo, spendable_hi, unconfirmed_lo, unconfirmed_hi, immature_hi, immature_lo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return fmt.Errorf("failed to prepare statement to insert wallet metric: %w", err)
	}
	defer insertStmt.Close()

	for _, metric := range metrics {
		res, err := insertStmt.Exec(ctx,
			time.Now().UTC(),
			UnixTimeMS(metric.Timestamp),
			Unsigned64(metric.Confirmed.Lo),
			Unsigned64(metric.Confirmed.Hi),
			Unsigned64(metric.Spendable.Lo),
			Unsigned64(metric.Spendable.Hi),
			Unsigned64(metric.Unconfirmed.Lo),
			Unsigned64(metric.Unconfirmed.Hi),
			Unsigned64(metric.Immature.Lo),
			Unsigned64(metric.Immature.Hi),
		)
		if err != nil {
			return fmt.Errorf("failed to insert wallet metric: %w", err)
		} else if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n == 0 {
			return fmt.Errorf("failed to insert wallet metric: no rows affected")
		}
	}

	return nil
}

func WalletMetrics(ctx context.Context, tx sql.Tx, start time.Time, n uint64, interval time.Duration, opts api.WalletMetricsQueryOpts) ([]api.WalletMetric, error) {
	return queryPeriods(ctx, tx, start, n, interval, opts, func(rows *sql.LoggedRows) (m api.WalletMetric, err error) {
		var placeHolder int64
		var placeHolderTime time.Time
		var timestamp UnixTimeMS
		err = rows.Scan(
			&placeHolder,
			&placeHolderTime,
			&timestamp,
			(*Unsigned64)(&m.Confirmed.Lo), (*Unsigned64)(&m.Confirmed.Hi),
			(*Unsigned64)(&m.Spendable.Lo), (*Unsigned64)(&m.Spendable.Hi),
			(*Unsigned64)(&m.Unconfirmed.Lo), (*Unsigned64)(&m.Unconfirmed.Hi),
			(*Unsigned64)(&m.Immature.Lo), (*Unsigned64)(&m.Immature.Hi),
		)
		if err != nil {
			err = fmt.Errorf("failed to scan contract set metric: %w", err)
			return
		}
		m.Timestamp = api.TimeRFC3339(normaliseTimestamp(start, interval, timestamp))
		return
	})
}

func queryPeriods[T any](ctx context.Context, tx sql.Tx, start time.Time, n uint64, interval time.Duration, opts interface{}, scanRowFn func(*sql.LoggedRows) (T, error)) ([]T, error) {
	if n > api.MetricMaxIntervals {
		return nil, api.ErrMaxIntervalsExceeded
	}
	params := []interface{}{
		UnixTimeMS(start),
		interval.Milliseconds(),
		UnixTimeMS(start.Add(time.Duration(n) * interval)),
		interval.Milliseconds(),
		interval.Milliseconds(),
	}

	query := "1=1"
	var table string
	switch opts := opts.(type) {
	case api.ContractMetricsQueryOpts:
		table = "contracts"
		if opts.ContractID != (types.FileContractID{}) {
			query += " AND fcid = ?"
			params = append(params, FileContractID(opts.ContractID))
		}
		if opts.HostKey != (types.PublicKey{}) {
			query += " AND host = ?"
			params = append(params, PublicKey(opts.HostKey))
		}
	case api.ContractPruneMetricsQueryOpts:
		table = "contract_prunes"
		if opts.ContractID != (types.FileContractID{}) {
			query += " AND fcid = ?"
			params = append(params, FileContractID(opts.ContractID))
		}
		if opts.HostKey != (types.PublicKey{}) {
			query += " AND host = ?"
			params = append(params, PublicKey(opts.HostKey))
		}
		if opts.HostVersion != "" {
			query += " AND host_version = ?"
			params = append(params, opts.HostVersion)
		}
	case api.ContractSetChurnMetricsQueryOpts:
		table = "contract_sets_churn"
		if opts.Name != "" {
			query += " AND name = ?"
			params = append(params, opts.Name)
		}
		if opts.Direction != "" {
			query += " AND direction = ?"
			params = append(params, opts.Direction)
		}
		if opts.Reason != "" {
			query += " AND reason = ?"
			params = append(params, opts.Reason)
		}
	case api.ContractSetMetricsQueryOpts:
		table = "contract_sets"
		if opts.Name != "" {
			query += " AND name = ?"
			params = append(params, opts.Name)
		}
	case api.PerformanceMetricsQueryOpts:
		table = "performance"
		if opts.Action != "" {
			query += " AND action = ?"
			params = append(params, opts.Action)
		}
		if opts.HostKey != (types.PublicKey{}) {
			query += " AND host = ?"
			params = append(params, PublicKey(opts.HostKey))
		}
		if opts.Origin != "" {
			query += " AND origin = ?"
			params = append(params, opts.Origin)
		}
	case api.WalletMetricsQueryOpts:
		table = "wallets"
	default:
		return nil, fmt.Errorf("unknown query opts type: %T", opts)
	}

	rows, err := tx.Query(ctx, fmt.Sprintf(`
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
		WHERE %s
		GROUP BY
			p.period_start
		) i ON %s.id = i.id ORDER BY Period ASC
	`, table, table, table, query, table), params...)
	if err != nil {
		return nil, fmt.Errorf("failed to query periods: %w", err)
	}
	defer rows.Close()

	var result []T
	for rows.Next() {
		m, err := scanRowFn(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		result = append(result, m)
	}
	return result, nil
}

func queryAggregatedPeriods(ctx context.Context, tx sql.Tx, start time.Time, n uint64, interval time.Duration, indexHint string, scanRowFn func(int64 *sql.LoggedRows) (api.ContractMetric, error)) ([]api.ContractMetric, error) {
	if n > api.MetricMaxIntervals {
		return nil, api.ErrMaxIntervalsExceeded
	}
	end := start.Add(time.Duration(n) * interval)

	// fetch distinct contract ids
	rows, err := tx.Query(ctx,
		"SELECT DISTINCT fcid FROM contracts WHERE contracts.timestamp >= ? AND contracts.timestamp < ?",
		UnixTimeMS(start),
		UnixTimeMS(end),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch distinct contract ids: %w", err)
	}
	defer rows.Close()

	var fcids []FileContractID
	for rows.Next() {
		var fcid FileContractID
		if err := rows.Scan(&fcid); err != nil {
			return nil, fmt.Errorf("failed to scan contract id: %w", err)
		}
		fcids = append(fcids, fcid)
	}

	// prepare statement to fetch contract metrics
	queryStmt, err := tx.Prepare(ctx, fmt.Sprintf("SELECT * FROM contracts %s WHERE contracts.timestamp >= ? AND contracts.timestamp < ? AND contracts.fcid = ? LIMIT 1", indexHint))
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement to fetch contract metrics: %w", err)
	}
	defer queryStmt.Close()

	var result []api.ContractMetric
	currentPeriod := int64(math.MinInt64)
	for intervalStart := start; intervalStart.Before(end); intervalStart = intervalStart.Add(interval) {
		intervalEnd := intervalStart.Add(interval)
		period := intervalStart.UnixMilli()
		for _, fcid := range fcids {
			rows, err := queryStmt.Query(ctx, UnixTimeMS(intervalStart), UnixTimeMS(intervalEnd), FileContractID(fcid))
			if err != nil {
				return nil, fmt.Errorf("failed to fetch contract metrics: %w", err)
			}
			for rows.Next() {
				m, err := scanRowFn(rows)
				if err != nil {
					rows.Close()
					return nil, fmt.Errorf("failed to scan metric: %w", err)
				}

				if period != currentPeriod {
					result = append(result, m)
					currentPeriod = period
				} else {
					result[len(result)-1] = aggregateMetrics(result[len(result)-1], m)
				}
			}
			rows.Close()
		}
	}

	return result, nil
}

func aggregateMetrics(x, y api.ContractMetric) (out api.ContractMetric) {
	out = x
	out.RemainingCollateral, _ = out.RemainingCollateral.AddWithOverflow(y.RemainingCollateral)
	out.RemainingFunds, _ = out.RemainingFunds.AddWithOverflow(y.RemainingFunds)
	out.UploadSpending, _ = out.UploadSpending.AddWithOverflow(y.UploadSpending)
	out.DownloadSpending, _ = out.DownloadSpending.AddWithOverflow(y.DownloadSpending)
	out.FundAccountSpending, _ = out.FundAccountSpending.AddWithOverflow(y.FundAccountSpending)
	out.DeleteSpending, _ = out.DeleteSpending.AddWithOverflow(y.DeleteSpending)
	out.ListSpending, _ = out.ListSpending.AddWithOverflow(y.ListSpending)
	return
}

func normaliseTimestamp(start time.Time, interval time.Duration, t UnixTimeMS) UnixTimeMS {
	startMS := start.UnixMilli()
	toNormaliseMS := time.Time(t).UnixMilli()
	intervalMS := interval.Milliseconds()
	if startMS > toNormaliseMS {
		return UnixTimeMS(start)
	}
	normalizedMS := (toNormaliseMS-startMS)/intervalMS*intervalMS + start.UnixMilli()
	return UnixTimeMS(time.UnixMilli(normalizedMS))
}
