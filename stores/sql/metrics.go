package sql

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/sql"
)

const (
	tableContractPruneMetric = "contract_prunes"
)

func ContractPruneMetrics(ctx context.Context, tx sql.Tx, start time.Time, n uint64, interval time.Duration, opts api.ContractPruneMetricsQueryOpts) (metrics []api.ContractPruneMetric, _ error) {
	rows, err := queryPeriods(ctx, tx, start, n, interval, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contract metrics: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var cpm api.ContractPruneMetric
		var placeHolder int64
		var placeHolderTime time.Time
		var timestamp UnixTimeMS
		if err := rows.Scan(&placeHolder, &placeHolderTime, &timestamp, (*FileContractID)(&cpm.ContractID), (*PublicKey)(&cpm.HostKey), &cpm.HostVersion, (*Unsigned64)(&cpm.Pruned), (*Unsigned64)(&cpm.Remaining), &cpm.Duration); err != nil {
			return nil, fmt.Errorf("failed to scan contract prune metric: %w", err)
		}
		cpm.Timestamp = api.TimeRFC3339(timestamp)
		metrics = append(metrics, cpm)
	}

	return metrics, nil
}

func RecordContractPruneMetric(ctx context.Context, tx sql.Tx, metrics ...api.ContractPruneMetric) error {
	insertStmt, err := tx.Prepare(ctx, fmt.Sprintf("INSERT INTO %s (created_at, timestamp, fcid, host, host_version, pruned, remaining, duration) VALUES (?, ?,?, ?, ?, ?, ?, ?)", tableContractPruneMetric))
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
			metric.Duration,
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

func queryPeriods(ctx context.Context, tx sql.Tx, start time.Time, n uint64, interval time.Duration, opts interface{}) (*sql.LoggedRows, error) {
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

	where, err := whereClauseFromQueryOpts(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build where clause: %w", err)
	} else if len(where.params) > 0 {
		params = append(params, where.params...)
	}

	return tx.Query(ctx, fmt.Sprintf(`
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
`, where.table, where.table, where.table, where.query, where.table), params...)
}

type whereClause struct {
	table  string
	query  string
	params []interface{}
}

func whereClauseFromQueryOpts(opts interface{}) (where whereClause, _ error) {
	where.query = "1=1"

	switch opts := opts.(type) {
	case api.ContractPruneMetricsQueryOpts:
		where.table = tableContractPruneMetric
		if opts.ContractID != (types.FileContractID{}) {
			where.query += " AND fcid = ?"
			where.params = append(where.params, FileContractID(opts.ContractID))
		}
		if opts.HostKey != (types.PublicKey{}) {
			where.query += " AND host = ?"
			where.params = append(where.params, PublicKey(opts.HostKey))
		}
		if opts.HostVersion != "" {
			where.query += " AND host_version = ?"
			where.params = append(where.params, opts.HostVersion)
		}
	default:
		return whereClause{}, fmt.Errorf("unknown query opts type: %T", opts)
	}

	return
}
