package sql

import (
	"context"
	"database/sql"
	"time"

	"go.uber.org/zap"
)

// The following types are wrappers for the sql package types, adding logging
// capabilities.
type (
	LoggedStmt struct {
		*sql.Stmt
		query             string
		log               *zap.Logger
		longQueryDuration time.Duration
	}

	loggedTxn struct {
		*sql.Tx
		log               *zap.Logger
		longQueryDuration time.Duration
	}

	LoggedRow struct {
		*sql.Row
		log               *zap.Logger
		longQueryDuration time.Duration
	}

	LoggedRows struct {
		*sql.Rows
		log               *zap.Logger
		longQueryDuration time.Duration
	}
)

func (lr *LoggedRows) Next() bool {
	start := time.Now()
	next := lr.Rows.Next()
	if dur := time.Since(start); dur > lr.longQueryDuration {
		lr.log.Warn("slow next", zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return next
}

func (lr *LoggedRows) Scan(dest ...any) error {
	start := time.Now()
	err := lr.Rows.Scan(dest...)
	if dur := time.Since(start); dur > lr.longQueryDuration {
		lr.log.Warn("slow scan", zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return err
}

func (lr *LoggedRow) Scan(dest ...any) error {
	start := time.Now()
	err := lr.Row.Scan(dest...)
	if dur := time.Since(start); dur > lr.longQueryDuration {
		lr.log.Warn("slow scan", zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return err
}

func (ls *LoggedStmt) Exec(ctx context.Context, args ...any) (sql.Result, error) {
	start := time.Now()
	result, err := ls.Stmt.ExecContext(ctx, args...)
	if dur := time.Since(start); dur > ls.longQueryDuration {
		ls.log.Warn("slow exec", zap.String("query", ls.query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return result, err
}

func (ls *LoggedStmt) Query(ctx context.Context, args ...any) (*LoggedRows, error) {
	start := time.Now()
	rows, err := ls.Stmt.QueryContext(ctx, args...)
	if dur := time.Since(start); dur > ls.longQueryDuration {
		ls.log.Warn("slow query", zap.String("query", ls.query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &LoggedRows{rows, ls.log.Named("rows"), ls.longQueryDuration}, err
}

func (ls *LoggedStmt) QueryRow(ctx context.Context, args ...any) *LoggedRow {
	start := time.Now()
	row := ls.Stmt.QueryRowContext(ctx, args...)
	if dur := time.Since(start); dur > ls.longQueryDuration {
		ls.log.Warn("slow query row", zap.String("query", ls.query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &LoggedRow{row, ls.log.Named("row"), ls.longQueryDuration}
}

// Exec executes a query without returning any rows. The args are for
// any placeholder parameters in the query.
func (lt *loggedTxn) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	start := time.Now()
	result, err := lt.Tx.ExecContext(ctx, query, args...)
	if dur := time.Since(start); dur > lt.longQueryDuration {
		lt.log.Warn("slow exec", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return result, err
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement. The caller must call the statement's Close method
// when the statement is no longer needed.
func (lt *loggedTxn) Prepare(ctx context.Context, query string) (*LoggedStmt, error) {
	start := time.Now()
	stmt, err := lt.Tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	} else if dur := time.Since(start); dur > lt.longQueryDuration {
		lt.log.Warn("slow prepare", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &LoggedStmt{
		Stmt:              stmt,
		query:             query,
		log:               lt.log.Named("statement"),
		longQueryDuration: lt.longQueryDuration,
	}, nil
}

// Query executes a query that returns rows, typically a SELECT. The
// args are for any placeholder parameters in the query.
func (lt *loggedTxn) Query(ctx context.Context, query string, args ...any) (*LoggedRows, error) {
	start := time.Now()
	rows, err := lt.Tx.QueryContext(ctx, query, args...)
	if dur := time.Since(start); dur > lt.longQueryDuration {
		lt.log.Warn("slow query", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &LoggedRows{rows, lt.log.Named("rows"), lt.longQueryDuration}, err
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called. If the query selects no rows, the *Row's
// Scan will return ErrNoRows. Otherwise, the *Row's Scan scans the
// first selected row and discards the rest.
func (lt *loggedTxn) QueryRow(ctx context.Context, query string, args ...any) *LoggedRow {
	start := time.Now()
	row := lt.Tx.QueryRowContext(ctx, query, args...)
	if dur := time.Since(start); dur > lt.longQueryDuration {
		lt.log.Warn("slow query row", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &LoggedRow{row, lt.log.Named("row"), lt.longQueryDuration}
}
