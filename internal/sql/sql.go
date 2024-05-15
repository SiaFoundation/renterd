package sql

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	maxRetryAttempts = 30  // 30 attempts
	factor           = 1.8 // factor ^ retryAttempts = backoff time in milliseconds
	maxBackoff       = 15 * time.Second

	DirectoriesRootID = 1
)

var (
	ErrRunV072               = errors.New("can't upgrade to >=v1.0.0 from your current version - please upgrade to v0.7.2 first (https://github.com/SiaFoundation/renterd/releases/tag/v0.7.2)")
	ErrMySQLNoSuperPrivilege = errors.New("You do not have the SUPER privilege and binary logging is enabled")
)

type (
	// A DB is a wrapper around a *sql.DB that provides additional utility
	DB struct {
		dbLockedMsg       string
		db                *sql.DB
		log               *zap.Logger
		longQueryDuration time.Duration
		longTxDuration    time.Duration
	}

	// A txn is an interface for executing queries within a transaction.
	Tx interface {
		// Exec executes a query without returning any rows. The args are for
		// any placeholder parameters in the query.
		Exec(query string, args ...any) (sql.Result, error)
		// Prepare creates a prepared statement for later queries or executions.
		// Multiple queries or executions may be run concurrently from the
		// returned statement. The caller must call the statement's Close method
		// when the statement is no longer needed.
		Prepare(query string) (*loggedStmt, error)
		// Query executes a query that returns rows, typically a SELECT. The
		// args are for any placeholder parameters in the query.
		Query(query string, args ...any) (*loggedRows, error)
		// QueryRow executes a query that is expected to return at most one row.
		// QueryRow always returns a non-nil value. Errors are deferred until
		// Row's Scan method is called. If the query selects no rows, the *Row's
		// Scan will return ErrNoRows. Otherwise, the *Row's Scan scans the
		// first selected row and discards the rest.
		QueryRow(query string, args ...any) *loggedRow
	}
)

func NewDB(db *sql.DB, log *zap.Logger, dbLockedMsg string, longQueryDuration, longTxDuration time.Duration) *DB {
	return &DB{
		dbLockedMsg:       dbLockedMsg,
		db:                db,
		log:               log,
		longQueryDuration: longQueryDuration,
		longTxDuration:    longTxDuration,
	}
}

// exec executes a query without returning any rows. The args are for
// any placeholder parameters in the query.
func (s *DB) Exec(query string, args ...any) (sql.Result, error) {
	start := time.Now()
	result, err := s.db.Exec(query, args...)
	if dur := time.Since(start); dur > s.longQueryDuration {
		s.log.Debug("slow exec", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return result, err
}

// prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement. The caller must call the statement's Close method
// when the statement is no longer needed.
func (s *DB) Prepare(query string) (*loggedStmt, error) {
	start := time.Now()
	stmt, err := s.db.Prepare(query)
	if dur := time.Since(start); dur > s.longQueryDuration {
		s.log.Debug("slow prepare", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	} else if err != nil {
		return nil, err
	}
	return &loggedStmt{
		Stmt:              stmt,
		query:             query,
		log:               s.log.Named("statement"),
		longQueryDuration: s.longQueryDuration,
	}, nil
}

// query executes a query that returns rows, typically a SELECT. The
// args are for any placeholder parameters in the query.
func (s *DB) Query(query string, args ...any) (*loggedRows, error) {
	start := time.Now()
	rows, err := s.db.Query(query, args...)
	if dur := time.Since(start); dur > s.longQueryDuration {
		s.log.Debug("slow query", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &loggedRows{rows, s.log.Named("rows"), s.longQueryDuration}, err
}

// queryRow executes a query that is expected to return at most one row.
// QueryRow always returns a non-nil value. Errors are deferred until
// Row's Scan method is called. If the query selects no rows, the *Row's
// Scan will return ErrNoRows. Otherwise, the *Row's Scan scans the
// first selected row and discards the rest.
func (s *DB) QueryRow(query string, args ...any) *loggedRow {
	start := time.Now()
	row := s.db.QueryRow(query, args...)
	if dur := time.Since(start); dur > s.longQueryDuration {
		s.log.Debug("slow query row", zap.String("query", query), zap.Duration("elapsed", dur), zap.Stack("stack"))
	}
	return &loggedRow{row, s.log.Named("row"), s.longQueryDuration}
}

// transaction executes a function within a database transaction. If the
// function returns an error, the transaction is rolled back. Otherwise, the
// transaction is committed. If the transaction fails due to a busy error, it is
// retried up to 'maxRetryAttempts' times before returning.
func (s *DB) Transaction(fn func(Tx) error) error {
	var err error
	txnID := hex.EncodeToString(frand.Bytes(4))
	log := s.log.Named("transaction").With(zap.String("id", txnID))
	start := time.Now()
	attempt := 1
	for ; attempt < maxRetryAttempts; attempt++ {
		attemptStart := time.Now()
		log := log.With(zap.Int("attempt", attempt))
		err = s.transaction(s.db, log, fn)
		if err == nil {
			// no error, break out of the loop
			return nil
		}

		// return immediately if the error is not a busy error
		if !strings.Contains(err.Error(), s.dbLockedMsg) {
			break
		}
		// exponential backoff
		sleep := time.Duration(math.Pow(factor, float64(attempt))) * time.Millisecond
		if sleep > maxBackoff {
			sleep = maxBackoff
		}
		log.Debug("database locked", zap.Duration("elapsed", time.Since(attemptStart)), zap.Duration("totalElapsed", time.Since(start)), zap.Stack("stack"), zap.Duration("retry", sleep))
		jitterSleep(sleep)
	}
	return fmt.Errorf("transaction failed (attempt %d): %w", attempt, err)
}

// Close closes the underlying database.
func (s *DB) Close() error {
	return s.db.Close()
}

// transaction is a helper function to execute a function within a transaction.
// If fn returns an error, the transaction is rolled back. Otherwise, the
// transaction is committed.
func (s *DB) transaction(db *sql.DB, log *zap.Logger, fn func(tx Tx) error) error {
	start := time.Now()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			s.log.Error("failed to roll back transaction", zap.Error(err))
		}
	}()
	defer func() {
		// log the transaction if it took longer than txn duration
		if time.Since(start) > s.longTxDuration {
			log.Debug("long transaction", zap.Duration("elapsed", time.Since(start)), zap.Stack("stack"), zap.Bool("failed", err != nil))
		}
	}()

	ltx := &loggedTxn{
		Tx:                tx,
		log:               log,
		longQueryDuration: s.longQueryDuration,
	}
	if err := fn(ltx); err != nil {
		return err
	} else if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// jitterSleep sleeps for a random duration between t and t*1.5.
func jitterSleep(t time.Duration) {
	time.Sleep(t + time.Duration(rand.Int63n(int64(t/2))))
}
