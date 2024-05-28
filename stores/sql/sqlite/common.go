package sqlite

import (
	"context"
	dsql "database/sql"
	"embed"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/renterd/internal/sql"
	"go.uber.org/zap"
)

var deadlockMsgs = []string{
	"database is locked",
	"database table is locked",
}

//go:embed all:migrations/*
var migrationsFs embed.FS

func applyMigration(ctx context.Context, db *sql.DB, fn func(tx sql.Tx) (bool, error)) (err error) {
	if _, err := db.Exec(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		return fmt.Errorf("failed to disable foreign keys: %w", err)
	}
	defer func() {
		_, err2 := db.Exec(ctx, "PRAGMA foreign_keys=ON")
		err = errors.Join(err, err2)
	}()
	return db.Transaction(ctx, func(tx sql.Tx) error {
		// execute migration
		if migrated, err := fn(tx); err != nil {
			return err
		} else if !migrated {
			return nil
		}
		// perform foreign key integrity check
		if err := tx.QueryRow(ctx, "PRAGMA foreign_key_check").Scan(); !errors.Is(err, dsql.ErrNoRows) {
			return fmt.Errorf("foreign key constraints are not satisfied")
		}
		return nil
	})
}

func closeDB(db *sql.DB, log *zap.SugaredLogger) error {
	// NOTE: as recommended by https://www.sqlite.org/lang_analyze.html
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, err := db.Exec(ctx, "PRAGMA analysis_limit=400; PRAGMA optimize;")
	if err != nil {
		log.With(zap.Error(err)).Error("failed to optimize database before closing")
	}
	return db.Close()
}

func createMigrationTable(ctx context.Context, db *sql.DB) error {
	if _, err := db.Exec(ctx, "CREATE TABLE IF NOT EXISTS `migrations` (`id` text,PRIMARY KEY (`id`))"); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}
	return nil
}

func version(ctx context.Context, db *sql.DB) (string, string, error) {
	var version string
	if err := db.QueryRow(ctx, "select sqlite_version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "SQLite", version, nil
}
