package sqlite

import (
	"embed"
	"fmt"

	"go.sia.tech/renterd/internal/sql"
)

//go:embed all:migrations/*
var migrationsFs embed.FS

func applyMigration(db *sql.DB, fn func(tx sql.Tx) error) error {
	return db.Transaction(func(tx sql.Tx) error {
		// defer foreign_keys to avoid triggering unwanted CASCADEs or
		// constraint failures
		if _, err := tx.Exec("PRAGMA defer_foreign_keys = ON"); err != nil {
			return fmt.Errorf("failed to defer foreign keys: %w", err)
		}
		return fn(tx)
	})
}

func createMigrationTable(db *sql.DB) error {
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS `migrations` (`id` text,PRIMARY KEY (`id`))"); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}
	return nil
}

func version(db *sql.DB) (string, string, error) {
	var version string
	if err := db.QueryRow("select sqlite_version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "SQLite", version, nil
}
