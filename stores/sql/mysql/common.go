package mysql

import (
	"embed"
	"fmt"

	"go.sia.tech/renterd/internal/sql"
)

//go:embed all:migrations/*
var migrationsFs embed.FS

func applyMigration(db *sql.DB, fn func(tx sql.Tx) error) error {
	return db.Transaction(fn)
}

func createMigrationTable(db *sql.DB) error {
	if _, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS migrations (
				id varchar(255) NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}
	return nil
}

func version(db *sql.DB) (string, string, error) {
	var version string
	if err := db.QueryRow("select version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "MySQL", version, nil
}
