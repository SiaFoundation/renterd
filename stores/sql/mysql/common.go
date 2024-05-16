package mysql

import (
	"context"
	"embed"
	"fmt"

	"go.sia.tech/renterd/internal/sql"
)

var deadlockMsgs = []string{
	"Deadlock found when trying to get lock",
}

//go:embed all:migrations/*
var migrationsFs embed.FS

func applyMigration(ctx context.Context, db *sql.DB, fn func(tx sql.Tx) (bool, error)) error {
	return db.Transaction(ctx, func(tx sql.Tx) error {
		_, err := fn(tx)
		return err
	})
}

func createMigrationTable(ctx context.Context, db *sql.DB) error {
	if _, err := db.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS migrations (
				id varchar(255) NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}
	return nil
}

func version(ctx context.Context, db *sql.DB) (string, string, error) {
	var version string
	if err := db.QueryRow(ctx, "select version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "MySQL", version, nil
}
