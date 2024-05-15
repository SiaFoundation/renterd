package mysql

import (
	"embed"
	"fmt"

	"go.sia.tech/renterd/internal/sql"
)

//go:embed all:migrations/*
var migrationsFs embed.FS

func performMigrations(db *sql.DB, identifier string, migrations []sql.Migration) error {
	// try to create migrations table
	if _, err := db.Exec(`
			CREATE TABLE migrations (
				id varchar(255) NOT NULL,
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;`); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// check if the migrations table is empty
	var isEmpty bool
	if err := db.QueryRow("SELECT COUNT(*) = 0 FROM migrations").Scan(&isEmpty); err != nil {
		return fmt.Errorf("failed to count rows in migrations table: %w", err)
	} else if isEmpty {
		// table is empty, init schema
		return sql.InitSchema(db, migrationsFs, identifier, migrations)
	}

	// apply missing migrations
	for _, migration := range migrations {
		if err := db.Transaction(func(tx sql.Tx) error {
			// check if migration was already applied
			var applied bool
			if err := tx.QueryRow("SELECT EXISTS (SELECT 1 FROM migrations WHERE id = ?)", migration.ID).Scan(&applied); err != nil {
				return fmt.Errorf("failed to check if migration '%s' was already applied: %w", migration.ID, err)
			} else if applied {
				return nil
			}

			// run migration
			return migration.Migrate(tx)
		}); err != nil {
			return fmt.Errorf("migration '%s' failed: %w", migration.ID, err)
		}
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
