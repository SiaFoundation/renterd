package sqlite

import (
	"embed"
	"fmt"

	"go.sia.tech/renterd/internal/sql"
)

//go:embed all:migrations/*
var migrationsFs embed.FS

func performMigrations(db *sql.DB, identifier string, migrations []sql.Migration) error {
	// try to create migrations table
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS `migrations` (`id` text,PRIMARY KEY (`id`))"); err != nil {
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

			// defer foreign_keys to avoid triggering unwanted CASCADEs or
			// constraint failures
			if _, err := tx.Exec("PRAGMA defer_foreign_keys = ON"); err != nil {
				return fmt.Errorf("failed to defer foreign keys: %w", err)
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
	if err := db.QueryRow("select sqlite_version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "SQLite", version, nil
}
