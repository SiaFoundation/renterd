package sqlite

import (
	dsql "database/sql"
	"embed"
	"errors"
	"fmt"

	"go.sia.tech/renterd/internal/sql"
	"go.uber.org/zap"
)

//go:embed all:migrations/*
var migrations embed.FS

type migration struct {
	ID      string
	Migrate func(tx sql.Tx) error
}

func performMigrations(db *sql.DB, identifier string, migrations []migration, l *zap.SugaredLogger) error {
	// check if the migrations table exists
	var hasTable bool
	if err := db.QueryRow("SELECT 1 FROM sqlite_master WHERE type='table' AND name='migrations'").Scan(&hasTable); err != nil && !errors.Is(err, dsql.ErrNoRows) {
		return fmt.Errorf("failed to check for migrations table: %w", err)
	}
	if !hasTable {
		// init schema if it doesn't
		return initSchema(db, identifier, migrations, l)
	}

	// check if the migrations table is empty
	var isEmpty bool
	if err := db.QueryRow("SELECT COUNT(*) = 0 FROM migrations").Scan(&isEmpty); err != nil {
		return fmt.Errorf("failed to count rows in migrations table: %w", err)
	} else if isEmpty {
		// table is empty, init schema
		return initSchema(db, identifier, migrations, l)
	}

	// check if the schema was initialised already
	var initialised bool
	if err := db.QueryRow("SELECT EXISTS (SELECT 1 FROM migrations WHERE id = ?)", sql.SCHEMA_INIT).Scan(&initialised); err != nil {
		return fmt.Errorf("failed to check if schema was initialised: %w", err)
	} else if !initialised {
		return fmt.Errorf("schema was not initialised but has a non-empty migration table")
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

func execSQLFile(tx sql.Tx, folder, filename string) error {
	path := fmt.Sprintf("migrations/%s/%s.sql", folder, filename)

	// read file
	file, err := migrations.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", path, err)
	}

	// execute it
	if _, err := tx.Exec(string(file)); err != nil {
		return fmt.Errorf("failed to execute %s: %w", path, err)
	}
	return nil
}

// initSchema is executed only on a clean database. Otherwise the individual
// migrations are executed.
func initSchema(db *sql.DB, identifier string, migrations []migration, logger *zap.SugaredLogger) error {
	return db.Transaction(func(tx sql.Tx) error {
		logger.Infof("initializing '%s' schema", identifier)

		// create migrations table if necessary
		if _, err := tx.Exec("CREATE TABLE IF NOT EXISTS `migrations` (`id` text,PRIMARY KEY (`id`))"); err != nil {
			return fmt.Errorf("failed to create migrations table: %w", err)
		}
		// insert SCHEMA_INIT
		if _, err := tx.Exec("INSERT INTO migrations (id) VALUES (?)", sql.SCHEMA_INIT); err != nil {
			return fmt.Errorf("failed to insert SCHEMA_INIT: %w", err)
		}
		// insert migration ids
		for _, migration := range migrations {
			if _, err := tx.Exec("INSERT INTO migrations (id) VALUES (?)", migration.ID); err != nil {
				return fmt.Errorf("failed to insert migration '%s': %w", migration.ID, err)
			}
		}
		// create remaining schema
		if err := execSQLFile(tx, identifier, "schema"); err != nil {
			return fmt.Errorf("failed to execute schema: %w", err)
		}

		logger.Infof("initialization complete")
		return nil
	})
}

func performMigration(tx sql.Tx, kind, migration string, logger *zap.SugaredLogger) error {
	logger.Infof("performing %s migration '%s'", kind, migration)
	if err := execSQLFile(tx, kind, fmt.Sprintf("migration_%s", migration)); err != nil {
		return err
	}
	logger.Info("migration '%s' complete", migration)
	return nil
}

func version(db *sql.DB) (string, string, error) {
	var version string
	if err := db.QueryRow("select sqlite_version()").Scan(&version); err != nil {
		return "", "", err
	}
	return "SQLite", version, nil
}
