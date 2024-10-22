package sql

import (
	"context"
	"embed"
	"fmt"

	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

type (
	Migration struct {
		ID      string
		Migrate func(tx Tx) error
	}

	// Migrator is an interface for defining database-specific helper methods
	// required during migrations
	Migrator interface {
		ApplyMigration(ctx context.Context, fn func(tx Tx) (bool, error)) error
		CreateMigrationTable(ctx context.Context) error
		DB() *DB
	}

	MainMigrator interface {
		Migrator
		InsertDirectoriesMemoized(ctx context.Context, tx Tx, bucketID int64, path string, memo map[string]int64) (int64, error)
	}
)

var (
	MainMigrations = func(ctx context.Context, m MainMigrator, migrationsFs embed.FS, log *zap.SugaredLogger) []Migration {
		dbIdentifier := "main"
		return []Migration{
			{
				ID:      "00001_init",
				Migrate: func(tx Tx) error { return ErrRunV072 },
			},
			{
				ID: "00001_object_metadata",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00001_object_metadata", log)
				},
			},
			{
				ID: "00002_prune_slabs_trigger",
				Migrate: func(tx Tx) error {
					err := performMigration(ctx, tx, migrationsFs, dbIdentifier, "00002_prune_slabs_trigger", log)
					if utils.IsErr(err, ErrMySQLNoSuperPrivilege) {
						log.Warn("migration 00002_prune_slabs_trigger requires the user to have the SUPER privilege to register triggers")
					}
					return err
				},
			},
			{
				ID: "00003_idx_objects_size",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00003_idx_objects_size", log)
				},
			},
			{
				ID: "00004_prune_slabs_cascade",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00004_prune_slabs_cascade", log)
				},
			},
			{
				ID: "00005_zero_size_object_health",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00005_zero_size_object_health", log)
				},
			},
			{
				ID: "00006_idx_objects_created_at",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00006_idx_objects_created_at", log)
				},
			},
			{
				ID: "00007_host_checks",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00007_host_checks", log)
				},
			},
			{
				ID: "00008_directories",
				Migrate: func(tx Tx) error {
					// TODO: alternative to tweak old version of MakeDirsForPath
					log.Infof("skipping main migration '00008_directories' in favour of '00018_directory_buckets'")
					return nil
				},
			},
			{
				ID: "00009_json_settings",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00009_json_settings", log)
				},
			},
			{
				ID: "00010_webhook_headers",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00010_webhook_headers", log)
				},
			},
			{
				ID: "00011_host_subnets",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00011_host_subnets", log)
				},
			},
			{
				ID: "00012_peer_store",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00012_peer_store", log)
				},
			},
			{
				ID: "00013_coreutils_wallet",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00013_coreutils_wallet", log)
				},
			},
			{
				ID: "00014_hosts_resolvedaddresses",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00014_hosts_resolvedaddresses", log)
				},
			},
			{
				ID: "00015_reset_drift",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00015_reset_drift", log)
				},
			},
			{
				ID: "00016_account_owner",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00016_account_owner", log)
				},
			},
			{
				ID: "00017_unix_ms",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00017_unix_ms", log)
				},
			},
			{
				ID: "00018_directory_buckets",
				Migrate: func(tx Tx) error {
					// recreate directories table
					if err := performMigration(ctx, tx, migrationsFs, dbIdentifier, "00018_recreate_directories", log); err != nil {
						return fmt.Errorf("failed to migrate: %v", err)
					}

					// fetch all objects
					rows, err := tx.Query(ctx, "SELECT o.id, o.db_bucket_id, o.object_id FROM objects o")
					if err != nil {
						return fmt.Errorf("failed to fetch objects: %w", err)
					}
					defer rows.Close()

					// gather all necessary data to update the directory id of every object
					updates := make(map[int64]int64)
					memo := make(map[string]int64)
					for rows.Next() {
						var oID, bID int64
						var path string
						if err := rows.Scan(&oID, &bID, &path); err != nil {
							return fmt.Errorf("failed to scan object: %w", err)
						}

						dirID, err := m.InsertDirectoriesMemoized(ctx, tx, bID, path, memo)
						if err != nil {
							return fmt.Errorf("failed to create directory %s: %w", path, err)
						}
						updates[oID] = dirID
					}

					// prepare an update statement
					stmt, err := tx.Prepare(ctx, "UPDATE objects SET db_directory_id = ? WHERE id = ?")
					if err != nil {
						return fmt.Errorf("failed to prepare update statement: %w", err)
					}
					defer stmt.Close()

					// update all objects
					for id, dirID := range updates {
						if _, err := stmt.Exec(ctx, dirID, id); err != nil {
							return fmt.Errorf("failed to update object %d: %w", id, err)
						}
					}

					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00017_unix_ms", log)
				},
			},
		}
	}
	MetricsMigrations = func(ctx context.Context, migrationsFs embed.FS, log *zap.SugaredLogger) []Migration {
		dbIdentifier := "metrics"
		return []Migration{
			{
				ID:      "00001_init",
				Migrate: func(tx Tx) error { return ErrRunV072 },
			},
			{
				ID: "00001_idx_contracts_fcid_timestamp",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00001_idx_contracts_fcid_timestamp", log)
				},
			},
			{
				ID: "00002_idx_wallet_metrics_immature",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00002_idx_wallet_metrics_immature", log)
				},
			},
			{
				ID: "00003_unix_ms",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00003_unix_ms", log)
				},
			},
		}
	}
)

func PerformMigrations(ctx context.Context, m Migrator, fs embed.FS, identifier string, migrations []Migration) error {
	// try to create migrations table
	err := m.CreateMigrationTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// check if the migrations table is empty
	var isEmpty bool
	if err := m.DB().QueryRow(ctx, "SELECT COUNT(*) = 0 FROM migrations").Scan(&isEmpty); err != nil {
		return fmt.Errorf("failed to count rows in migrations table: %w", err)
	} else if isEmpty {
		// table is empty, init schema
		return initSchema(ctx, m.DB(), fs, identifier, migrations)
	}

	// apply missing migrations
	for _, migration := range migrations {
		if err := m.ApplyMigration(ctx, func(tx Tx) (bool, error) {
			// check if migration was already applied
			var applied bool
			if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM migrations WHERE id = ?)", migration.ID).Scan(&applied); err != nil {
				return false, fmt.Errorf("failed to check if migration '%s' was already applied: %w", migration.ID, err)
			} else if applied {
				return false, nil
			}
			// run migration
			if err := migration.Migrate(tx); err != nil {
				return false, fmt.Errorf("migration '%s' failed: %w", migration.ID, err)
			}
			// insert migration
			if _, err := tx.Exec(ctx, "INSERT INTO migrations (id) VALUES (?)", migration.ID); err != nil {
				return false, fmt.Errorf("failed to insert migration '%s': %w", migration.ID, err)
			}
			return true, nil
		}); err != nil {
			return fmt.Errorf("migration '%s' failed: %w", migration.ID, err)
		}
	}
	return nil
}

func execSQLFile(ctx context.Context, tx Tx, fs embed.FS, folder, filename string) error {
	path := fmt.Sprintf("migrations/%s/%s.sql", folder, filename)

	// read file
	file, err := fs.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", path, err)
	}

	// execute it
	if _, err := tx.Exec(ctx, string(file)); err != nil {
		return fmt.Errorf("failed to execute %s: %w", path, err)
	}
	return nil
}

func initSchema(ctx context.Context, db *DB, fs embed.FS, identifier string, migrations []Migration) error {
	return db.Transaction(ctx, func(tx Tx) error {
		// init schema
		if err := execSQLFile(ctx, tx, fs, identifier, "schema"); err != nil {
			return fmt.Errorf("failed to execute schema: %w", err)
		}
		// insert migration ids
		for _, migration := range migrations {
			if _, err := tx.Exec(ctx, "INSERT INTO migrations (id) VALUES (?)", migration.ID); err != nil {
				return fmt.Errorf("failed to insert migration '%s': %w", migration.ID, err)
			}
		}
		return nil
	})
}

func performMigration(ctx context.Context, tx Tx, fs embed.FS, kind, migration string, logger *zap.SugaredLogger) error {
	logger.Infof("performing %s migration '%s'", kind, migration)
	if err := execSQLFile(ctx, tx, fs, kind, fmt.Sprintf("migration_%s", migration)); err != nil {
		return err
	}
	logger.Infof("migration '%s' complete", migration)
	return nil
}
