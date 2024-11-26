package sql

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	dsql "database/sql"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
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
		InitAutopilot(ctx context.Context, tx Tx) error
		InsertDirectories(ctx context.Context, tx Tx, bucket, path string) (int64, error)
		MakeDirsForPath(ctx context.Context, tx Tx, path string) (int64, error)
		SlabBuffers(ctx context.Context, tx Tx) ([]string, error)
		UpdateSetting(ctx context.Context, tx Tx, key, value string) error
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
					if err := performMigration(ctx, tx, migrationsFs, dbIdentifier, "00008_directories_1", log); err != nil {
						return fmt.Errorf("failed to migrate: %v", err)
					}
					// helper type
					type obj struct {
						ID       uint
						ObjectID string
					}
					// loop over all objects and deduplicate dirs to create
					log.Info("beginning post-migration directory creation, this might take a while")
					batchSize := 10000
					processedDirs := make(map[string]struct{})
					for offset := 0; ; offset += batchSize {
						if offset > 0 && offset%batchSize == 0 {
							log.Infof("processed %v objects", offset)
						}
						var objBatch []obj
						rows, err := tx.Query(ctx, "SELECT id, object_id FROM objects ORDER BY id LIMIT ? OFFSET ?", batchSize, offset)
						if err != nil {
							return fmt.Errorf("failed to fetch objects: %v", err)
						}
						for rows.Next() {
							var o obj
							if err := rows.Scan(&o.ID, &o.ObjectID); err != nil {
								_ = rows.Close()
								return fmt.Errorf("failed to scan object: %v", err)
							}
							objBatch = append(objBatch, o)
						}
						if err := rows.Close(); err != nil {
							return fmt.Errorf("failed to close rows: %v", err)
						}
						if len(objBatch) == 0 {
							break // done
						}
						for _, obj := range objBatch {
							// check if dir was processed
							dir := "" // root
							if i := strings.LastIndex(obj.ObjectID, "/"); i > -1 {
								dir = obj.ObjectID[:i+1]
							}
							_, exists := processedDirs[dir]
							if exists {
								continue // already processed
							}
							processedDirs[dir] = struct{}{}

							// process
							dirID, err := m.MakeDirsForPath(ctx, tx, obj.ObjectID)
							if err != nil {
								return fmt.Errorf("failed to create directory %s: %w", obj.ObjectID, err)
							}

							if _, err := tx.Exec(ctx, `
							UPDATE objects
							SET db_directory_id = ?
							WHERE object_id LIKE ? AND
							SUBSTR(object_id, 1, ?) = ? AND
							INSTR(SUBSTR(object_id, ?), '/') = 0
						`,
								dirID,
								dir+"%",
								utf8.RuneCountInString(dir), dir,
								utf8.RuneCountInString(dir)+1); err != nil {
								return fmt.Errorf("failed to update object %s: %w", obj.ObjectID, err)
							}
						}
					}
					log.Info("post-migration directory creation complete")
					if err := performMigration(ctx, tx, migrationsFs, dbIdentifier, "00008_directories_2", log); err != nil {
						return fmt.Errorf("failed to migrate: %v", err)
					}
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
					// recreate the directories table
					if err := performMigration(ctx, tx, migrationsFs, dbIdentifier, "00018_directory_buckets_1", log); err != nil {
						return fmt.Errorf("failed to migrate: %v", err)
					}

					// fetch all objects
					type obj struct {
						ID     int64
						Path   string
						Bucket string
					}

					rows, err := tx.Query(ctx, "SELECT o.id, o.object_id, b.name FROM objects o INNER JOIN buckets b ON o.db_bucket_id = b.id")
					if err != nil {
						return fmt.Errorf("failed to fetch objects: %w", err)
					}
					defer rows.Close()

					var objects []obj
					for rows.Next() {
						var o obj
						if err := rows.Scan(&o.ID, &o.Path, &o.Bucket); err != nil {
							return fmt.Errorf("failed to scan object: %w", err)
						}
						objects = append(objects, o)
					}

					// re-insert directories and collect object updates
					memo := make(map[string]int64)
					updates := make(map[int64]int64)
					for _, o := range objects {
						// build path directories
						dirs := object.Directories(o.Path)
						last := dirs[len(dirs)-1]
						if _, ok := memo[last]; ok {
							updates[o.ID] = memo[last]
							continue
						}

						// insert directories
						dirID, err := m.InsertDirectories(ctx, tx, o.Bucket, o.Path)
						if err != nil {
							return fmt.Errorf("failed to create directory %s in bucket %s: %w", o.Path, o.Bucket, err)
						}
						updates[o.ID] = dirID
						memo[last] = dirID
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

					// re-add the foreign key check (only for MySQL)
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00018_directory_buckets_2", log)
				},
			},
			{
				ID: "00018_gouging_units", // NOTE: duplicate ID (00018) due to directories hotfix
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00018_gouging_units", log)
				},
			},
			{
				ID: "00019_settings",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00019_settings", log)
				},
			},
			{
				ID: "00019_scan_reset",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00019_scan_reset", log)
				},
			}, // NOTE: duplicate ID (00019) due to updating core deps directly on master
			{
				ID: "00020_idx_db_directory",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00020_idx_db_directory", log)
				},
			},
			{
				ID: "00021_archived_contracts",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00021_archived_contracts", log)
				},
			},
			{
				ID: "00022_remove_triggers",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00022_remove_triggers", log)
				},
			},
			{
				ID: "00023_key_prefix",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00023_key_prefix", log)
				},
			},
			{
				ID: "00024_contract_v2",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00024_contract_v2", log)
				},
			},
			{
				ID: "00025_latest_host",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00025_latest_host", log)
				},
			},
			{
				ID: "00026_key_prefix",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00026_key_prefix", log)
				},
			},
			{
				ID: "00027_remove_directories",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00027_remove_directories", log)
				},
			},
			{
				ID: "00028_contract_usability",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00028_contract_usability", log)
				},
			},
			{
				ID: "00029_autopilot",
				Migrate: func(tx Tx) error {
					// remove all references to the autopilots table, without dropping the table
					if err := performMigration(ctx, tx, migrationsFs, dbIdentifier, "00029_autopilot_1", log); err != nil {
						return fmt.Errorf("failed to migrate: %v", err)
					}

					// make sure the autopilot config is initialized
					if err := m.InitAutopilot(ctx, tx); err != nil {
						return fmt.Errorf("failed to initialize autopilot: %w", err)
					}

					// fetch existing autopilot and override the blank config
					var cfgraw []byte
					var cfg api.AutopilotConfig
					err := tx.QueryRow(ctx, `SELECT config FROM autopilots WHERE identifier = "autopilot"`).Scan(&cfgraw)
					if errors.Is(dsql.ErrNoRows, err) {
						log.Warn("existing autopilot not found, the autopilot will be recreated with default values and the period will be reset")
					} else if err := json.Unmarshal(cfgraw, &cfg); err != nil {
						log.Warnf("existing autopilot config not valid JSON, err %v", err)
					} else {
						var enabled bool
						if err := cfg.Contracts.Validate(); err != nil {
							log.Warnf("existing contracts config is invalid, autopilot will be disabled, err: %v", err)
						} else if err := cfg.Hosts.Validate(); err != nil {
							log.Warnf("existing hosts config is invalid, autopilot will be disabled, err: %v", err)
						} else {
							enabled = true
						}
						res, err := tx.Exec(ctx, `UPDATE autopilot_config SET enabled = ?, contracts_amount = ?, contracts_period = ?, contracts_renew_window = ?, contracts_download = ?, contracts_upload = ?, contracts_storage = ?, contracts_prune = ?, hosts_max_downtime_hours = ?, hosts_min_protocol_version = ?, hosts_max_consecutive_scan_failures = ? WHERE id = ?`,
							enabled,
							cfg.Contracts.Amount,
							cfg.Contracts.Period,
							cfg.Contracts.RenewWindow,
							cfg.Contracts.Download,
							cfg.Contracts.Upload,
							cfg.Contracts.Storage,
							cfg.Contracts.Prune,
							cfg.Hosts.MaxDowntimeHours,
							cfg.Hosts.MinProtocolVersion,
							cfg.Hosts.MaxConsecutiveScanFailures,
							AutopilotID)
						if err != nil {
							return fmt.Errorf("failed to update autopilot config: %w", err)
						} else if n, err := res.RowsAffected(); err != nil {
							return fmt.Errorf("failed to fetch rows affected: %w", err)
						} else if n == 0 {
							return fmt.Errorf("failed to override blank autopilot config not found")
						}
					}

					// drop autopilots table
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00029_autopilot_2", log)
				},
			},
			{
				ID: "00030_remove_contract_sets",
				Migrate: func(tx Tx) error {
					// prepare statement to rename the buffer
					stmt, err := tx.Prepare(ctx, "UPDATE buffered_slabs SET filename = ? WHERE filename = ?")
					if err != nil {
						return fmt.Errorf("failed to prepare update statement: %w", err)
					}
					defer stmt.Close()

					// prepare a helper to safely copy and sync the file
					copyBuffer := func(path string) (string, error) {
						parts := strings.Split(filepath.Base(path), "-")
						if len(parts) != 4 {
							return "", fmt.Errorf("invalid path '%s'", path)
						}

						src, err := os.Open(path)
						if err != nil {
							return "", fmt.Errorf("failed to open buffer: %w", err)
						}
						defer src.Close()

						dstFilename := strings.Join(parts[1:], "-")
						dstPath := filepath.Join(filepath.Dir(path), dstFilename)
						err = os.Remove(dstPath)
						if err != nil && !os.IsNotExist(err) {
							return "", fmt.Errorf("failed to remove existing file at '%s': %w", dstPath, err)
						}

						dstFile, err := os.Create(dstPath)
						if err != nil {
							return "", fmt.Errorf("failed to create destination buffer at '%s': %w", dstPath, err)
						}
						defer dstFile.Close()

						_, err = io.Copy(dstFile, src)
						if err != nil {
							return "", fmt.Errorf("failed to copy buffer: %w", err)
						}

						err = dstFile.Sync()
						if err != nil {
							return "", fmt.Errorf("failed to sync buffer: %w", err)
						}

						return dstFilename, nil
					}

					// fetch all buffers
					buffers, err := m.SlabBuffers(ctx, tx)
					if err != nil {
						return err
					}

					// copy all buffers
					for _, path := range buffers {
						filename := filepath.Base(path)
						if updated, err := copyBuffer(path); err != nil {
							return fmt.Errorf("failed to copy buffer '%s': %w", path, err)
						} else if res, err := stmt.Exec(ctx, updated, filename); err != nil {
							return fmt.Errorf("failed to update buffer '%s': %w", filepath.Base(path), err)
						} else if n, err := res.RowsAffected(); err != nil {
							return fmt.Errorf("failed to fetch rows affected: %w", err)
						} else if n != 1 {
							return fmt.Errorf("failed to update buffer, no rows affected when updating '%s' -> %s", filename, updated)
						}
					}

					// remove original buffers
					for _, path := range buffers {
						if err := os.Remove(path); err != nil {
							return fmt.Errorf("failed to remove buffer '%s': %w", path, err)
						}
					}

					// perform database migration
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00030_remove_contract_sets", log)
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
			{
				ID: "00004_contract_spending",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00004_contract_spending", log)
				},
			},
			{
				ID: "00005_remove_contract_sets",
				Migrate: func(tx Tx) error {
					return performMigration(ctx, tx, migrationsFs, dbIdentifier, "00005_remove_contract_sets", log)
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
	} else if len(bytes.TrimSpace(file)) == 0 {
		return nil
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
