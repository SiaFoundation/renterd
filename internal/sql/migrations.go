package sql

import (
	"embed"
	"fmt"
	"strings"
	"unicode/utf8"

	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

type Migration struct {
	ID      string
	Migrate func(tx Tx) error
}

var (
	MainMigrations = func(migrationsFs embed.FS, log *zap.SugaredLogger) []Migration {
		dbIdentifier := "main"
		return []Migration{
			{
				ID:      "00001_init",
				Migrate: func(tx Tx) error { return ErrRunV072 },
			},
			{
				ID: "00001_object_metadata",
				Migrate: func(tx Tx) error {
					return performMigration(tx, migrationsFs, dbIdentifier, "00001_object_metadata", log)
				},
			},
			{
				ID: "00002_prune_slabs_trigger",
				Migrate: func(tx Tx) error {
					err := performMigration(tx, migrationsFs, dbIdentifier, "00002_prune_slabs_trigger", log)
					if utils.IsErr(err, ErrMySQLNoSuperPrivilege) {
						log.Warn("migration 00002_prune_slabs_trigger requires the user to have the SUPER privilege to register triggers")
					}
					return err
				},
			},
			{
				ID: "00003_idx_objects_size",
				Migrate: func(tx Tx) error {
					return performMigration(tx, migrationsFs, dbIdentifier, "00003_idx_objects_size", log)
				},
			},
			{
				ID: "00004_prune_slabs_cascade",
				Migrate: func(tx Tx) error {
					return performMigration(tx, migrationsFs, dbIdentifier, "00004_prune_slabs_cascade", log)
				},
			},
			{
				ID: "00005_zero_size_object_health",
				Migrate: func(tx Tx) error {
					return performMigration(tx, migrationsFs, dbIdentifier, "00005_zero_size_object_health", log)
				},
			},
			{
				ID: "00006_idx_objects_created_at",
				Migrate: func(tx Tx) error {
					return performMigration(tx, migrationsFs, dbIdentifier, "00006_idx_objects_created_at", log)
				},
			},
			{
				ID: "00007_host_checks",
				Migrate: func(tx Tx) error {
					return performMigration(tx, migrationsFs, dbIdentifier, "00007_host_checks", log)
				},
			},
			{
				ID: "00008_directories",
				Migrate: func(tx Tx) error {
					if err := performMigration(tx, migrationsFs, dbIdentifier, "00008_directories_1", log); err != nil {
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
						rows, err := tx.Query("SELECT id, object_id FROM objects ORDER BY id LIMIT ? OFFSET ?", batchSize, offset)
						if err != nil {
							return fmt.Errorf("failed to fetch objects: %v", err)
						}
						for rows.Next() {
							var o obj
							if err := rows.Scan(&o.ID, &o.ObjectID); err != nil {
								return fmt.Errorf("failed to scan object: %v", err)
							}
							objBatch = append(objBatch, o)
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
							dirID, err := MakeDirsForPath(tx, obj.ObjectID)
							if err != nil {
								return fmt.Errorf("failed to create directory %s: %w", obj.ObjectID, err)
							}

							if _, err := tx.Exec(`
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
					if err := performMigration(tx, migrationsFs, dbIdentifier, "00008_directories_2", log); err != nil {
						return fmt.Errorf("failed to migrate: %v", err)
					}
					return nil
				},
			},
		}
	}
	MetricsMigrations = func(migrationsFs embed.FS, log *zap.SugaredLogger) []Migration {
		dbIdentifier := "metrics"
		return []Migration{
			{
				ID:      "00001_init",
				Migrate: func(tx Tx) error { return ErrRunV072 },
			},
			{
				ID: "00001_idx_contracts_fcid_timestamp",
				Migrate: func(tx Tx) error {
					return performMigration(tx, migrationsFs, dbIdentifier, "00001_idx_contracts_fcid_timestamp", log)
				},
			},
		}
	}
)

func InitSchema(db *DB, fs embed.FS, identifier string, migrations []Migration) error {
	return db.Transaction(func(tx Tx) error {
		// init schema
		if err := execSQLFile(tx, fs, identifier, "schema"); err != nil {
			return fmt.Errorf("failed to execute schema: %w", err)
		}
		// insert migration ids
		for _, migration := range migrations {
			if _, err := tx.Exec("INSERT INTO migrations (id) VALUES (?)", migration.ID); err != nil {
				return fmt.Errorf("failed to insert migration '%s': %w", migration.ID, err)
			}
		}
		return nil
	})
}

func execSQLFile(tx Tx, fs embed.FS, folder, filename string) error {
	path := fmt.Sprintf("migrations/%s/%s.sql", folder, filename)

	// read file
	file, err := fs.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", path, err)
	}

	// execute it
	if _, err := tx.Exec(string(file)); err != nil {
		return fmt.Errorf("failed to execute %s: %w", path, err)
	}
	return nil
}

func performMigration(tx Tx, fs embed.FS, kind, migration string, logger *zap.SugaredLogger) error {
	logger.Infof("performing %s migration '%s'", kind, migration)
	if err := execSQLFile(tx, fs, kind, fmt.Sprintf("migration_%s", migration)); err != nil {
		return err
	}
	logger.Info("migration '%s' complete", migration)
	return nil
}
