package sqlite

import (
	"context"
	dsql "database/sql"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"go.sia.tech/renterd/internal/sql"
	"go.sia.tech/renterd/internal/utils"

	"go.uber.org/zap"
)

type MainDatabase struct {
	db  *sql.DB
	log *zap.SugaredLogger
}

// NewMainDatabase creates a new SQLite backend.
func NewMainDatabase(db *dsql.DB, log *zap.SugaredLogger, lqd, ltd time.Duration) *MainDatabase {
	store := sql.NewDB(db, log.Desugar(), "database is locked", lqd, ltd)
	return &MainDatabase{
		db:  store,
		log: log,
	}
}

func (b *MainDatabase) Close() error {
	return b.db.Close()
}

func (b *MainDatabase) Version(_ context.Context) (string, string, error) {
	return version(b.db)
}

func (b *MainDatabase) Migrate() error {
	dbIdentifier := "main"
	return performMigrations(b.db, dbIdentifier, []migration{
		{
			ID:      "00001_init",
			Migrate: func(tx sql.Tx) error { return sql.ErrRunV072 },
		},
		{
			ID: "00001_object_metadata",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00001_object_metadata", b.log)
			},
		},
		{
			ID: "00002_prune_slabs_trigger",
			Migrate: func(tx sql.Tx) error {
				err := performMigration(tx, dbIdentifier, "00002_prune_slabs_trigger", b.log)
				if utils.IsErr(err, sql.ErrMySQLNoSuperPrivilege) {
					b.log.Warn("migration 00002_prune_slabs_trigger requires the user to have the SUPER privilege to register triggers")
				}
				return err
			},
		},
		{
			ID: "00003_idx_objects_size",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00003_idx_objects_size", b.log)
			},
		},
		{
			ID: "00004_prune_slabs_cascade",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00004_prune_slabs_cascade", b.log)
			},
		},
		{
			ID: "00005_zero_size_object_health",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00005_zero_size_object_health", b.log)
			},
		},
		{
			ID: "00006_idx_objects_created_at",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00006_idx_objects_created_at", b.log)
			},
		},
		{
			ID: "00007_host_checks",
			Migrate: func(tx sql.Tx) error {
				return performMigration(tx, dbIdentifier, "00007_host_checks", b.log)
			},
		},
		{
			ID: "00008_directories",
			Migrate: func(tx sql.Tx) error {
				if err := performMigration(tx, dbIdentifier, "00008_directories", b.log); err != nil {
					return fmt.Errorf("failed to migrate: %v", err)
				}
				// helper type
				type obj struct {
					ID       uint
					ObjectID string
				}
				// loop over all objects and deduplicate dirs to create
				b.log.Info("beginning post-migration directory creation, this might take a while")
				batchSize := 10000
				processedDirs := make(map[string]struct{})
				for offset := 0; ; offset += batchSize {
					if offset > 0 && offset%batchSize == 0 {
						b.log.Infof("processed %v objects", offset)
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
						dirID, err := sql.MakeDirsForPath(tx, obj.ObjectID)
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
				b.log.Info("post-migration directory creation complete")
				return nil
			},
		},
	}, b.log)
}
