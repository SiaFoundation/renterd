package stores

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/go-gormigrate/gormigrate/v2"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var (
	errRunV072               = errors.New("can't upgrade to >=v1.0.0 from your current version - please upgrade to v0.7.2 first (https://github.com/SiaFoundation/renterd/releases/tag/v0.7.2)")
	errMySQLNoSuperPrivilege = errors.New("You do not have the SUPER privilege and binary logging is enabled")
)

func performMigrations(db *gorm.DB, logger *zap.SugaredLogger) error {
	dbIdentifier := "main"
	migrations := []*gormigrate.Migration{
		{
			ID:      "00001_init",
			Migrate: func(tx *gorm.DB) error { return errRunV072 },
		},
		{
			ID: "00001_object_metadata",
			Migrate: func(tx *gorm.DB) error {
				return performMigration(tx, dbIdentifier, "00001_object_metadata", logger)
			},
		},
		{
			ID: "00002_prune_slabs_trigger",
			Migrate: func(tx *gorm.DB) error {
				err := performMigration(tx, dbIdentifier, "00002_prune_slabs_trigger", logger)
				if utils.IsErr(err, errMySQLNoSuperPrivilege) {
					logger.Warn("migration 00002_prune_slabs_trigger requires the user to have the SUPER privilege to register triggers")
				}
				return err
			},
		},
		{
			ID: "00003_idx_objects_size",
			Migrate: func(tx *gorm.DB) error {
				return performMigration(tx, dbIdentifier, "00003_idx_objects_size", logger)
			},
		},
		{
			ID: "00004_prune_slabs_cascade",
			Migrate: func(tx *gorm.DB) error {
				return performMigration(tx, dbIdentifier, "00004_prune_slabs_cascade", logger)
			},
		},
		{
			ID: "00005_zero_size_object_health",
			Migrate: func(tx *gorm.DB) error {
				return performMigration(tx, dbIdentifier, "00005_zero_size_object_health", logger)
			},
		},
		{
			ID: "00006_idx_objects_created_at",
			Migrate: func(tx *gorm.DB) error {
				return performMigration(tx, dbIdentifier, "00006_idx_objects_created_at", logger)
			},
		},
		{
			ID: "00007_host_checks",
			Migrate: func(tx *gorm.DB) error {
				return performMigration(tx, dbIdentifier, "00007_host_checks", logger)
			},
		},
		{
			ID: "00008_directories",
			Migrate: func(tx *gorm.DB) error {
				if err := performMigration(tx, dbIdentifier, "00008_directories_1", logger); err != nil {
					return fmt.Errorf("failed to migrate: %v", err)
				}
				// loop over all objects and deduplicate dirs to create
				logger.Info("beginning post-migration directory creation, this might take a while")
				batchSize := 10000
				processedDirs := make(map[string]struct{})
				for offset := 0; ; offset += batchSize {
					if offset > 0 && offset%batchSize == 0 {
						logger.Infof("processed %v objects", offset)
					}
					var objBatch []dbObject
					if err := tx.
						Offset(offset).
						Limit(batchSize).
						Select("id", "object_id").
						Find(&objBatch).Error; err != nil {
						return fmt.Errorf("failed to fetch objects: %v", err)
					} else if len(objBatch) == 0 {
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
						dirID, err := makeDirsForPath(tx, obj.ObjectID)
						if err != nil {
							return fmt.Errorf("failed to create directory %s: %w", obj.ObjectID, err)
						}
						if err := tx.Model(&dbObject{}).
							Where("object_id LIKE ?", dir+"%").                                     // uses index but case sensitive
							Where("SUBSTR(object_id, 1, ?) = ?", utf8.RuneCountInString(dir), dir). // exact comparison
							Where("INSTR(SUBSTR(object_id, ?), '/') = 0", utf8.RuneCountInString(dir)+1).
							Update("db_directory_id", dirID).Error; err != nil {
							return fmt.Errorf("failed to update object %s: %w", obj.ObjectID, err)
						}
					}
				}
				logger.Info("post-migration directory creation complete")
				if err := performMigration(tx, dbIdentifier, "00008_directories_2", logger); err != nil {
					return fmt.Errorf("failed to migrate: %v", err)
				}
				return nil
			},
		},
	}

	// Run migration
	return db.Transaction(func(tx *gorm.DB) error {
		if isSQLite(tx) {
			if err := tx.Exec("PRAGMA defer_foreign_keys = ON").Error; err != nil {
				return fmt.Errorf("failed to defer foreign keys: %v", err)
			}
		}
		m := gormigrate.New(tx, gormigrate.DefaultOptions, migrations)

		// Set init function.
		m.InitSchema(initSchema(dbIdentifier, logger))

		// Perform migrations.
		if err := m.Migrate(); err != nil {
			return fmt.Errorf("failed to migrate: %v", err)
		}
		return nil
	})
}
