package stores

import (
	"errors"
	"fmt"

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
				if err := performMigration(tx, dbIdentifier, "00008_directories", logger); err != nil {
					return fmt.Errorf("failed to migrate: %v", err)
				}
				// loop over all objects one-by-one and create the corresponding directory
				logger.Info("beginning post-migration directory creation, this might take a while")
				for offset := 0; ; offset++ {
					if offset > 0 && offset%1000 == 0 {
						logger.Infof("processed %v objects", offset)
					}
					var obj dbObject
					if err := tx.Offset(offset).Limit(1).Take(&obj).Error; errors.Is(err, gorm.ErrRecordNotFound) {
						break // done
					} else if err != nil {
						return fmt.Errorf("failed to fetch object: %v", err)
					}
					dirID, err := makeDirsForPath(tx, obj.ObjectID)
					if err != nil {
						return fmt.Errorf("failed to create directory %s: %w", obj.ObjectID, err)
					}
					if err := tx.Where(obj).Update("db_directory_id", dirID).Error; err != nil {
						return fmt.Errorf("failed to update object %s: %w", obj.ObjectID, err)
					}
				}
				logger.Info("post-migration directory creation complete")
				return nil
			},
		},
	}

	// Create migrator.
	m := gormigrate.New(db, gormigrate.DefaultOptions, migrations)

	// Set init function.
	m.InitSchema(initSchema(dbIdentifier, logger))

	// Perform migrations.
	if err := m.Migrate(); err != nil {
		return fmt.Errorf("failed to migrate: %v", err)
	}
	return nil
}
