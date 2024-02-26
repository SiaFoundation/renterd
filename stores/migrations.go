package stores

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-gormigrate/gormigrate/v2"
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
				if err != nil && strings.Contains(err.Error(), errMySQLNoSuperPrivilege.Error()) {
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
	}

	// Create migrator.
	m := gormigrate.New(db, gormigrate.DefaultOptions, migrations)

	// Set init function.
	m.InitSchema(initSchema(db, dbIdentifier, logger))

	// Perform migrations.
	if err := m.Migrate(); err != nil {
		return fmt.Errorf("failed to migrate: %v", err)
	}
	return nil
}
