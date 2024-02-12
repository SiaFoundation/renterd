package stores

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-gormigrate/gormigrate/v2"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var (
	errRunV072               = errors.New("can't upgrade to >=v1.0.0 from your current version - please upgrade to v0.7.2 first (https://github.com/SiaFoundation/renterd/releases/tag/v0.7.2)")
	errMySQLNoSuperPrivilege = errors.New("You do not have the SUPER privilege and binary logging is enabled")
)

// initSchema is executed only on a clean database. Otherwise the individual
// migrations are executed.
func initSchema(tx *gorm.DB) (err error) {
	// Pick the right migrations.
	var schema []byte
	if isSQLite(tx) {
		schema, err = migrations.ReadFile("migrations/sqlite/main/schema.sql")
	} else {
		schema, err = migrations.ReadFile("migrations/mysql/main/schema.sql")
	}
	if err != nil {
		return
	}

	// Run it.
	err = tx.Exec(string(schema)).Error
	if err != nil {
		return fmt.Errorf("failed to init schema: %w", err)
	}

	// Add default bucket.
	return tx.Create(&dbBucket{
		Name: api.DefaultBucketName,
	}).Error
}

func performMigrations(db *gorm.DB, logger *zap.SugaredLogger) error {
	migrations := []*gormigrate.Migration{
		{
			ID:      "00001_init",
			Migrate: func(tx *gorm.DB) error { return errRunV072 },
		},
		{
			ID: "00001_object_metadata",
			Migrate: func(tx *gorm.DB) error {
				return performMigration(tx, "00001_object_metadata", false, logger)
			},
		},
		{
			ID: "00002_prune_slabs_trigger",
			Migrate: func(tx *gorm.DB) error {
				err := performMigration(tx, "00002_prune_slabs_trigger", false, logger)
				if err != nil && strings.Contains(err.Error(), errMySQLNoSuperPrivilege.Error()) {
					logger.Warn("migration 00002_prune_slabs_trigger requires the user to have the SUPER privilege to register triggers")
				}
				return err
			},
		},
	}

	// Create migrator.
	m := gormigrate.New(db, gormigrate.DefaultOptions, migrations)

	// Set init function.
	m.InitSchema(initSchema)

	// Perform migrations.
	if err := m.Migrate(); err != nil {
		return fmt.Errorf("failed to migrate: %v", err)
	}
	return nil
}

func performMigration(db *gorm.DB, name string, metrics bool, logger *zap.SugaredLogger) error {
	logger.Infof("performing migration %s", name)

	// build path
	folder := "main"
	if metrics {
		folder = "metrics"
	}
	protocol := "mysql"
	if isSQLite(db) {
		protocol = "sqlite"
	}
	path := fmt.Sprintf("migrations/%s/%s/migration_%s.sql", protocol, folder, name)

	// read migration file
	migration, err := migrations.ReadFile(path)
	if err != nil {
		return fmt.Errorf("migration %s failed: %w", name, err)
	}

	// execute it
	err = db.Exec(string(migration)).Error
	if err != nil {
		return fmt.Errorf("migration %s failed: %w", name, err)
	}

	logger.Infof("migration %s complete", name)
	return nil
}
