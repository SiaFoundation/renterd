package stores

import (
	"fmt"

	"github.com/go-gormigrate/gormigrate/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func performMetricsMigrations(db *gorm.DB, logger *zap.SugaredLogger) error {
	dbIdentifier := "metrics"
	migrations := []*gormigrate.Migration{
		{
			ID:      "00001_init",
			Migrate: func(tx *gorm.DB) error { return errRunV072 },
		},
		{
			ID: "00001_idx_contracts_fcid_timestamp",
			Migrate: func(tx *gorm.DB) error {
				return performMigration(tx, dbIdentifier, "00001_idx_contracts_fcid_timestamp", logger)
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
