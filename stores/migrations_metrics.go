package stores

import (
	"fmt"

	"github.com/go-gormigrate/gormigrate/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// initMetricsSchema is executed only on a clean database. Otherwise the individual
// migrations are executed.
func initMetricsSchema(tx *gorm.DB) error {
	// Pick the right migrations.
	var schema []byte
	var err error
	if isSQLite(tx) {
		schema, err = migrations.ReadFile("migrations/sqlite/metrics/schema.sql")
	} else {
		schema, err = migrations.ReadFile("migrations/mysql/metrics/schema.sql")
	}
	if err != nil {
		return err
	}

	// Run it.
	err = tx.Exec(string(schema)).Error
	if err != nil {
		return fmt.Errorf("failed to init schema: %w", err)
	}
	return nil
}

func performMetricsMigrations(db *gorm.DB, logger *zap.SugaredLogger) error {
	migrations := []*gormigrate.Migration{
		{
			ID:      "00001_init",
			Migrate: func(tx *gorm.DB) error { return errRunV072 },
		},
		{
			ID: "00001_slab_metrics",
			Migrate: func(tx *gorm.DB) error {
				return performMigration(tx, "00001_slab_metrics", logger)
			},
		},
	}

	// Create migrator.
	m := gormigrate.New(db, gormigrate.DefaultOptions, migrations)

	// Set init function.
	m.InitSchema(initMetricsSchema)

	// Perform migrations.
	if err := m.Migrate(); err != nil {
		return fmt.Errorf("failed to migrate: %v", err)
	}
	return nil
}
