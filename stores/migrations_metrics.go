package stores

import (
	"fmt"

	"github.com/go-gormigrate/gormigrate/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var (
	metricsTables = []interface{}{
		&dbContractMetric{},
		&dbContractPruneMetric{},
		&dbContractSetMetric{},
		&dbContractSetChurnMetric{},
		&dbPerformanceMetric{},
		&dbWalletMetric{},
	}
)

// initMetricsSchema is executed only on a clean database. Otherwise the individual
// migrations are executed.
func initMetricsSchema(tx *gorm.DB) error {
	// Run auto migrations.
	err := tx.AutoMigrate(metricsTables...)
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
