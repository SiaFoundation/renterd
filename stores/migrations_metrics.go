package stores

import (
	"fmt"

	"github.com/go-gormigrate/gormigrate/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func performMetricsMigrations(tx *gorm.DB, logger *zap.SugaredLogger) error {
	migrations := []*gormigrate.Migration{
		{
			ID:      "00001_init",
			Migrate: func(tx *gorm.DB) error { return errRunV072 },
		},
	}

	// Create migrator.
	m := gormigrate.New(tx, gormigrate.DefaultOptions, migrations)

	// Set init function.
	m.InitSchema(initSchema(tx, "metrics", logger))

	// Perform migrations.
	if err := m.Migrate(); err != nil {
		return fmt.Errorf("failed to migrate: %v", err)
	}
	return nil
}
