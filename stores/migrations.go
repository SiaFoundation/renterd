package stores

import (
	"errors"
	"fmt"

	"github.com/go-gormigrate/gormigrate/v2"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var errRunV072 = errors.New("can't upgrade to >=v1.0.0 from your current version - please upgrade to v0.7.2 first (https://github.com/SiaFoundation/renterd/releases/tag/v0.7.2)")

var (
	tables = []interface{}{
		// bus.MetadataStore tables
		&dbArchivedContract{},
		&dbContract{},
		&dbContractSet{},
		&dbObject{},
		&dbMultipartUpload{},
		&dbBucket{},
		&dbBufferedSlab{},
		&dbSlab{},
		&dbSector{},
		&dbSlice{},

		// bus.HostDB tables
		&dbAnnouncement{},
		&dbConsensusInfo{},
		&dbHost{},
		&dbAllowlistEntry{},
		&dbBlocklistEntry{},

		// wallet tables
		&dbSiacoinElement{},
		&dbTransaction{},

		// bus.SettingStore tables
		&dbSetting{},

		// bus.EphemeralAccountStore tables
		&dbAccount{},

		// bus.AutopilotStore tables
		&dbAutopilot{},

		// webhooks.WebhookStore tables
		&dbWebhook{},
	}
)

// initSchema is executed only on a clean database. Otherwise the individual
// migrations are executed.
func initSchema(tx *gorm.DB) error {
	// Setup join tables.
	err := setupJoinTables(tx)
	if err != nil {
		return fmt.Errorf("failed to setup join tables: %w", err)
	}

	// Pick the right migrations.
	var schema []byte
	if isSQLite(tx) {
		schema, err = migrations.ReadFile("migrations/sqlite/main/schema.sql")
	} else {
		schema, err = migrations.ReadFile("migrations/mysql/main/schema.sql")
	}
	if err != nil {
		return err
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
