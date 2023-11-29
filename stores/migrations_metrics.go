package stores

import (
	"fmt"
	"time"

	"github.com/go-gormigrate/gormigrate/v2"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var (
	metricsTables = []interface{}{
		&dbContractMetric{},
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
			ID: "00001_wallet_metrics",
			Migrate: func(tx *gorm.DB) error {
				return performMigration00001_wallet_metrics(tx, logger)
			},
			Rollback: nil,
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

func performMigration00001_wallet_metrics(txn *gorm.DB, logger *zap.SugaredLogger) error {
	logger.Info("performing migration 00001_wallet_metrics")
	if err := txn.Table("wallets").Migrator().AutoMigrate(&struct {
		ID        uint `gorm:"primarykey"`
		CreatedAt time.Time

		Timestamp unixTimeMS `gorm:"index;NOT NULL"`

		ConfirmedLo   unsigned64 `gorm:"index:idx_confirmed;NOT NULL"`
		ConfirmedHi   unsigned64 `gorm:"index:idx_confirmed;NOT NULL"`
		SpendableLo   unsigned64 `gorm:"index:idx_spendable;NOT NULL"`
		SpendableHi   unsigned64 `gorm:"index:idx_spendable;NOT NULL"`
		UnconfirmedLo unsigned64 `gorm:"index:idx_unconfirmed;NOT NULL"`
		UnconfirmedHi unsigned64 `gorm:"index:idx_unconfirmed;NOT NULL"`
	}{}); err != nil {
		return err
	}
	logger.Info("migration 00001_wallet_metrics complete")
	return nil
}
