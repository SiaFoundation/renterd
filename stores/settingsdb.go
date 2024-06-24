package stores

import (
	"context"
	"fmt"

	sql "go.sia.tech/renterd/stores/sql"
	"gorm.io/gorm/clause"
)

type (
	dbSetting struct {
		Model

		Key   string  `gorm:"unique;index;NOT NULL"`
		Value setting `gorm:"NOT NULL"`
	}
)

// TableName implements the gorm.Tabler interface.
func (dbSetting) TableName() string { return "settings" }

// DeleteSetting implements the bus.SettingStore interface.
func (s *SQLStore) DeleteSetting(ctx context.Context, key string) error {
	// Delete from cache.
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()
	delete(s.settings, key)

	// Delete from database.
	return s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.DeleteSettings(ctx, key)
	})
}

// Setting implements the bus.SettingStore interface.
func (s *SQLStore) Setting(ctx context.Context, key string) (string, error) {
	// Check cache first.
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()
	value, ok := s.settings[key]
	if ok {
		return value, nil
	}

	// Check database.
	var err error
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		value, err = tx.Setting(ctx, key)
		return err
	})
	if err != nil {
		return "", fmt.Errorf("failed to fetch setting from db: %w", err)
	}
	s.settings[key] = value
	return value, nil
}

// Settings implements the bus.SettingStore interface.
func (s *SQLStore) Settings(ctx context.Context) (settings []string, err error) {
	err = s.bMain.Transaction(ctx, func(tx sql.DatabaseTx) error {
		settings, err = tx.Settings(ctx)
		return err
	})
	return
}

// UpdateSetting implements the bus.SettingStore interface.
func (s *SQLStore) UpdateSetting(ctx context.Context, key, value string) error {
	// Update db first.
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()

	err := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(&dbSetting{
		Key:   key,
		Value: setting(value),
	}).Error
	if err != nil {
		return err
	}

	// Update cache second.
	s.settings[key] = value
	return nil
}
