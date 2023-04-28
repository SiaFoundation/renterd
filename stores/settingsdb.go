package stores

import (
	"context"
	"errors"
	"fmt"

	"go.sia.tech/renterd/api"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type (
	dbSetting struct {
		Model

		Key   string `gorm:"unique;index;NOT NULL"`
		Value string `gorm:"NOT NULL"`
	}
)

// TableName implements the gorm.Tabler interface.
func (dbSetting) TableName() string { return "settings" }

// DeleteSetting implements the bus.SettingStore interface.
func (s *SQLStore) DeleteSetting(ctx context.Context, key string) error {
	// Delete from cache.
	s.settingsMu.Lock()
	delete(s.settings, key)
	s.settingsMu.Unlock()

	// Delete from database.
	return s.db.Where(&dbSetting{Key: key}).Delete(&dbSetting{}).Error
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
	var entry dbSetting
	err := s.db.Where(&dbSetting{Key: key}).
		Take(&entry).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", fmt.Errorf("key '%s' err: %w", key, api.ErrSettingNotFound)
	} else if err != nil {
		return "", err
	}
	s.settings[key] = entry.Value
	return entry.Value, nil
}

// Settings implements the bus.SettingStore interface.
func (s *SQLStore) Settings(ctx context.Context) ([]string, error) {
	var keys []string
	tx := s.db.Model(&dbSetting{}).Select("Key").Find(&keys)
	return keys, tx.Error
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
		Value: value,
	}).Error
	if err != nil {
		return err
	}

	// Update cache second.
	s.settings[key] = value
	return nil
}
