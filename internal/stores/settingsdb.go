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

// Setting implements the bus.SettingStore interface.
func (s *SQLStore) Setting(ctx context.Context, key string) (string, error) {
	var entry dbSetting
	err := s.db.WithContext(ctx).Where(&dbSetting{Key: key}).
		Take(&entry).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", fmt.Errorf("key '%s' err: %w", key, api.ErrSettingNotFound)
	} else if err != nil {
		return "", err
	}

	return entry.Value, nil
}

// Settings implements the bus.SettingStore interface.
func (s *SQLStore) Settings(ctx context.Context) ([]string, error) {
	var keys []string
	tx := s.db.WithContext(ctx).Model(&dbSetting{}).Select("Key").Find(&keys)
	return keys, tx.Error
}

// UpdateSetting implements the bus.SettingStore interface.
func (s *SQLStore) UpdateSetting(ctx context.Context, key, value string) error {
	return s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(&dbSetting{
		Key:   key,
		Value: value,
	}).Error
}

// UpdateSettings implements the bus.SettingStore interface.
func (s *SQLStore) UpdateSettings(ctx context.Context, settings map[string]string) error {
	var dbSettings []dbSetting
	for key, value := range settings {
		dbSettings = append(dbSettings, dbSetting{
			Key:   key,
			Value: value,
		})
	}

	return s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(&dbSettings).Error
}
