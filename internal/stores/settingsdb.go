package stores

import (
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
func (s *SQLStore) Setting(key string) (string, error) {
	var entry dbSetting
	err := s.db.Where(&dbSetting{Key: key}).
		Take(&entry).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", fmt.Errorf("key '%s' err: %w", key, api.ErrSettingNotFound)
	} else if err != nil {
		return "", err
	}

	return entry.Value, nil
}

// Settings implements the bus.SettingStore interface.
func (s *SQLStore) Settings() ([]string, error) {
	var keys []string
	tx := s.db.Model(&dbSetting{}).Select("Key").Find(&keys)
	return keys, tx.Error
}

// Setting implements the bus.SettingStore interface.
func (s *SQLStore) UpdateSetting(key, value string) error {
	return s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).Create(&dbSetting{
		Key:   key,
		Value: value,
	}).Error
}
