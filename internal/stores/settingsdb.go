package stores

import (
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrSettingNotFound is returned if a specific setting can't be retrieved from the db.
var ErrSettingNotFound = errors.New("setting not found")

type (
	dbSetting struct {
		Model

		Key   string `gorm:"unique;index"`
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
		return "", ErrSettingNotFound
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
