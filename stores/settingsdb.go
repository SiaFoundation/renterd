package stores

import (
	"context"
	"fmt"

	sql "go.sia.tech/renterd/stores/sql"
)

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
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		value, err = tx.Setting(ctx, key)
		return err
	})
	if err != nil {
		return "", fmt.Errorf("failed to fetch setting from db: %w", err)
	}
	s.settings[key] = value
	return value, nil
}

// UpdateSetting implements the bus.SettingStore interface.
func (s *SQLStore) UpdateSetting(ctx context.Context, key, value string) error {
	// update db first
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()

	err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateSetting(ctx, key, value)
	})
	if err != nil {
		return err
	}

	// update cache second
	s.settings[key] = value
	return nil
}
