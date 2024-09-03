package stores

import (
	"context"
	"encoding/json"
	"fmt"

	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
)

const (
	SettingGouging = "gouging"
	SettingPinned  = "pinned"
	SettingS3      = "s3"
	SettingUpload  = "upload"
)

func (s *SQLStore) GougingSettings(ctx context.Context) (gs api.GougingSettings, err error) {
	err = s.fetchSetting(ctx, SettingGouging, &gs)
	return
}

func (s *SQLStore) UpdateGougingSettings(ctx context.Context, gs api.GougingSettings) error {
	data, err := json.Marshal(gs)
	if err != nil {
		return fmt.Errorf("couldn't marshal the given value, error: %v", err)
	}
	return s.updateSetting(ctx, SettingGouging, string(data))
}

func (s *SQLStore) PinnedSettings(ctx context.Context) (ps api.PinnedSettings, err error) {
	err = s.fetchSetting(ctx, SettingPinned, ps)
	return
}

func (s *SQLStore) UpdatePinnedSettings(ctx context.Context, ps api.PinnedSettings) error {
	data, err := json.Marshal(ps)
	if err != nil {
		return fmt.Errorf("couldn't marshal the given value, error: %v", err)
	}
	return s.updateSetting(ctx, SettingPinned, string(data))
}

func (s *SQLStore) UploadSettings(ctx context.Context) (us api.UploadSettings, err error) {
	err = s.fetchSetting(ctx, SettingUpload, us)
	return
}

func (s *SQLStore) UpdateUploadSettings(ctx context.Context, us api.UploadSettings) error {
	data, err := json.Marshal(us)
	if err != nil {
		return fmt.Errorf("couldn't marshal the given value, error: %v", err)
	}
	return s.updateSetting(ctx, SettingUpload, string(data))
}

func (s *SQLStore) S3Settings(ctx context.Context) (ss api.S3Settings, err error) {
	err = s.fetchSetting(ctx, SettingS3, ss)
	return
}

func (s *SQLStore) UpdateS3Settings(ctx context.Context, ss api.S3Settings) error {
	data, err := json.Marshal(ss)
	if err != nil {
		return fmt.Errorf("couldn't marshal the given value, error: %v", err)
	}
	return s.updateSetting(ctx, SettingS3, string(data))
}

func (s *SQLStore) DeleteSetting(ctx context.Context, key string) (err error) {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.DeleteSetting(ctx, key)
	})
}

func (s *SQLStore) Setting(ctx context.Context, key string, out interface{}) (err error) {
	var value string
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		value, err = tx.Setting(ctx, key)
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to fetch setting from db: %w", err)
	}

	return json.Unmarshal([]byte(value), &out)
}

func (s *SQLStore) fetchSetting(ctx context.Context, key string, out interface{}) error {
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()

	value, ok := s.settings[key]
	if !ok {
		var err error
		if err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
			value, err = tx.Setting(ctx, key)
			return err
		}); err != nil {
			return fmt.Errorf("failed to fetch setting from db: %w", err)
		}
		s.settings[key] = value
	}

	// unmarshal setting
	if err := json.Unmarshal([]byte(value), &out); err != nil {
		s.logger.Panicf("failed to unmarshal %s setting '%s': %v", key, value, err)
		return err
	}

	return nil
}

func (s *SQLStore) updateSetting(ctx context.Context, key, value string) error {
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
