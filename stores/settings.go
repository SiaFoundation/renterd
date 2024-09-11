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
	return s.updateSetting(ctx, SettingGouging, gs)
}

func (s *SQLStore) PinnedSettings(ctx context.Context) (ps api.PinnedSettings, err error) {
	err = s.fetchSetting(ctx, SettingPinned, &ps)
	return
}

func (s *SQLStore) UpdatePinnedSettings(ctx context.Context, ps api.PinnedSettings) error {
	return s.updateSetting(ctx, SettingPinned, ps)
}

func (s *SQLStore) UploadSettings(ctx context.Context) (us api.UploadSettings, err error) {
	err = s.fetchSetting(ctx, SettingUpload, &us)
	return
}

func (s *SQLStore) UpdateUploadSettings(ctx context.Context, us api.UploadSettings) error {
	return s.updateSetting(ctx, SettingUpload, us)
}

func (s *SQLStore) S3Settings(ctx context.Context) (ss api.S3Settings, err error) {
	err = s.fetchSetting(ctx, SettingS3, &ss)
	return
}

func (s *SQLStore) UpdateS3Settings(ctx context.Context, ss api.S3Settings) error {
	return s.updateSetting(ctx, SettingS3, ss)
}

func (s *SQLStore) fetchSetting(ctx context.Context, key string, out interface{}) error {
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()

	// fetch setting from cache
	value, ok := s.settings[key]
	if ok {
		_ = json.Unmarshal([]byte(value), &out) // cached values are always valid json
		return nil
	}

	// fetch setting from database
	var err error
	if err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		value, err = tx.Setting(ctx, key)
		return err
	}); err != nil {
		return err
	}

	// unmarshal setting
	if err := json.Unmarshal([]byte(value), &out); err != nil {
		return fmt.Errorf("failed to unmarshal setting '%s', err: %v", key, err)
	}

	// update cache
	s.settings[key] = value

	return nil
}

func (s *SQLStore) updateSetting(ctx context.Context, key string, value any) error {
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()

	// marshal the value
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("couldn't marshal the given value, error: %v", err)
	}

	// update db first
	err = s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		return tx.UpdateSetting(ctx, key, string(b))
	})
	if err != nil {
		return err
	}

	// update cache second
	s.settings[key] = string(b)
	return nil
}
