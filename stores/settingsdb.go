package stores

import (
	"context"
	"encoding/json"
	"fmt"

	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
)

func (s *SQLStore) GougingSettings(ctx context.Context) (gs api.GougingSettings, _ error) {
	value, err := s.fetchSetting(ctx, api.SettingGouging)
	if err != nil {
		return api.GougingSettings{}, err
	}

	if err := json.Unmarshal([]byte(value), &gs); err != nil {
		s.logger.Panicf("failed to unmarshal gouging settings '%s': %v", value, err)
		return api.GougingSettings{}, err
	}
	return
}

func (s *SQLStore) UpdateGougingSettings(ctx context.Context, gs api.GougingSettings) error {
	data, err := json.Marshal(gs)
	if err != nil {
		return fmt.Errorf("couldn't marshal the given value, error: %v", err)
	}
	return s.updateSetting(ctx, api.SettingGouging, string(data))
}

func (s *SQLStore) PinnedSettings(ctx context.Context) (pps api.PinnedSettings, _ error) {
	value, err := s.fetchSetting(ctx, api.SettingPinned)
	if err != nil {
		return api.PinnedSettings{}, err
	}

	if err := json.Unmarshal([]byte(value), &pps); err != nil {
		s.logger.Panicf("failed to unmarshal pinned settings '%s': %v", value, err)
		return api.PinnedSettings{}, err
	}
	return
}

func (s *SQLStore) UpdatePinnedSettings(ctx context.Context, pps api.PinnedSettings) error {
	data, err := json.Marshal(pps)
	if err != nil {
		return fmt.Errorf("couldn't marshal the given value, error: %v", err)
	}
	return s.updateSetting(ctx, api.SettingPinned, string(data))
}

func (s *SQLStore) UploadSettings(ctx context.Context) (us api.UploadSettings, _ error) {
	value, err := s.fetchSetting(ctx, api.SettingUpload)
	if err != nil {
		return api.UploadSettings{}, err
	}

	if err := json.Unmarshal([]byte(value), &us); err != nil {
		s.logger.Panicf("failed to unmarshal upload settings '%s': %v", value, err)
		return api.UploadSettings{}, err
	}
	return
}

func (s *SQLStore) UpdateUploadSettings(ctx context.Context, us api.UploadSettings) error {
	data, err := json.Marshal(us)
	if err != nil {
		return fmt.Errorf("couldn't marshal the given value, error: %v", err)
	}
	return s.updateSetting(ctx, api.SettingUpload, string(data))
}

func (s *SQLStore) S3Settings(ctx context.Context) (ss api.S3Settings, _ error) {
	value, err := s.fetchSetting(ctx, api.SettingS3)
	if err != nil {
		return api.S3Settings{}, err
	}

	if err := json.Unmarshal([]byte(value), &ss); err != nil {
		s.logger.Panicf("failed to unmarshal s3 settings '%s': %v", value, err)
		return api.S3Settings{}, err
	}
	return
}

func (s *SQLStore) UpdateS3Settings(ctx context.Context, ss api.S3Settings) error {
	data, err := json.Marshal(ss)
	if err != nil {
		return fmt.Errorf("couldn't marshal the given value, error: %v", err)
	}
	return s.updateSetting(ctx, api.SettingS3, string(data))
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

func (s *SQLStore) fetchSetting(ctx context.Context, key string) (string, error) {
	// check cache first
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()
	value, ok := s.settings[key]
	if ok {
		return value, nil
	}

	// check database
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
