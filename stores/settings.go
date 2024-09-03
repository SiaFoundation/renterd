package stores

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"go.sia.tech/renterd/api"
	sql "go.sia.tech/renterd/stores/sql"
	"go.uber.org/zap"
)

const (
	SettingGouging = "gouging"
	SettingPinned  = "pinned"
	SettingS3      = "s3"
	SettingUpload  = "upload"
)

func (s *SQLStore) GougingSettings(ctx context.Context) (gs api.GougingSettings, err error) {
	err = s.fetchSetting(ctx, SettingPinned, &gs)
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

// MigrateV2Settings migrates the settings from the old format to the new,
// migrating the existing settings over to the new types and removing the old
// settings. If a setting is not present in the database it will be set to its
// default setting. If an existing setting is not valid, the default will be
// used and a warning will get logged.
func (s *SQLStore) MigrateV2Settings(ctx context.Context) error {
	return s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
		// escape early if none of the old settings are present
		var found bool
		for _, key := range []string{
			"pricepinning",
			"s3authentication",
			"contractset",
			"redundancy",
			"uploadpacking",
		} {
			if _, err := tx.Setting(ctx, key); err != nil && !errors.Is(err, sql.ErrSettingNotFound) {
				return err
			} else if err == nil {
				found = true
				break
			}
		}
		if !found {
			return nil
		}

		s.logger.Info("migrating settings...")

		// migrate gouging settings
		value, err := tx.Setting(ctx, "gouging")
		if err != nil && !errors.Is(err, sql.ErrSettingNotFound) {
			return err
		} else if errors.Is(err, sql.ErrSettingNotFound) {
			if err := tx.UpdateSetting(ctx, SettingGouging, s.defaultSetting(SettingGouging)); err != nil {
				return err
			}
		}

		// migrate pinned settings
		value, err = tx.Setting(ctx, "pricepinning")
		if err != nil && !errors.Is(err, sql.ErrSettingNotFound) {
			return err
		} else if err == nil {
			var ps api.PinnedSettings
			if err := json.Unmarshal([]byte(value), &ps); err != nil {
				s.logger.Warnw("failed to unmarshal pinned settings, using default", zap.Error(err))
				value = s.defaultSetting(SettingPinned)
			} else if err := ps.Validate(); err != nil {
				s.logger.Warnw("failed to migrate pinned settings, using default", zap.Error(err))
				value = s.defaultSetting(SettingPinned)
			}

			// update setting and delete old value
			if err := tx.UpdateSetting(ctx, SettingPinned, value); err != nil {
				return err
			} else if err := tx.DeleteSetting(ctx, "pricepinning"); err != nil {
				return err
			}
		}

		// migrate s3 settings
		value, err = tx.Setting(ctx, "s3authentication")
		if err != nil && !errors.Is(err, sql.ErrSettingNotFound) {
			return err
		} else if err == nil {
			var s3s api.S3Settings
			if err := json.Unmarshal([]byte(value), &s3s.Authentication); err != nil {
				s.logger.Warnw("failed to unmarshal S3 authentication settings, using default", zap.Error(err))
				s3s = api.DefaultS3Settings
			} else if err := s3s.Validate(); err != nil {
				s.logger.Warnw("failed to migrate S3 settings, using default", zap.Error(err))
				s3s = api.DefaultS3Settings
			}

			// update setting and delete old value
			update, _ := json.Marshal(s3s)
			if err := tx.UpdateSetting(ctx, SettingS3, string(update)); err != nil {
				return err
			} else if err := tx.DeleteSetting(ctx, "s3authentication"); err != nil {
				return err
			}
		}

		us := api.DefaultUploadSettings(s.network)

		// migrate contractset settings
		value, err = tx.Setting(ctx, "contractset")
		if err != nil && !errors.Is(err, sql.ErrSettingNotFound) {
			return err
		} else if err == nil {
			var css struct {
				Default string `json:"default"`
			}
			if err := json.Unmarshal([]byte(value), &css); err != nil {
				s.logger.Warnw("failed to unmarshal contractset setting, using default", zap.Error(err))
			} else {
				us.DefaultContractSet = css.Default
			}

			// delete old value
			if err := tx.DeleteSetting(ctx, "contractset"); err != nil {
				return err
			}
		}

		// migrate redundancy settings
		value, err = tx.Setting(ctx, "redundancy")
		if err != nil && !errors.Is(err, sql.ErrSettingNotFound) {
			return err
		} else if err == nil {
			var rs api.RedundancySettings
			if err := json.Unmarshal([]byte(value), &rs); err != nil {
				s.logger.Warnw("failed to unmarshal redundancy settings, using default", zap.Error(err))
			} else if err := rs.Validate(); err != nil {
				s.logger.Warnw("failed to migrate redundancy settings, using default", zap.Error(err))
			} else {
				us.Redundancy = rs
			}

			// delete old value
			if err := tx.DeleteSetting(ctx, "redundancy"); err != nil {
				return err
			}
		}

		// migrate uploadpacking settings
		value, err = tx.Setting(ctx, "uploadpacking")
		if err != nil && !errors.Is(err, sql.ErrSettingNotFound) {
			return err
		} else if err == nil {
			var ups api.UploadPackingSettings
			if err := json.Unmarshal([]byte(value), &ups); err != nil {
				s.logger.Warnw("failed to unmarshal uploadpacking settings, using default", zap.Error(err))
			} else {
				us.Packing = ups
			}

			// delete old value
			if err := tx.DeleteSetting(ctx, "uploadpacking"); err != nil {
				return err
			}
		}

		// update upload settings
		if update, err := json.Marshal(us); err != nil {
			return fmt.Errorf("failed to marshal upload settings: %w", err)
		} else if err := tx.UpdateSetting(ctx, SettingUpload, string(update)); err != nil {
			return err
		}

		s.logger.Info("successfully migrated settings")
		return nil
	})
}

func (s *SQLStore) fetchSetting(ctx context.Context, key string, out interface{}) error {
	s.settingsMu.Lock()
	defer s.settingsMu.Unlock()

	// fetch setting value
	value, ok := s.settings[key]
	if !ok {
		var err error
		if err := s.db.Transaction(ctx, func(tx sql.DatabaseTx) error {
			value, err = tx.Setting(ctx, key)
			return err
		}); err != nil && !errors.Is(err, sql.ErrSettingNotFound) {
			return fmt.Errorf("failed to fetch setting from db: %w", err)
		} else if err != nil {
			value = s.defaultSetting(key)
		}
		s.settings[key] = value
	}

	// unmarshal setting
	if err := json.Unmarshal([]byte(value), &out); err != nil {
		s.logger.Warnf("failed to unmarshal %s setting '%s': %v, using default", key, value, err)
		return json.Unmarshal([]byte(s.defaultSetting(key)), &out)
	}

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

func (s *SQLStore) defaultSetting(key string) string {
	switch key {
	case SettingGouging:
		b, _ := json.Marshal(api.DefaultGougingSettings)
		return string(b)
	case SettingPinned:
		b, _ := json.Marshal(api.DefaultPinnedSettings)
		return string(b)
	case SettingS3:
		b, _ := json.Marshal(api.DefaultS3Settings)
		return string(b)
	case SettingUpload:
		b, _ := json.Marshal(api.DefaultUploadSettings(s.network))
		return string(b)
	default:
		panic("unknown setting") // developer error
	}
}
