package bus

import (
	"context"
	"errors"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/stores/sql"
)

func (b Bus) gougingSettings(ctx context.Context) (api.GougingSettings, error) {
	gs, err := b.store.GougingSettings(ctx)
	if errors.Is(err, sql.ErrSettingNotFound) {
		gs = api.DefaultGougingSettings
	} else if err != nil {
		return api.GougingSettings{}, err
	}
	return gs, nil
}

func (b Bus) pinnedSettings(ctx context.Context) (api.PinnedSettings, error) {
	ps, err := b.store.PinnedSettings(ctx)
	if errors.Is(err, sql.ErrSettingNotFound) {
		ps = api.DefaultPinnedSettings
	} else if err != nil {
		return api.PinnedSettings{}, err
	}
	return ps, nil
}

func (b Bus) s3Settings(ctx context.Context) (api.S3Settings, error) {
	s3s, err := b.store.S3Settings(ctx)
	if errors.Is(err, sql.ErrSettingNotFound) {
		s3s = api.DefaultS3Settings
	} else if err != nil {
		return api.S3Settings{}, err
	}
	return s3s, nil
}

func (b Bus) uploadSettings(ctx context.Context) (api.UploadSettings, error) {
	us, err := b.store.UploadSettings(ctx)
	if errors.Is(err, sql.ErrSettingNotFound) {
		us = api.DefaultUploadSettings(b.cm.TipState().Network.Name)
	} else if err != nil {
		return api.UploadSettings{}, err
	}
	return us, nil
}
