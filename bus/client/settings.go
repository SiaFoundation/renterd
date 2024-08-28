package client

import (
	"context"

	"go.sia.tech/renterd/api"
)

// GougingSettings returns the gouging settings.
func (c *Client) GougingSettings(ctx context.Context) (gs api.GougingSettings, err error) {
	err = c.c.WithContext(ctx).GET("/setting/gouging", &gs)
	return
}

// UpdateGougingSettings updates the given setting.
func (c *Client) UpdateGougingSettings(ctx context.Context, gs api.GougingSettings) error {
	return c.c.WithContext(ctx).PUT("/setting/gouging", gs)
}

// PricePinningSettings returns the contract set settings.
func (c *Client) PricePinningSettings(ctx context.Context) (pps api.PinnedSettings, err error) {
	err = c.c.WithContext(ctx).GET("/setting/pinned", &pps)
	return
}

// UpdatePinnedSettings updates the given setting.
func (c *Client) UpdatePinnedSettings(ctx context.Context, pps api.PinnedSettings) error {
	return c.c.WithContext(ctx).PUT("/setting/pinned", pps)
}

// S3Settings returns the S3 settings.
func (c *Client) S3Settings(ctx context.Context) (as api.S3Settings, err error) {
	err = c.c.WithContext(ctx).GET("/setting/s3", &as)
	return
}

// UpdateS3Settings updates the given setting.
func (c *Client) UpdateS3Settings(ctx context.Context, as api.S3Settings) error {
	return c.c.WithContext(ctx).PUT("/setting/s3", as)
}

// UploadSettings returns the upload settings.
func (c *Client) UploadSettings(ctx context.Context) (css api.UploadSettings, err error) {
	err = c.c.WithContext(ctx).GET("/setting/upload", &css)
	return
}

// UpdateUploadSettings update the given setting.
func (c *Client) UpdateUploadSettings(ctx context.Context, us api.UploadSettings) error {
	return c.c.WithContext(ctx).PUT("/setting/upload", us)
}
