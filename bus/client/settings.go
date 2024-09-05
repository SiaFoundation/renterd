package client

import (
	"context"

	"go.sia.tech/renterd/api"
)

// GougingSettings returns the gouging settings.
func (c *Client) GougingSettings(ctx context.Context) (gs api.GougingSettings, err error) {
	err = c.c.WithContext(ctx).GET("/settings/gouging", &gs)
	return
}

// UpdateGougingSettings updates the given setting.
func (c *Client) UpdateGougingSettings(ctx context.Context, gs api.GougingSettings) error {
	return c.c.WithContext(ctx).PUT("/settings/gouging", gs)
}

// PricePinningSettings returns the contract set settings.
func (c *Client) PricePinningSettings(ctx context.Context) (ps api.PinningSettings, err error) {
	err = c.c.WithContext(ctx).GET("/settings/pinned", &ps)
	return
}

// UpdatePinningSettings updates the given setting.
func (c *Client) UpdatePinningSettings(ctx context.Context, ps api.PinningSettings) error {
	return c.c.WithContext(ctx).PUT("/settings/pinned", ps)
}

// S3Settings returns the S3 settings.
func (c *Client) S3Settings(ctx context.Context) (as api.S3Settings, err error) {
	err = c.c.WithContext(ctx).GET("/settings/s3", &as)
	return
}

// UpdateS3Settings updates the given setting.
func (c *Client) UpdateS3Settings(ctx context.Context, as api.S3Settings) error {
	return c.c.WithContext(ctx).PUT("/settings/s3", as)
}

// UploadSettings returns the upload settings.
func (c *Client) UploadSettings(ctx context.Context) (css api.UploadSettings, err error) {
	err = c.c.WithContext(ctx).GET("/settings/upload", &css)
	return
}

// UpdateUploadSettings update the given setting.
func (c *Client) UpdateUploadSettings(ctx context.Context, us api.UploadSettings) error {
	return c.c.WithContext(ctx).PUT("/settings/upload", us)
}
