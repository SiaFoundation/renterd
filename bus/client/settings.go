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

// PatchGougingSettings applies the given patch to the gouging settings.
func (c *Client) PatchGougingSettings(ctx context.Context, patch map[string]any) (gs api.GougingSettings, err error) {
	err = c.c.WithContext(ctx).PATCH("/settings/gouging", patch, &gs)
	return
}

// UpdateGougingSettings updates the given setting.
func (c *Client) UpdateGougingSettings(ctx context.Context, gs api.GougingSettings) error {
	return c.c.WithContext(ctx).PUT("/settings/gouging", gs)
}

// PatchPinnedSettings applies the given patch to the pinned settings.
func (c *Client) PatchPinnedSettings(ctx context.Context, patch map[string]any) (ps api.PinnedSettings, err error) {
	err = c.c.WithContext(ctx).PATCH("/settings/pinned", patch, &ps)
	return
}

// PinnedSettings returns the pinned settings.
func (c *Client) PinnedSettings(ctx context.Context) (ps api.PinnedSettings, err error) {
	err = c.c.WithContext(ctx).GET("/settings/pinned", &ps)
	return
}

// UpdatePinnedSettings updates the given setting.
func (c *Client) UpdatePinnedSettings(ctx context.Context, ps api.PinnedSettings) error {
	return c.c.WithContext(ctx).PUT("/settings/pinned", ps)
}

// S3Settings returns the S3 settings.
func (c *Client) S3Settings(ctx context.Context) (as api.S3Settings, err error) {
	err = c.c.WithContext(ctx).GET("/settings/s3", &as)
	return
}

// PatchS3Settings applies the given patch to the S3 settings.
func (c *Client) PatchS3Settings(ctx context.Context, patch map[string]any) (as api.S3Settings, err error) {
	err = c.c.WithContext(ctx).PATCH("/settings/s3", patch, &as)
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

// PatchUploadSettings applies the given patch to the upload settings.
func (c *Client) PatchUploadSettings(ctx context.Context, patch map[string]any) (us api.UploadSettings, err error) {
	err = c.c.WithContext(ctx).PATCH("/settings/upload", patch, &us)
	return
}

// UpdateUploadSettings update the given setting.
func (c *Client) UpdateUploadSettings(ctx context.Context, us api.UploadSettings) error {
	return c.c.WithContext(ctx).PUT("/settings/upload", us)
}
