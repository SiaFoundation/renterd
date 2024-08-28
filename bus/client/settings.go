package client

import (
	"context"

	"go.sia.tech/renterd/api"
)

// ContractSetSettings returns the contract set settings.
func (c *Client) ContractSetSettings(ctx context.Context) (css api.ContractSetSetting, err error) {
	err = c.c.WithContext(ctx).GET("/setting/contractset", &css)
	return
}

// UpdateContractSetSetting updates the given setting.
func (c *Client) UpdateContractSetSetting(ctx context.Context, css api.ContractSetSetting) error {
	return c.c.WithContext(ctx).PUT("/setting/contractset", css)
}

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
func (c *Client) PricePinningSettings(ctx context.Context) (pps api.PricePinSettings, err error) {
	err = c.c.WithContext(ctx).GET("/setting/pinned", &pps)
	return
}

// UpdatePinnedSettings updates the given setting.
func (c *Client) UpdatePinnedSettings(ctx context.Context, pps api.PricePinSettings) error {
	return c.c.WithContext(ctx).PUT("/setting/pinned", pps)
}

// RedundancySettings returns the redundancy settings.
func (c *Client) RedundancySettings(ctx context.Context) (rs api.RedundancySettings, err error) {
	err = c.c.WithContext(ctx).GET("/setting/redundancy", &rs)
	return
}

// UpdateRedundancySettings updates the given setting.
func (c *Client) UpdateRedundancySettings(ctx context.Context, rs api.RedundancySettings) error {
	return c.c.WithContext(ctx).PUT("/setting/redundancy", rs)
}

// S3AuthenticationSettings returns the S3 authentication settings.
func (c *Client) S3AuthenticationSettings(ctx context.Context) (as api.S3AuthenticationSettings, err error) {
	err = c.c.WithContext(ctx).GET("/setting/s3authentication", &as)
	return
}

// UpdateS3AuthenticationSettings updates the given setting.
func (c *Client) UpdateS3AuthenticationSettings(ctx context.Context, as api.S3AuthenticationSettings) error {
	return c.c.WithContext(ctx).PUT("/setting/s3authentication", as)
}

// UploadPackingSettings returns the upload packing settings.
func (c *Client) UploadPackingSettings(ctx context.Context) (ups api.UploadPackingSettings, err error) {
	err = c.c.WithContext(ctx).GET("/setting/uploadpacking", &ups)
	return
}

// UpdateUploadPackingSettings updates the given setting.
func (c *Client) UpdateUploadPackingSettings(ctx context.Context, ups api.UploadPackingSettings) error {
	return c.c.WithContext(ctx).PUT("/setting/uploadpacking", ups)
}
