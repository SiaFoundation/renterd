package client

import (
	"context"
	"fmt"

	"go.sia.tech/renterd/api"
)

// ContractSetSettings returns the contract set settings.
func (c *Client) ContractSetSettings(ctx context.Context) (gs api.ContractSetSetting, err error) {
	err = c.Setting(ctx, api.SettingContractSet, &gs)
	return
}

// DeleteSetting will delete the setting with given key.
func (c *Client) DeleteSetting(ctx context.Context, key string) error {
	return c.c.WithContext(ctx).DELETE(fmt.Sprintf("/setting/%s", key))
}

// GougingSettings returns the gouging settings.
func (c *Client) GougingSettings(ctx context.Context) (gs api.GougingSettings, err error) {
	err = c.Setting(ctx, api.SettingGouging, &gs)
	return
}

// RedundancySettings returns the redundancy settings.
func (c *Client) RedundancySettings(ctx context.Context) (rs api.RedundancySettings, err error) {
	err = c.Setting(ctx, api.SettingRedundancy, &rs)
	return
}

// S3AuthenticationSettings returns the S3 authentication settings.
func (c *Client) S3AuthenticationSettings(ctx context.Context) (as api.S3AuthenticationSettings, err error) {
	err = c.Setting(ctx, api.SettingS3Authentication, &as)
	return
}

// Setting returns the value for the setting with given key.
func (c *Client) Setting(ctx context.Context, key string, value interface{}) (err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/setting/%s", key), &value)
	return
}

// Settings returns the keys of all settings.
func (c *Client) Settings(ctx context.Context) (settings []string, err error) {
	err = c.c.WithContext(ctx).GET("/settings", &settings)
	return
}

// UpdateSetting will update the given setting under the given key.
func (c *Client) UpdateSetting(ctx context.Context, key string, value interface{}) error {
	return c.c.WithContext(ctx).PUT(fmt.Sprintf("/setting/%s", key), value)
}

// UploadPackingSettings returns the upload packing settings.
func (c *Client) UploadPackingSettings(ctx context.Context) (ups api.UploadPackingSettings, err error) {
	err = c.Setting(ctx, api.SettingUploadPacking, &ups)
	return
}
