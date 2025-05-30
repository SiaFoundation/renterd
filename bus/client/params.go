package client

import (
	"context"

	"go.sia.tech/renterd/v2/api"
)

// GougingParams returns parameters used for performing gouging checks.
func (c *Client) GougingParams(ctx context.Context) (gp api.GougingParams, err error) {
	err = c.c.GET(ctx, "/params/gouging", &gp)
	return
}

// UploadParams returns parameters used for uploading slabs.
func (c *Client) UploadParams(ctx context.Context) (up api.UploadParams, err error) {
	err = c.c.GET(ctx, "/params/upload", &up)
	return
}
