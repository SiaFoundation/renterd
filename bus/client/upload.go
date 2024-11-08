package client

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

// AddUploadingSectors adds the given sectors to the upload with given id.
func (c *Client) AddUploadingSectors(ctx context.Context, uID api.UploadID, roots []types.Hash256) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/upload/%s/sector", uID), api.UploadSectorRequest{
		Roots: roots,
	}, nil)
	return
}

// FinishUpload marks the given upload as finished.
func (c *Client) FinishUpload(ctx context.Context, uID api.UploadID) (err error) {
	err = c.c.WithContext(ctx).DELETE(fmt.Sprintf("/upload/%s", uID))
	return
}

// TrackUpload tracks the upload with given id in the bus.
func (c *Client) TrackUpload(ctx context.Context, uID api.UploadID) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/upload/%s", uID), nil, nil)
	return
}
