package client

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

// AddUploadingSector adds the given sector to the upload with given id.
func (c *Client) AddUploadingSector(ctx context.Context, uID api.UploadID, id types.FileContractID, root types.Hash256) (err error) {
	err = c.c.WithContext(ctx).POST(fmt.Sprintf("/upload/%s/sector", uID), api.UploadSectorRequest{
		ContractID: id,
		Root:       root,
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
