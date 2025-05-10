package client

import (
	"context"

	"go.sia.tech/renterd/v2/api"
)

// RenterKey calls the /renterkey endpoint on the bus.
func (c *Client) RenterKey(ctx context.Context) (resp api.RenterKeyResponse, err error) {
	err = c.c.GET(ctx, "/renterkey", &resp)
	return
}
