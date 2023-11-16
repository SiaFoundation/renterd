package client

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
)

// DeleteHostSector deletes the given sector on host with given host key.
func (c *Client) DeleteHostSector(ctx context.Context, hostKey types.PublicKey, sectorRoot types.Hash256) error {
	return c.c.WithContext(ctx).DELETE(fmt.Sprintf("/sectors/%s/%s", hostKey, sectorRoot))
}
