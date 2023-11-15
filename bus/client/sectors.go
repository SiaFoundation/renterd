package client

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
)

func (c *Client) DeleteHostSector(ctx context.Context, hostKey types.PublicKey, root types.Hash256) error {
	return c.c.WithContext(ctx).DELETE(fmt.Sprintf("/sectors/%s/%s", hostKey, root))
}
