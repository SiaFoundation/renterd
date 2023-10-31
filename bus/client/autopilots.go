package client

import (
	"context"
	"fmt"

	"go.sia.tech/renterd/api"
)

// Autopilot returns the autopilot with the given ID.
func (c *Client) Autopilot(ctx context.Context, id string) (autopilot api.Autopilot, err error) {
	err = c.c.WithContext(ctx).GET(fmt.Sprintf("/autopilot/%s", id), &autopilot)
	return
}

// Autopilots returns all autopilots in the autopilots store.
func (c *Client) Autopilots(ctx context.Context) (autopilots []api.Autopilot, err error) {
	err = c.c.WithContext(ctx).GET("/autopilots", &autopilots)
	return
}

// UpdateAutopilot updates the given autopilot in the store.
func (c *Client) UpdateAutopilot(ctx context.Context, autopilot api.Autopilot) (err error) {
	err = c.c.WithContext(ctx).PUT(fmt.Sprintf("/autopilot/%s", autopilot.ID), autopilot)
	return
}
