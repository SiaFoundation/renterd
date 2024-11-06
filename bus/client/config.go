package client

import (
	"context"

	"go.sia.tech/renterd/api"
)

// AutopilotConfig returns the autopilot configuration.
func (c *Client) AutopilotConfig(ctx context.Context) (cfg api.AutopilotConfig, err error) {
	err = c.c.WithContext(ctx).GET("/config/autopilot", &cfg)
	return
}

// UpdateAutopilotConfig updates the autopilot configuration in the bus.
func (c *Client) UpdateAutopilotConfig(ctx context.Context, cfg api.AutopilotConfig) (err error) {
	err = c.c.WithContext(ctx).PUT("/config/autopilot", cfg)
	return
}
