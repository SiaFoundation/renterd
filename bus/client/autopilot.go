package client

import (
	"context"

	"go.sia.tech/renterd/api"
)

// AutopilotConfig returns the autopilot config.
func (c *Client) AutopilotConfig(ctx context.Context) (cfg api.AutopilotConfig, err error) {
	err = c.c.WithContext(ctx).GET("/autopilot/config", &cfg)
	return
}

// UpdateAutopilotConfig updates the autopilot configuration in the bus.
func (c *Client) UpdateAutopilotConfig(ctx context.Context, cfg api.AutopilotConfig) (err error) {
	err = c.c.WithContext(ctx).PUT("/autopilot/config", cfg)
	return
}

// AutopilotPeriod returns the current period.
func (c *Client) AutopilotPeriod(ctx context.Context) (period uint64, err error) {
	err = c.c.WithContext(ctx).GET("/autopilot/period", &period)
	return
}

// UpdateAutopilotPeriod updates the current period in the bus.
func (c *Client) UpdateAutopilotPeriod(ctx context.Context, period uint64) (err error) {
	err = c.c.WithContext(ctx).PUT("/autopilot/period", period)
	return
}
