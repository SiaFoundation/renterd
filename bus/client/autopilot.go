package client

import (
	"context"

	"go.sia.tech/renterd/api"
)

// AutopilotState returns the autopilot state.
func (c *Client) AutopilotState(ctx context.Context) (cfg api.AutopilotState, err error) {
	err = c.c.WithContext(ctx).GET("/state/autopilot", &cfg)
	return
}

// UpdateAutopilotConfig updates the autopilot configuration in the bus.
func (c *Client) UpdateAutopilotConfig(ctx context.Context, cfg api.AutopilotConfig) (err error) {
	err = c.c.WithContext(ctx).PUT("/config/autopilot", cfg)
	return
}

// UpdateAutopilotPeriod updates the current period in the bus.
func (c *Client) UpdateAutopilotPeriod(ctx context.Context, period uint64) (err error) {
	err = c.c.WithContext(ctx).PUT("/state/autopilot/period", period)
	return
}
