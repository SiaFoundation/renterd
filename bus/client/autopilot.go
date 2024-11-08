package client

import (
	"context"

	"go.sia.tech/renterd/api"
)

// Autopilot returns the autopilot.
func (c *Client) Autopilot(ctx context.Context) (ap api.Autopilot, err error) {
	err = c.c.WithContext(ctx).GET("/autopilot", &ap)
	return
}

// EnableAutopilot allows the autopilot to be enabled or disabled.
func (c *Client) EnableAutopilot(ctx context.Context, enable bool) error {
	return c.c.WithContext(ctx).PUT("/autopilot/enable", enable)
}

// UpdateContractsConfig updates the autopilot's contract configuration in the bus.
func (c *Client) UpdateContractsConfig(ctx context.Context, cfg api.ContractsConfig) error {
	return c.c.WithContext(ctx).PUT("/autopilot/config/contracts", cfg)
}

// UpdateHostsConfig updates the autopilot's hosts configuration in the bus.
func (c *Client) UpdateHostsConfig(ctx context.Context, cfg api.HostsConfig) error {
	return c.c.WithContext(ctx).PUT("/autopilot/config/hosts", cfg)
}

// AutopilotPeriod returns the current period.
func (c *Client) UpdateCurrentPeriod(ctx context.Context, period uint64) error {
	return c.c.WithContext(ctx).PUT("/autopilot/period", period)
}
