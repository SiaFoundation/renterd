package client

import (
	"context"

	"go.sia.tech/renterd/api"
)

var (
	enable  = true
	disable = false
)

// Autopilot returns the autopilot.
func (c *Client) Autopilot(ctx context.Context) (ap api.Autopilot, err error) {
	err = c.c.WithContext(ctx).GET("/autopilot", &ap)
	return
}

// DisableAutopilot disables the autopilot.
func (c *Client) DisableAutopilot(ctx context.Context) error {
	return c.updateAutopilot(ctx, &disable, nil, nil, nil)
}

// EnableAutopilot enables the autopilot.
func (c *Client) EnableAutopilot(ctx context.Context) error {
	return c.updateAutopilot(ctx, &enable, nil, nil, nil)
}

// UpdateCurrentPeriod updates the autopilot's current period in the bus.
func (c *Client) UpdateCurrentPeriod(ctx context.Context, currentPeriod uint64) error {
	return c.updateAutopilot(ctx, nil, nil, nil, &currentPeriod)
}

// UpdateContractsConfig updates the autopilot's contract configuration in the bus.
func (c *Client) UpdateContractsConfig(ctx context.Context, cfg api.ContractsConfig) error {
	return c.updateAutopilot(ctx, nil, &cfg, nil, nil)
}

// UpdateHostsConfig updates the autopilot's hosts configuration in the bus.
func (c *Client) UpdateHostsConfig(ctx context.Context, cfg api.HostsConfig) error {
	return c.updateAutopilot(ctx, nil, nil, &cfg, nil)
}

func (c *Client) updateAutopilot(ctx context.Context, enabled *bool, contracts *api.ContractsConfig, hosts *api.HostsConfig, currentPeriod *uint64) error {
	return c.c.WithContext(ctx).PUT("/autopilot", api.UpdateAutopilotRequest{
		Enabled:       enabled,
		Contracts:     contracts,
		CurrentPeriod: currentPeriod,
		Hosts:         hosts,
	})
}
