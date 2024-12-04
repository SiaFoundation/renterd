package client

import (
	"context"

	"go.sia.tech/renterd/api"
)

type UpdateAutopilotOption func(*api.UpdateAutopilotRequest)

func WithAutopilotEnabled(enabled bool) UpdateAutopilotOption {
	return func(req *api.UpdateAutopilotRequest) {
		req.Enabled = &enabled
	}
}
func WithContractsConfig(cfg api.ContractsConfig) UpdateAutopilotOption {
	return func(req *api.UpdateAutopilotRequest) {
		req.Contracts = &cfg
	}
}
func WithHostsConfig(cfg api.HostsConfig) UpdateAutopilotOption {
	return func(req *api.UpdateAutopilotRequest) {
		req.Hosts = &cfg
	}
}

// Autopilot returns the autopilot configuration.
func (c *Client) AutopilotConfig(ctx context.Context) (ap api.AutopilotConfig, err error) {
	err = c.c.WithContext(ctx).GET("/autopilot", &ap)
	return
}

// UpdateAutopilotConfig updates the autopilot configuration.
func (c *Client) UpdateAutopilotConfig(ctx context.Context, opts ...UpdateAutopilotOption) error {
	var req api.UpdateAutopilotRequest
	for _, opt := range opts {
		opt(&req)
	}
	return c.c.WithContext(ctx).PUT("/autopilot", req)
}
