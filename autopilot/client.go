package autopilot

import (
	"context"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
)

// A Client provides methods for interacting with an autopilot.
type Client struct {
	c jape.Client
}

// NewClient returns a new autopilot client.
func NewClient(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}

// Config returns the autopilot config.
func (c *Client) Config() (cfg api.AutopilotConfig, err error) {
	err = c.c.GET("/config", &cfg)
	return
}

// UpdateConfig updates the autopilot config.
func (c *Client) UpdateConfig(cfg api.AutopilotConfig) error {
	return c.c.PUT("/config", cfg)
}

// State returns the current state of the autopilot.
func (c *Client) State() (state api.AutopilotStateResponse, err error) {
	err = c.c.GET("/state", &state)
	return
}

// Trigger triggers an iteration of the autopilot's main loop.
func (c *Client) Trigger(forceScan bool) (_ bool, err error) {
	var resp api.AutopilotTriggerResponse
	err = c.c.POST("/trigger", api.AutopilotTriggerRequest{ForceScan: forceScan}, &resp)
	return resp.Triggered, err
}

// EvaluateConfig evaluates an autopilot config using the given gouging and
// redundancy settings.
func (c *Client) EvaluateConfig(ctx context.Context, cfg api.AutopilotConfig, gs api.GougingSettings, rs api.RedundancySettings) (resp api.ConfigEvaluationResponse, err error) {
	err = c.c.WithContext(ctx).POST("/config", api.ConfigEvaluationRequest{
		AutopilotConfig:    cfg,
		GougingSettings:    gs,
		RedundancySettings: rs,
	}, &resp)
	return
}
