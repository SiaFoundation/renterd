package autopilot

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
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

// HostInfo returns information about the host with given host key.
func (c *Client) HostInfo(hostKey types.PublicKey) (resp api.HostHandlerResponse, err error) {
	err = c.c.GET(fmt.Sprintf("/host/%s", hostKey), &resp)
	return
}

// HostInfo returns information about all hosts.
func (c *Client) HostInfos(ctx context.Context, filterMode, usabilityMode, addressContains string, keyIn []types.PublicKey, offset, limit int) (resp []api.HostHandlerResponse, err error) {
	err = c.c.POST("/hosts", api.SearchHostsRequest{
		Offset:          offset,
		Limit:           limit,
		FilterMode:      filterMode,
		UsabilityMode:   usabilityMode,
		AddressContains: addressContains,
		KeyIn:           keyIn,
	}, &resp)
	return
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

// EvalutateConfig evaluates an autopilot config using the given gouging and
// redundancy settings.
func (c *Client) EvaluateConfig(ctx context.Context, cfg api.AutopilotConfig, gs api.GougingSettings, rs api.RedundancySettings) (resp api.ConfigEvaluationResponse, err error) {
	err = c.c.WithContext(ctx).POST("/config", api.ConfigEvaluationRequest{
		AutopilotConfig:    cfg,
		GougingSettings:    gs,
		RedundancySettings: rs,
	}, &resp)
	return
}
