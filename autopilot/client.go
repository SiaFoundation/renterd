package autopilot

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
)

// A Client provides methods for interacting with a renterd API server.
type Client struct {
	c jape.Client
}

// NewClient returns a client that communicates with a renterd store server
// listening on the specified address.
func NewClient(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}

func (c *Client) Config() (cfg api.AutopilotConfig, err error) {
	err = c.c.GET("/config", &cfg)
	return
}

func (c *Client) UpdateConfig(cfg api.AutopilotConfig) error {
	return c.c.PUT("/config", cfg)
}

func (c *Client) HostInfo(hostKey types.PublicKey) (resp api.HostHandlerResponse, err error) {
	err = c.c.GET(fmt.Sprintf("/host/%s", hostKey), &resp)
	return
}

func (c *Client) HostInfos(ctx context.Context, filterMode, usabilityMode string, addressContains string, keyIn []types.PublicKey, offset, limit int) (resp []api.HostHandlerResponse, err error) {
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

func (c *Client) Trigger(forceScan bool) (_ bool, err error) {
	var resp api.AutopilotTriggerResponse
	err = c.c.POST("/debug/trigger", api.AutopilotTriggerRequest{ForceScan: forceScan}, &resp)
	return resp.Triggered, err
}
