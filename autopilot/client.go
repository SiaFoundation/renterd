package autopilot

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/webhooks"
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

// Alerts fetches the active alerts from the bus.
func (c *Client) Alerts() (alerts []alerts.Alert, err error) {
	err = c.c.GET("/alerts", &alerts)
	return
}

// DismissAlerts dimisses the alerts with the given IDs.
func (c *Client) DismissAlerts(ids ...types.Hash256) error {
	return c.c.POST("/alerts/dismiss", ids, nil)
}

// RegisterAlertHook registers a new alert hook for the given URL.
func (c *Client) RegisterAlertHook(ctx context.Context, url, module, event string) error {
	err := c.c.WithContext(ctx).POST("/webhooks", api.Webhook{
		Event:  event,
		Module: module,
		URL:    url,
	}, nil)
	return err
}

// DeleteAlertHook deletes the alert hook with the given ID.
func (c *Client) DeleteAlertHook(ctx context.Context, id types.Hash256) error {
	return c.c.DELETE(fmt.Sprintf("/webhook/%s", id))
}

// AlertHooks returns all alert hooks currently registered.
func (c *Client) AlertHooks(ctx context.Context) (hooks []webhooks.Webhook, err error) {
	err = c.c.WithContext(ctx).GET("/webhooks", &hooks)
	return
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

func (c *Client) Status() (resp api.AutopilotStatusResponse, err error) {
	err = c.c.GET("/status", &resp)
	return
}

func (c *Client) Trigger(forceScan bool) (_ bool, err error) {
	var resp api.AutopilotTriggerResponse
	err = c.c.POST("/debug/trigger", api.AutopilotTriggerRequest{ForceScan: forceScan}, &resp)
	return resp.Triggered, err
}
