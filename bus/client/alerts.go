package client

import (
	"context"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
)

// Alerts fetches the active alerts from the bus.
func (c *Client) Alerts() (alerts []alerts.Alert, err error) {
	err = c.c.GET("/alerts", &alerts)
	return
}

// DismissAlerts dimisses the alerts with the given IDs.
func (c *Client) DismissAlerts(ctx context.Context, ids ...types.Hash256) error {
	return c.c.WithContext(ctx).POST("/alerts/dismiss", ids, nil)
}

// RegisterAlert registers the given alert.
func (c *Client) RegisterAlert(ctx context.Context, alert alerts.Alert) error {
	return c.c.WithContext(ctx).POST("/alerts/register", alert, nil)
}
