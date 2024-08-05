package client

import (
	"context"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/webhooks"
)

// BroadcastAction broadcasts an action that triggers a webhook.
func (c *Client) BroadcastAction(ctx context.Context, action webhooks.Event) error {
	err := c.c.WithContext(ctx).POST("/webhooks/action", action, nil)
	return err
}

// UnregisterWebhook unregisters the given webhook.
func (c *Client) UnregisterWebhook(ctx context.Context, webhook webhooks.Webhook) error {
	return c.c.POST("/webhook/delete", webhook, nil)
}

// RegisterWebhook registers the given webhook.
func (c *Client) RegisterWebhook(ctx context.Context, webhook webhooks.Webhook) error {
	err := c.c.WithContext(ctx).POST("/webhooks", webhook, nil)
	return err
}

// Webhooks returns all webhooks currently registered.
func (c *Client) Webhooks(ctx context.Context) (resp api.WebhookResponse, err error) {
	err = c.c.WithContext(ctx).GET("/webhooks", &resp)
	return
}
