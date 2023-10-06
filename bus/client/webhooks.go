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

// DeleteWebhook deletes the webhook with the given ID.
func (c *Client) DeleteWebhook(ctx context.Context, url, module, event string) error {
	return c.c.POST("/webhook/delete", webhooks.Webhook{
		URL:    url,
		Module: module,
		Event:  event,
	}, nil)
}

// RegisterWebhook registers a new webhook for the given URL.
func (c *Client) RegisterWebhook(ctx context.Context, url, module, event string) error {
	err := c.c.WithContext(ctx).POST("/webhooks", webhooks.Webhook{
		Event:  event,
		Module: module,
		URL:    url,
	}, nil)
	return err
}

// Webhooks returns all webhooks currently registered.
func (c *Client) Webhooks(ctx context.Context) (resp api.WebHookResponse, err error) {
	err = c.c.WithContext(ctx).GET("/webhooks", &resp)
	return
}
