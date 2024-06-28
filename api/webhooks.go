package api

import "go.sia.tech/renterd/webhooks"

type WebhookResponse struct {
	Webhooks []webhooks.Webhook          `json:"webhooks"`
	Queues   []webhooks.WebhookQueueInfo `json:"queues"`
}
