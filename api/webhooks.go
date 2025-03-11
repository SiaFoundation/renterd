package api

import "go.sia.tech/renterd/v2/webhooks"

type WebhookResponse struct {
	Webhooks []webhooks.Webhook          `json:"webhooks"`
	Queues   []webhooks.WebhookQueueInfo `json:"queues"`
}
