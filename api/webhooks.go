package api

import "go.sia.tech/renterd/webhooks"

type WebHookResponse struct {
	Webhooks []webhooks.Webhook          `json:"webhooks"`
	Queues   []webhooks.WebhookQueueInfo `json:"queues"`
}
