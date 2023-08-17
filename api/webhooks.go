package api

type Webhook struct {
	Event  string `json:"event"`
	Module string `json:"module"`
	URL    string `json:"url"`
}

type WebhookQueueInfo struct {
	URL  string `json:"url"`
	Size int    `json:"size"`
}

type WebHookResponse struct {
	Webhooks []Webhook          `json:"webhooks"`
	Queues   []WebhookQueueInfo `json:"queues"`
}
