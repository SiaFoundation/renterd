package bus

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
)

type (
	EventBroadcaster struct {
		broadcaster webhooks.Broadcaster
		logger      *zap.SugaredLogger
	}

	Event interface {
		Kind() (string, string)
	}
)

func NewEventBroadcaster(b webhooks.Broadcaster, l *zap.SugaredLogger) EventBroadcaster {
	return EventBroadcaster{
		broadcaster: b,
		logger:      l,
	}
}

func NewEventWebhook(url string, e Event) webhooks.Webhook {
	module, event := e.Kind()
	return webhooks.Webhook{
		Module: module,
		Event:  event,
		URL:    url,
	}
}

func (b EventBroadcaster) BroadcastEvent(e Event) {
	module, event := e.Kind()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	if err := b.broadcaster.BroadcastAction(ctx, webhooks.Event{
		Module:  module,
		Event:   event,
		Payload: e,
	}); err != nil {
		b.logger.Errorw(fmt.Sprintf("failed to broadcast event %s %s", module, event), "event", e, "error", err)
	}
	cancel()
}
