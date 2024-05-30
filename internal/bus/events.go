package bus

import (
	"context"
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
		Event() webhooks.Event
	}
)

func NewEventBroadcaster(b webhooks.Broadcaster, l *zap.SugaredLogger) EventBroadcaster {
	return EventBroadcaster{
		broadcaster: b,
		logger:      l,
	}
}

func NewEventWebhook(url string, e Event) webhooks.Webhook {
	return webhooks.Webhook{
		Module: e.Event().Module,
		Event:  e.Event().Event,
		URL:    url,
	}
}

func (b EventBroadcaster) BroadcastEvent(e Event) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	if err := b.broadcaster.BroadcastAction(ctx, e.Event()); err != nil {
		b.logger.Errorw("failed to broadcast event", "event", e, "error", err)
	}
	cancel()
}
