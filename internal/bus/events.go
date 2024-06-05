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
)

func NewEventBroadcaster(b webhooks.Broadcaster, l *zap.Logger) EventBroadcaster {
	return EventBroadcaster{
		broadcaster: b,
		logger:      l.Named("events").Sugar(),
	}
}

func (b EventBroadcaster) BroadcastEvent(e webhooks.WebhookEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	if err := b.broadcaster.BroadcastAction(ctx, e.Event()); err != nil {
		b.logger.Errorw("failed to broadcast event", "event", e, "error", err)
	}
	cancel()
}
