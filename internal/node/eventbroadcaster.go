package node

import (
	"context"
	"time"

	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
)

type (
	EventBroadcaster interface {
		BroadcastEvent(e webhooks.EventWebhook)
	}

	eventBroadcaster struct {
		broadcaster webhooks.Broadcaster
		logger      *zap.SugaredLogger
	}
)

func NewEventBroadcaster(b webhooks.Broadcaster, l *zap.Logger) EventBroadcaster {
	return &eventBroadcaster{
		broadcaster: b,
		logger:      l.Sugar().Named("events"),
	}
}

func (eb *eventBroadcaster) BroadcastEvent(e webhooks.EventWebhook) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	if err := eb.broadcaster.BroadcastAction(ctx, e.Event()); err != nil {
		eb.logger.Errorw("failed to broadcast event", "event", e, "error", err)
	}
	cancel()
}
