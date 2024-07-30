package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
)

type (
	EventSubscriber interface {
		AddEventHandler(id string, h EventHandler) (chan struct{}, error)
		ProcessEvent(event webhooks.Event)
		Register(ctx context.Context, eventURL string, opts ...webhooks.HeaderOption) error
		Shutdown(context.Context) error
	}

	EventHandler interface {
		HandleEvent(event webhooks.Event) error
		Subscribe(e EventSubscriber) error
	}

	WebhookManager interface {
		RegisterWebhook(ctx context.Context, wh webhooks.Webhook) error
		UnregisterWebhook(ctx context.Context, wh webhooks.Webhook) error
	}
)

type (
	eventSubscriber struct {
		webhooks WebhookManager
		logger   *zap.SugaredLogger

		registerInterval time.Duration

		mu             sync.Mutex
		handlers       map[string]EventHandler
		registered     []webhooks.Webhook
		registeredChan chan struct{}
	}
)

func NewEventSubscriber(w WebhookManager, l *zap.Logger, registerInterval time.Duration) EventSubscriber {
	return &eventSubscriber{
		webhooks: w,
		logger:   l.Sugar().Named("events"),

		registeredChan: make(chan struct{}),

		handlers:         make(map[string]EventHandler),
		registerInterval: registerInterval,
	}
}

func (e *eventSubscriber) AddEventHandler(id string, h EventHandler) (chan struct{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.handlers[id]
	if ok {
		return nil, fmt.Errorf("subscriber with id %v already exists", id)
	}
	e.handlers[id] = h

	return e.registeredChan, nil
}

func (e *eventSubscriber) ProcessEvent(event webhooks.Event) {
	log := e.logger.With(
		zap.String("module", event.Module),
		zap.String("event", event.Event),
	)

	for id, s := range e.handlers {
		if err := s.HandleEvent(event); err != nil {
			log.Errorw("failed to handle event",
				zap.Error(err),
				zap.String("subscriber", id),
			)
		} else {
			log.Debugw("handled event",
				zap.String("subscriber", id),
			)
		}
	}
}

func (e *eventSubscriber) Register(ctx context.Context, eventsURL string, opts ...webhooks.HeaderOption) error {
	select {
	case <-e.registeredChan:
		return fmt.Errorf("already registered") // developer error
	default:
	}

	// prepare headers
	headers := make(map[string]string)
	for _, opt := range opts {
		opt(headers)
	}

	// prepare webhooks
	webhooks := []webhooks.Webhook{
		api.WebhookConsensusUpdate(eventsURL, headers),
		api.WebhookContractArchive(eventsURL, headers),
		api.WebhookContractRenew(eventsURL, headers),
		api.WebhookHostUpdate(eventsURL, headers),
		api.WebhookSettingUpdate(eventsURL, headers),
	}

	// try and register the webhooks in a loop
	for {
		if e.registerWebhooks(ctx, webhooks) {
			close(e.registeredChan)
			break
		}

		// sleep for a bit before trying again
		e.logger.Warnf("failed to register webhooks, retrying in %v", e.registerInterval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(e.registerInterval):
		}
	}

	return nil
}

func (e *eventSubscriber) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// unregister webhooks
	var errs []error
	for _, wh := range e.registered {
		if err := e.webhooks.UnregisterWebhook(ctx, wh); err != nil {
			e.logger.Errorw("failed to unregister webhook",
				zap.Error(err),
				zap.Stringer("webhook", wh),
			)
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (e *eventSubscriber) registerWebhooks(ctx context.Context, webhooks []webhooks.Webhook) bool {
	for _, wh := range webhooks {
		if err := e.webhooks.RegisterWebhook(ctx, wh); err != nil {
			e.logger.Errorw("failed to register webhook",
				zap.Error(err),
				zap.Stringer("webhook", wh),
			)
			return false
		}
	}

	// save webhooks so we can unregister them on shutdown
	e.mu.Lock()
	e.registered = webhooks
	e.mu.Unlock()
	return true
}
