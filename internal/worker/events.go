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
	EventManager interface {
		AddSubscriber(id string, s EventSubscriber) (chan struct{}, error)
		HandleEvent(event webhooks.Event)
		Run(ctx context.Context, eventURL string, opts ...webhooks.HeaderOption) error
		Shutdown(context.Context) error
	}

	EventSubscriber interface {
		HandleEvent(event webhooks.Event) error
		Subscribe(e EventManager) error
	}

	WebhookManager interface {
		RegisterWebhook(ctx context.Context, wh webhooks.Webhook) error
		UnregisterWebhook(ctx context.Context, wh webhooks.Webhook) error
	}
)

type (
	eventManager struct {
		webhooks WebhookManager
		logger   *zap.SugaredLogger

		registerInterval time.Duration

		mu         sync.Mutex
		subs       map[string]eventSubscriber
		registered []webhooks.Webhook
	}

	eventSubscriber struct {
		EventSubscriber
		readyChan chan struct{}
	}
)

func NewEventManager(w WebhookManager, l *zap.Logger, registerInterval time.Duration) EventManager {
	return &eventManager{
		webhooks: w,
		logger:   l.Sugar().Named("events"),

		registerInterval: registerInterval,

		subs: make(map[string]eventSubscriber),
	}
}

func (e *eventManager) AddSubscriber(id string, s EventSubscriber) (chan struct{}, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.subs[id]
	if ok {
		return nil, fmt.Errorf("subscriber with id %v already exists", id)
	}

	readyChan := make(chan struct{})
	if len(e.registered) > 0 {
		close(readyChan)
	}

	e.subs[id] = eventSubscriber{s, readyChan}
	return readyChan, nil
}

func (e *eventManager) HandleEvent(event webhooks.Event) {
	for id, s := range e.subs {
		if err := s.HandleEvent(event); err != nil {
			e.logger.Errorw("failed to handle event",
				zap.Error(err),
				zap.String("subscriber", id),
				zap.String("module", event.Module),
				zap.String("event", event.Event),
			)
		} else {
			e.logger.Debugw("handled event",
				zap.String("subscriber", id),
				zap.String("module", event.Module),
				zap.String("event", event.Event),
			)
		}
	}
}

func (e *eventManager) Run(ctx context.Context, eventsURL string, opts ...webhooks.HeaderOption) error {
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
		api.WebhookSettingUpdate(eventsURL, headers),
	}

	// try and register the webhooks in a loop
	for {
		if e.registerWebhooks(ctx, webhooks) {
			e.mu.Lock()
			for _, s := range e.subs {
				close(s.readyChan)
			}
			e.mu.Unlock()
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

func (e *eventManager) Shutdown(ctx context.Context) error {
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

func (e *eventManager) registerWebhooks(ctx context.Context, webhooks []webhooks.Webhook) bool {
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
