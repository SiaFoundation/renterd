package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	webhookTimeout   = 10 * time.Second
	WebhookEventPing = "ping"
)

type (
	WebHookRegisterRequest struct {
		Event Event  `json:"event"`
		URL   string `json:"url"`
	}
	WebHookRegisterResponse struct {
		ID types.Hash256 `json:"id"`
	}

	WebHook struct {
		ID    types.Hash256 `json:"id"`
		Event Event         `json:"event"`
		URL   string        `json:"url"`
	}

	registeredHook struct {
		Event
		url string
	}

	Event struct {
		Module  string      `json:"module"`
		ID      string      `json:"id"`
		Payload interface{} `json:"payload,omitempty"`
	}
)

type Webhooks struct {
	mu     sync.Mutex
	hooks  map[types.Hash256]*registeredHook
	logger *zap.SugaredLogger
}

func (w *Webhooks) Register(url string, event Event) (types.Hash256, error) {
	ctx, cancel := context.WithTimeout(context.Background(), webhookTimeout)
	defer cancel()

	// Test URL.
	err := sendEvent(ctx, url, Event{
		ID: WebhookEventPing,
	})
	if err != nil {
		return types.Hash256{}, err
	}

	// Add webhook.
	id := frand.Entropy256()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.hooks[id] = &registeredHook{
		Event: event,
		url:   url,
	}
	return id, nil
}

func (w *Webhooks) Delete(id types.Hash256) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, exists := w.hooks[id]
	delete(w.hooks, id)
	return exists
}

func (w *Webhooks) List() []WebHook {
	w.mu.Lock()
	defer w.mu.Unlock()
	var hooks []WebHook
	for id, hook := range w.hooks {
		hooks = append(hooks, WebHook{
			ID:    id,
			Event: hook.Event,
			URL:   hook.url,
		})
	}
	return hooks
}

func (e Event) String() string {
	return e.Module + "." + e.ID
}

func (e *Event) LoadString(s string) error {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) == 0 {
		return fmt.Errorf("invalid event string %v", s)
	} else if len(parts) == 1 {
		e.Module = parts[0]
	} else if len(parts) == 2 {
		e.Module, e.ID = parts[0], parts[1]
	} else {
		panic("unreachable")
	}
	return nil
}

func (w *Webhooks) Broadcast(event Event) {
	w.mu.Lock()
	var hooks []*registeredHook
	for _, hook := range w.hooks {
		hooks = append(hooks, hook)
	}
	w.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), webhookTimeout)
	defer cancel()
	var wg sync.WaitGroup
	for _, hook := range hooks {
		if !hook.Matches(event) {
			continue
		}
		wg.Add(1)
		go func(hook *registeredHook) {
			defer wg.Done()
			err := sendEvent(ctx, hook.url, event)
			if err != nil {
				w.logger.Errorf("failed to send webhook event %v to %v: %v", event, hook.url, err)
				return
			}
		}(hook)
	}
	wg.Wait()
}

func (w *registeredHook) Matches(event Event) bool {
	if w.Module != event.Module {
		return false
	}
	return w.ID == "" || w.ID == event.ID
}

func New(logger *zap.SugaredLogger) *Webhooks {
	return &Webhooks{
		hooks:  make(map[types.Hash256]*registeredHook),
		logger: logger.Named("webhooks"),
	}
}

func sendEvent(ctx context.Context, url string, event Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer io.ReadAll(req.Body) // always drain body

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		errStr, err := io.ReadAll(req.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body: %w", err)
		}
		return fmt.Errorf("webhook returned unexpected status %v: %v", resp.StatusCode, string(errStr))
	}
	return nil
}
