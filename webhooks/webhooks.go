package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
)

type Broadcaster interface {
	Broadcast(action Action)
}

const (
	webhookTimeout   = 10 * time.Second
	WebhookEventPing = "ping"
)

type (
	Webhook struct {
		Module string `json:"module"`
		Event  string `json:"event"`
		URL    string `json:"url"`
	}

	// Action describes an event that has been triggered.
	Action struct {
		Module  string      `json:"module"`
		ID      string      `json:"id"`
		Payload interface{} `json:"payload,omitempty"`
	}
)

type Manager struct {
	logger *zap.SugaredLogger

	mu       sync.Mutex
	webhooks map[string]Webhook
}

func (w Webhook) String() string {
	return fmt.Sprintf("%v.%v.%v", w.URL, w.Module, w.Event)
}

func (w *Manager) Register(wh Webhook) error {
	ctx, cancel := context.WithTimeout(context.Background(), webhookTimeout)
	defer cancel()

	// Test URL.
	err := sendEvent(ctx, wh.URL, Action{
		ID: WebhookEventPing,
	})
	if err != nil {
		return err
	}

	// Add webhook.
	w.mu.Lock()
	defer w.mu.Unlock()
	w.webhooks[wh.String()] = wh
	return nil
}

func (w *Manager) Delete(wh Webhook) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, exists := w.webhooks[wh.String()]
	delete(w.webhooks, wh.String())
	return exists
}

func (w *Manager) List() []Webhook {
	w.mu.Lock()
	defer w.mu.Unlock()
	var hooks []Webhook
	for _, hook := range w.webhooks {
		hooks = append(hooks, Webhook{
			Event: hook.Event,
			URL:   hook.URL,
		})
	}
	return hooks
}

func (a Action) String() string {
	return a.Module + "." + a.ID
}

func (w *Manager) Broadcast(event Action) {
	w.mu.Lock()
	var hooks []Webhook
	for _, hook := range w.webhooks {
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
		go func(hook Webhook) {
			defer wg.Done()
			err := sendEvent(ctx, hook.URL, event)
			if err != nil {
				w.logger.Errorf("failed to send webhook event %v to %v: %v", event.String(), hook.URL, err)
				return
			}
		}(hook)
	}
	wg.Wait()
}

func (w Webhook) Matches(action Action) bool {
	if w.Module != action.Module {
		return false
	}
	return w.Event == "" || w.Event == action.ID
}

func NewManager(logger *zap.SugaredLogger) *Manager {
	return &Manager{
		webhooks: make(map[string]Webhook),
		logger:   logger.Named("webhooks"),
	}
}

func sendEvent(ctx context.Context, url string, action Action) error {
	body, err := json.Marshal(action)
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

func (w *Manager) HandlerDelete() jape.Handler {
	return func(jc jape.Context) {
		var wh Webhook
		if jc.Decode(&wh) != nil {
			return
		}
		if !w.Delete(wh) {
			jc.Error(fmt.Errorf("webhook for URL %v and event %v.%v not found", wh.URL, wh.Module, wh.Event), http.StatusNotFound)
			return
		}
	}
}

func (w *Manager) HandlerList() jape.Handler {
	return func(jc jape.Context) {
		jc.Encode(w.List())
	}
}

func (w *Manager) HandlerAdd() jape.Handler {
	return func(jc jape.Context) {
		var req api.WebHookRegisterRequest
		if jc.Decode(&req) != nil {
			return
		}
		err := w.Register(Webhook{
			Event:  req.Event,
			Module: req.Module,
			URL:    req.URL,
		})
		if err != nil {
			jc.Error(fmt.Errorf("failed to add webhook: %w", err), http.StatusInternalServerError)
			return
		}
	}
}
