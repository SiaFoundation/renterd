package webhooks

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

var ErrWebhookNotFound = errors.New("Webhook not found")

type (
	WebhookStore interface {
		DeleteWebhook(ctx context.Context, wh Webhook) error
		AddWebhook(ctx context.Context, wh Webhook) error
		Webhooks(ctx context.Context) ([]Webhook, error)
	}

	Broadcaster interface {
		BroadcastAction(ctx context.Context, action Event) error
	}
)

type HeaderOption func(headers map[string]string)

func WithBasicAuth(username, password string) HeaderOption {
	return func(headers map[string]string) {
		headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
	}
}

type NoopBroadcaster struct{}

func (NoopBroadcaster) BroadcastAction(_ context.Context, _ Event) error { return nil }

const (
	webhookTimeout   = 10 * time.Second
	WebhookEventPing = "ping"
)

type (
	Webhook struct {
		Module  string            `json:"module"`
		Event   string            `json:"event"`
		URL     string            `json:"url"`
		Headers map[string]string `json:"headers,omitempty"`
	}

	WebhookQueueInfo struct {
		URL  string `json:"url"`
		Size int    `json:"size"`
	}

	// Event describes an event that has been triggered.
	Event struct {
		Module  string      `json:"module"`
		Event   string      `json:"event"`
		Payload interface{} `json:"payload,omitempty"`
	}
)

type Manager struct {
	logger *zap.SugaredLogger
	wg     sync.WaitGroup
	store  WebhookStore

	shutdownCtx       context.Context
	shutdownCtxCancel context.CancelFunc

	mu       sync.Mutex
	queues   map[string]*eventQueue // URL -> queue
	webhooks map[string]Webhook
}

type eventQueue struct {
	ctx     context.Context
	logger  *zap.SugaredLogger
	headers map[string]string
	url     string

	mu           sync.Mutex
	isDequeueing bool
	events       []Event
}

func (m *Manager) BroadcastAction(_ context.Context, event Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, hook := range m.webhooks {
		if !hook.Matches(event) {
			continue
		}

		// Find queue or create one.
		queue, exists := m.queues[hook.URL]
		if !exists {
			queue = &eventQueue{
				ctx:     m.shutdownCtx,
				logger:  m.logger,
				headers: hook.Headers,
				url:     hook.URL,
			}
			m.queues[hook.URL] = queue
		}

		// Add event and launch goroutine to start dequeueing if necessary.
		queue.mu.Lock()
		queue.events = append(queue.events, event)
		if !queue.isDequeueing {
			queue.isDequeueing = true
			m.wg.Add(1)
			go func() {
				queue.dequeue()
				m.wg.Done()
			}()
		}
		queue.mu.Unlock()
	}
	return nil
}

func (m *Manager) Delete(ctx context.Context, wh Webhook) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.store.DeleteWebhook(ctx, wh); err != nil {
		return err
	}
	delete(m.webhooks, wh.String())
	return nil
}

func (m *Manager) Info() ([]Webhook, []WebhookQueueInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var hooks []Webhook
	for _, hook := range m.webhooks {
		hooks = append(hooks, Webhook{
			Event:  hook.Event,
			Module: hook.Module,
			URL:    hook.URL,
		})
	}
	var queueInfos []WebhookQueueInfo
	for _, queue := range m.queues {
		queue.mu.Lock()
		queueInfos = append(queueInfos, WebhookQueueInfo{
			URL:  queue.url,
			Size: len(queue.events),
		})
		queue.mu.Unlock()
	}
	return hooks, queueInfos
}

func (m *Manager) Register(ctx context.Context, wh Webhook) error {
	ctx, cancel := context.WithTimeout(m.shutdownCtx, webhookTimeout)
	defer cancel()

	// Test URL.
	err := sendEvent(ctx, wh.URL, wh.Headers, Event{
		Event: WebhookEventPing,
	})
	if err != nil {
		return err
	}

	// Add Webhook.
	if err := m.store.AddWebhook(ctx, wh); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.webhooks[wh.String()] = wh
	return nil
}

func (m *Manager) Shutdown(ctx context.Context) error {
	m.shutdownCtxCancel()

	waitChan := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitChan:
	}
	return nil
}

func (a Event) String() string {
	return a.Module + "." + a.Event
}

func (q *eventQueue) dequeue() {
	for {
		q.mu.Lock()
		if len(q.events) == 0 {
			q.isDequeueing = false
			q.mu.Unlock()
			return
		}
		next := q.events[0]
		q.events = q.events[1:]
		q.mu.Unlock()

		err := sendEvent(q.ctx, q.url, q.headers, next)
		if err != nil {
			q.logger.Errorf("failed to send Webhook event %v to %v: %v", next.String(), q.url, err)
		}
	}
}

func (w Webhook) Matches(action Event) bool {
	if w.Module != action.Module {
		return false
	}
	return w.Event == "" || w.Event == action.Event
}

func (w Webhook) String() string {
	return fmt.Sprintf("%v.%v.%v", w.URL, w.Module, w.Event)
}

func NewManager(store WebhookStore, logger *zap.Logger) (*Manager, error) {
	shutdownCtx, shutdownCtxCancel := context.WithCancel(context.Background())
	m := &Manager{
		logger: logger.Named("webhooks").Sugar(),
		store:  store,

		shutdownCtx:       shutdownCtx,
		shutdownCtxCancel: shutdownCtxCancel,

		queues:   make(map[string]*eventQueue),
		webhooks: make(map[string]Webhook),
	}
	hooks, err := store.Webhooks(shutdownCtx)
	if err != nil {
		return nil, err
	}
	for _, hook := range hooks {
		m.webhooks[hook.String()] = hook
	}
	return m, nil
}

func sendEvent(ctx context.Context, url string, headers map[string]string, action Event) error {
	body, err := json.Marshal(action)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	_, _, err = utils.SendRequest(req, nil)
	return err
}
