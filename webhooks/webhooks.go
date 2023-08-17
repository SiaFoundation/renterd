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
	ctx       context.Context
	ctxCancel context.CancelFunc
	logger    *zap.SugaredLogger
	wg        sync.WaitGroup

	mu       sync.Mutex
	queues   map[string]*actionQueue // URL -> queue
	webhooks map[string]Webhook
}

type actionQueue struct {
	ctx    context.Context
	logger *zap.SugaredLogger
	url    string

	mu           sync.Mutex
	isDequeueing bool
	actions      []Action
}

func (w *Manager) Close() error {
	w.ctxCancel()
	w.wg.Wait()
	return nil
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

	// Add Webhook.
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

func (w *Manager) Info() ([]api.Webhook, []api.WebhookQueueInfo) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var hooks []api.Webhook
	for _, hook := range w.webhooks {
		hooks = append(hooks, api.Webhook{
			Event:  hook.Event,
			Module: hook.Module,
			URL:    hook.URL,
		})
	}
	var queueInfos []api.WebhookQueueInfo
	for _, queue := range w.queues {
		queue.mu.Lock()
		queueInfos = append(queueInfos, api.WebhookQueueInfo{
			URL:  queue.url,
			Size: len(queue.actions),
		})
		queue.mu.Unlock()
	}
	return hooks, queueInfos
}

func (a Action) String() string {
	return a.Module + "." + a.ID
}

func (w *Manager) Broadcast(action Action) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, hook := range w.webhooks {
		if !hook.Matches(action) {
			continue
		}

		// Find queue or create one.
		queue, exists := w.queues[hook.URL]
		if !exists {
			queue = &actionQueue{
				ctx:    w.ctx,
				logger: w.logger,
				url:    hook.URL,
			}
			w.queues[hook.URL] = queue
		}

		// Add action an launch goroutine to start dequeueing if necessary.
		queue.mu.Lock()
		queue.actions = append(queue.actions, action)
		if !queue.isDequeueing {
			w.wg.Add(1)
			go func() {
				queue.dequeue()
				w.wg.Done()
			}()
		}
		queue.mu.Unlock()
	}
}

func (q *actionQueue) dequeue() {
	for {
		q.mu.Lock()
		if len(q.actions) == 0 {
			q.isDequeueing = false
			q.mu.Unlock()
		}
		next := q.actions[0]
		q.actions = q.actions[1:]
		q.mu.Unlock()

		err := sendEvent(q.ctx, q.url, next)
		if err != nil {
			q.logger.Errorf("failed to send Webhook event %v to %v: %v", next.String(), q.url, err)
			return
		}
	}
}

func (w Webhook) Matches(action Action) bool {
	if w.Module != action.Module {
		return false
	}
	return w.Event == "" || w.Event == action.ID
}

func NewManager(logger *zap.SugaredLogger) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		ctx:       ctx,
		ctxCancel: cancel,
		logger:    logger.Named("webhooks"),
		queues:    make(map[string]*actionQueue),
		webhooks:  make(map[string]Webhook),
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
		return fmt.Errorf("Webhook returned unexpected status %v: %v", resp.StatusCode, string(errStr))
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
			jc.Error(fmt.Errorf("Webhook for URL %v and event %v.%v not found", wh.URL, wh.Module, wh.Event), http.StatusNotFound)
			return
		}
	}
}

func (w *Manager) HandlerInfo() jape.Handler {
	return func(jc jape.Context) {
		webhooks, queueInfos := w.Info()
		jc.Encode(api.WebHookResponse{
			Queues:   queueInfos,
			Webhooks: webhooks,
		})
	}
}

func (w *Manager) HandlerAdd() jape.Handler {
	return func(jc jape.Context) {
		var req api.Webhook
		if jc.Decode(&req) != nil {
			return
		}
		err := w.Register(Webhook{
			Event:  req.Event,
			Module: req.Module,
			URL:    req.URL,
		})
		if err != nil {
			jc.Error(fmt.Errorf("failed to add Webhook: %w", err), http.StatusInternalServerError)
			return
		}
	}
}
