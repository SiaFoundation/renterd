package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const testRegisterInterval = 100 * time.Millisecond

type mockAlerter struct{}

func (a *mockAlerter) Alerts(ctx context.Context, opts alerts.AlertsOpts) (alerts.AlertsResponse, error) {
	return alerts.AlertsResponse{}, nil
}
func (a *mockAlerter) RegisterAlert(ctx context.Context, alert alerts.Alert) error   { return nil }
func (a *mockAlerter) DismissAlerts(ctx context.Context, ids ...types.Hash256) error { return nil }

type mockEventHandler struct {
	id        string
	readyChan chan struct{}

	mu     sync.Mutex
	events []webhooks.Event
}

func (s *mockEventHandler) Events() []webhooks.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.events
}

func (s *mockEventHandler) HandleEvent(event webhooks.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.readyChan:
	default:
		return fmt.Errorf("subscriber not ready")
	}

	s.events = append(s.events, event)
	return nil
}

func (s *mockEventHandler) Subscribe(e EventSubscriber) error {
	s.readyChan, _ = e.AddEventHandler(s.id, s)
	return nil
}

type mockWebhookManager struct {
	blockChan chan struct{}

	mu         sync.Mutex
	registered []webhooks.Webhook
}

func (m *mockWebhookManager) RegisterWebhook(ctx context.Context, webhook webhooks.Webhook) error {
	<-m.blockChan

	m.mu.Lock()
	defer m.mu.Unlock()
	m.registered = append(m.registered, webhook)
	return nil
}

func (m *mockWebhookManager) UnregisterWebhook(ctx context.Context, webhook webhooks.Webhook) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, wh := range m.registered {
		if wh.String() == webhook.String() {
			m.registered = append(m.registered[:i], m.registered[i+1:]...)
			return nil
		}
	}
	return nil
}

func (m *mockWebhookManager) Webhooks() []webhooks.Webhook {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.registered
}

func TestEventSubscriber(t *testing.T) {
	// observe logs
	observedZapCore, observedLogs := observer.New(zap.DebugLevel)

	// create mocks
	a := &mockAlerter{}
	w := &mockWebhookManager{blockChan: make(chan struct{})}
	h := &mockEventHandler{id: t.Name()}

	// create event subscriber
	s := NewEventSubscriber(a, w, zap.New(observedZapCore), testRegisterInterval)

	// subscribe the event handler
	if err := h.Subscribe(s); err != nil {
		t.Fatal(err)
	}

	// setup a server
	mux := jape.Mux(map[string]jape.Handler{"POST /event": func(jc jape.Context) {
		var event webhooks.Event
		if jc.Decode(&event) != nil {
			return
		} else if event.Event == webhooks.WebhookEventPing {
			jc.ResponseWriter.WriteHeader(http.StatusOK)
			return
		} else {
			s.ProcessEvent(event)
		}
	}})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// register the subscriber
	eventsURL := fmt.Sprintf("http://%v/event", srv.Listener.Addr().String())
	go func() {
		if err := s.Register(context.Background(), eventsURL); err != nil {
			t.Error(err)
		}
	}()

	// send an event before unblocking webhooks registration
	err := sendEvent(eventsURL, webhooks.Event{Module: api.ModuleConsensus, Event: api.EventUpdate})
	if err != nil {
		t.Fatal(err)
	}
	logs := observedLogs.TakeAll()
	if len(logs) != 1 {
		t.Fatal("expected 1 log, got", len(logs))
	} else if entry := logs[0]; entry.Message != "failed to handle event" || entry.ContextMap()["error"] != "subscriber not ready" {
		t.Fatal("expected different log entry, got", entry)
	}

	// unblock the webhooks registration
	close(w.blockChan)
	time.Sleep(testRegisterInterval)

	// assert webhook was registered
	if webhooks := w.Webhooks(); len(webhooks) != 7 {
		t.Fatal("expected 7 webhooks, got", len(webhooks))
	}

	// send the same event again
	err = sendEvent(eventsURL, webhooks.Event{Module: api.ModuleConsensus, Event: api.EventUpdate})
	if err != nil {
		t.Fatal(err)
	}
	logs = observedLogs.TakeAll()
	if len(logs) != 1 {
		t.Fatal("expected 1 log, got", len(logs))
	} else if entry := logs[0]; entry.Message != "handled event" || entry.ContextMap()["subscriber"] != t.Name() {
		t.Fatal("expected different log entry, got", entry)
	}

	// assert the subscriber handled the event
	if events := h.Events(); len(events) != 1 {
		t.Fatal("expected 1 event, got", len(events))
	}

	// shutdown event subscriber
	err = s.Shutdown(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// assert webhook was unregistered
	if webhooks := w.Webhooks(); len(webhooks) != 0 {
		t.Fatal("expected 0 webhooks, got", len(webhooks))
	}
}

func sendEvent(url string, event webhooks.Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	_, _, err = utils.DoRequest(req, nil)
	return err
}
