package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const testRegisterInterval = 100 * time.Millisecond

type mockSubscriber struct {
	id        string
	readyChan chan struct{}

	mu     sync.Mutex
	events []webhooks.Event
}

func (s *mockSubscriber) Events() []webhooks.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.events
}

func (s *mockSubscriber) HandleEvent(event webhooks.Event) error {
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

func (s *mockSubscriber) Subscribe(e EventManager) error {
	s.readyChan, _ = e.AddSubscriber(s.id, s)
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

func TestEventManager(t *testing.T) {
	// observe logs
	observedZapCore, observedLogs := observer.New(zap.DebugLevel)

	// create mocks
	w := &mockWebhookManager{blockChan: make(chan struct{})}
	s := &mockSubscriber{id: t.Name()}

	// create event manager
	e := NewEventManager(w, zap.New(observedZapCore), testRegisterInterval)

	// subscribe to event manager
	if err := s.Subscribe(e); err != nil {
		t.Fatal(err)
	}

	// setup a server
	mux := jape.Mux(map[string]jape.Handler{"POST /events": e.Handler()})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// run event manager
	eventsURL := fmt.Sprintf("http://%v/events", srv.Listener.Addr().String())
	go func() {
		if err := e.Run(context.Background(), eventsURL); err != nil {
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
	if webhooks := w.Webhooks(); len(webhooks) != 4 {
		t.Fatal("expected 4 webhooks, got", len(webhooks))
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
	if events := s.Events(); len(events) != 1 {
		t.Fatal("expected 1 event, got", len(events))
	}

	// shutdown event manager
	err = e.Shutdown(context.Background())
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
