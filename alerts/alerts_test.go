package alerts

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
)

func TestWebhooks(t *testing.T) {
	alerts := NewManager(webhooks.New(zap.NewNop().Sugar()))

	mux := http.NewServeMux()
	var events []webhooks.Event
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		var event webhooks.Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// register a hook
	url := fmt.Sprintf("http://%v/events", srv.Listener.Addr().String())
	id, err := alerts.hooks.Register(url, webhooks.Event{
		Module: webhookModule,
	})
	if err != nil {
		t.Fatal(err)
	}
	if id == (types.Hash256{}) {
		t.Fatal("no id returned")
	}

	// perform some actions that should trigger the endpoint
	a := Alert{
		ID:        types.Hash256{1},
		Severity:  SeverityWarning,
		Timestamp: time.Unix(0, 0),
	}
	alerts.Register(a)
	alerts.Dismiss(types.Hash256{1})

	// list hooks
	hooks := alerts.hooks.List()
	if len(hooks) != 1 {
		t.Fatal("wrong number of hooks")
	} else if hooks[0].ID != id {
		t.Fatal("wrong hook id")
	}

	// unregister hook
	if !alerts.hooks.Delete(id) {
		t.Fatal("hook not deleted")
	}

	// perform an action that should not trigger the endpoint
	alerts.Register(Alert{
		ID:        types.Hash256{2},
		Severity:  SeverityWarning,
		Timestamp: time.Now(),
	})

	// check events
	if len(events) != 3 {
		t.Fatal("wrong number of hits", len(events))
	}
	assertEvent := func(event webhooks.Event, module, id string, hasPayload bool) {
		t.Helper()
		if event.Module != module {
			t.Fatal("wrong event module", event.Module, module)
		} else if event.ID != id {
			t.Fatal("wrong event id", event.ID, id)
		} else if hasPayload && event.Payload == nil {
			t.Fatal("missing payload")
		}
	}
	assertEvent(events[0], "", webhooks.WebhookEventPing, false)
	assertEvent(events[1], webhookModule, webhookEventRegister, true)
	assertEvent(events[2], webhookModule, webhookEventDismiss, true)
}
