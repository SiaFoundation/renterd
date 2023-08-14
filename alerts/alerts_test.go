package alerts

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/types"
)

func TestWebhooks(t *testing.T) {
	alerts := NewManager()

	mux := http.NewServeMux()
	var hits uint64
	var registeredAlerts []Alert
	var dismissedAlerts []types.Hash256
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		hits++
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}

		var req webHookActionCommon
		if err := json.Unmarshal(body, &req); err != nil {
			t.Fatal(err)
		}
		switch req.Type {
		case webhookTypePing:
		case webhookTypeRegister:
			var rr webHookActionRegister
			if err := json.Unmarshal(body, &rr); err != nil {
				t.Fatal(err)
			}
			registeredAlerts = append(registeredAlerts, rr.Alert)
		case webhookTypeDismiss:
			var dr webHookActionDismiss
			if err := json.Unmarshal(body, &dr); err != nil {
				t.Fatal(err)
			}
			dismissedAlerts = append(dismissedAlerts, dr.ToDismiss...)
		default:
			t.Fatal("unrecognized type")
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// register a hook
	url := fmt.Sprintf("http://%v/events", srv.Listener.Addr().String())
	id, err := alerts.AddWebhook(url)
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
	hooks := alerts.ListWebhooks()
	if len(hooks) != 1 {
		t.Fatal("wrong number of hooks")
	} else if hooks[0].id != id {
		t.Fatal("wrong hook id")
	}

	// unregister hook
	if !alerts.DeleteWebhook(id) {
		t.Fatal("hook not deleted")
	}

	// perform an action that should not trigger the endpoint
	alerts.Register(Alert{
		ID:        types.Hash256{2},
		Severity:  SeverityWarning,
		Timestamp: time.Now(),
	})

	// check endpoint hits
	// 1 ping, 1 register, 1 dismiss
	if hits != 3 {
		t.Fatal("wrong number of hits", hits)
	}
	if len(registeredAlerts) != 1 {
		t.Fatal("wrong number of registered alerts")
	} else if !reflect.DeepEqual(registeredAlerts[0], a) {
		fmt.Println(registeredAlerts[0])
		fmt.Println(a)
		t.Fatal("wrong registered alert")
	}
	if len(dismissedAlerts) != 1 {
		t.Fatal("wrong number of dismissed alerts")
	} else if dismissedAlerts[0] != (types.Hash256{1}) {
		t.Fatal("wrong dismissed alert")
	}
}
