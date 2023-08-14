package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

const (
	// SeverityInfo indicates that the alert is informational.
	SeverityInfo Severity = iota + 1
	// SeverityWarning indicates that the alert is a warning.
	SeverityWarning
	// SeverityError indicates that the alert is an error.
	SeverityError
	// SeverityCritical indicates that the alert is critical.
	SeverityCritical

	severityInfoStr     = "info"
	severityWarningStr  = "warning"
	severityErrorStr    = "error"
	severityCriticalStr = "critical"

	webhookTimeout      = 10 * time.Second
	webhookTypePing     = "ping"
	webhookTypeRegister = "register"
	webhookTypeDismiss  = "dismiss"
)

type (
	// Severity indicates the severity of an alert.
	Severity uint8

	// An Alert is a dismissible message that is displayed to the user.
	Alert struct {
		// ID is a unique identifier for the alert.
		ID types.Hash256 `json:"id"`
		// Severity is the severity of the alert.
		Severity Severity `json:"severity"`
		// Message is a human-readable message describing the alert.
		Message string `json:"message"`
		// Data is a map of arbitrary data that can be used to provide
		// additional context to the alert.
		Data      map[string]any `json:"data,omitempty"`
		Timestamp time.Time      `json:"timestamp"`
	}

	WebHookRegisterRequest struct {
		URL string `json:"url"`
	}
	WebHookRegisterResponse struct {
		ID types.Hash256 `json:"id"`
	}

	WebHook struct {
		id  types.Hash256
		url string
	}

	webHookActionCommon struct {
		Type string `json:"type"`
	}

	webHookActionDismiss struct {
		webHookActionCommon
		ToDismiss []types.Hash256 `json:"toDismiss"`
	}

	webHookActionRegister struct {
		webHookActionCommon
		Alert Alert `json:"alert"`
	}

	// A Manager manages the host's alerts.
	Manager struct {
		mu sync.Mutex
		// alerts is a map of alert IDs to their current alert.
		alerts map[types.Hash256]Alert
		hooks  map[types.Hash256]*WebHook
	}
)

// String implements the fmt.Stringer interface.
func (s Severity) String() string {
	switch s {
	case SeverityInfo:
		return severityInfoStr
	case SeverityWarning:
		return severityWarningStr
	case SeverityError:
		return severityErrorStr
	case SeverityCritical:
		return severityCriticalStr
	default:
		panic(fmt.Sprintf("unrecognized severity %d", s))
	}
}

// MarshalJSON implements the json.Marshaler interface.
func (s Severity) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`%q`, s.String())), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (s *Severity) UnmarshalJSON(b []byte) error {
	status := strings.Trim(string(b), `"`)
	switch status {
	case severityInfoStr:
		*s = SeverityInfo
	case severityWarningStr:
		*s = SeverityWarning
	case severityErrorStr:
		*s = SeverityError
	case severityCriticalStr:
		*s = SeverityCritical
	default:
		return fmt.Errorf("unrecognized severity: %v", status)
	}
	return nil
}

func (m *Manager) broadcastWebhookAction(action interface{}) {
	m.mu.Lock()
	var hooks []*WebHook
	for _, hook := range m.hooks {
		hooks = append(hooks, hook)
	}
	m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), webhookTimeout)
	defer cancel()
	var wg sync.WaitGroup
	for _, hook := range hooks {
		wg.Add(1)
		go func(hook *WebHook) {
			defer wg.Done()
			err := sendWebhookAction(ctx, hook.url, action)
			if err != nil {
				// TODO: log
			}
		}(hook)
	}
	wg.Wait()
}

func sendWebhookAction(ctx context.Context, url string, action interface{}) error {
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

func (m *Manager) AddWebhook(url string) (types.Hash256, error) {
	ctx, cancel := context.WithTimeout(context.Background(), webhookTimeout)
	defer cancel()

	// Test URL.
	err := sendWebhookAction(ctx, url, webHookActionCommon{
		Type: webhookTypePing,
	})
	if err != nil {
		return types.Hash256{}, err
	}

	// Add webhook.
	id := frand.Entropy256()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hooks[id] = &WebHook{
		id:  id,
		url: url,
	}
	return id, nil
}

func (m *Manager) DeleteWebhook(id types.Hash256) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.hooks[id]
	delete(m.hooks, id)
	return exists
}

func (m *Manager) ListWebhooks() []WebHook {
	m.mu.Lock()
	defer m.mu.Unlock()
	var hooks []WebHook
	for _, hook := range m.hooks {
		hooks = append(hooks, *hook)
	}
	return hooks
}

// Register registers a new alert with the manager
func (m *Manager) Register(a Alert) {
	if a.ID == (types.Hash256{}) {
		panic("cannot register alert with empty ID") // developer error
	} else if a.Timestamp.IsZero() {
		panic("cannot register alert with zero timestamp") // developer error
	}

	m.mu.Lock()
	m.alerts[a.ID] = a
	m.mu.Unlock()

	m.broadcastWebhookAction(webHookActionRegister{
		webHookActionCommon: webHookActionCommon{
			Type: webhookTypeRegister,
		},
		Alert: a,
	})
}

// Dismiss removes the alerts with the given IDs.
func (m *Manager) Dismiss(ids ...types.Hash256) {
	m.mu.Lock()
	for _, id := range ids {
		delete(m.alerts, id)
	}
	m.mu.Unlock()

	m.broadcastWebhookAction(webHookActionDismiss{
		webHookActionCommon: webHookActionCommon{
			Type: webhookTypeDismiss,
		},
		ToDismiss: ids,
	})
}

// Active returns the host's active alerts.
func (m *Manager) Active() []Alert {
	m.mu.Lock()
	defer m.mu.Unlock()

	alerts := make([]Alert, 0, len(m.alerts))
	for _, a := range m.alerts {
		alerts = append(alerts, a)
	}
	sort.Slice(alerts, func(i, j int) bool {
		return alerts[i].Timestamp.After(alerts[j].Timestamp)
	})
	return alerts
}

// NewManager initializes a new alerts manager.
func NewManager() *Manager {
	return &Manager{
		alerts: make(map[types.Hash256]Alert),
		hooks:  make(map[types.Hash256]*WebHook),
	}
}
