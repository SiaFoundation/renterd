package alerts

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/types"
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
)

type (
	Alerter interface {
		RegisterAlert(_ context.Context, a Alert) error
		DismissAlerts(_ context.Context, ids ...types.Hash256) error
	}

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

	// A Manager manages the host's alerts.
	Manager struct {
		mu sync.Mutex
		// alerts is a map of alert IDs to their current alert.
		alerts map[types.Hash256]Alert
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

// RegisterAlert implements the Alerter interface.
func (m *Manager) RegisterAlert(_ context.Context, alert Alert) error {
	if alert.ID == (types.Hash256{}) {
		return errors.New("cannot register alert with zero id")
	} else if alert.Timestamp.IsZero() {
		return errors.New("cannot register alert with zero timestamp")
	} else if alert.Severity == 0 {
		return errors.New("cannot register alert without severity")
	} else if alert.Message == "" {
		return errors.New("cannot register alert without a message")
	} else if alert.Data == nil || alert.Data["origin"] == "" {
		return errors.New("caannot register alert without origin")
	}

	m.mu.Lock()
	m.alerts[alert.ID] = alert
	m.mu.Unlock()
	return nil
}

// DismissAlerts implements the Alerter interface.
func (m *Manager) DismissAlerts(_ context.Context, ids ...types.Hash256) error {
	m.mu.Lock()
	for _, id := range ids {
		delete(m.alerts, id)
	}
	if len(m.alerts) == 0 {
		m.alerts = make(map[types.Hash256]Alert) // reclaim memory
	}
	m.mu.Unlock()
	return nil
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
	}
}

type originAlerter struct {
	alerter Alerter
	origin  string
}

// WithOrigin wraps an Alerter in an originAlerter which always attaches the
// origin field to alerts.
func WithOrigin(alerter Alerter, origin string) Alerter {
	return &originAlerter{
		alerter: alerter,
		origin:  origin,
	}
}

// RegisterAlert implements the Alerter interface.
func (a *originAlerter) RegisterAlert(ctx context.Context, alert Alert) error {
	if alert.Data == nil {
		alert.Data = make(map[string]any)
	}
	alert.Data["origin"] = a.origin
	return a.alerter.RegisterAlert(ctx, alert)
}

// DismissAlerts implements the Alerter interface.
func (a *originAlerter) DismissAlerts(ctx context.Context, ids ...types.Hash256) error {
	return a.alerter.DismissAlerts(ctx, ids...)
}
