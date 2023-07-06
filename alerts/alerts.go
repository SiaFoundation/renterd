package alerts

import (
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
}

// Dismiss removes the alerts with the given IDs.
func (m *Manager) Dismiss(ids ...types.Hash256) {
	m.mu.Lock()
	for _, id := range ids {
		delete(m.alerts, id)
	}
	m.mu.Unlock()
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
