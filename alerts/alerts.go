package alerts

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/webhooks"
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

	webhookModule        = "alerts"
	webhookEventDismiss  = "dismiss"
	webhookEventRegister = "register"
)

type (
	Alerter interface {
		Alerts(_ context.Context, opts AlertsOpts) (resp AlertsResponse, err error)
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
		alerts             map[types.Hash256]Alert
		webhookBroadcaster webhooks.Broadcaster
	}

	AlertsOpts struct {
		Offset   int
		Limit    int
		Severity Severity
	}

	AlertsResponse struct {
		Alerts  []Alert `json:"alerts"`
		HasMore bool    `json:"hasMore"`
		Totals  struct {
			Info     int `json:"info"`
			Warning  int `json:"warning"`
			Error    int `json:"error"`
			Critical int `json:"critical"`
		} `json:"totals"`
	}
)

func IDForAccount(alertID [32]byte, id rhpv3.Account) types.Hash256 {
	return types.HashBytes(append(alertID[:], id[:]...))
}

func IDForContract(alertID [32]byte, fcid types.FileContractID) types.Hash256 {
	return types.HashBytes(append(alertID[:], fcid[:]...))
}

func IDForHost(alertID [32]byte, hk types.PublicKey) types.Hash256 {
	return types.HashBytes(append(alertID[:], hk[:]...))
}

func IDForSlab(alertID [32]byte, slabKey object.EncryptionKey) types.Hash256 {
	return types.HashBytes(append(alertID[:], []byte(slabKey.String())...))
}

func RandomAlertID() types.Hash256 {
	return frand.Entropy256()
}

func (ar AlertsResponse) Total() int {
	return ar.Totals.Info + ar.Totals.Warning + ar.Totals.Error + ar.Totals.Critical
}

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

func (s *Severity) LoadString(str string) error {
	switch str {
	case severityInfoStr:
		*s = SeverityInfo
	case severityWarningStr:
		*s = SeverityWarning
	case severityErrorStr:
		*s = SeverityError
	case severityCriticalStr:
		*s = SeverityCritical
	default:
		return fmt.Errorf("unrecognized severity: %v", str)
	}
	return nil
}

// MarshalJSON implements the json.Marshaler interface.
func (s Severity) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`%q`, s.String())), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (s *Severity) UnmarshalJSON(b []byte) error {
	return s.LoadString(strings.Trim(string(b), `"`))
}

// RegisterAlert implements the Alerter interface.
func (m *Manager) RegisterAlert(ctx context.Context, alert Alert) error {
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
	wb := m.webhookBroadcaster
	m.mu.Unlock()

	return wb.BroadcastAction(ctx, webhooks.Event{
		Module:  webhookModule,
		Event:   webhookEventRegister,
		Payload: alert,
	})
}

// DismissAlerts implements the Alerter interface.
func (m *Manager) DismissAlerts(ctx context.Context, ids ...types.Hash256) error {
	var dismissed []types.Hash256
	m.mu.Lock()
	for _, id := range ids {
		_, exists := m.alerts[id]
		if !exists {
			continue
		}
		delete(m.alerts, id)
		dismissed = append(dismissed, id)
	}
	if len(m.alerts) == 0 {
		m.alerts = make(map[types.Hash256]Alert) // reclaim memory
	}
	wb := m.webhookBroadcaster
	m.mu.Unlock()

	if len(dismissed) == 0 {
		return nil // don't fire webhook to avoid spam
	}
	return wb.BroadcastAction(ctx, webhooks.Event{
		Module:  webhookModule,
		Event:   webhookEventDismiss,
		Payload: dismissed,
	})
}

// Alerts returns the host's active alerts.
func (m *Manager) Alerts(_ context.Context, opts AlertsOpts) (AlertsResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	offset, limit := opts.Offset, opts.Limit
	resp := AlertsResponse{}

	if offset >= len(m.alerts) {
		return resp, nil
	} else if limit == -1 {
		limit = len(m.alerts)
	}

	alerts := make([]Alert, 0, len(m.alerts))
	for _, a := range m.alerts {
		if a.Severity == SeverityInfo {
			resp.Totals.Info++
		} else if a.Severity == SeverityWarning {
			resp.Totals.Warning++
		} else if a.Severity == SeverityError {
			resp.Totals.Error++
		} else if a.Severity == SeverityCritical {
			resp.Totals.Critical++
		}
		if opts.Severity != 0 && a.Severity != opts.Severity {
			continue // filter by severity
		}
		alerts = append(alerts, a)
	}
	sort.Slice(alerts, func(i, j int) bool {
		return alerts[i].Timestamp.After(alerts[j].Timestamp)
	})
	alerts = alerts[offset:]
	if limit < len(alerts) {
		alerts = alerts[:limit]
		resp.HasMore = true
	}
	resp.Alerts = alerts
	return resp, nil
}

func (m *Manager) RegisterWebhookBroadcaster(b webhooks.Broadcaster) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.webhookBroadcaster.(*webhooks.NoopBroadcaster); !ok {
		panic("webhook broadcaster already registered")
	}
	m.webhookBroadcaster = b
}

// NewManager initializes a new alerts manager.
func NewManager() *Manager {
	return &Manager{
		alerts:             make(map[types.Hash256]Alert),
		webhookBroadcaster: &webhooks.NoopBroadcaster{},
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

// Alerts implements the Alerter interface.
func (a *originAlerter) Alerts(ctx context.Context, opts AlertsOpts) (resp AlertsResponse, err error) {
	return a.alerter.Alerts(ctx, opts)
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
