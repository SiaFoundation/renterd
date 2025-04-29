package alerts

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/core/types"
)

func TestAlertManager(t *testing.T) {
	mgr := NewManager()

	var cnt uint8
	newAlert := func(severity Severity) Alert {
		cnt++
		a := Alert{
			ID:        types.Hash256{cnt},
			Severity:  severity,
			Message:   fmt.Sprintf("alert %d", cnt),
			Timestamp: time.Now(),
			Data:      map[string]any{"origin": t.Name()},
		}
		return a
	}

	err1 := mgr.RegisterAlert(context.Background(), newAlert(SeverityInfo))
	err2 := mgr.RegisterAlert(context.Background(), newAlert(SeverityWarning))
	err3 := mgr.RegisterAlert(context.Background(), newAlert(SeverityError))
	err4 := mgr.RegisterAlert(context.Background(), newAlert(SeverityCritical))
	if err := errors.Join(err1, err2, err3, err4); err != nil {
		t.Fatal("failed to register alerts", err)
	}

	res, err := mgr.Alerts(context.Background(), AlertsOpts{Limit: -1})
	if err != nil {
		t.Fatal("failed to get alerts")
	} else if len(res.Alerts) != 4 {
		t.Fatalf("wrong number of alerts: %v != 4", len(res.Alerts))
	} else if res.HasMore {
		t.Fatal("unexpected hasMore")
	}

	res, err = mgr.Alerts(context.Background(), AlertsOpts{Offset: 1, Limit: 2})
	if err != nil {
		t.Fatal("failed to get alerts")
	} else if len(res.Alerts) != 2 {
		t.Fatalf("wrong number of alerts: %v != 2", len(res.Alerts))
	} else if !res.HasMore {
		t.Fatal("unexpected hasMore")
	}

	res, err = mgr.Alerts(context.Background(), AlertsOpts{Offset: 4, Limit: 1})
	if err != nil {
		t.Fatal("failed to get alerts")
	} else if len(res.Alerts) != 0 {
		t.Fatalf("wrong number of alerts: %v != 0", len(res.Alerts))
	} else if res.HasMore {
		t.Fatal("unexpected hasMore")
	}

	for s := 1; s <= 4; s++ {
		res, err := mgr.Alerts(context.Background(), AlertsOpts{Severity: Severity(s), Limit: -1})
		if err != nil {
			t.Fatal("failed to get alerts", Severity(s))
		} else if len(res.Alerts) != 1 {
			t.Fatalf("wrong number of alerts: %v != 1", len(res.Alerts))
		} else if res.HasMore {
			t.Fatal("unexpected hasMore")
		}

		res, err = mgr.Alerts(context.Background(), AlertsOpts{Severity: Severity(s), Offset: 2, Limit: -1})
		if err != nil {
			t.Fatal("failed to get alerts", Severity(s))
		} else if len(res.Alerts) != 0 {
			t.Fatalf("wrong number of alerts: %v != 0", len(res.Alerts))
		} else if res.HasMore {
			t.Fatal("unexpected hasMore")
		}
	}
}
