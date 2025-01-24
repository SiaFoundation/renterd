package migrator

import (
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
)

var (
	alertHealthRefreshID     = alerts.RandomAlertID() // constant until restarted
	alertMigrationID         = alerts.RandomAlertID() // constant until restarted
	alertOngoingMigrationsID = alerts.RandomAlertID() // constant until restarted
)

func filterMigrationFailedAlertIDs(a []alerts.Alert) (ids []types.Hash256) {
	for _, alert := range a {
		var sk object.EncryptionKey
		if val, ok := alert.Data["slabKey"]; !ok {
			continue
		} else if sks, ok := val.(string); !ok {
			continue
		} else if err := sk.UnmarshalText([]byte(sks)); err != nil {
			continue
		} else if alert.ID == alerts.IDForSlab(alertMigrationID, sk) {
			ids = append(ids, alert.ID)
		}
	}
	return
}

func newMigrationFailedAlert(slabKey object.EncryptionKey, health float64, objects []api.ObjectMetadata, err error) alerts.Alert {
	data := map[string]interface{}{
		"error":   err.Error(),
		"health":  health,
		"slabKey": slabKey.String(), // used to clear migration alerts
		"hint":    "Migration failures can be temporary, but if they persist it can eventually lead to data loss and should therefor be taken very seriously.",
	}

	if len(objects) > 0 {
		data["objects"] = objects
	}

	hostErr := err
	for errors.Unwrap(hostErr) != nil {
		hostErr = errors.Unwrap(hostErr)
	}
	if set, ok := hostErr.(utils.HostErrorSet); ok {
		hostErrors := make(map[string]string, len(set))
		for hk, err := range set {
			hostErrors[hk.String()] = err.Error()
		}
		data["hosts"] = hostErrors
	}

	severity := alerts.SeverityError
	if health < 0.25 {
		severity = alerts.SeverityCritical
	} else if health < 0.5 {
		severity = alerts.SeverityWarning
	}

	return alerts.Alert{
		ID:        alerts.IDForSlab(alertMigrationID, slabKey),
		Severity:  severity,
		Message:   "Slab migration failed",
		Data:      data,
		Timestamp: time.Now(),
	}
}

func newOngoingMigrationsAlert(n int, estimate time.Duration) alerts.Alert {
	data := make(map[string]interface{})
	if rounded := estimate.Round(time.Minute); rounded > 0 {
		data["estimate"] = fmt.Sprintf("~%v remaining", rounded)
	}

	return alerts.Alert{
		ID:        alertOngoingMigrationsID,
		Severity:  alerts.SeverityInfo,
		Message:   fmt.Sprintf("Migrating %d slabs", n),
		Timestamp: time.Now(),
		Data:      data,
	}
}

func newRefreshHealthFailedAlert(err error) alerts.Alert {
	return alerts.Alert{
		ID:       alertHealthRefreshID,
		Severity: alerts.SeverityCritical,
		Message:  "Health refresh failed",
		Data: map[string]interface{}{
			"error": err.Error(),
		},
		Timestamp: time.Now(),
	}
}
