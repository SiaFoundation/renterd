package migrator

import (
	"errors"
	"fmt"
	"time"

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

func newMigrationFailedAlert(slabKey object.EncryptionKey, health float64, objects []api.ObjectMetadata, err error) alerts.Alert {
	data := map[string]interface{}{
		"error":   err.Error(),
		"health":  health,
		"slabKey": slabKey.String(),
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
