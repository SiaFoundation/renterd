package worker

import (
	"errors"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"lukechampine.com/frand"
)

var (
	alertMigrationID = alerts.RandomAlertID() // constant until restarted
)

func randomAlertID() types.Hash256 {
	return frand.Entropy256()
}

func newDownloadFailedAlert(bucket, key string, offset, length, contracts int64, err error) alerts.Alert {
	return alerts.Alert{
		ID:       randomAlertID(),
		Severity: alerts.SeverityError,
		Message:  "Download failed",
		Data: map[string]any{
			"bucket":    bucket,
			"key":       key,
			"offset":    offset,
			"length":    length,
			"contracts": contracts,
			"error":     err.Error(),
		},
		Timestamp: time.Now(),
	}
}

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

func newUploadFailedAlert(bucket, path, contractSet, mimeType string, minShards, totalShards, contracts int, packing, multipart bool, err error) alerts.Alert {
	data := map[string]any{
		"bucket":      bucket,
		"path":        path,
		"contractSet": contractSet,
		"minShards":   minShards,
		"totalShards": totalShards,
		"packing":     packing,
		"contracts":   contracts,
		"error":       err.Error(),
	}
	if mimeType != "" {
		data["mimeType"] = mimeType
	}
	if multipart {
		data["multipart"] = true
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

	return alerts.Alert{
		ID:        randomAlertID(),
		Severity:  alerts.SeverityError,
		Message:   "Upload failed",
		Data:      data,
		Timestamp: time.Now(),
	}
}
