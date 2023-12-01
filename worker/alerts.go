package worker

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"lukechampine.com/frand"
)

func randomAlertID() types.Hash256 {
	return frand.Entropy256()
}

func newDownloadFailedAlert(bucket, path, prefix, marker string, offset, length, contracts int64, err error) alerts.Alert {
	return alerts.Alert{
		ID:       randomAlertID(),
		Severity: alerts.SeverityError,
		Message:  "Download failed",
		Data: map[string]any{
			"bucket":    bucket,
			"path":      path,
			"prefix":    prefix,
			"marker":    marker,
			"offset":    offset,
			"length":    length,
			"contracts": contracts,
			"error":     err.Error(),
		},
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

	return alerts.Alert{
		ID:        randomAlertID(),
		Severity:  alerts.SeverityError,
		Message:   "Upload failed",
		Data:      data,
		Timestamp: time.Now(),
	}
}
