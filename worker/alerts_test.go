package worker

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
)

// TestUploadFailedAlertErrorSet is a test to verify that an upload failing with
// a HostErrorSet error registers an alert with all the individual errors of any
// host in the payload.
func TestUploadFailedAlertErrorSet(t *testing.T) {
	hostErrSet := HostErrorSet{
		types.PublicKey{1, 1, 1}: errors.New("test"),
	}
	wrapped := fmt.Errorf("wrapped error: %w", hostErrSet)

	alert := newUploadFailedAlert("bucket", "path", "set", "mimeType", 1, 2, 3, true, false, wrapped)

	alert.ID = types.Hash256{1, 2, 3}
	alert.Timestamp = time.Time{}

	expectedAlert := alerts.Alert{
		ID:       types.Hash256{1, 2, 3},
		Severity: alerts.SeverityError,
		Message:  "Upload failed",
		Data: map[string]any{
			"bucket":      "bucket",
			"contractSet": "set",
			"contracts":   3,
			"error":       wrapped.Error(),
			"hosts": map[string]string{
				types.PublicKey{1, 1, 1}.String(): "test",
			},
			"mimeType":    "mimeType",
			"minShards":   1,
			"packing":     true,
			"path":        "path",
			"totalShards": 2,
		},
	}
	if !cmp.Equal(alert, expectedAlert) {
		t.Fatal(cmp.Diff(alert, expectedAlert))
	}
}
