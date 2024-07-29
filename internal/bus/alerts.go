package bus

import (
	"fmt"
	"time"

	"go.sia.tech/renterd/alerts"
)

var (
	alertPricePinningID = alerts.RandomAlertID() // constant until restarted
)

func newPricePinningFailedAlert(err error) alerts.Alert {
	return alerts.Alert{
		ID:       alertPricePinningID,
		Severity: alerts.SeverityWarning,
		Message:  "Price pinning failed",
		Data: map[string]any{
			"error": err.Error(),
			"hint":  fmt.Sprintf("This might happen when the forex API is temporarily unreachable. This alert will disappear the next time prices were updated successfully"),
		},
		Timestamp: time.Now(),
	}
}
