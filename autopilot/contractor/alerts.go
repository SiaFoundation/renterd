package contractor

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
)

var (
	alertChurnID         = alerts.RandomAlertID() // constant until restarted
	alertLostSectorsID   = alerts.RandomAlertID() // constant until restarted
	alertRenewalFailedID = alerts.RandomAlertID() // constant until restarted
)

func newContractRenewalFailedAlert(contract api.ContractMetadata, interrupted bool, err error) alerts.Alert {
	severity := alerts.SeverityWarning
	if interrupted {
		severity = alerts.SeverityCritical
	}

	return alerts.Alert{
		ID:       alerts.IDForContract(alertRenewalFailedID, contract.ID),
		Severity: severity,
		Message:  "Contract renewal failed",
		Data: map[string]interface{}{
			"error":               err.Error(),
			"renewalsInterrupted": interrupted,
			"contractID":          contract.ID.String(),
			"hostKey":             contract.HostKey.String(),
		},
		Timestamp: time.Now(),
	}
}

func newLostSectorsAlert(hk types.PublicKey, lostSectors uint64) alerts.Alert {
	return alerts.Alert{
		ID:       alerts.IDForHost(alertLostSectorsID, hk),
		Severity: alerts.SeverityWarning,
		Message:  "Host has lost sectors",
		Data: map[string]interface{}{
			"lostSectors": lostSectors,
			"hostKey":     hk.String(),
			"hint":        "The host has reported that it can't serve at least one sector. Consider blocking this host through the blocklist feature. If you think this was a mistake and you want to ignore this warning for now you can reset the lost sector count",
		},
		Timestamp: time.Now(),
	}
}
