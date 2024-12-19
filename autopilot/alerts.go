package autopilot

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
)

var (
	alertLowBalanceID = alerts.RandomAlertID() // constant until restarted
	alertPruningID    = alerts.RandomAlertID() // constant until restarted
)

func (ap *Autopilot) RegisterAlert(ctx context.Context, a alerts.Alert) {
	if err := ap.alerts.RegisterAlert(ctx, a); err != nil {
		ap.logger.Errorf("failed to register alert: %v", err)
	}
}

func (ap *Autopilot) DismissAlert(ctx context.Context, ids ...types.Hash256) {
	if err := ap.alerts.DismissAlerts(ctx, ids...); err != nil {
		ap.logger.Errorf("failed to dismiss alert: %v", err)
	}
}

func newAccountLowBalanceAlert(address types.Address, balance, initialFunding types.Currency) alerts.Alert {
	return alerts.Alert{
		ID:       alertLowBalanceID,
		Severity: alerts.SeverityWarning,
		Message:  "Wallet is low on funds",
		Data: map[string]any{
			"address":        address,
			"balance":        balance,
			"initialFunding": initialFunding,
			"hint":           fmt.Sprintf("The current wallet balance of %v is less than the configured initialFunding of %v times the number of contracts to form. Ideally, a wallet holds at least enough funds to make sure it can form a fresh set of contracts.", balance, initialFunding),
		},
		Timestamp: time.Now(),
	}
}

func newContractPruningFailedAlert(hk types.PublicKey, version, release string, fcid types.FileContractID, err error) alerts.Alert {
	return alerts.Alert{
		ID:       alerts.IDForContract(alertPruningID, fcid),
		Severity: alerts.SeverityWarning,
		Message:  "Contract pruning failed",
		Data: map[string]interface{}{
			"contractID":  fcid.String(),
			"error":       err.Error(),
			"hostKey":     hk.String(),
			"hostVersion": version,
			"hostRelease": release,
		},
		Timestamp: time.Now(),
	}
}
