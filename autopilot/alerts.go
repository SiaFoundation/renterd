package autopilot

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
)

var (
	alertHealthRefreshID     = alerts.RandomAlertID() // constant until restarted
	alertLowBalanceID        = alerts.RandomAlertID() // constant until restarted
	alertOngoingMigrationsID = alerts.RandomAlertID() // constant until restarted
	alertPruningID           = alerts.RandomAlertID() // constant until restarted
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

func newAccountLowBalanceAlert(address types.Address, balance, allowance types.Currency, bh, renewWindow, endHeight uint64) alerts.Alert {
	severity := alerts.SeverityInfo
	if bh+renewWindow/2 >= endHeight {
		severity = alerts.SeverityCritical
	} else if bh+renewWindow >= endHeight {
		severity = alerts.SeverityWarning
	}

	return alerts.Alert{
		ID:       alertLowBalanceID,
		Severity: severity,
		Message:  "Wallet is low on funds",
		Data: map[string]any{
			"address":   address,
			"balance":   balance,
			"allowance": allowance,
			"hint":      fmt.Sprintf("The current wallet balance of %v is less than the configured allowance of %v. Ideally, a wallet holds at least one allowance worth of funds to make sure it can renew all its contracts.", balance, allowance),
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
