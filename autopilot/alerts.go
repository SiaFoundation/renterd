package autopilot

import (
	"context"
	"fmt"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
)

var (
	alertAccountRefillID = alerts.RandomAlertID() // constant until restarted
	alertLowBalanceID    = alerts.RandomAlertID() // constant until restarted
	alertMigrationID     = alerts.RandomAlertID() // constant until restarted
	alertPruningID       = alerts.RandomAlertID() // constant until restarted
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

func newAccountRefillAlert(id rhpv3.Account, contract api.ContractMetadata, err refillError) alerts.Alert {
	data := map[string]interface{}{
		"error":      err.Error(),
		"accountID":  id.String(),
		"contractID": contract.ID.String(),
		"hostKey":    contract.HostKey.String(),
	}
	for i := 0; i < len(err.keysAndValues); i += 2 {
		data[fmt.Sprint(err.keysAndValues[i])] = err.keysAndValues[i+1]
	}

	return alerts.Alert{
		ID:        alerts.IDForAccount(alertAccountRefillID, id),
		Severity:  alerts.SeverityError,
		Message:   "Ephemeral account refill failed",
		Data:      data,
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
		ID:        alertMigrationID,
		Severity:  alerts.SeverityInfo,
		Message:   fmt.Sprintf("Migrating %d slabs", n),
		Timestamp: time.Now(),
		Data:      data,
	}
}

func newCriticalMigrationSucceededAlert(slabKey object.EncryptionKey) alerts.Alert {
	return alerts.Alert{
		ID:       alerts.IDForSlab(alertMigrationID, slabKey),
		Severity: alerts.SeverityInfo,
		Message:  "Critical migration succeeded",
		Data: map[string]interface{}{
			"slabKey": slabKey.String(),
			"hint":    "This migration succeeded thanks to the MigrationSurchargeMultiplier in the gouging settings that allowed overpaying hosts on some critical sector downloads",
		},
		Timestamp: time.Now(),
	}
}

func newCriticalMigrationFailedAlert(slabKey object.EncryptionKey, health float64, objectIds map[string][]string, err error) alerts.Alert {
	data := map[string]interface{}{
		"error":   err.Error(),
		"health":  health,
		"slabKey": slabKey.String(),
		"hint":    "If migrations of low-health slabs fail, it might be necessary to increase the MigrationSurchargeMultiplier in the gouging settings to ensure it has every chance of succeeding.",
	}
	if objectIds != nil {
		data["objectIDs"] = objectIds
	}

	return alerts.Alert{
		ID:        alerts.IDForSlab(alertMigrationID, slabKey),
		Severity:  alerts.SeverityCritical,
		Message:   "Critical migration failed",
		Data:      data,
		Timestamp: time.Now(),
	}
}

func newMigrationFailedAlert(slabKey object.EncryptionKey, health float64, objectIds map[string][]string, err error) alerts.Alert {
	data := map[string]interface{}{
		"error":   err.Error(),
		"health":  health,
		"slabKey": slabKey.String(),
		"hint":    "Migration failures can be temporary, but if they persist it can eventually lead to data loss and should therefor be taken very seriously.",
	}
	if objectIds != nil {
		data["objectIDs"] = objectIds
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

func newRefreshHealthFailedAlert(err error) alerts.Alert {
	return alerts.Alert{
		ID:       alerts.RandomAlertID(),
		Severity: alerts.SeverityCritical,
		Message:  "Health refresh failed",
		Data: map[string]interface{}{
			"error": err.Error(),
		},
		Timestamp: time.Now(),
	}
}
