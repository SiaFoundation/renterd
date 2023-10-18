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
	"lukechampine.com/frand"
)

var (
	alertAccountRefillID = frand.Entropy256() // constant until restarted
	alertLowBalanceID    = frand.Entropy256() // constant until restarted
	alertMigrationID     = frand.Entropy256() // constant until restarted
	alertRenewalFailedID = frand.Entropy256() // constant until restarted
)

func alertIDForAccount(alertID [32]byte, id rhpv3.Account) types.Hash256 {
	return types.HashBytes(append(alertID[:], id[:]...))
}

func alertIDForContract(alertID [32]byte, contract api.ContractMetadata) types.Hash256 {
	return types.HashBytes(append(alertID[:], contract.ID[:]...))
}

func alertIDForSlab(alertID [32]byte, slab object.Slab) types.Hash256 {
	return types.HashBytes(append(alertID[:], []byte(slab.Key.String())...))
}

func randomAlertID() types.Hash256 {
	return frand.Entropy256()
}

func (ap *Autopilot) RegisterAlert(ctx context.Context, a alerts.Alert) {
	if err := ap.alerts.RegisterAlert(ctx, a); err != nil {
		ap.logger.Errorf("failed to register alert: %v", err)
	}
}

func (ap *Autopilot) DismissAlert(ctx context.Context, id types.Hash256) {
	if err := ap.alerts.DismissAlerts(ctx, id); err != nil {
		ap.logger.Errorf("failed to dismiss alert: %v", err)
	}
}

func newAccountLowBalanceAlert(address types.Address, balance types.Currency, severity alerts.Severity) alerts.Alert {
	return alerts.Alert{
		ID:       alertLowBalanceID,
		Severity: severity,
		Message:  "Wallet is low on funds",
		Data: map[string]any{
			"address": address,
			"balance": balance,
		},
		Timestamp: time.Now(),
	}
}

func newAccountRefillAlert(id rhpv3.Account, contract api.ContractMetadata, err refillError) alerts.Alert {
	data := map[string]interface{}{
		"error":      err,
		"accountID":  id.String(),
		"contractID": contract.ID.String(),
		"hostKey":    contract.HostKey.String(),
	}
	for i := 0; i < len(err.keysAndValues); i += 2 {
		data[fmt.Sprint(err.keysAndValues[i])] = err.keysAndValues[i+1]
	}

	return alerts.Alert{
		ID:        alertIDForAccount(alertAccountRefillID, id),
		Severity:  alerts.SeverityError,
		Message:   "Ephemeral account refill failed",
		Data:      data,
		Timestamp: time.Now(),
	}
}

func newContractRenewalFailedAlert(contract api.ContractMetadata, interrupted bool, err error) alerts.Alert {
	return alerts.Alert{
		ID:       alertIDForContract(alertRenewalFailedID, contract),
		Severity: alerts.SeverityCritical,
		Message:  "Contract renewal failed",
		Data: map[string]interface{}{
			"error":               err,
			"renewalsInterrupted": interrupted,
			"contractID":          contract.ID.String(),
			"hostKey":             contract.HostKey.String(),
		},
		Timestamp: time.Now(),
	}
}

func newContractSetChangeAlert(name string, added, removed int, removedReasons map[string]string) alerts.Alert {
	return alerts.Alert{
		ID:       randomAlertID(),
		Severity: alerts.SeverityInfo,
		Message:  "Contract set changed",
		Data: map[string]any{
			"name":     name,
			"added":    added,
			"removed":  removed,
			"removals": removedReasons,
		},
		Timestamp: time.Now(),
	}
}

func newOngoingMigrationsAlert(n int) alerts.Alert {
	return alerts.Alert{
		ID:        alertMigrationID,
		Severity:  alerts.SeverityInfo,
		Message:   fmt.Sprintf("Migrating %d slabs", n),
		Timestamp: time.Now(),
	}
}

func newSlabMigrationFailedAlert(slab object.Slab, health float64, err error) alerts.Alert {
	severity := alerts.SeverityWarning
	if health < 0.5 {
		severity = alerts.SeverityCritical
	}

	return alerts.Alert{
		ID:       alertIDForSlab(alertMigrationID, slab),
		Severity: severity,
		Message:  "Slab migration failed",
		Data: map[string]interface{}{
			"error":   err,
			"health":  health,
			"slabKey": slab.Key.String(),
		},
		Timestamp: time.Now(),
	}
}

func newRefreshHealthFailedAlert(err error) alerts.Alert {
	return alerts.Alert{
		ID:       randomAlertID(), // TODO: could be constant between runs
		Severity: alerts.SeverityCritical,
		Message:  "Health refresh failed",
		Data: map[string]interface{}{
			"migrationsInterrupted": true,
			"error":                 err,
		},
		Timestamp: time.Now(),
	}
}
