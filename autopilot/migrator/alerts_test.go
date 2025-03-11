package migrator

import (
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/alerts"
	"go.sia.tech/renterd/v2/object"
)

func TestFilterMigrationFailedAlertIDs(t *testing.T) {
	sk := object.GenerateEncryptionKey(object.EncryptionKeyTypeSalted)
	if ids := filterMigrationFailedAlertIDs([]alerts.Alert{
		{
			ID:       types.Hash256{1},
			Severity: alerts.SeverityCritical,
			Message:  "Contract renewal failed",
			Data:     map[string]any{"contractID": "775262e96fe67ff4c7de6d78da6d180cd7e37c584c34c0813153286ff63aae40"},
		},
		{
			ID:       alerts.IDForSlab(alertMigrationID, sk),
			Severity: alerts.SeverityCritical,
			Message:  "Slab migration failed",
			Data:     map[string]any{"slabKey": sk.String()},
		},
		{
			ID:       types.Hash256{2},
			Severity: alerts.SeverityInfo,
			Message:  "Contract usability updated",
			Data:     map[string]any{},
		},
	}); len(ids) != 1 {
		t.Fatal("unexpected number of alerts", len(ids))
	} else if ids[0] != alerts.IDForSlab(alertMigrationID, sk) {
		t.Fatal("unexpected alert ID")
	}
}
