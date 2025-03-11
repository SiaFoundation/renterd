package contractor

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/alerts"
	"go.sia.tech/renterd/v2/api"
)

type (
	accumulatedChurn map[types.FileContractID][]churnUpdate

	churnUpdate struct {
		Time    api.TimeRFC3339 `json:"time"`
		From    string          `json:"from"`
		To      string          `json:"to"`
		Reason  string          `json:"reason"`
		HostKey types.PublicKey `json:"hostKey"`
		Size    uint64          `json:"size"`
	}

	usabilityUpdate struct {
		hk     types.PublicKey
		fcid   types.FileContractID
		size   uint64
		from   string
		to     string
		reason string
	}
)

func (c accumulatedChurn) ApplyUpdates(updates []usabilityUpdate) alerts.Alert {
	now := time.Now()
	for _, u := range updates {
		c[u.fcid] = append(c[u.fcid], churnUpdate{
			Time:    api.TimeRFC3339(now),
			From:    u.from,
			To:      u.to,
			Reason:  u.reason,
			HostKey: u.hk,
			Size:    u.size,
		})
	}

	var hint string
	for _, updates := range c {
		if updates[len(updates)-1].To == api.ContractUsabilityBad {
			hint = "High usability churn can lead to a lot of unnecessary migrations, it might be necessary to tweak your configuration depending on the reason hosts are being discarded."
		}
	}

	return alerts.Alert{
		ID:       alertContractUsabilityUpdated,
		Severity: alerts.SeverityInfo,
		Message:  "Contract usability updated",
		Data: map[string]interface{}{
			"churn": c,
			"hint":  hint,
		},
		Timestamp: now,
	}
}

func (c *accumulatedChurn) Reset() {
	*c = make(accumulatedChurn)
}
