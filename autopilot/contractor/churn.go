package contractor

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
)

type (
	accumulatedChurn struct {
		updates map[types.FileContractID][]usabilityUpdate
	}

	usabilityUpdate struct {
		Time   api.TimeRFC3339 `json:"time"`
		From   string          `json:"from"`
		To     string          `json:"to"`
		Reason string          `json:"reason"`
	}
)

func newAccumulatedChurn() *accumulatedChurn {
	return &accumulatedChurn{updates: make(map[types.FileContractID][]usabilityUpdate)}
}

func (c *accumulatedChurn) Update(markedGood, markedBad map[types.FileContractID]string) alerts.Alert {
	now := time.Now()
	for fcid, reason := range markedGood {
		c.updates[fcid] = append(c.updates[fcid], usabilityUpdate{
			Time:   api.TimeRFC3339(now),
			From:   api.ContractUsabilityBad,
			To:     api.ContractUsabilityGood,
			Reason: reason,
		})
	}
	for fcid, reason := range markedBad {
		c.updates[fcid] = append(c.updates[fcid], usabilityUpdate{
			Time:   api.TimeRFC3339(now),
			From:   api.ContractUsabilityGood,
			To:     api.ContractUsabilityBad,
			Reason: reason,
		})
	}
	var hint string
	for _, updates := range c.updates {
		if updates[len(updates)-1].To == api.ContractUsabilityBad {
			hint = "High usability churn can lead to a lot of unnecessary migrations, it might be necessary to tweak your configuration depending on the reason hosts are being discarded."
		}
	}

	return alerts.Alert{
		ID:       alertContractUsabilityUpdated,
		Severity: alerts.SeverityInfo,
		Message:  "Contract usability updated",
		Data: map[string]interface{}{
			"updates": c.updates,
			"hint":    hint,
		},
		Timestamp: now,
	}
}

func (c *accumulatedChurn) Reset() {
	c.updates = make(map[types.FileContractID][]usabilityUpdate)
}
