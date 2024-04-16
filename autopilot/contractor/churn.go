package contractor

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
)

type (
	accumulatedChurn struct {
		additions map[types.FileContractID]contractSetAdditions
		removals  map[types.FileContractID]contractSetRemovals
	}
)

func newAccumulatedChurn() *accumulatedChurn {
	return &accumulatedChurn{
		additions: make(map[types.FileContractID]contractSetAdditions),
		removals:  make(map[types.FileContractID]contractSetRemovals),
	}
}

func (c *accumulatedChurn) Alert(name string) alerts.Alert {
	var hint string
	if len(c.removals) > 0 {
		hint = "A high churn rate can lead to a lot of unnecessary migrations, it might be necessary to tweak your configuration depending on the reason hosts are being discarded from the set."
	}

	return alerts.Alert{
		ID:       alertChurnID,
		Severity: alerts.SeverityInfo,
		Message:  "Contract set changed",
		Data: map[string]any{
			"name":         name,
			"setAdditions": c.additions,
			"setRemovals":  c.removals,
			"hint":         hint,
		},
		Timestamp: time.Now(),
	}
}

func (c *accumulatedChurn) Apply(additions map[types.FileContractID]contractSetAdditions, removals map[types.FileContractID]contractSetRemovals) {
	for fcid, a := range additions {
		if _, exists := c.additions[fcid]; !exists {
			c.additions[fcid] = a
		} else {
			additions := c.additions[fcid]
			additions.Additions = append(additions.Additions, a.Additions...)
			c.additions[fcid] = additions
		}
	}
	for fcid, r := range removals {
		if _, exists := c.removals[fcid]; !exists {
			c.removals[fcid] = r
		} else {
			removals := c.removals[fcid]
			removals.Removals = append(removals.Removals, r.Removals...)
			c.removals[fcid] = removals
		}
	}
}

func (c *accumulatedChurn) Reset() {
	c.additions = make(map[types.FileContractID]contractSetAdditions)
	c.removals = make(map[types.FileContractID]contractSetRemovals)
}
