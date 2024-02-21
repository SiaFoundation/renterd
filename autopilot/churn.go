package autopilot

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
)

type (
	accumulatedChurn struct {
		additions map[types.FileContractID][]contractSetAddition
		removals  map[types.FileContractID][]contractSetRemoval
	}
)

func newAccumulatedChurn() *accumulatedChurn {
	return &accumulatedChurn{
		additions: make(map[types.FileContractID][]contractSetAddition),
		removals:  make(map[types.FileContractID][]contractSetRemoval),
	}
}

func (c *accumulatedChurn) Alert(name string) alerts.Alert {
	var hint string
	if len(c.removals) > 0 {
		hint = "A high churn rate can lead to a lot of unnecessary migrations, it might be necessary to tweak your configuration depending on the reason hosts are being discarded from the set."
	}

	removedReasons := make(map[string][]string, len(c.removals))
	for fcid, contractRemovals := range c.removals {
		for _, removal := range contractRemovals {
			removedReasons[fcid.String()] = append(removedReasons[fcid.String()], removal.Reason)
		}
	}

	return alerts.Alert{
		ID:       alertChurnID,
		Severity: alerts.SeverityInfo,
		Message:  "Contract set changed",
		Data: map[string]any{
			"name":          name,
			"set_additions": c.additions,
			"set_removals":  c.removals,
			"hint":          hint,
		},
		Timestamp: time.Now(),
	}
}

func (c *accumulatedChurn) Apply(additions map[types.FileContractID]contractSetAddition, removals map[types.FileContractID]contractSetRemoval) {
	for fcid, addition := range additions {
		c.additions[fcid] = append(c.additions[fcid], addition)
	}
	for fcid, removal := range removals {
		c.removals[fcid] = append(c.removals[fcid], removal)
	}
}

func (c *accumulatedChurn) Reset() {
	c.additions = make(map[types.FileContractID][]contractSetAddition)
	c.removals = make(map[types.FileContractID][]contractSetRemoval)
}
