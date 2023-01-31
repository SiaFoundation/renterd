package autopilot

import (
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

func (c *contractor) currentPeriod() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currPeriod
}

func (c *contractor) updateCurrentPeriod(cfg api.AutopilotConfig, cs api.ConsensusState) {
	c.mu.Lock()
	defer func(prevPeriod uint64) {
		if c.currPeriod != prevPeriod {
			c.logger.Debugf("updated current period, %d->%d", prevPeriod, c.currPeriod)
		}
		c.mu.Unlock()
	}(c.currPeriod)

	if c.currPeriod == 0 {
		c.currPeriod = cs.BlockHeight
	} else if cs.BlockHeight >= c.currPeriod+cfg.Contracts.Period {
		c.currPeriod += cfg.Contracts.Period
	}
}

func (c *contractor) contractSpending(contract api.Contract, currentPeriod uint64) (api.ContractSpending, error) {
	ancestors, err := c.ap.bus.AncestorContracts(contract.ID, currentPeriod)
	if err != nil {
		return api.ContractSpending{}, err
	}
	// compute total spending
	total := contract.Spending
	for _, ancestor := range ancestors {
		total = total.Add(ancestor.Spending)
	}
	return total, nil
}

func (c *contractor) currentPeriodSpending(contracts []api.Contract) (types.Currency, error) {
	totalCosts := make(map[types.FileContractID]types.Currency)
	for _, c := range contracts {
		totalCosts[c.ID] = c.TotalCost
	}

	// filter contracts in the current period
	var filtered []api.Contract
	c.mu.Lock()
	for _, rev := range contracts {
		if rev.EndHeight() <= c.currPeriod {
			filtered = append(filtered, rev)
		}
	}
	c.mu.Unlock()

	// calculate the money spent
	var spent types.Currency
	for _, rev := range filtered {
		remaining := rev.RenterFunds()
		totalCost := totalCosts[rev.ID]
		if remaining.Cmp(totalCost) <= 0 {
			spent = spent.Add(totalCost.Sub(remaining))
		} else {
			c.logger.DPanicw("sanity check failed", "remaining", remaining, "totalcost", totalCost)
		}
	}

	return spent, nil
}

func (c *contractor) remainingFunds(cfg api.AutopilotConfig, contracts []api.Contract) (types.Currency, error) {
	// find out how much we spent in the current period
	spent, err := c.currentPeriodSpending(contracts)
	if err != nil {
		return types.ZeroCurrency, err
	}

	// figure out remaining funds
	var remaining types.Currency
	if cfg.Contracts.Allowance.Cmp(spent) > 0 {
		remaining = cfg.Contracts.Allowance.Sub(spent)
	}
	return remaining, nil
}
