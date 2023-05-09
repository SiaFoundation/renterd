package autopilot

import (
	"context"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

func (c *contractor) currentPeriod() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currPeriod
}

func (c *contractor) updateCurrentPeriod() {
	c.mu.Lock()
	defer func(prevPeriod uint64) {
		if c.currPeriod != prevPeriod {
			c.logger.Debugf("updated current period, %d->%d", prevPeriod, c.currPeriod)
		}
		c.mu.Unlock()
	}(c.currPeriod)

	cfg := c.ap.state.cfg
	cs := c.ap.state.cs

	if c.currPeriod == 0 {
		c.currPeriod = cs.BlockHeight
	} else if cs.BlockHeight >= c.currPeriod+cfg.Contracts.Period {
		c.currPeriod += cfg.Contracts.Period
	}
}

func (c *contractor) contractSpending(ctx context.Context, contract api.Contract, currentPeriod uint64) (api.ContractSpending, error) {
	ancestors, err := c.ap.bus.AncestorContracts(ctx, contract.ID, currentPeriod)
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
	var filtered []api.ContractMetadata
	c.mu.Lock()
	for _, contract := range contracts {
		if contract.WindowStart <= c.currPeriod {
			filtered = append(filtered, contract.ContractMetadata)
		}
	}
	c.mu.Unlock()

	// calculate the money allocated
	var totalAllocated types.Currency
	for _, contract := range filtered {
		totalAllocated = totalAllocated.Add(contract.TotalCost)
	}
	return totalAllocated, nil
}

func (c *contractor) remainingFunds(contracts []api.Contract) (types.Currency, error) {
	cfg := c.ap.state.cfg

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
