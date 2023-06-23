package autopilot

import (
	"context"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

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

func (c *contractor) currentPeriodSpending(contracts []api.Contract, currentPeriod uint64) (types.Currency, error) {
	totalCosts := make(map[types.FileContractID]types.Currency)
	for _, c := range contracts {
		totalCosts[c.ID] = c.TotalCost
	}

	// filter contracts in the current period
	var filtered []api.ContractMetadata
	c.mu.Lock()
	for _, contract := range contracts {
		if contract.WindowStart <= currentPeriod {
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
	state := c.ap.State()

	// find out how much we spent in the current period
	spent, err := c.currentPeriodSpending(contracts, state.period)
	if err != nil {
		return types.ZeroCurrency, err
	}

	// figure out remaining funds
	var remaining types.Currency
	if state.cfg.Contracts.Allowance.Cmp(spent) > 0 {
		remaining = state.cfg.Contracts.Allowance.Sub(spent)
	}
	return remaining, nil
}
