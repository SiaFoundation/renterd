package contractor

import (
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

func (c *Contractor) currentPeriodSpending(contracts []api.Contract, currentPeriod uint64) types.Currency {
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
	return totalAllocated
}

func (c *Contractor) remainingFunds(contracts []api.Contract, state *MaintenanceState) types.Currency {
	// find out how much we spent in the current period
	spent := c.currentPeriodSpending(contracts, state.Period())

	// figure out remaining funds
	var remaining types.Currency
	if state.Allowance().Cmp(spent) > 0 {
		remaining = state.Allowance().Sub(spent)
	}
	return remaining
}
