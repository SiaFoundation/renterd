package contractor

import (
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

func currentPeriodSpending(contracts []api.ContractMetadata, currentPeriod uint64) types.Currency {
	totalCosts := make(map[types.FileContractID]types.Currency)
	for _, c := range contracts {
		totalCosts[c.ID] = c.TotalCost
	}

	// filter contracts in the current period
	var filtered []api.ContractMetadata
	for _, contract := range contracts {
		if contract.WindowStart <= currentPeriod {
			filtered = append(filtered, contract)
		}
	}

	// calculate the money allocated
	var totalAllocated types.Currency
	for _, contract := range filtered {
		totalAllocated = totalAllocated.Add(contract.TotalCost)
	}
	return totalAllocated
}

func remainingFunds(contracts []api.ContractMetadata, state *MaintenanceState) types.Currency {
	// find out how much we spent in the current period
	spent := currentPeriodSpending(contracts, state.Period())

	// figure out remaining funds
	var remaining types.Currency
	if state.Allowance().Cmp(spent) > 0 {
		remaining = state.Allowance().Sub(spent)
	}
	return remaining
}
