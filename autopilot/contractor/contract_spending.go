package contractor

import (
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

func currentPeriodSpending(contracts []api.ContractMetadata, currentPeriod uint64) types.Currency {
	totalCosts := make(map[types.FileContractID]types.Currency)
	for _, c := range contracts {
		totalCosts[c.ID] = c.InitialRenterFunds
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
		totalAllocated = totalAllocated.Add(contract.InitialRenterFunds)
	}
	return totalAllocated
}
