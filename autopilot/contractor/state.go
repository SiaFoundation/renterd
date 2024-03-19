package contractor

import (
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

type (
	// MaintenanceState serves as input for the contractor's maintenance. It contains all
	// state that should remain constant across a single round of contract
	// performance.
	MaintenanceState struct {
		GS api.GougingSettings
		RS api.RedundancySettings
		AP api.Autopilot

		Address                types.Address
		Fee                    types.Currency
		SkipContractFormations bool
	}
)

func (state *MaintenanceState) AllowRedundantIPs() bool {
	return state.AP.Config.Hosts.AllowRedundantIPs
}

func (state *MaintenanceState) Allowance() types.Currency {
	return state.AP.Config.Contracts.Allowance
}

func (state *MaintenanceState) AutopilotConfig() api.AutopilotConfig {
	return state.AP.Config
}

func (state *MaintenanceState) ContractsConfig() api.ContractsConfig {
	return state.AP.Config.Contracts
}

func (state *MaintenanceState) ContractSet() string {
	return state.AP.Config.Contracts.Set
}

func (state *MaintenanceState) EndHeight() uint64 {
	return state.AP.EndHeight()
}

func (state *MaintenanceState) WantedContracts() uint64 {
	return state.AP.Config.Contracts.Amount
}

func (state *MaintenanceState) Period() uint64 {
	return state.AP.Config.Contracts.Period
}

func (state *MaintenanceState) RenewWindow() uint64 {
	return state.AP.Config.Contracts.RenewWindow
}
