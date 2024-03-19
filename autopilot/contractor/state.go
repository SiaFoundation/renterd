package contractor

import (
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

type (
	// State serves as input for the contractor's maintenance. It contains all
	// state that should remain constant across a single round of contract
	// performance.
	State struct {
		GS api.GougingSettings
		RS api.RedundancySettings
		AP api.Autopilot

		Address types.Address
		Fee     types.Currency
	}
)

func (state *State) AllowRedundantIPs() bool {
	return state.AP.Config.Hosts.AllowRedundantIPs
}

func (state *State) Allowance() types.Currency {
	return state.AP.Config.Contracts.Allowance
}

func (state *State) Config() api.AutopilotConfig {
	return state.AP.Config
}

func (state *State) ContractSet() string {
	return state.AP.Config.Contracts.Set
}

func (s *State) EndHeight() uint64 {
	return s.AP.EndHeight()
}

func (state *State) WantedContracts() uint64 {
	return state.AP.Config.Contracts.Amount
}

func (state *State) Period() uint64 {
	return state.AP.Config.Contracts.Period
}

func (state *State) RenewWindow() uint64 {
	return state.AP.Config.Contracts.RenewWindow
}
