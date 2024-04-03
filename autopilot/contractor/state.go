package contractor

import (
	"context"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/worker"
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

	mCtx struct {
		ctx   context.Context
		state *MaintenanceState
	}
)

func newMaintenanceCtx(ctx context.Context, state *MaintenanceState) *mCtx {
	return &mCtx{
		ctx:   ctx,
		state: state,
	}
}

func (ctx *mCtx) ApID() string {
	return ctx.state.AP.ID
}

func (ctx *mCtx) Deadline() (deadline time.Time, ok bool) {
	return ctx.ctx.Deadline()
}

func (ctx *mCtx) Done() <-chan struct{} {
	return ctx.ctx.Done()
}

func (ctx *mCtx) Err() error {
	return ctx.ctx.Err()
}

func (ctx *mCtx) Value(key interface{}) interface{} {
	return ctx.ctx.Value(key)
}

func (ctx *mCtx) AllowRedundantIPs() bool {
	return ctx.state.AP.Config.Hosts.AllowRedundantIPs
}

func (ctx *mCtx) Allowance() types.Currency {
	return ctx.state.Allowance()
}

func (ctx *mCtx) AutopilotConfig() api.AutopilotConfig {
	return ctx.state.AP.Config
}

func (ctx *mCtx) ContractsConfig() api.ContractsConfig {
	return ctx.state.ContractsConfig()
}

func (ctx *mCtx) ContractSet() string {
	return ctx.state.AP.Config.Contracts.Set
}

func (ctx *mCtx) EndHeight() uint64 {
	return ctx.state.AP.EndHeight()
}

func (ctx *mCtx) GougingChecker(cs api.ConsensusState) worker.GougingChecker {
	return worker.NewGougingChecker(ctx.state.GS, cs, ctx.state.Fee, ctx.Period(), ctx.RenewWindow())
}

func (ctx *mCtx) WantedContracts() uint64 {
	return ctx.state.AP.Config.Contracts.Amount
}

func (ctx *mCtx) Period() uint64 {
	return ctx.state.Period()
}

func (ctx *mCtx) RenewWindow() uint64 {
	return ctx.state.AP.Config.Contracts.RenewWindow
}

func (state *MaintenanceState) Allowance() types.Currency {
	return state.AP.Config.Contracts.Allowance
}

func (state *MaintenanceState) ContractsConfig() api.ContractsConfig {
	return state.AP.Config.Contracts
}

func (state *MaintenanceState) Period() uint64 {
	return state.AP.Config.Contracts.Period
}
