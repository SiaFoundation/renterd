package contractor

import (
	"context"
	"errors"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
)

type (
	// MaintenanceState serves as input for the contractor's maintenance. It contains all
	// state that should remain constant across a single round of contract
	// performance.
	MaintenanceState struct {
		GS api.GougingSettings
		RS api.RedundancySettings
		AP api.AutopilotState

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

func (ctx *mCtx) AutopilotConfig() api.AutopilotConfig {
	return ctx.state.AP.AutopilotConfig
}

func (ctx *mCtx) ContractsConfig() api.ContractsConfig {
	return ctx.state.ContractsConfig()
}

func (ctx *mCtx) ContractSet() string {
	return ctx.state.ContractsConfig().Set
}

func (ctx *mCtx) Deadline() (deadline time.Time, ok bool) {
	return ctx.ctx.Deadline()
}

func (ctx *mCtx) Done() <-chan struct{} {
	return ctx.ctx.Done()
}

func (ctx *mCtx) EndHeight() uint64 {
	return ctx.state.AP.EndHeight()
}

func (ctx *mCtx) Err() error {
	return ctx.ctx.Err()
}

func (ctx *mCtx) GougingChecker(cs api.ConsensusState) gouging.Checker {
	period, renewWindow := ctx.Period(), ctx.RenewWindow()
	return gouging.NewChecker(ctx.state.GS, cs, &period, &renewWindow)
}

func (ctx *mCtx) HostScore(h api.Host) (sb api.HostScoreBreakdown, err error) {
	// host settings that cause a panic should result in a score of 0
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("panic while scoring host")
		}
	}()
	return hostScore(ctx.AutopilotConfig(), ctx.state.GS, h, ctx.state.RS.Redundancy()), nil
}

func (ctx *mCtx) Period() uint64 {
	return ctx.state.Period()
}

func (ctx *mCtx) RenewWindow() uint64 {
	return ctx.state.AP.Contracts.RenewWindow
}

func (ctx *mCtx) ShouldFilterRedundantIPs() bool {
	return !ctx.state.AP.Hosts.AllowRedundantIPs
}

func (ctx *mCtx) Value(key interface{}) interface{} {
	return ctx.ctx.Value(key)
}

func (ctx *mCtx) WantedContracts() uint64 {
	return ctx.state.AP.Contracts.Amount
}

func (ctx *mCtx) SortContractsForMaintenance(contracts []contract) {
	sortContractsForMaintenance(ctx.state.ContractsConfig(), contracts)
}

func (state *MaintenanceState) ContractsConfig() api.ContractsConfig {
	return state.AP.Contracts
}

func (state *MaintenanceState) Period() uint64 {
	return state.AP.Contracts.Period
}
