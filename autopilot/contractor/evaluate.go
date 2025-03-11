package contractor

import (
	"errors"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/gouging"
)

var ErrMissingRequiredFields = errors.New("missing required fields in configuration, amount must be set")

func countUsableHosts(cfg api.AutopilotConfig, cs api.ConsensusState, period uint64, rs api.RedundancySettings, gs api.GougingSettings, hosts []api.Host) (usables uint64) {
	gc := gouging.NewChecker(gs, cs)
	for _, host := range hosts {
		hc := checkHost(gc, scoreHost(host, cfg, gs, rs.Redundancy()), minValidScore, period)
		if hc.UsabilityBreakdown.IsUsable() {
			usables++
		}
	}
	return
}

// EvaluateConfig evaluates the given configuration and if the gouging settings
// are too strict for the number of contracts required by 'cfg', it will provide
// a recommendation on how to loosen it.
func EvaluateConfig(cfg api.AutopilotConfig, cs api.ConsensusState, rs api.RedundancySettings, gs api.GougingSettings, hosts []api.Host) (resp api.ConfigEvaluationResponse, _ error) {
	// we need an amount of contracts to evaluate
	if cfg.Contracts.Amount == 0 {
		return api.ConfigEvaluationResponse{}, ErrMissingRequiredFields
	}

	period := cfg.Contracts.Period
	gc := gouging.NewChecker(gs, cs)

	resp.Hosts = uint64(len(hosts))
	for i := range hosts {
		// ignore block height
		hosts[i].PriceTable.HostBlockHeight = cs.BlockHeight
		hosts[i].V2Settings.Prices.TipHeight = cs.BlockHeight
		hc := checkHost(gc, scoreHost(hosts[i], cfg, gs, rs.Redundancy()), minValidScore, cfg.Contracts.Period)
		if hc.UsabilityBreakdown.IsUsable() {
			resp.Usable++
			continue
		}
		if hc.UsabilityBreakdown.Blocked {
			resp.Unusable.Blocked++
		}
		if hc.UsabilityBreakdown.LowMaxDuration {
			resp.Unusable.LowMaxDuration++
		}
		if hc.UsabilityBreakdown.NotAcceptingContracts {
			resp.Unusable.NotAcceptingContracts++
		}
		if hc.UsabilityBreakdown.NotCompletingScan {
			resp.Unusable.NotScanned++
		}
		if hc.GougingBreakdown.DownloadErr != "" {
			resp.Unusable.Gouging.Download++
		}
		if hc.GougingBreakdown.GougingErr != "" {
			resp.Unusable.Gouging.Gouging++
		}
		if hc.GougingBreakdown.PruneErr != "" {
			resp.Unusable.Gouging.Pruning++
		}
		if hc.GougingBreakdown.UploadErr != "" {
			resp.Unusable.Gouging.Upload++
		}
	}

	if resp.Usable >= cfg.Contracts.Amount {
		return // no recommendation needed
	}

	// optimise gouging settings
	maxGS := func() api.GougingSettings {
		return api.GougingSettings{
			// these are the fields we optimise one-by-one
			MaxRPCPrice:      types.MaxCurrency,
			MaxContractPrice: types.MaxCurrency,
			MaxDownloadPrice: types.MaxCurrency,
			MaxUploadPrice:   types.MaxCurrency,
			MaxStoragePrice:  types.MaxCurrency,

			// these are not optimised, so we keep the same values as the user
			// provided
			HostBlockHeightLeeway:         gs.HostBlockHeightLeeway,
			MinPriceTableValidity:         gs.MinPriceTableValidity,
			MinAccountExpiry:              gs.MinAccountExpiry,
			MinMaxEphemeralAccountBalance: gs.MinMaxEphemeralAccountBalance,
		}
	}

	// use the input gouging settings as the starting point and try to optimise
	// each field independent of the other fields we want to optimise
	optimisedGS := gs
	success := false

	// MaxRPCPrice
	tmpGS := maxGS()
	tmpGS.MaxRPCPrice = gs.MaxRPCPrice
	if optimiseGougingSetting(&tmpGS, &tmpGS.MaxRPCPrice, cfg, cs, period, rs, hosts) {
		optimisedGS.MaxRPCPrice = tmpGS.MaxRPCPrice
		success = true
	}
	// MaxContractPrice
	tmpGS = maxGS()
	tmpGS.MaxContractPrice = gs.MaxContractPrice
	if optimiseGougingSetting(&tmpGS, &tmpGS.MaxContractPrice, cfg, cs, period, rs, hosts) {
		optimisedGS.MaxContractPrice = tmpGS.MaxContractPrice
		success = true
	}
	// MaxDownloadPrice
	tmpGS = maxGS()
	tmpGS.MaxDownloadPrice = gs.MaxDownloadPrice
	if optimiseGougingSetting(&tmpGS, &tmpGS.MaxDownloadPrice, cfg, cs, period, rs, hosts) {
		optimisedGS.MaxDownloadPrice = tmpGS.MaxDownloadPrice
		success = true
	}
	// MaxUploadPrice
	tmpGS = maxGS()
	tmpGS.MaxUploadPrice = gs.MaxUploadPrice
	if optimiseGougingSetting(&tmpGS, &tmpGS.MaxUploadPrice, cfg, cs, period, rs, hosts) {
		optimisedGS.MaxUploadPrice = tmpGS.MaxUploadPrice
		success = true
	}
	// MaxStoragePrice
	tmpGS = maxGS()
	tmpGS.MaxStoragePrice = gs.MaxStoragePrice
	if optimiseGougingSetting(&tmpGS, &tmpGS.MaxStoragePrice, cfg, cs, period, rs, hosts) {
		optimisedGS.MaxStoragePrice = tmpGS.MaxStoragePrice
		success = true
	}
	// If one of the optimisations was successful, we return the optimised
	// gouging settings
	if success {
		resp.Recommendation = &api.ConfigRecommendation{
			GougingSettings: optimisedGS,
		}
	}
	return
}

// optimiseGougingSetting tries to optimise one field of the gouging settings to
// try and hit the target number of contracts.
func optimiseGougingSetting(gs *api.GougingSettings, field *types.Currency, cfg api.AutopilotConfig, cs api.ConsensusState, period uint64, rs api.RedundancySettings, hosts []api.Host) bool {
	if cfg.Contracts.Amount == 0 {
		return true // nothing to do
	}
	stepSize := []uint64{200, 150, 125, 110, 105}
	maxSteps := 12

	stepIdx := 0
	nSteps := 0
	prevVal := *field // to keep accurate value
	for {
		nUsable := countUsableHosts(cfg, cs, period, rs, *gs, hosts)
		targetHit := nUsable >= cfg.Contracts.Amount

		if targetHit && nSteps == 0 {
			return true // target already hit without optimising
		} else if targetHit && stepIdx == len(stepSize)-1 {
			return true // target hit after optimising
		} else if targetHit {
			// move one step back and decrease step size
			stepIdx++
			nSteps--
			*field = prevVal
		} else if nSteps >= maxSteps {
			return false // ran out of steps
		}

		// apply next step
		prevVal = *field
		newValue, overflow := prevVal.Mul64WithOverflow(stepSize[stepIdx])
		if overflow {
			return false
		}
		newValue = newValue.Div64(100)
		*field = newValue
		nSteps++
	}
}
