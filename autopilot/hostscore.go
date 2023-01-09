package autopilot

import (
	"math"
	"math/big"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/siad/build"
	"lukechampine.com/frand"
)

const (
	// maxBaseRPCPriceVsBandwidth is the max ratio for sane pricing between the
	// MinBaseRPCPrice and the MinDownloadBandwidthPrice. This ensures that 1
	// million base RPC charges are at most 1% of the cost to download 4TB. This
	// ratio should be used by checking that the MinBaseRPCPrice is less than or
	// equal to the MinDownloadBandwidthPrice multiplied by this constant
	maxBaseRPCPriceVsBandwidth = uint64(40e3)

	// maxSectorAccessPriceVsBandwidth is the max ratio for sane pricing between
	// the MinSectorAccessPrice and the MinDownloadBandwidthPrice. This ensures
	// that 1 million base accesses are at most 10% of the cost to download 4TB.
	// This ratio should be used by checking that the MinSectorAccessPrice is
	// less than or equal to the MinDownloadBandwidthPrice multiplied by this
	// constant
	maxSectorAccessPriceVsBandwidth = uint64(400e3)
)

func hostScore(cfg api.AutopilotConfig, h Host) float64 {
	// TODO: priceAdjustmentScore
	// TODO: storageRemainingScore
	return ageScore(h) *
		collateralScore(cfg, h) *
		interactionScore(h) *
		uptimeScore(h) *
		versionScore(h)
}

func ageScore(h Host) float64 {
	// sanity check
	if h.KnownSince.IsZero() {
		return 0
	}

	const day = 24 * time.Hour
	weights := []struct {
		age    time.Duration
		factor float64
	}{
		{128 * day, 1.5},
		{64 * day, 2},
		{32 * day, 2},
		{16 * day, 2},
		{8 * day, 3},
		{4 * day, 3},
		{2 * day, 3},
		{1 * day, 3},
	}

	age := time.Since(h.KnownSince)
	weight := 1.0
	for _, w := range weights {
		if age >= w.age {
			break
		}
		weight /= w.factor
	}

	return weight
}

func collateralScore(cfg api.AutopilotConfig, h Host) float64 {
	// sanity check - host should have been filtered
	settings := h.Settings
	if settings == nil {
		return 0
	}

	// NOTE: This math is copied directly from the old siad hostdb. It would
	// probably benefit from a thorough review.

	// NOTE: We are not multiplying the `Storage` with redundancy, which is
	// different from the siad implementation

	// convenience variables
	allowance := cfg.Contracts.Allowance
	numhosts := cfg.Contracts.Hosts
	duration := cfg.Contracts.Period
	storage := cfg.Contracts.Storage

	// calculate the collateral
	contractCollateral := settings.Collateral.Mul64(storage).Mul64(duration)
	contractCollateralMax := settings.MaxCollateral.Div64(2) // 2x buffer - renter may end up storing extra data
	if contractCollateral.Cmp(contractCollateralMax) > 0 {
		contractCollateral = contractCollateralMax
	}
	collateral, _ := new(big.Rat).SetInt(contractCollateral.Big()).Float64()
	collateral = math.Max(1, collateral) // ensure collateral is at least 1

	// calculate the cutoff
	expectedFundsPerHost := allowance.Div64(numhosts)
	cutoff, _ := new(big.Rat).SetInt(expectedFundsPerHost.Div64(5).Big()).Float64()
	cutoff = math.Max(1, cutoff)          // ensure cutoff is at least 1
	cutoff = math.Min(cutoff, collateral) // ensure cutoff is not greater than collateral

	// calculate the weight
	ratio := collateral / cutoff
	smallWeight := math.Pow(cutoff, 4)
	largeWeight := math.Pow(ratio, 0.5)
	weight := smallWeight * largeWeight
	return weight
}

func interactionScore(h Host) float64 {
	success, fail := 30.0, 1.0
	success += h.Interactions.SuccessfulInteractions
	fail += h.Interactions.FailedInteractions
	return math.Pow(success/(success+fail), 10)
}

func uptimeScore(h Host) float64 {
	prevScanSuccess := h.Interactions.PreviousScanSuccess
	lastScanSuccess := h.Interactions.LastScanSuccess
	uptime := h.Interactions.Uptime
	downtime := h.Interactions.Downtime
	totalScans := h.Interactions.TotalScans

	// special cases
	switch totalScans {
	case 0:
		return 0.25 // no scans yet
	case 1:
		if lastScanSuccess {
			return 0.75 // 1 successful scan
		} else {
			return 0.25 // 1 failed scan
		}
	case 2:
		if lastScanSuccess && prevScanSuccess {
			return 0.85
		} else if lastScanSuccess || prevScanSuccess {
			return 0.5
		} else {
			return 0.05
		}
	}

	// account for the interval between the most recent interaction and the
	// current time
	finalInterval := time.Since(h.Interactions.LastScan)
	if lastScanSuccess {
		uptime += finalInterval
	} else {
		downtime += finalInterval
	}
	ratio := float64(uptime) / float64(uptime+downtime)

	// unconditionally forgive up to 2% downtime
	if ratio >= 0.98 {
		ratio = 1
	}

	// forgive downtime inversely proportional to the number of interactions;
	// e.g. if we have only interacted 4 times, and half of the interactions
	// failed, assume a ratio of 88% rather than 50%
	ratio = math.Max(ratio, 1-(0.03*float64(totalScans)))

	// Calculate the penalty for poor uptime. Penalties increase extremely
	// quickly as uptime falls away from 95%.
	return math.Pow(ratio, 200*math.Min(1-ratio, 0.30))
}

func versionScore(h Host) float64 {
	// sanity check - host should have been filtered
	settings := h.Settings
	if settings == nil {
		return 0
	}

	versions := []struct {
		version string
		penalty float64
	}{
		{"1.5.10", 1.0},
		{"1.5.9", 0.99},
		{"1.5.8", 0.99},
		{"1.5.6", 0.90},
	}
	weight := 1.0
	for _, v := range versions {
		if build.VersionCmp(settings.Version, v.version) < 0 {
			weight *= v.penalty
		}
	}
	return weight
}

func randSelectByWeight(weights []float64) int {
	// deep copy the input
	weights = append([]float64{}, weights...)

	// normalize
	var total float64
	for _, w := range weights {
		total += w
	}
	for i, w := range weights {
		weights[i] = w / total
	}

	// select
	r := frand.Float64()
	var sum float64
	for i, w := range weights {
		sum += w
		if r < sum {
			return i
		}
	}
	return len(weights) - 1
}
