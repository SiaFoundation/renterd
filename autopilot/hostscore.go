package autopilot

import (
	"math"
	"math/big"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
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

func hostScore(cfg api.AutopilotConfig, h hostdb.Host, storedData uint64, expectedRedundancy float64) float64 {
	// TODO: priceAdjustmentScore
	return ageScore(h) *
		collateralScore(cfg, *h.Settings, expectedRedundancy) *
		interactionScore(h) *
		storageRemainingScore(cfg, *h.Settings, 0, 3) *
		uptimeScore(h) *
		versionScore(*h.Settings)
}

func storageRemainingScore(cfg api.AutopilotConfig, h rhpv2.HostSettings, storedData uint64, expectedRedundancy float64) float64 {
	// idealDataPerHost is the amount of data that we would have to put on each
	// host assuming that our storage requirements were spread evenly across
	// every single host.
	idealDataPerHost := float64(cfg.Contracts.Storage) * expectedRedundancy / float64(cfg.Contracts.Amount)
	// allocationPerHost is the amount of data that we would like to be able to
	// put on each host, because data is not always spread evenly across the
	// hosts during upload. Slower hosts may get very little data, more
	// expensive hosts may get very little data, and other factors can skew the
	// distribution. allocationPerHost takes into account the skew and tries to
	// ensure that there's enough allocation per host to accommodate for a skew.
	// NOTE: assume that data is not spread evenly and the host with the most
	// data will store twice the expectation
	allocationPerHost := idealDataPerHost * 2
	// hostExpectedStorage is the amount of storage that we expect to be able to
	// store on this host overall, which should include the stored data that is
	// already on the host.
	hostExpectedStorage := (float64(h.RemainingStorage) * 0.25) + float64(storedData)
	// The score for the host is the square of the amount of storage we
	// expected divided by the amount of storage we want. If we expect to be
	// able to store more data on the host than we need to allocate, the host
	// gets full score for storage.
	if hostExpectedStorage >= allocationPerHost {
		return 1
	}
	// Otherwise, the score of the host is the fraction of the data we expect
	// raised to the storage penalty exponentiation.
	storageRatio := hostExpectedStorage / allocationPerHost
	return math.Pow(storageRatio, 2.0)
}

func ageScore(h hostdb.Host) float64 {
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

func collateralScore(cfg api.AutopilotConfig, s rhpv2.HostSettings, expectedRedundancy float64) float64 {
	// Ignore hosts which have set their max collateral too low.
	if s.MaxCollateral.IsZero() || s.MaxCollateral.Cmp(cfg.Hosts.MinMaxCollateral) < 0 {
		return 0
	}

	// NOTE: This math is copied directly from the old siad hostdb. It would
	// probably benefit from a thorough review.

	// convenience variables
	allowance := cfg.Contracts.Allowance
	numContracts := cfg.Contracts.Amount
	duration := cfg.Contracts.Period
	storage := float64(cfg.Contracts.Storage) * expectedRedundancy

	// calculate the collateral
	contractCollateral := s.Collateral.Mul64(uint64(storage)).Mul64(duration)
	contractCollateralMax := s.MaxCollateral.Div64(2) // 2x buffer - renter may end up storing extra data
	if contractCollateral.Cmp(contractCollateralMax) > 0 {
		contractCollateral = contractCollateralMax
	}
	collateral, _ := new(big.Rat).SetInt(contractCollateral.Big()).Float64()
	collateral = math.Max(1, collateral) // ensure collateral is at least 1

	// calculate the cutoff
	expectedFundsPerHost := allowance.Div64(numContracts)
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

func interactionScore(h hostdb.Host) float64 {
	success, fail := 30.0, 1.0
	success += h.Interactions.SuccessfulInteractions
	fail += h.Interactions.FailedInteractions
	return math.Pow(success/(success+fail), 10)
}

func uptimeScore(h hostdb.Host) float64 {
	secondToLastScanSuccess := h.Interactions.SecondToLastScanSuccess
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
		if lastScanSuccess && secondToLastScanSuccess {
			return 0.85
		} else if lastScanSuccess || secondToLastScanSuccess {
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

func versionScore(settings rhpv2.HostSettings) float64 {
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
