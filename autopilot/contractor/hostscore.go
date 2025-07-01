package contractor

import (
	"math"
	"math/big"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

const (
	// minValidScore is the smallest score that a host can have before
	// being ignored.
	minValidScore = math.SmallestNonzeroFloat64

	// minSubScore is the smallest score that a host can have for a given
	// sub-score such as its pricing. By choosing 0.1, a host that has a very bad
	// price score but is otherwise perfect can at most be 90% less likely to be
	// picked than a host that has a perfect score.
	minSubScore = 0.1
)

// clampScore makes sure that a score can not be smaller than 'minSubScore'.
// Unless the score is 0, which indicates a more severe issue with the host. By
// doing so, we limit the impact of a single sub-score on the overall score of a
// host.
func clampScore(score float64) float64 {
	if score <= 0.0 {
		return 0.0
	} else if score < minSubScore {
		return minSubScore
	} else if score > 1.0 {
		return 1.0
	}
	return score
}

func hostScore(cfg api.AutopilotConfig, gs api.GougingSettings, h api.Host, expectedRedundancy float64) (sb api.HostScoreBreakdown) {
	cCfg := cfg.Contracts

	// idealDataPerHost is the amount of data that we would have to put on each
	// host assuming that our storage requirements were spread evenly across
	// every single host.
	idealDataPerHost := float64(cCfg.Storage) * expectedRedundancy / float64(cCfg.Amount)

	// allocationPerHost is the amount of data that we would like to be able to
	// put on each host, because data is not always spread evenly across the
	// hosts during upload. Slower hosts may get very little data, more
	// expensive hosts may get very little data, and other factors can skew the
	// distribution. allocationPerHost takes into account the skew and tries to
	// ensure that there's enough allocation per host to accommodate for a skew.
	// NOTE: assume that data is not spread evenly and the host with the most
	// data will store twice the expectation
	allocationPerHost := idealDataPerHost * 2

	// extract inputs from the host depending on whether the host supports v1 or v2
	appendCost := h.V2Settings.Prices.RPCAppendSectorsCost(1, cfg.Contracts.Period).RenterCost()
	uploadCost := h.V2Settings.Prices.RPCWriteSectorCost(rhpv4.SectorSize).RenterCost()
	uploadSectorCost := appendCost.Add(uploadCost)
	maxCollateral := h.V2Settings.MaxCollateral
	collateral := h.V2Settings.Prices.Collateral
	egressPrice := h.V2Settings.Prices.RPCReadSectorCost(rhpv4.SectorSize).RenterCost().Div64(rhpv4.SectorSize)
	ingressPrice := h.V2Settings.Prices.RPCWriteSectorCost(rhpv4.SectorSize).RenterCost().Div64(rhpv4.SectorSize)
	storagePrice := h.V2Settings.Prices.RPCAppendSectorsCost(1, cfg.Contracts.Period).RenterCost().Div64(rhpv4.SectorSize)
	remainingStorage := h.V2Settings.RemainingStorage * rhpv4.SectorSize
	version := 1.0 // v2 only has one version

	return api.HostScoreBreakdown{
		Age:              ageScore(h), // not clamped since values are hardcoded
		Collateral:       clampScore(collateralScore(uploadSectorCost, maxCollateral, collateral, uint64(allocationPerHost), cCfg.Period)),
		Interactions:     clampScore(interactionScore(h)),
		Prices:           clampScore(priceAdjustmentScore(egressPrice, ingressPrice, storagePrice, gs)),
		StorageRemaining: clampScore(storageRemainingScore(remainingStorage, h.StoredData, allocationPerHost)),
		Uptime:           clampScore(uptimeScore(h)),
		Version:          version,
	}
}

// priceAdjustmentScore computes a score between 0 and 1 for a host giving its
// price settings and the autopilot's configuration.
//   - If the given config is missing required fields (e.g. allowance or amount),
//     math.SmallestNonzeroFloat64 is returned.
//   - 0.5 is returned if the host's costs exactly match the settings.
//   - If the host is cheaper than expected, a linear bonus is applied. The best
//     score of 1 is reached when the ratio between host cost and expectations is
//     10x.
//   - If the host is more expensive than expected, an exponential malus is applied.
//     A 2x ratio will already cause the score to drop to 0.16 and a 3x ratio causes
//     it to drop to 0.05.
func priceAdjustmentScore(dppb, uppb, sppb types.Currency, gs api.GougingSettings) float64 {
	priceScore := func(actual, max types.Currency) float64 {
		threshold := max.Div64(2)
		if threshold.IsZero() {
			return 1.0 // no gouging settings defined
		}

		ratio := new(big.Rat).SetFrac(actual.Big(), threshold.Big())
		fRatio, _ := ratio.Float64()
		switch ratio.Cmp(new(big.Rat).SetUint64(1)) {
		case 0:
			return 0.5 // ratio is exactly 1 -> score is 0.5
		case 1:
			// actual is greater than threshold -> score is in range (0; 0.5)
			//
			return 1.5 / math.Pow(3, fRatio)
		case -1:
			// actual < threshold -> score is (0.5; 1]
			s := 0.5 * (1 / fRatio)
			if s > 1.0 {
				s = 1.0
			}
			return s
		}
		panic("unreachable")
	}

	// compute scores for download, upload and storage and combine them
	downloadScore := priceScore(dppb, gs.MaxDownloadPrice)
	uploadScore := priceScore(uppb, gs.MaxUploadPrice)
	storageScore := priceScore(sppb, gs.MaxStoragePrice)
	return (downloadScore + uploadScore + storageScore) / 3.0
}

func storageRemainingScore(remainingStorage, storedData uint64, allocationPerHost float64) float64 {
	// hostExpectedStorage is the amount of storage that we expect to be able to
	// store on this host overall, which should include the stored data that is
	// already on the host.
	hostExpectedStorage := (float64(remainingStorage) * 0.25) + float64(storedData)
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

func ageScore(h api.Host) float64 {
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

// collateralScore computes the score a host receives for its collateral
// settings relative to its prices. The params have the following meaning
// 'uploadSectorCost' - the cost of uploading and storing a sector worth of data
// 'maxCollateral' - the maximum collateral the host is willing to put up
// 'collateralCost' - the amount of collateral the host is willing to put up per byte
// 'allocationPerHost' - amount of data we expect to store on the host
// 'period' - the period for which the collateral is put up
func collateralScore(uploadSectorCost, maxCollateral, collateralCost types.Currency, allocationPerHost, period uint64) float64 {
	// Ignore hosts which have set their max collateral to 0.
	if maxCollateral.IsZero() || collateralCost.IsZero() {
		return 0
	}

	// convenience variables
	ratioNum := uint64(3)
	ratioDenom := uint64(2)

	// compute the cost of storing
	numSectors := bytesToSectors(allocationPerHost)
	storageCost := uploadSectorCost.Mul64(numSectors)

	// calculate the expected collateral for the host allocation.
	expectedCollateral := collateralCost.Mul64(allocationPerHost).Mul64(period)
	if expectedCollateral.Cmp(maxCollateral) > 0 {
		expectedCollateral = maxCollateral
	}

	// avoid division by zero
	if expectedCollateral.IsZero() {
		expectedCollateral = types.NewCurrency64(1)
	}

	// determine a cutoff at 150% of the storage cost. Meaning that a host
	// should be willing to put in at least 1.5x the amount of money the renter
	// expects to spend on storage on that host.
	cutoff := storageCost.Mul64(ratioNum).Div64(ratioDenom)

	// the score is a linear function between 0 and 1 where the upper limit is
	// 4 times the cutoff. Beyond that, we don't care if a host puts in more
	// collateral.
	cutoffMultiplier := uint64(4)

	if expectedCollateral.Cmp(cutoff) < 0 {
		return math.SmallestNonzeroFloat64 // expectedCollateral <= cutoff -> score is basically 0
	} else if expectedCollateral.Cmp(cutoff.Mul64(cutoffMultiplier)) >= 0 {
		return 1 // expectedCollateral is 10x cutoff -> score is 1
	} else {
		// Perform linear interpolation for all other values.
		slope := new(big.Rat).SetFrac(new(big.Int).SetInt64(1), cutoff.Mul64(cutoffMultiplier).Big())
		intercept := new(big.Rat).Mul(slope, new(big.Rat).SetInt(cutoff.Big())).Neg(slope)
		score := new(big.Rat).SetInt(expectedCollateral.Big())
		score = score.Mul(score, slope)
		score = score.Add(score, intercept)
		fScore, _ := score.Float64()
		if fScore > 1 {
			return 1.0
		}
		return fScore
	}
}

func interactionScore(h api.Host) float64 {
	success, fail := 30.0, 1.0
	success += h.Interactions.SuccessfulInteractions
	fail += h.Interactions.FailedInteractions
	return math.Pow(success/(success+fail), 10)
}

func uptimeScore(h api.Host) float64 {
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
			return minSubScore
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

func bytesToSectors(bytes uint64) uint64 {
	numSectors := bytes / rhpv4.SectorSize
	if bytes%rhpv4.SectorSize != 0 {
		numSectors++
	}
	return numSectors
}
