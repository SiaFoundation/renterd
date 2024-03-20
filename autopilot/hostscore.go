package autopilot

import (
	"math"
	"math/big"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/siad/build"
)

const smallestValidScore = math.SmallestNonzeroFloat64

func hostScore(cfg api.AutopilotConfig, h hostdb.Host, storedData uint64, expectedRedundancy float64) api.HostScoreBreakdown {
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
	// hostPeriodCost is the amount of money we expect to spend on a host in a period.
	hostPeriodCost := hostPeriodCostForScore(h, cfg, expectedRedundancy)
	return api.HostScoreBreakdown{
		Age:              ageScore(h),
		Collateral:       collateralScore(cfg, h.PriceTable.HostPriceTable, uint64(allocationPerHost)),
		Interactions:     interactionScore(h),
		Prices:           priceAdjustmentScore(hostPeriodCost, cfg),
		StorageRemaining: storageRemainingScore(h.Settings, storedData, allocationPerHost),
		Uptime:           uptimeScore(h),
		Version:          versionScore(h.Settings),
	}
}

// priceAdjustmentScore computes a score between 0 and 1 for a host giving its
// price settings and the autopilot's configuration.
//   - 0.5 is returned if the host's costs exactly match the settings.
//   - If the host is cheaper than expected, a linear bonus is applied. The best
//     score of 1 is reached when the ratio between host cost and expectations is
//     10x.
//   - If the host is more expensive than expected, an exponential malus is applied.
//     A 2x ratio will already cause the score to drop to 0.16 and a 3x ratio causes
//     it to drop to 0.05.
func priceAdjustmentScore(hostCostPerPeriod types.Currency, cfg api.AutopilotConfig) float64 {
	hostPeriodBudget := cfg.Contracts.Allowance.Div64(cfg.Contracts.Amount)

	ratio := new(big.Rat).SetFrac(hostCostPerPeriod.Big(), hostPeriodBudget.Big())
	fRatio, _ := ratio.Float64()
	switch ratio.Cmp(new(big.Rat).SetUint64(1)) {
	case 0:
		return 0.5 // ratio is exactly 1 -> score is 0.5
	case 1:
		// cost is greater than budget -> score is in range (0; 0.5)
		//
		return 1.5 / math.Pow(3, fRatio)
	case -1:
		// cost < budget -> score is (0.5; 1]
		s := 0.44 + 0.06*(1/fRatio)
		if s > 1.0 {
			s = 1.0
		}
		return s
	}
	panic("unreachable")
}

func storageRemainingScore(h rhpv2.HostSettings, storedData uint64, allocationPerHost float64) float64 {
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

func collateralScore(cfg api.AutopilotConfig, pt rhpv3.HostPriceTable, allocationPerHost uint64) float64 {
	// Ignore hosts which have set their max collateral to 0.
	if pt.MaxCollateral.IsZero() || pt.CollateralCost.IsZero() {
		return 0
	}

	// convenience variables
	ratioNum := uint64(3)
	ratioDenom := uint64(2)

	// compute the cost of storing
	numSectors := bytesToSectors(allocationPerHost)
	storageCost := pt.AppendSectorCost(cfg.Contracts.Period).Storage.Mul64(numSectors)

	// calculate the expected collateral for the host allocation.
	expectedCollateral := pt.CollateralCost.Mul64(allocationPerHost).Mul64(cfg.Contracts.Period)
	if expectedCollateral.Cmp(pt.MaxCollateral) > 0 {
		expectedCollateral = pt.MaxCollateral
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
		{"1.6.0", 0.99},
		{"1.5.9", 0.00},
	}
	weight := 1.0
	for _, v := range versions {
		if build.VersionCmp(settings.Version, v.version) < 0 {
			weight *= v.penalty
		}
	}
	return weight
}

// contractPriceForScore returns the contract price of the host used for
// scoring. Since we don't know whether rhpv2 or rhpv3 are used, we return the
// bigger one for a pesimistic score.
func contractPriceForScore(h hostdb.Host) types.Currency {
	cp := h.Settings.ContractPrice
	if cp.Cmp(h.PriceTable.ContractPrice) > 0 {
		cp = h.PriceTable.ContractPrice
	}
	return cp
}

func bytesToSectors(bytes uint64) uint64 {
	numSectors := bytes / rhpv2.SectorSize
	if bytes%rhpv2.SectorSize != 0 {
		numSectors++
	}
	return numSectors
}

func sectorStorageCost(pt rhpv3.HostPriceTable, duration uint64) types.Currency {
	asc := pt.BaseCost().Add(pt.AppendSectorCost(duration))
	return asc.Storage
}

func sectorUploadCost(pt rhpv3.HostPriceTable, duration uint64) types.Currency {
	asc := pt.BaseCost().Add(pt.AppendSectorCost(duration))
	uploadSectorCostRHPv3, _ := asc.Total()
	return uploadSectorCostRHPv3
}

func uploadCostForScore(cfg api.AutopilotConfig, h hostdb.Host, bytes uint64) types.Currency {
	uploadSectorCostRHPv3 := sectorUploadCost(h.PriceTable.HostPriceTable, cfg.Contracts.Period)
	numSectors := bytesToSectors(bytes)
	return uploadSectorCostRHPv3.Mul64(numSectors)
}

func downloadCostForScore(h hostdb.Host, bytes uint64) types.Currency {
	rsc := h.PriceTable.BaseCost().Add(h.PriceTable.ReadSectorCost(rhpv2.SectorSize))
	downloadSectorCostRHPv3, _ := rsc.Total()
	numSectors := bytesToSectors(bytes)
	return downloadSectorCostRHPv3.Mul64(numSectors)
}

func storageCostForScore(cfg api.AutopilotConfig, h hostdb.Host, bytes uint64) types.Currency {
	storeSectorCostRHPv3 := sectorStorageCost(h.PriceTable.HostPriceTable, cfg.Contracts.Period)
	numSectors := bytesToSectors(bytes)
	return storeSectorCostRHPv3.Mul64(numSectors)
}

func hostPeriodCostForScore(h hostdb.Host, cfg api.AutopilotConfig, expectedRedundancy float64) types.Currency {
	// compute how much data we upload, download and store.
	uploadPerHost := uint64(float64(cfg.Contracts.Upload) * expectedRedundancy / float64(cfg.Contracts.Amount))
	downloadPerHost := uint64(float64(cfg.Contracts.Download) * expectedRedundancy / float64(cfg.Contracts.Amount))
	storagePerHost := uint64(float64(cfg.Contracts.Storage) * expectedRedundancy / float64(cfg.Contracts.Amount))

	// compute the individual costs.
	hostCollateral := rhpv2.ContractFormationCollateral(cfg.Contracts.Period, storagePerHost, h.Settings)
	hostContractPrice := contractPriceForScore(h)
	hostUploadCost := uploadCostForScore(cfg, h, uploadPerHost)
	hostDownloadCost := downloadCostForScore(h, downloadPerHost)
	hostStorageCost := storageCostForScore(cfg, h, storagePerHost)
	siafundFee := hostCollateral.
		Add(hostContractPrice).
		Add(hostUploadCost).
		Add(hostDownloadCost).
		Add(hostStorageCost).
		Mul64(39).
		Div64(1000)

	// add it all up. We multiply the contract price here since we might refresh
	// a contract multiple times.
	return hostContractPrice.Mul64(3).
		Add(hostUploadCost).
		Add(hostDownloadCost).
		Add(hostStorageCost).
		Add(siafundFee)
}
