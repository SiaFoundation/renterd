package autopilot

import (
	"math"
	"math/big"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
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

func hostScore(cfg api.AutopilotConfig, h hostdb.Host, storedData uint64, expectedRedundancy float64) api.HostScoreBreakdown {
	hostPeriodCost := hostPeriodCostForScore(h, cfg, expectedRedundancy)
	return api.HostScoreBreakdown{
		Age:              ageScore(h),
		Collateral:       collateralScore(cfg, hostPeriodCost, h.Settings, expectedRedundancy),
		Interactions:     interactionScore(h),
		StorageRemaining: storageRemainingScore(cfg, h.Settings, storedData, expectedRedundancy),
		Uptime:           uptimeScore(h),
		Version:          versionScore(h.Settings),
		Prices:           priceAdjustmentScore(hostPeriodCost, cfg),
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

func collateralScore(cfg api.AutopilotConfig, hostCostPerPeriod types.Currency, s rhpv2.HostSettings, expectedRedundancy float64) float64 {
	// Ignore hosts which have set their max collateral to 0.
	if s.MaxCollateral.IsZero() || s.Collateral.IsZero() {
		return 0
	}

	// convenience variables
	duration := cfg.Contracts.Period
	storage := float64(cfg.Contracts.Storage) * expectedRedundancy

	// calculate the expected collateral
	expectedCollateral := s.Collateral.Mul64(uint64(storage)).Mul64(duration)
	expectedCollateralMax := s.MaxCollateral.Div64(2) // 2x buffer - renter may end up storing extra data
	if expectedCollateral.Cmp(expectedCollateralMax) > 0 {
		expectedCollateral = expectedCollateralMax
	}

	// determine a cutoff at 20% of the budgeted per-host funds.
	// Meaning that an 'ok' host puts in 1/5 of what the renter puts into a
	// contract. Beyond that the score increases linearly and below that
	// decreases exponentially.
	cutoff := hostCostPerPeriod.Div64(5)

	// calculate the weight. We use the same approach here as in
	// priceAdjustScore but with a different cutoff.
	ratio := new(big.Rat).SetFrac(cutoff.Big(), expectedCollateral.Big())
	fRatio, _ := ratio.Float64()
	switch ratio.Cmp(new(big.Rat).SetUint64(1)) {
	case 0:
		return 0.5 // ratio is exactly 1 -> score is 0.5
	case 1:
		// collateral is below cutoff -> score is in range (0; 0.5)
		//
		return 1.5 / math.Pow(3, fRatio)
	case -1:
		// collateral is beyond cutoff -> score is (0.5; 1]
		s := 0.5 * (1 / fRatio)
		if s > 1.0 {
			s = 1.0
		}
		return s
	}
	panic("unreachable")
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

func uploadCostForScore(cfg api.AutopilotConfig, h hostdb.Host, bytes uint64) types.Currency {
	uploadSectorCostRHPv2, _ := rhpv2.RPCAppendCost(h.Settings, cfg.Contracts.Period)

	asc := h.PriceTable.AppendSectorCost(cfg.Contracts.Period)
	uploadSectorCostRHPv3, _ := asc.Total()

	numSectors := bytesToSectors(bytes)
	if uploadSectorCostRHPv2.Cmp(uploadSectorCostRHPv3) > 0 {
		return uploadSectorCostRHPv2.Mul64(numSectors)
	}
	return uploadSectorCostRHPv3.Mul64(numSectors)
}

func downloadCostForScore(cfg api.AutopilotConfig, h hostdb.Host, bytes uint64) types.Currency {
	downloadSectorCostRHPv2 := rhpv2.RPCReadCost(h.Settings, []rhpv2.RPCReadRequestSection{{Offset: 0, Length: rhpv2.SectorSize}})
	rsc := h.PriceTable.ReadSectorCost(rhpv2.SectorSize)
	downloadSectorCostRHPv3, _ := rsc.Total()

	numSectors := bytesToSectors(bytes)
	if downloadSectorCostRHPv2.Cmp(downloadSectorCostRHPv3) > 0 {
		return downloadSectorCostRHPv2.Mul64(numSectors)
	}
	return downloadSectorCostRHPv3.Mul64(numSectors)
}

func storageCostForScore(cfg api.AutopilotConfig, h hostdb.Host, bytes uint64) types.Currency {
	storeSectorCostRHPv2 := h.Settings.StoragePrice.Mul64(bytes)

	asc := h.PriceTable.AppendSectorCost(cfg.Contracts.Period)
	storeSectorCostRHPv3 := asc.Storage

	numSectors := bytesToSectors(bytes)
	if storeSectorCostRHPv2.Cmp(storeSectorCostRHPv3) > 0 {
		return storeSectorCostRHPv2.Mul64(numSectors)
	}
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
	hostDownloadCost := downloadCostForScore(cfg, h, downloadPerHost)
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
