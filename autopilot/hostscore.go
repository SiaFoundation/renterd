package autopilot

import (
	"math"
	"math/big"
	"sort"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/siad/build"
	"go.sia.tech/siad/types"
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

type hostScore struct {
	host  host
	score float64
}

func newHostScore(h host) *hostScore {
	return &hostScore{
		host:  h,
		score: 1,
	}
}

func (s *hostScore) finalize() float64 {
	return s.score
}

func (s *hostScore) withAgeScore() *hostScore {
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

	age := time.Since(s.host.Announcements[0].Timestamp)
	weight := 1.0
	for _, w := range weights {
		if age >= w.age {
			break
		}
		weight /= w.factor
	}

	s.score *= weight
	return s
}

func (s *hostScore) withCollateralScore(cfg Config) *hostScore {
	settings, _, ok := s.host.LastKnownSettings()
	if !ok {
		s.score *= math.SmallestNonzeroFloat64
		return s
	}

	// NOTE: This math is copied directly from the old siad hostdb. It would
	// probably benefit from a thorough review.

	var fundsPerHost types.Currency = cfg.Contracts.Allowance.Div64(cfg.Contracts.Hosts)
	var storage, duration uint64 // TODO

	contractCollateral := settings.Collateral.Mul64(storage).Mul64(duration)
	if maxCollateral := settings.MaxCollateral.Div64(2); contractCollateral.Cmp(maxCollateral) > 0 {
		contractCollateral = maxCollateral
	}
	collateral, _ := new(big.Rat).SetInt(contractCollateral.Big()).Float64()
	cutoff, _ := new(big.Rat).SetInt(fundsPerHost.Div64(5).Big()).Float64()
	collateral = math.Max(1, collateral)               // ensure 1 <= collateral
	cutoff = math.Min(math.Max(1, cutoff), collateral) // ensure 1 <= cutoff <= collateral

	weight := math.Pow(cutoff, 4) * math.Pow(collateral/cutoff, 0.5)
	s.score *= weight
	return s
}

func (s *hostScore) withInteractionScore() *hostScore {
	success, fail := 30.0, 1.0
	for _, hi := range s.host.Interactions {
		if hi.Success {
			success++
		} else {
			fail++
		}
	}

	weight := math.Pow(success/(success+fail), 10)
	s.score *= weight
	return s
}

func (s *hostScore) withSettingsScore(cfg Config) *hostScore {
	settings, _, found := s.host.LastKnownSettings()
	if !found {
		s.score *= math.SmallestNonzeroFloat64
		return s
	}

	maxBaseRPCPrice := settings.DownloadBandwidthPrice.Mul64(maxBaseRPCPriceVsBandwidth)
	maxSectorAccessPrice := settings.DownloadBandwidthPrice.Mul64(maxSectorAccessPriceVsBandwidth)

	if !settings.AcceptingContracts ||
		cfg.Contracts.Period+cfg.Contracts.RenewWindow > settings.MaxDuration ||
		settings.BaseRPCPrice.Cmp(maxBaseRPCPrice) > 0 ||
		settings.SectorAccessPrice.Cmp(maxSectorAccessPrice) > 0 {
		s.score *= math.SmallestNonzeroFloat64
		return s
	}

	return s
}

func (s *hostScore) withUptimeScore() *hostScore {
	sorted := append([]hostdb.Interaction(nil), s.host.Interactions...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.Before(sorted[j].Timestamp)
	})

	// special cases
	switch len(sorted) {
	case 0:
		s.score *= 0.25
		return s
	case 1:
		if sorted[0].Success {
			s.score *= 0.75
			return s
		}
		s.score *= 0.25
		return s
	case 2:
		if sorted[0].Success && sorted[1].Success {
			s.score *= 0.85
			return s
		} else if sorted[0].Success || sorted[1].Success {
			s.score *= 0.5
			return s
		}
		s.score *= 0.05
		return s
	default:
	}

	// compute the total uptime and downtime by assuming that a host's
	// online/offline state persists in the interval between each interaction
	var downtime, uptime time.Duration
	for i := 1; i < len(sorted); i++ {
		prev, cur := sorted[i-1], sorted[i]
		interval := cur.Timestamp.Sub(prev.Timestamp)
		if prev.Success {
			uptime += interval
		} else {
			downtime += interval
		}
	}
	// account for the interval between the most recent interaction and the
	// current time
	finalInterval := time.Since(sorted[len(sorted)-1].Timestamp)
	if sorted[len(sorted)-1].Success {
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
	ratio = math.Max(ratio, 1-(0.03*float64(len(sorted))))

	// Calculate the penalty for poor uptime. Penalties increase extremely
	// quickly as uptime falls away from 95%.
	weight := math.Pow(ratio, 200*math.Min(1-ratio, 0.30))
	s.score *= weight
	return s
}

func (s *hostScore) withVersionScore() *hostScore {
	settings, _, ok := s.host.LastKnownSettings()
	if !ok {
		s.score *= math.SmallestNonzeroFloat64
		return s
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
	s.score *= weight
	return s
}

func randSelectByWeight(weights []float64) int {
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
