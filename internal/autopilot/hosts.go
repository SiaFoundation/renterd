package autopilot

import (
	"encoding/json"
	"math"
	"math/big"
	"net"
	"sort"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/build"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

func (ap *Autopilot) hostScanLoop() {
	for {
		select {
		case <-ap.stopChan:
			return
		case <-time.After(time.Minute):
			// scan up to 10 hosts that we haven't interacted with in at least 1 week
			hosts, err := ap.renter.Hosts(time.Now().Add(-7*24*time.Hour), 10)
			if err != nil {
				return
			}
			type res struct {
				hostKey  consensus.PublicKey
				settings rhpv2.HostSettings
				err      error
			}
			resChan := make(chan res)
			for _, h := range hosts {
				go func(h hostdb.Host) {
					settings, err := ap.renter.RHPScan(h.PublicKey, h.NetAddress())
					resChan <- res{h.PublicKey, settings, err}
				}(h)
			}
			for range hosts {
				r := <-resChan
				if r.err != nil {
					err := ap.renter.RecordHostInteraction(r.hostKey, hostdb.Interaction{
						Timestamp: time.Now(),
						Type:      "scan",
						Success:   false,
						Result:    json.RawMessage(`{"error": "` + r.err.Error() + `"}`),
					})
					_ = err // TODO
				} else {
					js, _ := json.Marshal(r.settings)
					err := ap.renter.RecordHostInteraction(r.hostKey, hostdb.Interaction{
						Timestamp: time.Now(),
						Type:      "scan",
						Success:   true,
						Result:    json.RawMessage(js),
					})
					_ = err // TODO
				}
			}
		}
	}
}

func (ap *Autopilot) hostsForContracts(n int) ([]consensus.PublicKey, error) {
	// select randomly using score as weights, excluding existing contracts

	config := ap.Config()
	contracts, err := ap.renter.HostSetContracts("autopilot")
	if err != nil {
		return nil, err
	}
	filterApplies := func(f string, h hostdb.Host) bool {
		if f == h.PublicKey.String() {
			return true
		}
		addr := h.NetAddress()
		if addr == "" {
			return false
		} else if f == addr {
			return true
		}
		_, ipNet, err := net.ParseCIDR(f)
		if err != nil {
			return false
		}
		ip, err := net.ResolveIPAddr("ip", addr)
		if err != nil {
			return false
		}
		return ipNet.Contains(ip.IP)
	}
	whitelisted := func(h hostdb.Host) bool {
		if len(config.Hosts.Whitelist) == 0 {
			return true
		}
		for _, w := range config.Hosts.Whitelist {
			if filterApplies(w, h) {
				return true
			}
		}
		return false
	}
	blacklisted := func(h hostdb.Host) bool {
		for _, b := range config.Hosts.Blacklist {
			if filterApplies(b, h) {
				return true
			}
		}
		// exclude current contracts
		for _, c := range contracts {
			if c.HostKey == h.PublicKey && c.ID != (types.FileContractID{}) {
				return true
			}
		}
		return false
	}

	hosts, err := ap.renter.AllHosts()
	if err != nil {
		return nil, err
	}
	rem := hosts[:0]
	for _, h := range hosts {
		if whitelisted(h) && !blacklisted(h) {
			rem = append(rem, h)
		}
	}
	hosts = rem

	// score each host
	scores := make([]float64, len(hosts))
	for i, h := range hosts {
		scores[i] = ap.calculateHostScore(h)
	}

	// select n hosts according to weight
	selectHost := func(scores []float64) int {
		// normalize
		var total float64
		for _, s := range scores {
			total += s
		}
		for i, s := range scores {
			scores[i] = s / total
		}
		// select
		r := frand.Float64()
		var sum float64
		for i, s := range scores {
			sum += s
			if r < sum {
				return i
			}
		}
		return len(scores) - 1
	}
	var selected []consensus.PublicKey
	if n > len(hosts) {
		n = len(hosts)
	}
	for len(selected) < n {
		i := selectHost(scores)
		selected = append(selected, hosts[i].PublicKey)
		// remove selected host
		hosts[i], hosts = hosts[len(hosts)-1], hosts[:len(hosts)-1]
		scores[i], scores = scores[len(scores)-1], scores[:len(scores)-1]
	}

	return selected, nil
}

func (ap *Autopilot) calculateHostScore(h hostdb.Host) float64 {
	lastKnownSettings := func(h hostdb.Host) (rhpv2.HostSettings, time.Time, bool) {
		for i := len(h.Interactions) - 1; i >= 0; i-- {
			if !h.Interactions[i].Success {
				continue
			}
			var settings rhpv2.HostSettings
			if err := json.Unmarshal(h.Interactions[i].Result, &settings); err != nil {
				continue
			}
			return settings, h.Interactions[i].Timestamp, true
		}
		return rhpv2.HostSettings{}, time.Time{}, false
	}

	ageWeight := func(h hostdb.Host) float64 {
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

		age := time.Since(h.Announcements[0].Timestamp)
		weight := 1.0
		for _, w := range weights {
			if age >= w.age {
				break
			}
			weight /= w.factor
		}
		return weight
	}

	interactionWeight := func(h hostdb.Host) float64 {
		// NOTE: the old siad hostdb uses a baseline success rate of 30 to 1, and an
		// exponent of 10, both of which we replicate here

		success, fail := 30.0, 1.0
		for _, hi := range h.Interactions {
			if hi.Success {
				success++
			} else {
				fail++
			}
		}
		return math.Pow(success/(success+fail), 10)
	}

	uptimeWeight := func(h hostdb.Host) float64 {
		sorted := append([]hostdb.Interaction(nil), h.Interactions...)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Timestamp.Before(sorted[j].Timestamp)
		})

		// special cases
		switch len(sorted) {
		case 0:
			return 0.25
		case 1:
			if sorted[0].Success {
				return 0.75
			}
			return 0.25
		case 2:
			if sorted[0].Success && sorted[1].Success {
				return 0.85
			} else if sorted[0].Success || sorted[1].Success {
				return 0.50
			}
			return 0.05
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
		return math.Pow(ratio, 200*math.Min(1-ratio, 0.30))
	}

	versionWeight := func(h hostdb.Host) float64 {
		settings, _, ok := lastKnownSettings(h)
		if !ok {
			return 0.01
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

	collateralWeight := func(h hostdb.Host) float64 {
		settings, _, ok := lastKnownSettings(h)
		if !ok {
			return 0.01
		}

		// NOTE: This math is copied directly from the old siad hostdb. It would
		// probably benefit from a thorough review.

		config := ap.Config()
		var fundsPerHost types.Currency = config.Contracts.Allowance.Div64(config.Contracts.Hosts)
		var storage, duration uint64 // TODO

		contractCollateral := settings.Collateral.Mul64(storage).Mul64(duration)
		if maxCollateral := settings.MaxCollateral.Div64(2); contractCollateral.Cmp(maxCollateral) > 0 {
			contractCollateral = maxCollateral
		}
		collateral, _ := new(big.Rat).SetInt(contractCollateral.Big()).Float64()
		cutoff, _ := new(big.Rat).SetInt(fundsPerHost.Div64(5).Big()).Float64()
		collateral = math.Max(1, collateral)               // ensure 1 <= collateral
		cutoff = math.Min(math.Max(1, cutoff), collateral) // ensure 1 <= cutoff <= collateral
		return math.Pow(cutoff, 4) * math.Pow(collateral/cutoff, 0.5)
	}

	return 1.0 * ageWeight(h) *
		1.0 * interactionWeight(h) *
		1.0 * uptimeWeight(h) *
		1.0 * versionWeight(h) *
		1.0 * collateralWeight(h)
}
