package autopilot

import (
	"fmt"
	"math"
	"math/big"
	"net"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

const (
	// number of unique bits the host IP must have to prevent it from being filtered
	IPv4FilterRange = 24
	IPv6FilterRange = 54

	// minContractFundUploadThreshold is the percentage of contract funds
	// remaining at which the contract gets marked as not good for upload
	minContractFundUploadThreshold = float64(0.05) // 5%
)

type (
	filter func(h host) filterResult

	filterResult struct {
		reason      string
		transformer func(m *bus.ContractMetadata)
	}
)

func noopfilter(h host) filterResult {
	return filterResult{
		transformer: func(m *bus.ContractMetadata) {},
	}
}

func (r filterResult) filtered() bool {
	return r.reason != ""
}

func (r *filterResult) setNotGFUnotGFR(reason string) {
	r.reason = reason
	r.transformer = func(m *bus.ContractMetadata) {
		m.GoodForUpload = false
		m.GoodForRenew = false
	}
}

func (r *filterResult) setNotGFU(reason string) {
	r.reason = reason
	r.transformer = func(m *bus.ContractMetadata) {
		m.GoodForUpload = false
	}
}

func filterRangeForIP(ip net.IP) int {
	if ip.To4() != nil {
		return IPv4FilterRange
	}
	return IPv6FilterRange
}

func hostFilter(filters ...filter) filter {
	if len(filters) == 0 {
		return noopfilter
	}
	return func(h host) (result filterResult) {
		for _, filter := range filters {
			if result = filter(h); result.filtered() {
				break
			}
		}
		return
	}
}

func isLowScore(cfg Config, threshold float64) filter {
	return func(h host) (result filterResult) {
		// calculate host score
		score := HostScore(
			ageScore,
			collateralScore(cfg),
			interactionScore,
			settingsScore(cfg),
			uptimeScore,
			versionScore,
			// TODO: priceScore
			// TODO: storageRemainingScore
		)(h)

		// check whether it's below the threshold
		if score < threshold {
			result.setNotGFUnotGFR("host score too low")
		}
		return
	}
}

func isMaxRevision(h host) (result filterResult) {
	if h.Revision.NewRevisionNumber == math.MaxUint64 {
		result.setNotGFUnotGFR("max revision check failed")
	}
	return
}

func isOffline(h host) (result filterResult) {
	if !h.IsOnline() {
		result.setNotGFUnotGFR("host is not online")
	}
	return
}

func isOutOfFunds(cfg Config, metadata bus.ContractMetadata) filter {
	return func(h host) (result filterResult) {
		settings, _, found := h.LastKnownSettings()
		if !found {
			return
		}

		blockBytes := types.NewCurrency64(modules.SectorSize * cfg.Contracts.Period)
		sectorStoragePrice := settings.StoragePrice.Mul(blockBytes)
		sectorUploadBandwidthPrice := settings.UploadBandwidthPrice.Mul64(modules.SectorSize)
		sectorDownloadBandwidthPrice := settings.DownloadBandwidthPrice.Mul64(modules.SectorSize)
		sectorBandwidthPrice := sectorUploadBandwidthPrice.Add(sectorDownloadBandwidthPrice)
		sectorPrice := sectorStoragePrice.Add(sectorBandwidthPrice)
		percentRemaining, _ := big.NewRat(0, 1).SetFrac(h.RenterFunds().Big(), metadata.TotalCost.Big()).Float64()
		if h.RenterFunds().Cmp(sectorPrice.Mul64(3)) < 0 || percentRemaining < minContractFundUploadThreshold {
			result.setNotGFU("contract has insufficient funds")
		}
		return
	}
}

func isRedundantIP(subnets map[string]struct{}) filter {
	return func(h host) (result filterResult) {
		// resolve the host IP
		ip, err := net.ResolveIPAddr("ip", h.NetAddress())
		if err != nil {
			return
		}

		// fetch the ipnet
		cidr := fmt.Sprintf("%s/%d", ip.String(), filterRangeForIP(ip.IP))
		_, ipnet, err := net.ParseCIDR(cidr)
		if err != nil {
			return
		}

		// if it already exists it's considered redundant
		if _, exists := subnets[ipnet.String()]; exists {
			result.setNotGFUnotGFR("host IP is redundant")
		} else {
			subnets[ipnet.String()] = struct{}{} // add subnet
		}

		return
	}
}

// TODO: only mark a host as superfluous as soon as it drops below the desired
// amount of hosts
func isSuperfluous(cfg Config, numActive *uint64) filter {
	return func(h host) (result filterResult) {
		if *numActive > cfg.Contracts.Hosts {
			*numActive -= 1
			result.setNotGFUnotGFR("contract is superfluous")
		}
		return
	}
}

func isUpForRenewal(cfg Config, blockHeight uint64) filter {
	return func(h host) (result filterResult) {
		if blockHeight+cfg.Contracts.RenewWindow/2 >= h.EndHeight() {
			result.setNotGFU("contract is up for renewal")
		}
		return
	}
}

func (c *Config) isBlackListed(h host) (result filterResult) {
	for _, host := range c.Hosts.Blacklist {
		if h.IsHost(host) {
			result.setNotGFUnotGFR("host is blacklisted")
			break
		}
	}
	return
}

func (c *Config) isWhiteListed(h host) (result filterResult) {
	if len(c.Hosts.Whitelist) > 0 {
		var found bool
		for _, host := range c.Hosts.Whitelist {
			if h.IsHost(host) {
				found = true
				break
			}
		}
		if !found {
			result.setNotGFUnotGFR("host not whitelisted")
		}
	}
	return
}
