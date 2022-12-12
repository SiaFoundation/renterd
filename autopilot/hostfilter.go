package autopilot

import (
	"fmt"
	"math"
	"math/big"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

const (
	// minContractFundUploadThreshold is the percentage of contract funds
	// remaining at which the contract gets marked as not good for upload
	minContractFundUploadThreshold = float64(0.05) // 5%
)

// isUsableHost returns whether the given host is usable along with a list of
// reasons why it was deemed unusable.
func isUsableHost(cfg Config, f *ipFilter, h Host) (bool, []string) {
	var reasons []string

	if !cfg.isWhitelisted(h) {
		reasons = append(reasons, "not whitelisted")
	}
	if cfg.isBlacklisted(h) {
		reasons = append(reasons, "blacklisted")
	}
	if !h.IsOnline() {
		reasons = append(reasons, "offline")
	}
	if !cfg.Hosts.IgnoreRedundantIPs && f.isRedundantIP(h) {
		reasons = append(reasons, "redundant IP")
	}
	if bad, reason := hasBadSettings(cfg, h); bad {
		reasons = append(reasons, fmt.Sprintf("bad settings: %v", reason))
	}

	// sanity check - should never happen but this would cause a zero score
	if len(h.Announcements) == 0 {
		reasons = append(reasons, "not announced")
	}

	return len(reasons) == 0, reasons
}

// isUsableContract returns whether the given contract is usable and whether it
// can be renewed, along with a list of reasons why it was deemed unusable.
func isUsableContract(cfg Config, h Host, c bus.Contract, m bus.ContractMetadata, bh uint64) (bool, bool, []string) {
	var reasons []string
	renewable := true

	if isOutOfFunds(cfg, h, c, m) {
		reasons = append(reasons, "out of funds")
	}
	if isUpForRenewal(cfg, c, bh) {
		reasons = append(reasons, "up for renewal")
	}
	if isMaxRevision(c) {
		reasons = append(reasons, "max revision number")
		renewable = false
	}

	return len(reasons) == 0, renewable, reasons
}

func isMaxRevision(c bus.Contract) bool {
	return c.Revision.NewRevisionNumber == math.MaxUint64
}

func isOutOfFunds(cfg Config, h Host, c bus.Contract, m bus.ContractMetadata) bool {
	settings, _, found := h.LastKnownSettings()
	if !found {
		return false
	}

	blockBytes := types.NewCurrency64(modules.SectorSize * cfg.Contracts.Period)
	sectorStoragePrice := settings.StoragePrice.Mul(blockBytes)
	sectorUploadBandwidthPrice := settings.UploadBandwidthPrice.Mul64(modules.SectorSize)
	sectorDownloadBandwidthPrice := settings.DownloadBandwidthPrice.Mul64(modules.SectorSize)
	sectorBandwidthPrice := sectorUploadBandwidthPrice.Add(sectorDownloadBandwidthPrice)
	sectorPrice := sectorStoragePrice.Add(sectorBandwidthPrice)
	percentRemaining, _ := big.NewRat(0, 1).SetFrac(c.RenterFunds().Big(), m.TotalCost.Big()).Float64()

	return c.RenterFunds().Cmp(sectorPrice.Mul64(3)) < 0 || percentRemaining < minContractFundUploadThreshold
}

func isUpForRenewal(cfg Config, c bus.Contract, blockHeight uint64) bool {
	return blockHeight+cfg.Contracts.RenewWindow/2 >= c.EndHeight()
}

func hasBadSettings(cfg Config, h Host) (bool, string) {
	settings, _, found := h.LastKnownSettings()
	if !found {
		return true, "no settings"
	}
	if !settings.AcceptingContracts {
		return true, "not accepting contracts"
	}
	if cfg.Contracts.Period+cfg.Contracts.RenewWindow > settings.MaxDuration {
		return true, fmt.Sprintf("max duration too low, %v > %v", cfg.Contracts.Period+cfg.Contracts.RenewWindow, settings.MaxDuration)
	}
	maxBaseRPCPrice := settings.DownloadBandwidthPrice.Mul64(maxBaseRPCPriceVsBandwidth)
	if settings.BaseRPCPrice.Cmp(maxBaseRPCPrice) > 0 {
		return true, fmt.Sprintf("base RPC price too high, %v > %v", settings.BaseRPCPrice, maxBaseRPCPrice)
	}
	maxSectorAccessPrice := settings.DownloadBandwidthPrice.Mul64(maxSectorAccessPriceVsBandwidth)
	if settings.SectorAccessPrice.Cmp(maxSectorAccessPrice) > 0 {
		return true, fmt.Sprintf("sector access price too high, %v > %v", settings.BaseRPCPrice, maxBaseRPCPrice)
	}
	return false, ""
}

func (cfg Config) isBlacklisted(h Host) bool {
	for _, host := range cfg.Hosts.Blacklist {
		if h.IsHost(host) {
			return true
		}
	}
	return false
}

func (cfg Config) isWhitelisted(h Host) bool {
	if len(cfg.Hosts.Whitelist) > 0 {
		var found bool
		for _, host := range cfg.Hosts.Whitelist {
			if h.IsHost(host) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
