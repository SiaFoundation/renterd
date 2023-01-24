package autopilot

import (
	"fmt"
	"math"
	"math/big"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/modules"
)

const (
	// minContractFundUploadThreshold is the percentage of contract funds
	// remaining at which the contract gets marked as not good for upload
	minContractFundUploadThreshold = float64(0.05) // 5%
)

// isUsableHost returns whether the given host is usable along with a list of
// reasons why it was deemed unusable.
func isUsableHost(cfg api.AutopilotConfig, gs api.GougingSettings, rs api.RedundancySettings, f *ipFilter, h hostdb.Host) (bool, []string) {
	var reasons []string

	if !h.IsOnline() {
		reasons = append(reasons, "offline")
	}
	if !cfg.Hosts.IgnoreRedundantIPs && f.isRedundantIP(h) {
		reasons = append(reasons, "redundant IP")
	}
	if settings, bad, reason := hasBadSettings(cfg, h); bad {
		reasons = append(reasons, fmt.Sprintf("bad settings: %v", reason))
	} else if gouging, reason := isGouging(gs, rs, settings); gouging {
		reasons = append(reasons, fmt.Sprintf("price gouging: %v", reason))
	}

	// sanity check - should never happen but this would cause a zero score
	if h.NetAddress == "" {
		reasons = append(reasons, "not announced")
	}

	return len(reasons) == 0, reasons
}

// isUsableContract returns whether the given contract is usable and whether it
// can be renewed, along with a list of reasons why it was deemed unusable.
func isUsableContract(cfg api.AutopilotConfig, s rhp.HostSettings, c api.Contract, bh uint64) (usable bool, refresh bool, renew bool, reasons []string) {
	if isOutOfFunds(cfg, s, c) {
		reasons = append(reasons, "out of funds")
		renew = false
		refresh = true
	}
	if isUpForRenewal(cfg, c.Revision, bh) {
		reasons = append(reasons, "up for renewal")
		renew = true
		refresh = false
	}
	if c.Revision.RevisionNumber == math.MaxUint64 {
		reasons = append(reasons, "max revision number")
		renew = false
		refresh = false
	}
	if bh > c.EndHeight() {
		reasons = append(reasons, "expired")
		renew = false
		refresh = false
	}
	usable = len(reasons) == 0
	return
}

func isOutOfFunds(cfg api.AutopilotConfig, settings rhp.HostSettings, c api.Contract) bool {
	blockBytes := types.NewCurrency64(modules.SectorSize * cfg.Contracts.Period)
	sectorStoragePrice := settings.StoragePrice.Mul(blockBytes)
	sectorUploadBandwidthPrice := settings.UploadBandwidthPrice.Mul64(modules.SectorSize)
	sectorDownloadBandwidthPrice := settings.DownloadBandwidthPrice.Mul64(modules.SectorSize)
	sectorBandwidthPrice := sectorUploadBandwidthPrice.Add(sectorDownloadBandwidthPrice)
	sectorPrice := sectorStoragePrice.Add(sectorBandwidthPrice)
	percentRemaining, _ := big.NewRat(0, 1).SetFrac(c.RenterFunds().Big(), c.TotalCost.Big()).Float64()

	return c.RenterFunds().Cmp(sectorPrice.Mul64(3)) < 0 || percentRemaining < minContractFundUploadThreshold
}

func isUpForRenewal(cfg api.AutopilotConfig, r types.FileContractRevision, blockHeight uint64) bool {
	return blockHeight+cfg.Contracts.RenewWindow >= r.EndHeight()
}

func isGouging(gs api.GougingSettings, rs api.RedundancySettings, settings rhp.HostSettings) (bool, string) {
	return worker.IsGouging(gs, settings, rs.MinShards, rs.TotalShards)
}

func hasBadSettings(cfg api.AutopilotConfig, h hostdb.Host) (rhp.HostSettings, bool, string) {
	settings := h.Settings
	if settings == nil {
		return rhp.HostSettings{}, true, "no settings"
	}
	if !settings.AcceptingContracts {
		return *settings, true, "not accepting contracts"
	}
	if cfg.Contracts.Period+cfg.Contracts.RenewWindow > settings.MaxDuration {
		return *settings, true, fmt.Sprintf("max duration too low, %v > %v", cfg.Contracts.Period+cfg.Contracts.RenewWindow, settings.MaxDuration)
	}
	maxBaseRPCPrice := settings.DownloadBandwidthPrice.Mul64(maxBaseRPCPriceVsBandwidth)
	if settings.BaseRPCPrice.Cmp(maxBaseRPCPrice) > 0 {
		return *settings, true, fmt.Sprintf("base RPC price too high, %v > %v", settings.BaseRPCPrice, maxBaseRPCPrice)
	}
	maxSectorAccessPrice := settings.DownloadBandwidthPrice.Mul64(maxSectorAccessPriceVsBandwidth)
	if settings.SectorAccessPrice.Cmp(maxSectorAccessPrice) > 0 {
		return *settings, true, fmt.Sprintf("sector access price too high, %v > %v", settings.BaseRPCPrice, maxBaseRPCPrice)
	}
	return *settings, false, ""
}
