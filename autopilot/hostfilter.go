package autopilot

import (
	"errors"
	"fmt"
	"math"
	"math/big"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/modules"
)

const (
	// minContractFundUploadThreshold is the percentage of contract funds
	// remaining at which the contract gets marked as not good for upload
	minContractFundUploadThreshold = float64(0.05) // 5%

	// minContractCollateralThreshold is the percentage of collateral remaining
	// at which the contract gets refreshed, the default threshold of 2% is
	// defined by the following numerator and denominator constants.
	minContractCollateralThresholdNumerator   = 2
	minContractCollateralThresholdDenominator = 100
)

var (
	errHostOffline      = errors.New("host is offline")
	errHostRedundantIP  = errors.New("host has redundant IP")
	errHostBadSettings  = errors.New("host has bad settings")
	errHostPriceGouging = errors.New("host is price gouging")
	errHostNotAnnounced = errors.New("host is not announced")

	errContractOutOfCollateral   = errors.New("contract is out of collateral")
	errContractOutOfFunds        = errors.New("contract is out of funds")
	errContractUpForRenewal      = errors.New("contract is up for renewal")
	errContractMaxRevisionNumber = errors.New("contract has reached max revision number")
	errContractExpired           = errors.New("contract has expired")
)

// isUsableHost returns whether the given host is usable along with a list of
// reasons why it was deemed unusable.
func isUsableHost(cfg api.AutopilotConfig, gs api.GougingSettings, rs api.RedundancySettings, f *ipFilter, h hostdb.Host) (bool, []error) {
	var reasons []error

	if !h.IsOnline() {
		reasons = append(reasons, errHostOffline)
	}
	if !cfg.Hosts.IgnoreRedundantIPs && f.isRedundantIP(h) {
		reasons = append(reasons, errHostRedundantIP)
	}
	if settings, bad, reason := hasBadSettings(cfg, h); bad {
		reasons = append(reasons, fmt.Errorf("%w: %v", errHostBadSettings, reason))
	} else if gouging, reason := isGouging(gs, rs, settings); gouging {
		reasons = append(reasons, fmt.Errorf("%w: %v", errHostPriceGouging, reason))
	}

	// sanity check - should never happen but this would cause a zero score
	if h.NetAddress == "" {
		reasons = append(reasons, errHostNotAnnounced)
	}
	return len(reasons) == 0, reasons
}

// isUsableContract returns whether the given contract is usable and whether it
// can be renewed, along with a list of reasons why it was deemed unusable.
func isUsableContract(cfg api.AutopilotConfig, s rhpv2.HostSettings, c api.Contract, bh uint64) (usable bool, refresh bool, renew bool, reasons []error) {
	if isOutOfCollateral(cfg, s, c) {
		reasons = append(reasons, errContractOutOfCollateral)
		renew = false
		refresh = true
	}
	if isOutOfFunds(cfg, s, c) {
		reasons = append(reasons, errContractOutOfFunds)
		renew = false
		refresh = true
	}
	if isUpForRenewal(cfg, c.Revision, bh) {
		reasons = append(reasons, errContractUpForRenewal)
		renew = true
		refresh = false
	}
	if c.Revision.RevisionNumber == math.MaxUint64 {
		reasons = append(reasons, errContractMaxRevisionNumber)
		renew = false
		refresh = false
	}
	if bh > c.EndHeight() {
		reasons = append(reasons, errContractExpired)
		renew = false
		refresh = false
	}
	usable = len(reasons) == 0
	return
}

func isOutOfFunds(cfg api.AutopilotConfig, s rhpv2.HostSettings, c api.Contract) bool {
	blockBytes := types.NewCurrency64(modules.SectorSize * cfg.Contracts.Period)
	sectorStoragePrice := s.StoragePrice.Mul(blockBytes)
	sectorUploadBandwidthPrice := s.UploadBandwidthPrice.Mul64(modules.SectorSize)
	sectorDownloadBandwidthPrice := s.DownloadBandwidthPrice.Mul64(modules.SectorSize)
	sectorBandwidthPrice := sectorUploadBandwidthPrice.Add(sectorDownloadBandwidthPrice)
	sectorPrice := sectorStoragePrice.Add(sectorBandwidthPrice)
	percentRemaining, _ := big.NewRat(0, 1).SetFrac(c.RenterFunds().Big(), c.TotalCost.Big()).Float64()

	return c.RenterFunds().Cmp(sectorPrice.Mul64(3)) < 0 || percentRemaining < minContractFundUploadThreshold
}

func isOutOfCollateral(cfg api.AutopilotConfig, s rhpv2.HostSettings, c api.Contract) bool {
	return isBelowCollateralThreshold(cfg, s, c.RemainingCollateral(s))
}

// isBelowCollateralThreshold returns true if the given collateral is below the
// minimum contract collateral threshold, which is a percentage of the initial
// collateral.
func isBelowCollateralThreshold(cfg api.AutopilotConfig, s rhpv2.HostSettings, c types.Currency) bool {
	initialCollateral := rhpv2.ContractFormationCollateral(cfg.Contracts.Storage/cfg.Contracts.Amount, cfg.Contracts.Period, s)
	collateral := big.NewRat(0, 1).SetFrac(c.Big(), initialCollateral.Big())
	threshold := big.NewRat(minContractCollateralThresholdNumerator, minContractCollateralThresholdDenominator)
	return collateral.Cmp(threshold) < 0
}

func isUpForRenewal(cfg api.AutopilotConfig, r types.FileContractRevision, blockHeight uint64) bool {
	return blockHeight+cfg.Contracts.RenewWindow >= r.EndHeight()
}

func isGouging(gs api.GougingSettings, rs api.RedundancySettings, settings rhpv2.HostSettings) (bool, string) {
	return worker.IsGouging(gs, settings, rs.MinShards, rs.TotalShards)
}

func hasBadSettings(cfg api.AutopilotConfig, h hostdb.Host) (rhpv2.HostSettings, bool, string) {
	settings := h.Settings
	if settings == nil {
		return rhpv2.HostSettings{}, true, "no settings"
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
