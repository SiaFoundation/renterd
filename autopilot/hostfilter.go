package autopilot

import (
	"errors"
	"fmt"
	"math"
	"math/big"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/modules"
)

const (
	// minContractFundUploadThreshold is the percentage of contract funds
	// remaining at which the contract gets marked as not good for upload
	minContractFundUploadThreshold = float64(0.05) // 5%

	// minContractCollateralThreshold is 10% of the collateral that we would put
	// into a contract upon renewing it. That means, we consider a contract
	// worth renewing when that results in 10x the collateral of what it
	// currently has remaining.
	minContractCollateralThresholdNumerator   = 10
	minContractCollateralThresholdDenominator = 100
)

var (
	errHostBlocked      = errors.New("host is blocked")
	errHostOffline      = errors.New("host is offline")
	errLowScore         = errors.New("host's score is below minimum")
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
func isUsableHost(cfg api.AutopilotConfig, gs api.GougingSettings, rs api.RedundancySettings, f *ipFilter, h hostdb.Host, minScore float64, storedData uint64) (bool, []error) {
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
	} else if score := hostScore(cfg, h, storedData, rs.Redundancy()); score < minScore {
		reasons = append(reasons, fmt.Errorf("%w: %v < %v", errLowScore, score, minScore))
	}

	// sanity check - should never happen but this would cause a zero score
	if h.NetAddress == "" {
		reasons = append(reasons, errHostNotAnnounced)
	}
	return len(reasons) == 0, reasons
}

// isUsableContract returns whether the given contract is usable and whether it
// can be renewed, along with a list of reasons why it was deemed unusable.
func isUsableContract(cfg api.AutopilotConfig, ci contractInfo, bh uint64, renterFunds types.Currency) (usable bool, refresh bool, renew bool, reasons []error) {
	c, s := ci.contract, ci.settings
	if isOutOfCollateral(c, s, renterFunds, bh) {
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

// isOutOfCollateral returns 'true' if the remaining/unallocated collateral in
// the contract is below a certain threshold of the collateral we would try to
// put into a contract upon renew.
func isOutOfCollateral(c api.Contract, s rhpv2.HostSettings, renterFunds types.Currency, blockHeight uint64) bool {
	expectedStorage := renterFundsToExpectedStorage(renterFunds, c.EndHeight()-blockHeight, s)
	expectedCollateral := rhpv2.ContractRenewalCollateral(c.Revision.FileContract, expectedStorage, s, blockHeight, c.EndHeight())
	return isBelowCollateralThreshold(expectedCollateral, c.RemainingCollateral(s))
}

// isBelowCollateralThreshold returns true if the actualCollateral is below a
// certain percentage of expectedCollateral. The expectedCollateral is the
// amount of new collateral we intend to put into a contract when refreshing it
// and the actualCollateral is the amount we can actually put in after adjusting
// our expectation using the host settings.
func isBelowCollateralThreshold(expectedCollateral, actualCollateral types.Currency) bool {
	if expectedCollateral.IsZero() {
		return true // protect against division-by-zero
	}
	collateral := big.NewRat(0, 1).SetFrac(actualCollateral.Big(), expectedCollateral.Big())
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
