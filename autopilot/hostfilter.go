package autopilot

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/worker"
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
	errHostNoPriceTable = errors.New("host has no pricetable")

	errContractOutOfCollateral   = errors.New("contract is out of collateral")
	errContractOutOfFunds        = errors.New("contract is out of funds")
	errContractUpForRenewal      = errors.New("contract is up for renewal")
	errContractMaxRevisionNumber = errors.New("contract has reached max revision number")
	errContractExpired           = errors.New("contract has expired")
)

type unusableHostResult struct {
	blocked      uint64
	offline      uint64
	lowscore     uint64
	redundantip  uint64
	badsettings  uint64
	gouging      uint64
	notannounced uint64
	nopricetable uint64
	unknown      uint64

	// unusableHostResult is overloaded with a breakdown of the host score, this
	// field might not be set and is mostly ignored, it is currently only used
	// by the host infos endpoint `/hosts/:hostkey`
	scoreBreakdown api.HostScoreBreakdown
}

func newUnusableHostResult(errs []error, scoreBreakdown api.HostScoreBreakdown) (u unusableHostResult) {
	for _, err := range errs {
		if errors.Is(err, errHostBlocked) {
			u.blocked++
		} else if errors.Is(err, errHostOffline) {
			u.offline++
		} else if errors.Is(err, errLowScore) {
			u.lowscore++
		} else if errors.Is(err, errHostRedundantIP) {
			u.redundantip++
		} else if errors.Is(err, errHostBadSettings) {
			u.badsettings++
		} else if errors.Is(err, errHostPriceGouging) {
			u.gouging++
		} else if errors.Is(err, errHostNotAnnounced) {
			u.notannounced++
		} else if errors.Is(err, errHostNoPriceTable) {
			u.nopricetable++
		} else {
			u.unknown++
		}
	}

	u.scoreBreakdown = scoreBreakdown
	return
}

func (u unusableHostResult) String() string {
	return fmt.Sprintf("host is unusable because of the following reasons: %v", strings.Join(u.reasons(), ", "))
}

func (u unusableHostResult) reasons() []string {
	var reasons []string
	if u.blocked > 0 {
		reasons = append(reasons, errHostBlocked.Error())
	}
	if u.offline > 0 {
		reasons = append(reasons, errHostOffline.Error())
	}
	if u.lowscore > 0 {
		reasons = append(reasons, errLowScore.Error())
	}
	if u.redundantip > 0 {
		reasons = append(reasons, errHostRedundantIP.Error())
	}
	if u.badsettings > 0 {
		reasons = append(reasons, errHostBadSettings.Error())
	}
	if u.gouging > 0 {
		reasons = append(reasons, errHostPriceGouging.Error())
	}
	if u.notannounced > 0 {
		reasons = append(reasons, errHostNotAnnounced.Error())
	}
	if u.nopricetable > 0 {
		reasons = append(reasons, errHostNoPriceTable.Error())
	}
	if u.unknown > 0 {
		reasons = append(reasons, "unknown")
	}
	return reasons
}

func (u *unusableHostResult) merge(other unusableHostResult) {
	u.blocked += other.blocked
	u.offline += other.offline
	u.lowscore += other.lowscore
	u.redundantip += other.redundantip
	u.badsettings += other.badsettings
	u.gouging += other.gouging
	u.notannounced += other.notannounced
	u.nopricetable += other.nopricetable
	u.unknown += other.unknown

	// scoreBreakdown is not merged
}

func (u *unusableHostResult) keysAndValues() []interface{} {
	values := []interface{}{
		"blocked", u.blocked,
		"offline", u.offline,
		"lowscore", u.lowscore,
		"redundantip", u.redundantip,
		"badsettings", u.badsettings,
		"gouging", u.gouging,
		"notannounced", u.notannounced,
		"nopricetable", u.nopricetable,
		"unknown", u.unknown,
	}
	for i := 0; i < len(values); i += 2 {
		if values[i+1].(uint64) == 0 {
			values = append(values[:i], values[i+2:]...)
			i -= 2
		}
	}
	return values
}

// isUsableHost returns whether the given host is usable along with a list of
// reasons why it was deemed unusable.
func isUsableHost(cfg api.AutopilotConfig, gs api.GougingSettings, rs api.RedundancySettings, cs api.ConsensusState, f *ipFilter, h hostdb.Host, minScore float64, storedData uint64, txnFee types.Currency, ignoreBlockHeight bool) (bool, unusableHostResult) {
	if rs.Validate() != nil {
		panic("invalid redundancy settings were supplied - developer error")
	}
	var errs []error

	if !h.IsOnline() {
		errs = append(errs, errHostOffline)
	}
	if h.NetAddress == "" {
		errs = append(errs, errHostNotAnnounced)
	}
	if !cfg.Hosts.IgnoreRedundantIPs && f.isRedundantIP(h) {
		errs = append(errs, errHostRedundantIP)
	}

	var breakdown api.HostScoreBreakdown
	if settings, bad, reason := hasBadSettings(cfg, h); bad {
		errs = append(errs, fmt.Errorf("%w: %v", errHostBadSettings, reason))
	} else if h.PriceTable == nil {
		errs = append(errs, errHostNoPriceTable)
	} else if gouging, reason := worker.IsGouging(gs, rs, cs, settings, h.PriceTable, txnFee, cfg.Contracts.Period, cfg.Contracts.RenewWindow, ignoreBlockHeight); gouging {
		errs = append(errs, fmt.Errorf("%w: %v", errHostPriceGouging, reason))
	} else if breakdown = hostScore(cfg, h, storedData, rs.Redundancy()); breakdown.Score() < minScore {
		errs = append(errs, fmt.Errorf("%w: %v < %v", errLowScore, breakdown.Score(), minScore))
	}

	return len(errs) == 0, newUnusableHostResult(errs, breakdown)
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
	blockBytes := types.NewCurrency64(rhpv2.SectorSize * cfg.Contracts.Period)
	sectorStoragePrice := s.StoragePrice.Mul(blockBytes)
	sectorUploadBandwidthPrice := s.UploadBandwidthPrice.Mul64(rhpv2.SectorSize)
	sectorDownloadBandwidthPrice := s.DownloadBandwidthPrice.Mul64(rhpv2.SectorSize)
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

func hasBadSettings(cfg api.AutopilotConfig, h hostdb.Host) (*rhpv2.HostSettings, bool, string) {
	settings := h.Settings
	if settings == nil {
		return nil, true, "no settings"
	}
	if !settings.AcceptingContracts {
		return nil, true, "not accepting contracts"
	}
	if cfg.Contracts.Period+cfg.Contracts.RenewWindow > settings.MaxDuration {
		return nil, true, fmt.Sprintf("max duration too low, %v > %v", cfg.Contracts.Period+cfg.Contracts.RenewWindow, settings.MaxDuration)
	}
	maxBaseRPCPrice := settings.DownloadBandwidthPrice.Mul64(maxBaseRPCPriceVsBandwidth)
	if settings.BaseRPCPrice.Cmp(maxBaseRPCPrice) > 0 {
		return nil, true, fmt.Sprintf("base RPC price too high, %v > %v", settings.BaseRPCPrice, maxBaseRPCPrice)
	}
	maxSectorAccessPrice := settings.DownloadBandwidthPrice.Mul64(maxSectorAccessPriceVsBandwidth)
	if settings.SectorAccessPrice.Cmp(maxSectorAccessPrice) > 0 {
		return nil, true, fmt.Sprintf("sector access price too high, %v > %v", settings.BaseRPCPrice, maxBaseRPCPrice)
	}
	return settings, false, ""
}
