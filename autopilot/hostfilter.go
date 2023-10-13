package autopilot

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
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
	errHostBlocked               = errors.New("host is blocked")
	errHostNotFound              = errors.New("host not found")
	errHostOffline               = errors.New("host is offline")
	errLowScore                  = errors.New("host's score is below minimum")
	errHostRedundantIP           = errors.New("host has redundant IP")
	errHostPriceGouging          = errors.New("host is price gouging")
	errHostNotAcceptingContracts = errors.New("host is not accepting contracts")
	errHostNotCompletingScan     = errors.New("host is not completing scan")
	errHostNotAnnounced          = errors.New("host is not announced")

	errContractOutOfCollateral   = errors.New("contract is out of collateral")
	errContractOutOfFunds        = errors.New("contract is out of funds")
	errContractUpForRenewal      = errors.New("contract is up for renewal")
	errContractMaxRevisionNumber = errors.New("contract has reached max revision number")
	errContractNoRevision        = errors.New("contract has no revision")
	errContractExpired           = errors.New("contract has expired")
)

type unusableHostResult struct {
	blocked               uint64
	offline               uint64
	lowscore              uint64
	redundantip           uint64
	gouging               uint64
	notacceptingcontracts uint64
	notannounced          uint64
	notcompletingscan     uint64
	unknown               uint64

	// gougingBreakdown is mostly ignored, we overload the unusableHostResult
	// with a gouging breakdown to be able to return it in the host infos
	// endpoint `/hosts/:hostkey`
	gougingBreakdown api.HostGougingBreakdown

	// scoreBreakdown is mostly ignored, we overload the unusableHostResult with
	// a score breakdown to be able to return it in the host infos endpoint
	// `/hosts/:hostkey`
	scoreBreakdown api.HostScoreBreakdown
}

func newUnusableHostResult(errs []error, gougingBreakdown api.HostGougingBreakdown, scoreBreakdown api.HostScoreBreakdown) (u unusableHostResult) {
	for _, err := range errs {
		if errors.Is(err, errHostBlocked) {
			u.blocked++
		} else if errors.Is(err, errHostOffline) {
			u.offline++
		} else if errors.Is(err, errLowScore) {
			u.lowscore++
		} else if errors.Is(err, errHostRedundantIP) {
			u.redundantip++
		} else if errors.Is(err, errHostPriceGouging) {
			u.gouging++
		} else if errors.Is(err, errHostNotAcceptingContracts) {
			u.notacceptingcontracts++
		} else if errors.Is(err, errHostNotAnnounced) {
			u.notannounced++
		} else if errors.Is(err, errHostNotCompletingScan) {
			u.notcompletingscan++
		} else {
			u.unknown++
		}
	}

	u.gougingBreakdown = gougingBreakdown
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
	if u.gouging > 0 {
		reasons = append(reasons, errHostPriceGouging.Error())
	}
	if u.notacceptingcontracts > 0 {
		reasons = append(reasons, errHostNotAcceptingContracts.Error())
	}
	if u.notannounced > 0 {
		reasons = append(reasons, errHostNotAnnounced.Error())
	}
	if u.notcompletingscan > 0 {
		reasons = append(reasons, errHostNotCompletingScan.Error())
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
	u.gouging += other.gouging
	u.notacceptingcontracts += other.notacceptingcontracts
	u.notannounced += other.notannounced
	u.notcompletingscan += other.notcompletingscan
	u.unknown += other.unknown

	// scoreBreakdown is not merged
	//
	// gougingBreakdown is not merged
}

func (u *unusableHostResult) keysAndValues() []interface{} {
	values := []interface{}{
		"blocked", u.blocked,
		"offline", u.offline,
		"lowscore", u.lowscore,
		"redundantip", u.redundantip,
		"gouging", u.gouging,
		"notacceptingcontracts", u.notacceptingcontracts,
		"notcompletingscan", u.notcompletingscan,
		"notannounced", u.notannounced,
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
func isUsableHost(cfg api.AutopilotConfig, rs api.RedundancySettings, gc worker.GougingChecker, h hostdb.Host, minScore float64, storedData uint64) (bool, unusableHostResult) {
	if rs.Validate() != nil {
		panic("invalid redundancy settings were supplied - developer error")
	}

	var errs []error
	var gougingBreakdown api.HostGougingBreakdown
	var scoreBreakdown api.HostScoreBreakdown

	if !h.IsAnnounced() {
		errs = append(errs, errHostNotAnnounced)
	} else if !h.Scanned {
		errs = append(errs, errHostNotCompletingScan)
	} else {
		// online check
		if !h.IsOnline() {
			errs = append(errs, errHostOffline)
		}

		// accepting contracts check
		if !h.Settings.AcceptingContracts {
			errs = append(errs, errHostNotAcceptingContracts)
		}

		// perform gouging checks
		gougingBreakdown = gc.Check(&h.Settings, &h.PriceTable.HostPriceTable)
		if gougingBreakdown.Gouging() {
			errs = append(errs, fmt.Errorf("%w: %v", errHostPriceGouging, gougingBreakdown.Reasons()))
		} else {
			// perform scoring checks
			//
			// NOTE: only perform these scoring checks if we know the host is
			// not gouging, this because the core package does not have overflow
			// checks in its cost calculations needed to calculate the period
			// cost
			scoreBreakdown = hostScore(cfg, h, storedData, rs.Redundancy())
			if scoreBreakdown.Score() < minScore {
				errs = append(errs, fmt.Errorf("%w: (%s): %v < %v", errLowScore, scoreBreakdown.String(), scoreBreakdown.Score(), minScore))
			}
		}
	}

	return len(errs) == 0, newUnusableHostResult(errs, gougingBreakdown, scoreBreakdown)
}

// isUsableContract returns whether the given contract is
// - usable -> can be used in the contract set
// - recoverable -> can be usable in the contract set if it is refreshed/renewed
// - refresh -> should be refreshed
// - renew -> should be renewed
func (c *contractor) isUsableContract(cfg api.AutopilotConfig, ci contractInfo, bh uint64, renterFunds types.Currency, f *ipFilter) (usable, recoverable, refresh, renew bool, reasons []string) {
	contract, s, pt := ci.contract, ci.settings, ci.priceTable

	usable = true
	if bh > contract.EndHeight() {
		reasons = append(reasons, errContractExpired.Error())
		usable = false
		recoverable = false
		refresh = false
		renew = false
	} else if contract.Revision.RevisionNumber == math.MaxUint64 {
		reasons = append(reasons, errContractMaxRevisionNumber.Error())
		usable = false
		recoverable = false
		refresh = false
		renew = false
	} else {
		if isOutOfCollateral(contract, s, pt, renterFunds, cfg.Contracts.Period, bh) {
			reasons = append(reasons, errContractOutOfCollateral.Error())
			usable = false
			recoverable = true
			refresh = true
			renew = false
		}
		if isOutOfFunds(cfg, s, contract) {
			reasons = append(reasons, errContractOutOfFunds.Error())
			usable = false
			recoverable = true
			refresh = true
			renew = false
		}
		if shouldRenew, secondHalf := isUpForRenewal(cfg, *contract.Revision, bh); shouldRenew {
			reasons = append(reasons, fmt.Errorf("%w; second half: %t", errContractUpForRenewal, secondHalf).Error())
			usable = usable && !secondHalf // only unusable if in second half of renew window
			recoverable = true
			refresh = false
			renew = true
		}
	}

	// IP check should be last since it modifies the filter
	shouldFilter := !cfg.Hosts.AllowRedundantIPs && (usable || recoverable)
	if shouldFilter && f.IsRedundantIP(contract.HostIP, contract.HostKey) {
		reasons = append(reasons, errHostRedundantIP.Error())
		usable = false
		recoverable = false // do not use in the contract set, but keep it around for downloads
		renew = false       // do not renew, but allow refreshes so the contracts stays funded
	}
	return
}

func isOutOfFunds(cfg api.AutopilotConfig, s rhpv2.HostSettings, c api.Contract) bool {
	// TotalCost should never be zero but for legacy reasons we check and return
	// true should it be the case
	if c.TotalCost.IsZero() {
		return true
	}

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
func isOutOfCollateral(c api.Contract, s rhpv2.HostSettings, pt rhpv3.HostPriceTable, renterFunds types.Currency, period, blockHeight uint64) bool {
	// Compute the expected storage for the contract given the funds we are
	// willing to put into it.
	// Note: we use the full period here even though we are checking whether to
	// do a refresh. Otherwise, the 'expectedStorage' would would become
	// ridiculously large the closer the contract is to its end height.
	expectedStorage := renterFundsToExpectedStorage(renterFunds, period, pt)
	// Cap the expected storage at the remaining storage of the host. If the
	// host doesn't have any storage left, there is no point in adding
	// collateral.
	if expectedStorage > s.RemainingStorage {
		expectedStorage = s.RemainingStorage
	}
	newCollateral := rhpv2.ContractRenewalCollateral(c.Revision.FileContract, expectedStorage, s, blockHeight, c.EndHeight())
	return isBelowCollateralThreshold(newCollateral, c.RemainingCollateral(s))
}

// isBelowCollateralThreshold returns true if the actualCollateral is below a
// certain percentage of newCollateral. The newCollateral is the amount of
// unallocated collateral in a contract after refreshing it and the
// actualCollateral is the current amount of unallocated collateral in the
// contract.
func isBelowCollateralThreshold(newCollateral, actualCollateral types.Currency) bool {
	if newCollateral.IsZero() {
		// Protect against division-by-zero. This can happen for 2 reasons.
		// 1. the collateral is already at the host's max collateral so a
		// refresh wouldn't result in any new unallocated collateral.
		// 2. the host has no more remaining storage so a refresh would only
		// lead to unallocated collateral that we can't use.
		// In both cases we don't gain anything from refreshing the contract.
		// NOTE: This causes us to not immediately consider contracts as bad
		// even though we can't upload to them anymore. This is fine since the
		// collateral score or remaining storage score should filter these
		// contracts out eventually.
		return false
	}
	collateral := big.NewRat(0, 1).SetFrac(actualCollateral.Big(), newCollateral.Big())
	threshold := big.NewRat(minContractCollateralThresholdNumerator, minContractCollateralThresholdDenominator)
	return collateral.Cmp(threshold) < 0
}

func isUpForRenewal(cfg api.AutopilotConfig, r types.FileContractRevision, blockHeight uint64) (shouldRenew, secondHalf bool) {
	shouldRenew = blockHeight+cfg.Contracts.RenewWindow >= r.EndHeight()
	secondHalf = blockHeight+cfg.Contracts.RenewWindow/2 >= r.EndHeight()
	return
}
