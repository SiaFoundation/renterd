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
	errHostBlocked               = errors.New("host is blocked")
	errHostOffline               = errors.New("host is offline")
	errLowScore                  = errors.New("host's score is below minimum")
	errHostRedundantIP           = errors.New("host has redundant IP")
	errHostPriceGouging          = errors.New("host is price gouging")
	errHostNotAcceptingContracts = errors.New("host is not accepting contracts")
	errHostNotAnnounced          = errors.New("host is not announced")
	errHostNotScanned            = errors.New("host has not been scanned")
	errHostNoPriceTable          = errors.New("host has no pricetable")

	errContractOutOfCollateral   = errors.New("contract is out of collateral")
	errContractOutOfFunds        = errors.New("contract is out of funds")
	errContractUpForRenewal      = errors.New("contract is up for renewal")
	errContractMaxRevisionNumber = errors.New("contract has reached max revision number")
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
	notscanned            uint64
	nopricetable          uint64
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
		} else if errors.Is(err, errHostNotScanned) {
			u.notscanned++
		} else if errors.Is(err, errHostNoPriceTable) {
			u.nopricetable++
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
	if u.notscanned > 0 {
		reasons = append(reasons, errHostNotScanned.Error())
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
	u.gouging += other.gouging
	u.notacceptingcontracts += other.notacceptingcontracts
	u.notannounced += other.notannounced
	u.notscanned += other.notscanned
	u.nopricetable += other.nopricetable
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
		"notannounced", u.notannounced,
		"notscanned", u.notscanned,
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
func isUsableHost(cfg api.AutopilotConfig, rs api.RedundancySettings, gc worker.GougingChecker, f *ipFilter, h hostdb.Host, minScore float64, storedData uint64) (bool, unusableHostResult) {
	if rs.Validate() != nil {
		panic("invalid redundancy settings were supplied - developer error")
	}

	var errs []error
	var gougingBreakdown api.HostGougingBreakdown
	var scoreBreakdown api.HostScoreBreakdown

	if !h.IsAnnounced() {
		errs = append(errs, errHostNotAnnounced)
	} else if !h.Scanned {
		errs = append(errs, errHostNotScanned)
	} else {
		// online check
		if !h.IsOnline() {
			errs = append(errs, errHostOffline)
		}

		// redundant IP check
		if !cfg.Hosts.IgnoreRedundantIPs && f.isRedundantIP(h) {
			errs = append(errs, errHostRedundantIP)
		}

		// accepting contracts check
		if !h.Settings.AcceptingContracts {
			errs = append(errs, errHostNotAcceptingContracts)
		}

		// perform gouging checks
		if gougingBreakdown := gc.Check(&h.Settings, &h.PriceTable.HostPriceTable); gougingBreakdown.Gouging() {
			errs = append(errs, fmt.Errorf("%w: %v", errHostPriceGouging, gougingBreakdown.Reasons()))
		}

		// perform scoring checks
		if scoreBreakdown = hostScore(cfg, h, storedData, rs.Redundancy()); scoreBreakdown.Score() < minScore {
			errs = append(errs, fmt.Errorf("%w: %v < %v", errLowScore, scoreBreakdown.Score(), minScore))
		}
	}

	return len(errs) == 0, newUnusableHostResult(errs, gougingBreakdown, scoreBreakdown)
}

// isUsableContract returns whether the given contract is usable and whether it
// can be renewed, along with a list of reasons why it was deemed unusable.
func isUsableContract(cfg api.AutopilotConfig, ci contractInfo, bh uint64, renterFunds types.Currency) (usable, refresh, renew, archive bool, reasons []error) {
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
	if shouldRenew, secondHalf := isUpForRenewal(cfg, c.Revision, bh); shouldRenew {
		if secondHalf {
			reasons = append(reasons, errContractUpForRenewal) // only unusable if in second half of renew window
		}
		renew = true
		refresh = false
	}
	if c.Revision.RevisionNumber == math.MaxUint64 {
		reasons = append(reasons, errContractMaxRevisionNumber)
		archive = true // can't be revised anymore
		renew = false
		refresh = false
	}
	if bh > c.EndHeight() {
		reasons = append(reasons, errContractExpired)
		archive = true // expired
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

func isUpForRenewal(cfg api.AutopilotConfig, r types.FileContractRevision, blockHeight uint64) (shouldRenew, secondHalf bool) {
	shouldRenew = blockHeight+cfg.Contracts.RenewWindow >= r.EndHeight()
	secondHalf = blockHeight+cfg.Contracts.RenewWindow/2 >= r.EndHeight()
	return
}
