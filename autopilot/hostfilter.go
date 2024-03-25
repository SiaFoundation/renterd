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
	"go.sia.tech/renterd/worker"
)

const (
	// minContractFundUploadThreshold is the percentage of contract funds
	// remaining at which the contract gets marked as not good for upload
	minContractFundUploadThreshold = float64(0.05) // 5%

	// minContractCollateralDenominator is used to define the percentage of
	// remaining collateral in a contract in relation to its potential
	// acquirable storage below which the contract is considered to be
	// out-of-collateral.
	minContractCollateralDenominator = 20 // 5%

	// contractConfirmationDeadline is the number of blocks since its start
	// height we wait for a contract to appear on chain.
	contractConfirmationDeadline = 18
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
	errContractNotConfirmed      = errors.New("contract hasn't been confirmed on chain in time")
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
func isUsableHost(cfg api.AutopilotConfig, rs api.RedundancySettings, gc worker.GougingChecker, h api.Host, minScore float64, storedData uint64) (bool, unusableHostResult) {
	if rs.Validate() != nil {
		panic("invalid redundancy settings were supplied - developer error")
	}

	var errs []error
	if h.Blocked {
		errs = append(errs, errHostBlocked)
	}

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
			errs = append(errs, fmt.Errorf("%w: %v", errHostPriceGouging, gougingBreakdown))
		} else if minScore > 0 {
			// perform scoring checks
			//
			// NOTE: only perform these scoring checks if we know the host is
			// not gouging, this because the core package does not have overflow
			// checks in its cost calculations needed to calculate the period
			// cost
			scoreBreakdown = hostScore(cfg, h.Host, storedData, rs.Redundancy())
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
func (c *contractor) isUsableContract(cfg api.AutopilotConfig, state state, ci contractInfo, bh uint64, f *ipFilter) (usable, recoverable, refresh, renew bool, reasons []string) {
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
		if isOutOfCollateral(cfg, state.rs, contract, s, pt) {
			reasons = append(reasons, errContractOutOfCollateral.Error())
			usable = false
			recoverable = true
			refresh = true
			renew = false
		}
		if isOutOfFunds(cfg, pt, contract) {
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

func isOutOfFunds(cfg api.AutopilotConfig, pt rhpv3.HostPriceTable, c api.Contract) bool {
	// TotalCost should never be zero but for legacy reasons we check and return
	// true should it be the case
	if c.TotalCost.IsZero() {
		return true
	}

	sectorPrice, _ := pt.BaseCost().
		Add(pt.AppendSectorCost(cfg.Contracts.Period)).
		Add(pt.ReadSectorCost(rhpv2.SectorSize)).
		Total()
	percentRemaining, _ := big.NewRat(0, 1).SetFrac(c.RenterFunds().Big(), c.TotalCost.Big()).Float64()

	return c.RenterFunds().Cmp(sectorPrice.Mul64(3)) < 0 || percentRemaining < minContractFundUploadThreshold
}

// isOutOfCollateral returns 'true' if the remaining/unallocated collateral in
// the contract is below a certain threshold of the collateral we would try to
// put into a contract upon renew.
func isOutOfCollateral(cfg api.AutopilotConfig, rs api.RedundancySettings, c api.Contract, s rhpv2.HostSettings, pt rhpv3.HostPriceTable) bool {
	min := minRemainingCollateral(cfg, rs, c.RenterFunds(), s, pt)
	return c.RemainingCollateral().Cmp(min) < 0
}

// minNewCollateral returns the minimum amount of unallocated collateral that a
// contract should contain after a refresh given the current amount of
// unallocated collateral.
func minRemainingCollateral(cfg api.AutopilotConfig, rs api.RedundancySettings, renterFunds types.Currency, s rhpv2.HostSettings, pt rhpv3.HostPriceTable) types.Currency {
	// Compute the expected storage for the contract given its remaining funds.
	// Note: we use the full period here even though we are checking whether to
	// do a refresh. Otherwise, the 'expectedStorage' would would become
	// ridiculously large the closer the contract is to its end height.
	expectedStorage := renterFundsToExpectedStorage(renterFunds, cfg.Contracts.Period, pt)

	// Cap the expected storage at twice the ideal amount of data we expect to
	// store on a host. Even if we could afford more storage, there is no point
	// in locking up more collateral than we expect to require.
	idealDataPerHost := float64(cfg.Contracts.Storage) * rs.Redundancy() / float64(cfg.Contracts.Amount)
	allocationPerHost := idealDataPerHost * 2
	if expectedStorage > uint64(allocationPerHost) {
		expectedStorage = uint64(allocationPerHost)
	}

	// Cap the expected storage at the remaining storage of the host. If the
	// host doesn't have any storage left, there is no point in adding
	// collateral.
	if expectedStorage > s.RemainingStorage {
		expectedStorage = s.RemainingStorage
	}

	// If no storage is expected, return zero.
	if expectedStorage == 0 {
		return types.ZeroCurrency
	}

	// Computet the collateral for a single sector.
	_, sectorCollateral := pt.BaseCost().
		Add(pt.AppendSectorCost(cfg.Contracts.Period)).
		Add(pt.ReadSectorCost(rhpv2.SectorSize)).
		Total()

	// The expectedStorageCollateral is 5% of the collateral we'd need to store
	// all of the expectedStorage.
	minExpectedStorageCollateral := sectorCollateral.Mul64(expectedStorage / rhpv2.SectorSize).Div64(minContractCollateralDenominator)

	// The absolute minimum collateral we want to put into a contract is 3
	// sectors worth of collateral.
	minCollateral := sectorCollateral.Mul64(3)

	// Return the larger of the two.
	if minExpectedStorageCollateral.Cmp(minCollateral) > 0 {
		minCollateral = minExpectedStorageCollateral
	}
	return minCollateral
}

func isUpForRenewal(cfg api.AutopilotConfig, r types.FileContractRevision, blockHeight uint64) (shouldRenew, secondHalf bool) {
	shouldRenew = blockHeight+cfg.Contracts.RenewWindow >= r.EndHeight()
	secondHalf = blockHeight+cfg.Contracts.RenewWindow/2 >= r.EndHeight()
	return
}
