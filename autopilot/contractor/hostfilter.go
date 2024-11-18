package contractor

import (
	"errors"
	"fmt"
	"math"
	"math/big"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
)

const (
	// ContractConfirmationDeadline is the number of blocks since its start
	// height we wait for a contract to appear on chain.
	ContractConfirmationDeadline = 18

	// minContractFundUploadThreshold is the percentage of contract funds
	// remaining at which the contract gets marked as not good for upload
	minContractFundUploadThreshold = float64(0.05) // 5%

	// minContractCollateralDenominator is used to define the percentage of
	// remaining collateral in a contract in relation to its potential
	// acquirable storage below which the contract is considered to be
	// out-of-collateral.
	minContractCollateralDenominator = 20 // 5%
)

var (
	errContractBeyondV2RequireHeight = errors.New("contract is beyond v2 require height")
	errContractOutOfCollateral       = errors.New("contract is out of collateral")
	errContractOutOfFunds            = errors.New("contract is out of funds")
	errContractUpForRenewal          = errors.New("contract is up for renewal")
	errContractMaxRevisionNumber     = errors.New("contract has reached max revision number")
	errContractNoRevision            = errors.New("contract has no revision")
	errContractExpired               = errors.New("contract has expired")
	errContractNotConfirmed          = errors.New("contract hasn't been confirmed on chain in time")
)

type unusableHostsBreakdown struct {
	blocked               uint64
	offline               uint64
	lowscore              uint64
	redundantip           uint64
	gouging               uint64
	notacceptingcontracts uint64
	notannounced          uint64
	notcompletingscan     uint64
}

func (u *unusableHostsBreakdown) track(ub api.HostUsabilityBreakdown) {
	if ub.Blocked {
		u.blocked++
	}
	if ub.Offline {
		u.offline++
	}
	if ub.LowScore {
		u.lowscore++
	}
	if ub.RedundantIP {
		u.redundantip++
	}
	if ub.Gouging {
		u.gouging++
	}
	if ub.NotAcceptingContracts {
		u.notacceptingcontracts++
	}
	if ub.NotAnnounced {
		u.notannounced++
	}
	if ub.NotCompletingScan {
		u.notcompletingscan++
	}
}

func (u *unusableHostsBreakdown) keysAndValues() []interface{} {
	values := []interface{}{
		"blocked", u.blocked,
		"offline", u.offline,
		"lowscore", u.lowscore,
		"redundantip", u.redundantip,
		"gouging", u.gouging,
		"notacceptingcontracts", u.notacceptingcontracts,
		"notcompletingscan", u.notcompletingscan,
		"notannounced", u.notannounced,
	}
	for i := 0; i < len(values); i += 2 {
		if values[i+1].(uint64) == 0 {
			values = append(values[:i], values[i+2:]...)
			i -= 2
		}
	}
	return values
}

// isUsableContract returns whether the given contract is
// - usable -> can be used in the contract set
// - recoverable -> can be usable in the contract set if it is refreshed/renewed
// - refresh -> should be refreshed
// - renew -> should be renewed
func (c *Contractor) isUsableContract(cfg api.AutopilotConfig, s rhpv2.HostSettings, pt rhpv3.HostPriceTable, rs api.RedundancySettings, contract contract, inSet bool, bh uint64, f *hostSet) (usable, refresh, renew bool, reasons []string) {
	usable = true
	if bh > contract.EndHeight() {
		reasons = append(reasons, errContractExpired.Error())
		usable = false
		refresh = false
		renew = false
	} else if contract.Revision.RevisionNumber == math.MaxUint64 {
		reasons = append(reasons, errContractMaxRevisionNumber.Error())
		usable = false
		refresh = false
		renew = false
	} else {
		if isOutOfCollateral(cfg, rs, contract, s, pt) {
			reasons = append(reasons, errContractOutOfCollateral.Error())
			usable = usable && inSet && c.shouldForgiveFailedRefresh(contract.ID)
			refresh = true
			renew = false
		}
		if isOutOfFunds(cfg, pt, contract) {
			reasons = append(reasons, errContractOutOfFunds.Error())
			usable = usable && inSet && c.shouldForgiveFailedRefresh(contract.ID)
			refresh = true
			renew = false
		}
		if shouldRenew, secondHalf := isUpForRenewal(cfg, *contract.Revision, bh); shouldRenew {
			reasons = append(reasons, fmt.Errorf("%w; second half: %t", errContractUpForRenewal, secondHalf).Error())
			usable = usable && !secondHalf // only unusable if in second half of renew window
			refresh = false
			renew = true
		}
	}
	return
}

func isOutOfFunds(cfg api.AutopilotConfig, pt rhpv3.HostPriceTable, c contract) bool {
	// InitialRenterFunds should never be zero but for legacy reasons we check
	// and return true should it be the case
	if c.InitialRenterFunds.IsZero() {
		return true
	}

	sectorPrice, _ := pt.BaseCost().
		Add(pt.AppendSectorCost(cfg.Contracts.Period)).
		Add(pt.ReadSectorCost(rhpv2.SectorSize)).
		Total()
	percentRemaining, _ := big.NewRat(0, 1).SetFrac(c.RenterFunds().Big(), c.InitialRenterFunds.Big()).Float64()

	return c.RenterFunds().Cmp(sectorPrice.Mul64(3)) < 0 || percentRemaining < minContractFundUploadThreshold
}

// isOutOfCollateral returns 'true' if the remaining/unallocated collateral in
// the contract is below a certain threshold of the collateral we would try to
// put into a contract upon renew.
func isOutOfCollateral(cfg api.AutopilotConfig, rs api.RedundancySettings, c contract, s rhpv2.HostSettings, pt rhpv3.HostPriceTable) bool {
	minCollateral := minRemainingCollateral(cfg, rs, c.RenterFunds(), s, pt)
	return c.RemainingCollateral().Cmp(minCollateral) < 0
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

func isUpForRenewal(cfg api.AutopilotConfig, r api.Revision, blockHeight uint64) (shouldRenew, secondHalf bool) {
	shouldRenew = blockHeight+cfg.Contracts.RenewWindow >= r.EndHeight()
	secondHalf = blockHeight+cfg.Contracts.RenewWindow/2 >= r.EndHeight()
	return
}

// checkHost performs a series of checks on the host.
func checkHost(gc gouging.Checker, sh scoredHost, minScore float64, period uint64) *api.HostCheck {
	h := sh.host

	// prepare host breakdown fields
	var gb api.HostGougingBreakdown
	var ub api.HostUsabilityBreakdown

	// blocked status does not influence what host info is calculated
	if h.Blocked {
		ub.Blocked = true
	}

	// calculate remaining host info fields
	if !h.IsAnnounced() {
		ub.NotAnnounced = true
	} else if !h.Scanned {
		ub.NotCompletingScan = true
	} else {
		// online check
		if !h.IsOnline() {
			ub.Offline = true
		}

		if h.IsV2() {
			// accepting contracts check
			if !h.V2Settings.AcceptingContracts {
				ub.NotAcceptingContracts = true
			}

			// max duration check
			ub.LowMaxDuration = period > h.V2Settings.MaxContractDuration

			// gouging breakdown
			gb = gc.CheckV2(h.V2Settings)
		} else {
			// accepting contracts check
			if !h.Settings.AcceptingContracts {
				ub.NotAcceptingContracts = true
			}

			// max duration check
			ub.LowMaxDuration = period > h.Settings.MaxDuration || period > h.PriceTable.MaxDuration

			// gouging breakdown
			gb = gc.CheckV1(&h.Settings, &h.PriceTable.HostPriceTable)
		}

		// perform gouging and score checks
		if gb.Gouging() {
			ub.Gouging = true
		} else if minScore > 0 && !(sh.score > minScore) {
			ub.LowScore = true
		}
	}

	return &api.HostCheck{
		UsabilityBreakdown: ub,
		GougingBreakdown:   gb,
		ScoreBreakdown:     sh.sb,
	}
}

func newScoredHost(h api.Host, sb api.HostScoreBreakdown) scoredHost {
	return scoredHost{
		host:  h,
		sb:    sb,
		score: sb.Score(),
	}
}

func scoreHost(h api.Host, cfg api.AutopilotConfig, gs api.GougingSettings, expectedRedundancy float64) scoredHost {
	return newScoredHost(h, hostScore(cfg, gs, h, expectedRedundancy))
}
