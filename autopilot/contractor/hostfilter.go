package contractor

import (
	"errors"
	"fmt"
	"math"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
)

const (
	// ContractConfirmationDeadline is the number of blocks since its start
	// height we wait for a contract to appear on chain.
	ContractConfirmationDeadline = 18
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
func (c *Contractor) isUsableContract(cfg api.AutopilotConfig, contract contract, inSet bool, bh uint64, f *hostSet) (usable, refresh, renew bool, reasons []string) {
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
		if contract.IsOutOfCollateral() {
			reasons = append(reasons, errContractOutOfCollateral.Error())
			usable = usable && inSet && c.shouldForgiveFailedRefresh(contract.ID)
			refresh = true
			renew = false
		}
		if contract.IsOutOfFunds() {
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

func (c contract) IsOutOfFunds() bool {
	// InitialRenterFunds should never be zero but for legacy reasons we check
	// and return true should it be the case
	if c.InitialRenterFunds.IsZero() {
		return true
	}
	// contract is out of funds when the remaining funds are less than 10% of
	// the initial funds
	return c.RenterFunds().Cmp(c.InitialRenterFunds.Div64(10)) <= 0
}

func (c contract) IsOutOfCollateral() bool {
	// InitialRenterFunds should never be zero but for legacy reasons we check
	// and return true should it be the case
	if c.InitialRenterFunds.IsZero() {
		return true
	}
	// contract is out of collateral if the remaining collateral is below
	// MinCollateral
	return c.RemainingCollateral().Cmp(MinCollateral) <= 0
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
