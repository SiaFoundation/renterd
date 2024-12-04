package contractor

import (
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

type contract struct {
	Revision *api.Revision
	api.ContractMetadata
}

// EndHeight returns the height at which the host is no longer obligated to
// store contract data.
func (c contract) EndHeight() uint64 { return c.WindowStart }

// FileSize returns the current Size of the contract.
func (c contract) FileSize() uint64 {
	if c.Revision == nil {
		return c.Size // use latest recorded value if we don't have a recent revision
	}
	return c.Revision.Filesize
}

// RenterFunds returns the funds remaining in the contract's Renter payout.
func (c contract) RenterFunds() types.Currency {
	return c.Revision.RenterOutput.Value
}

// RemainingCollateral returns the remaining collateral in the contract.
func (c contract) RemainingCollateral() types.Currency {
	if c.Revision.MissedHostValue.Cmp(c.ContractPrice) < 0 {
		return types.ZeroCurrency
	}
	return c.Revision.MissedHostValue.Sub(c.ContractPrice)
}
