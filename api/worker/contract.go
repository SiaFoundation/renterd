package worker

import (
	"go.sia.tech/renterd/api/bus"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
)

type Contract struct {
	bus.Contract `json:"contract"`
	Revision     rhpv2.ContractRevision `json:"revision"`
}

// EndHeight returns the height at which the host is no longer obligated to
// store contract data.
func (c Contract) EndHeight() uint64 {
	return uint64(c.Revision.EndHeight())
}

// FileSize returns the current Size of the contract.
func (c Contract) FileSize() uint64 {
	return c.Revision.Revision.NewFileSize
}

// HostKey returns the public key of the host.
func (c Contract) HostKey() (pk consensus.PublicKey) {
	return c.Revision.HostKey()
}

// RenterFunds returns the funds remaining in the contract's Renter payout.
func (c Contract) RenterFunds() types.Currency {
	return c.Revision.RenterFunds()
}
