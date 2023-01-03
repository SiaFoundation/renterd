package api

import (
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
)

type (
	// A Contract contains all information about a contract with a host.
	Contract struct {
		ID          types.FileContractID `json:"id"`
		HostIP      string               `json:"hostIP"`
		HostKey     consensus.PublicKey  `json:"hostKey"`
		StartHeight uint64               `json:"startHeight"`

		ContractMetadata
	}

	// ContractMetadata contains all metadata for a contract.
	ContractMetadata struct {
		RenewedFrom types.FileContractID `json:"renewedFrom"`
		Spending    ContractSpending     `json:"spending"`
		TotalCost   types.Currency       `json:"totalCost"`
	}

	// ContractSpending contains all spending details for a contract.
	ContractSpending struct {
		Uploads     types.Currency `json:"uploads"`
		Downloads   types.Currency `json:"downloads"`
		FundAccount types.Currency `json:"fundAccount"`
	}

	// An ArchivedContract contains all information about a contract with a host
	// that has been moved to the archive either due to expiring or being renewed.
	ArchivedContract struct {
		ID        types.FileContractID `json:"id"`
		HostKey   consensus.PublicKey  `json:"hostKey"`
		RenewedTo types.FileContractID `json:"renewedTo"`
		Spending  ContractSpending     `json:"spending"`
	}

	Revision struct {
		Contract `json:"contract"`
		Revision rhpv2.ContractRevision `json:"revision"`
	}
)

// Add returns the sum of the current and given contract spending.
func (x ContractSpending) Add(y ContractSpending) (s ContractSpending) {
	s.Uploads = x.Uploads.Add(y.Uploads)
	s.Downloads = x.Downloads.Add(y.Downloads)
	s.FundAccount = x.FundAccount.Add(y.FundAccount)
	return
}

// EndHeight returns the height at which the host is no longer obligated to
// store contract data.
func (c Revision) EndHeight() uint64 {
	return uint64(c.Revision.EndHeight())
}

// FileSize returns the current Size of the contract.
func (c Revision) FileSize() uint64 {
	return c.Revision.Revision.NewFileSize
}

// HostKey returns the public key of the host.
func (c Revision) HostKey() (pk consensus.PublicKey) {
	return c.Revision.HostKey()
}

// RenterFunds returns the funds remaining in the contract's Renter payout.
func (c Revision) RenterFunds() types.Currency {
	return c.Revision.RenterFunds()
}
