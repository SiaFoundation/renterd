package api

import (
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/types"
)

type (
	// A Contract wraps the contract metadata with the latest contract revision.
	Contract struct {
		ContractMetadata
		Revision types.FileContractRevision `json:"revision"`
	}

	// ContractMetadata contains all metadata for a contract.
	ContractMetadata struct {
		ID          types.FileContractID `json:"id"`
		HostIP      string               `json:"hostIP"`
		HostKey     consensus.PublicKey  `json:"hostKey"`
		StartHeight uint64               `json:"startHeight"`

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
func (c Contract) EndHeight() uint64 {
	return uint64(c.Revision.EndHeight())
}

// FileSize returns the current Size of the contract.
func (c Contract) FileSize() uint64 {
	return c.Revision.NewFileSize
}

// HostKey returns the public key of the host.
func (c Contract) HostKey() (pk consensus.PublicKey) {
	copy(pk[:], c.Revision.UnlockConditions.PublicKeys[1].Key)
	return
}

// RenterFunds returns the funds remaining in the contract's Renter payout.
func (c Contract) RenterFunds() types.Currency {
	return c.Revision.NewValidProofOutputs[0].Value
}
