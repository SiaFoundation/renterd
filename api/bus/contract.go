package bus

import (
	"go.sia.tech/renterd/internal/consensus"
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
)

// Add returns the sum of the current and given contract spending.
func (x ContractSpending) Add(y ContractSpending) (s ContractSpending) {
	s.Uploads = x.Uploads.Add(y.Uploads)
	s.Downloads = x.Downloads.Add(y.Downloads)
	s.FundAccount = x.FundAccount.Add(y.FundAccount)
	return
}
