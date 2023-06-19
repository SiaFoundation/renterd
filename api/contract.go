package api

import (
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
)

type (
	// A Contract wraps the contract metadata with the latest contract revision.
	Contract struct {
		ContractMetadata
		Revision *types.FileContractRevision `json:"revision"`
	}

	// ContractMetadata contains all metadata for a contract.
	ContractMetadata struct {
		ID         types.FileContractID `json:"id"`
		HostIP     string               `json:"hostIP"`
		HostKey    types.PublicKey      `json:"hostKey"`
		SiamuxAddr string               `json:"siamuxAddr"`

		ProofHeight    uint64 `json:"proofHeight"`
		RevisionHeight uint64 `json:"revisionHeight"`
		RevisionNumber uint64 `json:"revisionNumber"`
		Size           uint64 `json:"size"`
		StartHeight    uint64 `json:"startHeight"`
		WindowStart    uint64 `json:"windowStart"`
		WindowEnd      uint64 `json:"windowEnd"`

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

	ContractSpendingRecord struct {
		ContractSpending
		ContractID     types.FileContractID `json:"contractID"`
		RevisionNumber uint64               `json:"revisionNumber"`
		Size           uint64               `json:"size"`
	}

	// An ArchivedContract contains all information about a contract with a host
	// that has been moved to the archive either due to expiring or being renewed.
	ArchivedContract struct {
		ID        types.FileContractID `json:"id"`
		HostKey   types.PublicKey      `json:"hostKey"`
		RenewedTo types.FileContractID `json:"renewedTo"`
		Spending  ContractSpending     `json:"spending"`

		ProofHeight    uint64 `json:"proofHeight"`
		RevisionHeight uint64 `json:"revisionHeight"`
		RevisionNumber uint64 `json:"revisionNumber"`
		Size           uint64 `json:"size"`
		StartHeight    uint64 `json:"startHeight"`
		WindowStart    uint64 `json:"windowStart"`
		WindowEnd      uint64 `json:"windowEnd"`
	}
)

// Add returns the sum of the current and given contract spending.
func (x ContractSpending) Add(y ContractSpending) (z ContractSpending) {
	z.Uploads = x.Uploads.Add(y.Uploads)
	z.Downloads = x.Downloads.Add(y.Downloads)
	z.FundAccount = x.FundAccount.Add(y.FundAccount)
	return
}

// EndHeight returns the height at which the host is no longer obligated to
// store contract data.
func (c Contract) EndHeight() uint64 { return c.WindowStart }

// FileSize returns the current Size of the contract.
func (c Contract) FileSize() uint64 {
	if c.Revision == nil {
		return c.Size // use latest recorded value if we don't have a recent revision
	}
	return c.Revision.Filesize
}

// RenterFunds returns the funds remaining in the contract's Renter payout.
func (c Contract) RenterFunds() types.Currency {
	return c.Revision.ValidRenterPayout()
}

// RemainingCollateral returns the remaining collateral in the contract.
func (c Contract) RemainingCollateral(s rhpv2.HostSettings) types.Currency {
	if c.Revision.MissedHostPayout().Cmp(s.ContractPrice) < 0 {
		return types.ZeroCurrency
	}
	return c.Revision.MissedHostPayout().Sub(s.ContractPrice)
}
