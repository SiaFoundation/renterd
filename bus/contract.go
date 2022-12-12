package bus

import (
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/types"
)

// A Contract contains all information about a contract with a host.
type Contract struct {
	HostIP      string `json:"hostIP"`
	StartHeight uint64 `json:"startHeight"`

	Revision   types.FileContractRevision    `json:"revision"`
	Signatures [2]types.TransactionSignature `json:"signatures"`

	ContractMetadata
}

// ContractMetadata contains all metadata for a contract.
type ContractMetadata struct {
	RenewedFrom types.FileContractID `json:"renewedFrom"`
	Spending    ContractSpending     `json:"spending"`
	TotalCost   types.Currency       `json:"totalCost"`
}

// ContractSpending contains all spending details for a contract.
type ContractSpending struct {
	Uploads     types.Currency `json:"uploads"`
	Downloads   types.Currency `json:"downloads"`
	FundAccount types.Currency `json:"fundAccount"`
}

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
	return uint64(c.Revision.NewWindowStart)
}

// ID returns the ID of the original FileContract.
func (c Contract) ID() types.FileContractID {
	return c.Revision.ParentID
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
