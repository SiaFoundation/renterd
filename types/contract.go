package types

import (
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/types"
)

// A Contract uniquely identifies a Sia file contract on a host, along with the
// host's IP.
type Contract struct {
	HostIP      string `json:"hostIP"`
	StartHeight uint64 `json:"startHeight"`

	Revision   types.FileContractRevision
	Signatures [2]types.TransactionSignature

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
