package api

import (
	"errors"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
)

const (
	ContractStateInvalid  = "invalid"
	ContractStateUnknown  = "unknown"
	ContractStatePending  = "pending"
	ContractStateActive   = "active"
	ContractStateComplete = "complete"
	ContractStateFailed   = "failed"
)

const (
	ContractArchivalReasonHostPruned = "hostpruned"
	ContractArchivalReasonRemoved    = "removed"
	ContractArchivalReasonRenewed    = "renewed"
)

var (
	// ErrContractNotFound is returned when a contract can't be retrieved from
	// the database.
	ErrContractNotFound = errors.New("couldn't find contract")

	// ErrContractSetNotFound is returned when a contract set can't be retrieved
	// from the database.
	ErrContractSetNotFound = errors.New("couldn't find contract set")
)

type (
	// A Contract wraps the contract metadata with the latest contract revision.
	Contract struct {
		ContractMetadata
		Revision *types.FileContractRevision `json:"revision"`
	}

	// ContractSize contains information about the size of the contract and
	// about how much of the contract data can be pruned.
	ContractSize struct {
		Prunable uint64 `json:"prunable"`
		Size     uint64 `json:"size"`
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
		State          string `json:"state"`
		WindowStart    uint64 `json:"windowStart"`
		WindowEnd      uint64 `json:"windowEnd"`

		ContractPrice types.Currency       `json:"contractPrice"`
		RenewedFrom   types.FileContractID `json:"renewedFrom"`
		Spending      ContractSpending     `json:"spending"`
		TotalCost     types.Currency       `json:"totalCost"`
	}

	// ContractPrunableData wraps a contract's size information with its id.
	ContractPrunableData struct {
		ID types.FileContractID `json:"id"`
		ContractSize
	}

	// ContractSpending contains all spending details for a contract.
	ContractSpending struct {
		Uploads     types.Currency `json:"uploads"`
		Downloads   types.Currency `json:"downloads"`
		FundAccount types.Currency `json:"fundAccount"`
		Deletions   types.Currency `json:"deletions"`
		SectorRoots types.Currency `json:"sectorRoots"`
	}

	ContractSpendingRecord struct {
		ContractSpending
		ContractID     types.FileContractID `json:"contractID"`
		RevisionNumber uint64               `json:"revisionNumber"`
		Size           uint64               `json:"size"`

		MissedHostPayout  types.Currency `json:"missedHostPayout"`
		ValidRenterPayout types.Currency `json:"validRenterPayout"`
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
		State          string `json:"state"`
		WindowStart    uint64 `json:"windowStart"`
		WindowEnd      uint64 `json:"windowEnd"`
	}
)

type (
	// ContractAcquireRequest is the request type for the /contract/acquire
	// endpoint.
	ContractAcquireRequest struct {
		Duration DurationMS `json:"duration"`
		Priority int        `json:"priority"`
	}

	// ContractAcquireResponse is the response type for the /contract/:id/acquire
	// endpoint.
	ContractAcquireResponse struct {
		LockID uint64 `json:"lockID"`
	}

	// ContractAddRequest is the request type for the /contract/:id endpoint.
	ContractAddRequest struct {
		Contract      rhpv2.ContractRevision `json:"contract"`
		ContractPrice types.Currency         `json:"contractPrice"`
		StartHeight   uint64                 `json:"startHeight"`
		State         string                 `json:"state,omitempty"`
		TotalCost     types.Currency         `json:"totalCost"`
	}

	// ContractKeepaliveRequest is the request type for the /contract/:id/keepalive
	// endpoint.
	ContractKeepaliveRequest struct {
		Duration DurationMS `json:"duration"`
		LockID   uint64     `json:"lockID"`
	}

	// ContractAcquireRequest is the request type for the /contract/:id/release
	// endpoint.
	ContractReleaseRequest struct {
		LockID uint64 `json:"lockID"`
	}

	// ContractRenewedRequest is the request type for the /contract/:id/renewed
	// endpoint.
	ContractRenewedRequest struct {
		Contract      rhpv2.ContractRevision `json:"contract"`
		ContractPrice types.Currency         `json:"contractPrice"`
		RenewedFrom   types.FileContractID   `json:"renewedFrom"`
		StartHeight   uint64                 `json:"startHeight"`
		State         string                 `json:"state,omitempty"`
		TotalCost     types.Currency         `json:"totalCost"`
	}

	// ContractRootsResponse is the response type for the /contract/:id/roots
	// endpoint.
	ContractRootsResponse struct {
		Roots     []types.Hash256 `json:"roots"`
		Uploading []types.Hash256 `json:"uploading"`
	}

	// ContractsArchiveRequest is the request type for the /contracts/archive endpoint.
	ContractsArchiveRequest = map[types.FileContractID]string

	// ContractsPrunableDataResponse is the response type for the
	// /contracts/prunable endpoint.
	ContractsPrunableDataResponse struct {
		Contracts     []ContractPrunableData `json:"contracts"`
		TotalPrunable uint64                 `json:"totalPrunable"`
		TotalSize     uint64                 `json:"totalSize"`
	}
)

// Add returns the sum of the current and given contract spending.
func (x ContractSpending) Add(y ContractSpending) (z ContractSpending) {
	z.Uploads = x.Uploads.Add(y.Uploads)
	z.Downloads = x.Downloads.Add(y.Downloads)
	z.FundAccount = x.FundAccount.Add(y.FundAccount)
	z.Deletions = x.Deletions.Add(y.Deletions)
	z.SectorRoots = x.SectorRoots.Add(y.SectorRoots)
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
