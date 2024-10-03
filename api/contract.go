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

type ContractState string

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
		ID      types.FileContractID `json:"id"`
		HostKey types.PublicKey      `json:"hostKey"`
		V2      bool                 `json:"v2"`

		ProofHeight    uint64               `json:"proofHeight"`
		RenewedFrom    types.FileContractID `json:"renewedFrom"`
		RevisionHeight uint64               `json:"revisionHeight"`
		RevisionNumber uint64               `json:"revisionNumber"`
		Size           uint64               `json:"size"`
		StartHeight    uint64               `json:"startHeight"`
		State          string               `json:"state"`
		WindowStart    uint64               `json:"windowStart"`
		WindowEnd      uint64               `json:"windowEnd"`

		// costs & spending
		ContractPrice      types.Currency   `json:"contractPrice"`
		InitialRenterFunds types.Currency   `json:"initialRenterFunds"`
		Spending           ContractSpending `json:"spending"`

		// following fields are decorated
		HostIP       string   `json:"hostIP"`
		ContractSets []string `json:"contractSets,omitempty"`
		SiamuxAddr   string   `json:"siamuxAddr,omitempty"`

		// following fields are only set on archived contracts
		ArchivalReason string               `json:"archivalReason,omitempty"`
		RenewedTo      types.FileContractID `json:"renewedTo,omitempty"`
	}

	// ContractPrunableData wraps a contract's size information with its id.
	ContractPrunableData struct {
		ID types.FileContractID `json:"id"`
		ContractSize
	}

	// ContractSpending contains all spending details for a contract.
	ContractSpending struct {
		Deletions   types.Currency `json:"deletions"`
		FundAccount types.Currency `json:"fundAccount"`
		SectorRoots types.Currency `json:"sectorRoots"`
		Uploads     types.Currency `json:"uploads"`
	}

	ContractSpendingRecord struct {
		ContractSpending
		ContractID     types.FileContractID `json:"contractID"`
		RevisionNumber uint64               `json:"revisionNumber"`
		Size           uint64               `json:"size"`

		MissedHostPayout  types.Currency `json:"missedHostPayout"`
		ValidRenterPayout types.Currency `json:"validRenterPayout"`
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
		ContractPrice      types.Currency         `json:"contractPrice"`
		InitialRenterFunds types.Currency         `json:"initialRenterFunds"`
		Revision           rhpv2.ContractRevision `json:"revision"`
		StartHeight        uint64                 `json:"startHeight"`
		State              string                 `json:"state,omitempty"`
	}

	// ContractFormRequest is the request type for the POST /contracts endpoint.
	ContractFormRequest struct {
		EndHeight      uint64          `json:"endHeight"`
		HostCollateral types.Currency  `json:"hostCollateral"`
		HostKey        types.PublicKey `json:"hostKey"`
		HostIP         string          `json:"hostIP"`
		RenterFunds    types.Currency  `json:"renterFunds"`
		RenterAddress  types.Address   `json:"renterAddress"`
	}

	// ContractKeepaliveRequest is the request type for the /contract/:id/keepalive
	// endpoint.
	ContractKeepaliveRequest struct {
		Duration DurationMS `json:"duration"`
		LockID   uint64     `json:"lockID"`
	}

	// ContractPruneRequest is the request type for the /contract/:id/prune
	// endpoint.
	ContractPruneRequest struct {
		Timeout DurationMS `json:"timeout"`
	}

	// ContractPruneResponse is the response type for the /contract/:id/prune
	// endpoint.
	ContractPruneResponse struct {
		ContractSize uint64 `json:"size"`
		Pruned       uint64 `json:"pruned"`
		Remaining    uint64 `json:"remaining"`
		Error        string `json:"error,omitempty"`
	}

	// ContractAcquireRequest is the request type for the /contract/:id/release
	// endpoint.
	ContractReleaseRequest struct {
		LockID uint64 `json:"lockID"`
	}

	// ContractRenewRequest is the request type for the /contract/:id/renew
	// endpoint.
	ContractRenewRequest struct {
		EndHeight          uint64         `json:"endHeight"`
		ExpectedNewStorage uint64         `json:"expectedNewStorage"`
		MaxFundAmount      types.Currency `json:"maxFundAmount"`
		MinNewCollateral   types.Currency `json:"minNewCollateral"`
		RenterFunds        types.Currency `json:"renterFunds"`
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

	ContractsOpts struct {
		ContractSet string `json:"contractset"`
		FilterMode  string `json:"filterMode"`
	}
)

// Total returns the total cost of the contract spending.
func (x ContractSpending) Total() types.Currency {
	return x.Uploads.Add(x.FundAccount).Add(x.Deletions).Add(x.SectorRoots)
}

// Add returns the sum of the current and given contract spending.
func (x ContractSpending) Add(y ContractSpending) (z ContractSpending) {
	z.Uploads = x.Uploads.Add(y.Uploads)
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
func (c Contract) RemainingCollateral() types.Currency {
	if c.Revision.MissedHostPayout().Cmp(c.ContractPrice) < 0 {
		return types.ZeroCurrency
	}
	return c.Revision.MissedHostPayout().Sub(c.ContractPrice)
}

// InSet returns whether the contract is in the given set.
func (cm ContractMetadata) InSet(set string) bool {
	for _, s := range cm.ContractSets {
		if s == set {
			return true
		}
	}
	return false
}
