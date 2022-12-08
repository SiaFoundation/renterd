package bus

import (
	"fmt"
	"net/url"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/types"
	siatypes "go.sia.tech/siad/types"
)

// exported types from internal/consensus
type (
	// A PublicKey is an Ed25519 public key.
	PublicKey = consensus.PublicKey

	// A PrivateKey is an Ed25519 private key.
	PrivateKey = consensus.PrivateKey

	// ChainState represents the full state of the chain as of a particular block.
	ChainState = consensus.State

	SlabID uint
)

// LoadString is implemented for jape's DecodeParam.
func (sid *SlabID) LoadString(s string) (err error) {
	var slabID uint
	_, err = fmt.Sscan(s, &slabID)
	*sid = SlabID(slabID)
	return
}

// String encodes the SlabID as a string.
func (sid SlabID) String() string {
	return fmt.Sprint(uint8(sid))
}

// ConsensusState holds the current blockheight and whether we are synced or not.
type ConsensusState struct {
	BlockHeight uint64
	Synced      bool
}

// for encoding/decoding time.Time values in API params
type paramTime time.Time

func (t paramTime) String() string                { return url.QueryEscape((time.Time)(t).Format(time.RFC3339)) }
func (t *paramTime) UnmarshalText(b []byte) error { return (*time.Time)(t).UnmarshalText(b) }

// ContractAcquireRequest is the request type for the /contracts/:id/acquire
// endpoint.
type ContractAcquireRequest struct {
	Duration time.Duration
}
type ContractsIDAddRequest struct {
	Contract  rhpv2.Contract `json:"contract"`
	TotalCost siatypes.Currency
}
type ContractsIDRenewedRequest struct {
	Contract    rhpv2.Contract `json:"contract"`
	RenewedFrom siatypes.FileContractID
	TotalCost   siatypes.Currency
}

// ContractAcquireResponse is the response type for the /contracts/:id/acquire
// endpoint.
type ContractAcquireResponse struct {
	Locked   bool                          `json:"locked"`
	Revision siatypes.FileContractRevision `json:"revision,omitempty"`
}

// WalletFundRequest is the request type for the /wallet/fund endpoint.
type WalletFundRequest struct {
	Transaction siatypes.Transaction `json:"transaction"`
	Amount      siatypes.Currency    `json:"amount"`
}

// WalletFundResponse is the response type for the /wallet/fund endpoint.
type WalletFundResponse struct {
	Transaction siatypes.Transaction   `json:"transaction"`
	ToSign      []siatypes.OutputID    `json:"toSign"`
	DependsOn   []siatypes.Transaction `json:"dependsOn"`
}

// WalletSignRequest is the request type for the /wallet/sign endpoint.
type WalletSignRequest struct {
	Transaction   siatypes.Transaction   `json:"transaction"`
	ToSign        []siatypes.OutputID    `json:"toSign"`
	CoveredFields siatypes.CoveredFields `json:"coveredFields"`
}

// WalletRedistributeRequest is the request type for the /wallet/redistribute
// endpoint.
type WalletRedistributeRequest struct {
	Amount  siatypes.Currency `json:"amount"`
	Outputs int               `json:"outputs"`
}

// WalletPrepareFormRequest is the request type for the /wallet/prepare/form
// endpoint.
type WalletPrepareFormRequest struct {
	RenterKey      PrivateKey          `json:"renterKey"`
	HostKey        PublicKey           `json:"hostKey"`
	RenterFunds    siatypes.Currency   `json:"renterFunds"`
	RenterAddress  siatypes.UnlockHash `json:"renterAddress"`
	HostCollateral siatypes.Currency   `json:"hostCollateral"`
	EndHeight      uint64              `json:"endHeight"`
	HostSettings   rhpv2.HostSettings  `json:"hostSettings"`
}

// WalletPrepareRenewRequest is the request type for the /wallet/prepare/renew
// endpoint.
type WalletPrepareRenewRequest struct {
	Contract       siatypes.FileContractRevision `json:"contract"`
	RenterKey      PrivateKey                    `json:"renterKey"`
	HostKey        PublicKey                     `json:"hostKey"`
	RenterFunds    siatypes.Currency             `json:"renterFunds"`
	RenterAddress  siatypes.UnlockHash           `json:"renterAddress"`
	HostCollateral siatypes.Currency             `json:"hostCollateral"`
	EndHeight      uint64                        `json:"endHeight"`
	HostSettings   rhpv2.HostSettings            `json:"hostSettings"`
}

// WalletPrepareRenewResponse is the response type for the /wallet/prepare/renew
// endpoint.
type WalletPrepareRenewResponse struct {
	TransactionSet []siatypes.Transaction `json:"transactionSet"`
	FinalPayment   siatypes.Currency      `json:"finalPayment"`
}

// ObjectsResponse is the response type for the /objects endpoint.
type ObjectsResponse struct {
	Entries []string       `json:"entries,omitempty"`
	Object  *object.Object `json:"object,omitempty"`
}

type ObjectsMarkSlabMigrationFailureRequest struct {
	SlabIDs []SlabID `json:"slabIDs"`
}

type ObjectsMarkSlabMigrationFailureResponse struct {
	Updates int `json:"updates"`
}

type ObjectsMigrateSlabsResponse struct {
	SlabIDs []SlabID `json:"slabIDs"`
}

type ObjectsMigrateSlabResponse struct {
	Contracts []types.Contract `json:"contracts"`
	Slab      object.Slab      `json:"slab"`
}

// AddObjectRequest is the request type for the /object/*key PUT endpoint.
type AddObjectRequest struct {
	Object        object.Object                                   `json:"object"`
	UsedContracts map[consensus.PublicKey]siatypes.FileContractID `json:"usedContracts"`
}

// UploadParams contains the metadata needed by a worker to upload an object.
type UploadParams struct {
	CurrentHeight uint64
	MinShards     uint8
	TotalShards   uint8
	ContractSet   string
}
