package bus

import (
	"fmt"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
)

// exported types from internal/consensus
type (
	// A ChainIndex pairs a block's height with its ID.
	ChainIndex = consensus.ChainIndex

	// A PublicKey is an Ed25519 public key.
	PublicKey = consensus.PublicKey

	// A PrivateKey is an Ed25519 private key.
	PrivateKey = consensus.PrivateKey

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

func (t paramTime) String() string                { return (time.Time)(t).Format(time.RFC3339) }
func (t *paramTime) UnmarshalText(b []byte) error { return (*time.Time)(t).UnmarshalText(b) }

// HealthResponse is the response returned by the /health endpoint.
type HealthResponse struct {
	Syncer          bool `json:"syncer"`
	ChainManager    bool `json:"chainManager"`
	TransactionPool bool `json:"transactionPool"`
	Wallet          bool `json:"wallet"`
	HostDB          bool `json:"hostDB"`
	ContractStore   bool `json:"contractStore"`
	HostSetStore    bool `json:"hostSetStore"`
	ObjectStore     bool `json:"objectStore"`
}

// WalletFundRequest is the request type for the /wallet/fund endpoint.
type WalletFundRequest struct {
	Transaction types.Transaction `json:"transaction"`
	Amount      types.Currency    `json:"amount"`
}

// WalletFundResponse is the response type for the /wallet/fund endpoint.
type WalletFundResponse struct {
	Transaction types.Transaction   `json:"transaction"`
	ToSign      []types.OutputID    `json:"toSign"`
	DependsOn   []types.Transaction `json:"dependsOn"`
}

// WalletSignRequest is the request type for the /wallet/sign endpoint.
type WalletSignRequest struct {
	Transaction   types.Transaction   `json:"transaction"`
	ToSign        []types.OutputID    `json:"toSign"`
	CoveredFields types.CoveredFields `json:"coveredFields"`
}

// WalletRedistributeRequest is the request type for the /wallet/redistribute
// endpoint.
type WalletRedistributeRequest struct {
	Amount  types.Currency `json:"amount"`
	Outputs int            `json:"outputs"`
}

// WalletPrepareFormRequest is the request type for the /wallet/prepare/form
// endpoint.
type WalletPrepareFormRequest struct {
	RenterKey      PrivateKey         `json:"renterKey"`
	HostKey        PublicKey          `json:"hostKey"`
	RenterFunds    types.Currency     `json:"renterFunds"`
	RenterAddress  types.UnlockHash   `json:"renterAddress"`
	HostCollateral types.Currency     `json:"hostCollateral"`
	EndHeight      uint64             `json:"endHeight"`
	HostSettings   rhpv2.HostSettings `json:"hostSettings"`
}

// WalletPrepareRenewRequest is the request type for the /wallet/prepare/renew
// endpoint.
type WalletPrepareRenewRequest struct {
	Contract       types.FileContractRevision `json:"contract"`
	RenterKey      PrivateKey                 `json:"renterKey"`
	HostKey        PublicKey                  `json:"hostKey"`
	RenterFunds    types.Currency             `json:"renterFunds"`
	RenterAddress  types.UnlockHash           `json:"renterAddress"`
	HostCollateral types.Currency             `json:"hostCollateral"`
	EndHeight      uint64                     `json:"endHeight"`
	HostSettings   rhpv2.HostSettings         `json:"hostSettings"`
}

// WalletPrepareRenewResponse is the response type for the /wallet/prepare/renew
// endpoint.
type WalletPrepareRenewResponse struct {
	TransactionSet []types.Transaction `json:"transactionSet"`
	FinalPayment   types.Currency      `json:"finalPayment"`
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
	Contracts []MigrationContract `json:"contracts"`
	Slab      object.Slab         `json:"slab"`
}

// AddObjectRequest is the request type for the /object/*key PUT endpoint.
type AddObjectRequest struct {
	Object        object.Object                                `json:"object"`
	UsedContracts map[consensus.PublicKey]types.FileContractID `json:"usedContracts"`
}

// A Contract uniquely identifies a Sia file contract on a host, along with the
// host's IP.
type Contract struct {
	ID          types.FileContractID `json:"id"`
	HostKey     PublicKey            `json:"hostKey"`
	HostIP      string               `json:"hostIP"`
	StartHeight uint64               `json:"startHeight"`
	EndHeight   uint64               `json:"endHeight"`
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

// A MigrationContract contains all the information necessary to access and revise an
// existing file contract.
// NOTE: This is essentially a copy of the worker.Contract minus the renter key
// but since we can't import that within the bus it's redeclared here. A better
// way to do this could be to migrate all API types to their own package.
type MigrationContract struct {
	HostKey PublicKey            `json:"hostKey"`
	HostIP  string               `json:"hostIP"`
	ID      types.FileContractID `json:"id"`
}
