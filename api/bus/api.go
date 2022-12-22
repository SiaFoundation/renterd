package bus

import (
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
)

// ConsensusState holds the current blockheight and whether we are synced or not.
type ConsensusState struct {
	BlockHeight uint64
	Synced      bool
}

// ContractAcquireRequest is the request type for the /contracts/:id/acquire
// endpoint.
type ContractAcquireRequest struct {
	Duration time.Duration
}
type ContractsIDAddRequest struct {
	Contract    rhpv2.ContractRevision `json:"contract"`
	StartHeight uint64                 `json:"startHeight"`
	TotalCost   types.Currency         `json:"totalCost"`
}
type ContractsIDRenewedRequest struct {
	Contract    rhpv2.ContractRevision `json:"contract"`
	RenewedFrom types.FileContractID   `json:"renewedFrom"`
	StartHeight uint64                 `json:"startHeight"`
	TotalCost   types.Currency         `json:"totalCost"`
}

// ContractAcquireResponse is the response type for the /contracts/:id/acquire
// endpoint.
type ContractAcquireResponse struct {
	Locked bool `json:"locked"`
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
	RenterKey      consensus.PrivateKey `json:"renterKey"`
	HostKey        consensus.PublicKey  `json:"hostKey"`
	RenterFunds    types.Currency       `json:"renterFunds"`
	RenterAddress  types.UnlockHash     `json:"renterAddress"`
	HostCollateral types.Currency       `json:"hostCollateral"`
	EndHeight      uint64               `json:"endHeight"`
	HostSettings   rhpv2.HostSettings   `json:"hostSettings"`
}

// WalletPrepareRenewRequest is the request type for the /wallet/prepare/renew
// endpoint.
type WalletPrepareRenewRequest struct {
	Contract      types.FileContractRevision `json:"contract"`
	RenterKey     consensus.PrivateKey       `json:"renterKey"`
	HostKey       consensus.PublicKey        `json:"hostKey"`
	RenterFunds   types.Currency             `json:"renterFunds"`
	RenterAddress types.UnlockHash           `json:"renterAddress"`
	EndHeight     uint64                     `json:"endHeight"`
	HostSettings  rhpv2.HostSettings         `json:"hostSettings"`
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

// AddObjectRequest is the request type for the /object/*key PUT endpoint.
type AddObjectRequest struct {
	Object        object.Object                                `json:"object"`
	UsedContracts map[consensus.PublicKey]types.FileContractID `json:"usedContracts"`
}

// DownloadParams contains the metadata needed by a worker to download an object.
type DownloadParams struct {
	ContractSet string
}

// UploadParams contains the metadata needed by a worker to upload an object.
type UploadParams struct {
	CurrentHeight uint64
	MinShards     uint8
	TotalShards   uint8
	ContractSet   string
}

// MigrateParams contains the metadata needed by a worker to migrate a slab.
type MigrateParams struct {
	CurrentHeight uint64
	FromContracts string
	ToContracts   string
}

// GougingSettings contain some price settings used in price gouging.
type GougingSettings struct {
	MaxRPCPrice      types.Currency
	MaxContractPrice types.Currency
	MaxDownloadPrice types.Currency // per TiB
	MaxUploadPrice   types.Currency // per TiB
}

// RedundancySettings contain settings that dictate an object's redundancy.
type RedundancySettings struct {
	MinShards   uint64
	TotalShards uint64
}
