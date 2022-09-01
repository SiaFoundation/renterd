package api

import (
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/renterd/slab"
	"go.sia.tech/siad/types"
)

// WalletFundRequest is the request type for /wallet/fund.
type WalletFundRequest struct {
	Transaction types.Transaction `json:"transaction"`
	Amount      types.Currency    `json:"amount"`
}

// WalletFundResponse is the response type for /wallet/fund.
type WalletFundResponse struct {
	Transaction types.Transaction   `json:"transaction"`
	ToSign      []types.OutputID    `json:"toSign"`
	DependsOn   []types.Transaction `json:"dependsOn"`
}

// WalletSignRequest is the request type for /wallet/sign.
type WalletSignRequest struct {
	Transaction types.Transaction `json:"transaction"`
	ToSign      []types.OutputID  `json:"toSign"`
}

// WalletPrepareRenewResponse is the response type for /wallet/prepare/renew.
type WalletPrepareRenewResponse struct {
	TransactionSet []types.Transaction `json:"transactionSet"`
	FinalPayment   types.Currency      `json:"finalPayment"`
}

// An RHPScanRequest contains the address and pubkey of the host to scan.
type RHPScanRequest struct {
	HostKey consensus.PublicKey `json:"hostKey"`
	HostIP  string              `json:"hostIP"`
}

// An RHPPrepareFormRequest prepares a new file contract.
type RHPPrepareFormRequest struct {
	RenterKey      consensus.PrivateKey `json:"renterKey"`
	HostKey        consensus.PublicKey  `json:"hostKey"`
	RenterFunds    types.Currency       `json:"renterFunds"`
	RenterAddress  types.UnlockHash     `json:"renterAddress"`
	HostCollateral types.Currency       `json:"hostCollateral"`
	EndHeight      uint64               `json:"endHeight"`
	HostSettings   rhpv2.HostSettings   `json:"hostSettings"`
}

// An RHPPrepareFormResponse is the response to /rhp/prepare/form.
type RHPPrepareFormResponse struct {
	Contract types.FileContract `json:"contract"`
	Cost     types.Currency     `json:"cost"`
}

// An RHPFormRequest requests that the host create a contract.
type RHPFormRequest struct {
	RenterKey      consensus.PrivateKey `json:"renterKey"`
	HostKey        consensus.PublicKey  `json:"hostKey"`
	HostIP         string               `json:"hostIP"`
	TransactionSet []types.Transaction  `json:"transactionSet"`
}

// An RHPFormResponse is the response to /rhp/form. It contains the formed
// contract and its transaction set.
type RHPFormResponse struct {
	Contract       rhpv2.Contract      `json:"contract"`
	TransactionSet []types.Transaction `json:"transactionSet"`
}

// An RHPPrepareRenewRequest prepares a file contract for renewal.
type RHPPrepareRenewRequest struct {
	Contract       types.FileContractRevision `json:"contract"`
	RenterKey      consensus.PrivateKey       `json:"renterKey"`
	HostKey        consensus.PublicKey        `json:"hostKey"`
	RenterFunds    types.Currency             `json:"renterFunds"`
	RenterAddress  types.UnlockHash           `json:"renterAddress"`
	HostCollateral types.Currency             `json:"hostCollateral"`
	EndHeight      uint64                     `json:"endHeight"`
	HostSettings   rhpv2.HostSettings         `json:"hostSettings"`
}

// An RHPPrepareRenewResponse is the response to /rhp/prepare/renew.
type RHPPrepareRenewResponse struct {
	Contract     types.FileContract `json:"contract"`
	Cost         types.Currency     `json:"cost"`
	FinalPayment types.Currency     `json:"finalPayment"`
}

// An RHPRenewRequest requests that the host renew a contract.
type RHPRenewRequest struct {
	RenterKey      consensus.PrivateKey `json:"renterKey"`
	HostKey        consensus.PublicKey  `json:"hostKey"`
	HostIP         string               `json:"hostIP"`
	ContractID     types.FileContractID `json:"contractID"`
	TransactionSet []types.Transaction  `json:"transactionSet"`
	FinalPayment   types.Currency       `json:"finalPayment"`
}

// An RHPRenewResponse is the response to /rhp/renew. It contains the renewed
// contract and its transaction set.
type RHPRenewResponse struct {
	Contract       rhpv2.Contract      `json:"contract"`
	TransactionSet []types.Transaction `json:"transactionSet"`
}

// An RHPFundRequest funds an ephemeral account.
type RHPFundRequest struct {
	Contract  types.FileContractRevision `json:"contract"`
	RenterKey consensus.PrivateKey       `json:"renterKey"`
	HostKey   consensus.PublicKey        `json:"hostKey"`
	HostIP    string                     `json:"hostIP"`
	Account   rhpv3.Account              `json:"account"`
	Amount    types.Currency             `json:"amount"`
}

// An RHPPaymentRequest creates a payment by spending value in an ephemeral
// account.
type RHPPaymentRequest struct {
	Account    rhpv3.Account        `json:"account"`
	Amount     types.Currency       `json:"amount"`
	Expiry     uint64               `json:"expiry"`
	AccountKey consensus.PrivateKey `json:"accountKey"`
}

// A Contract contains all the information necessary to access and revise an
// existing file contract.
type Contract struct {
	HostKey   consensus.PublicKey  `json:"hostKey"`
	HostIP    string               `json:"hostIP"`
	ID        types.FileContractID `json:"id"`
	RenterKey consensus.PrivateKey `json:"renterKey"`
}

// SlabsUploadRequest is the request type for the /slabs/upload endpoint.
type SlabsUploadRequest struct {
	MinShards     uint8      `json:"minShards"`
	TotalShards   uint8      `json:"totalShards"`
	Contracts     []Contract `json:"contracts"`
	CurrentHeight uint64     `json:"currentHeight"`
}

// SlabsDownloadRequest is the request type for the /slabs/download endpoint.
type SlabsDownloadRequest struct {
	Slabs     []slab.Slice `json:"slabs"`
	Offset    int64        `json:"offset"`
	Length    int64        `json:"length"`
	Contracts []Contract   `json:"contracts"`
}

// SlabsDeleteRequest is the request type for the /slabs/delete endpoint.
type SlabsDeleteRequest struct {
	Slabs     []slab.Slab `json:"slabs"`
	Contracts []Contract  `json:"contracts"`
}

// SlabsMigrateRequest is the request type for the /slabs/migrate endpoint.
type SlabsMigrateRequest struct {
	Slabs         []slab.Slab `json:"slabs"`
	From          []Contract  `json:"from"`
	To            []Contract  `json:"to"`
	CurrentHeight uint64      `json:"currentHeight"`
}
