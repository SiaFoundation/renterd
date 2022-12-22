package worker

import (
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	rhpv3 "go.sia.tech/renterd/rhp/v3"
	"go.sia.tech/siad/types"
)

// RHPScanRequest is the request type for the /rhp/scan endpoint.
type RHPScanRequest struct {
	HostKey consensus.PublicKey `json:"hostKey"`
	HostIP  string              `json:"hostIP"`
	Timeout time.Duration       `json:"timeout"`
}

// RHPScanResponse is the response type for the /rhp/scan endpoint.
type RHPScanResponse struct {
	Settings rhpv2.HostSettings `json:"settings"`
	Ping     api.Duration       `json:"ping"`
}

// RHPFormRequest is the request type for the /rhp/form endpoint.
type RHPFormRequest struct {
	EndHeight      uint64              `json:"endHeight"`
	HostCollateral types.Currency      `json:"hostCollateral"`
	HostKey        consensus.PublicKey `json:"hostKey"`
	HostSettings   rhpv2.HostSettings  `json:"hostSettings"`
	RenterFunds    types.Currency      `json:"renterFunds"`
	RenterAddress  types.UnlockHash    `json:"renterAddress"`
}

// RHPFormResponse is the response type for the /rhp/form endpoint.
type RHPFormResponse struct {
	ContractID     types.FileContractID   `json:"contractID"`
	Contract       rhpv2.ContractRevision `json:"contract"`
	TransactionSet []types.Transaction    `json:"transactionSet"`
}

// RHPRenewRequest is the request type for the /rhp/renew endpoint.
type RHPRenewRequest struct {
	ContractID    types.FileContractID `json:"contractID"`
	EndHeight     uint64               `json:"endHeight"`
	HostKey       consensus.PublicKey  `json:"hostKey"`
	HostSettings  rhpv2.HostSettings   `json:"hostSettings"`
	RenterAddress types.UnlockHash     `json:"renterAddress"`
	RenterFunds   types.Currency       `json:"renterFunds"`
}

// RHPRenewResponse is the response type for the /rhp/renew endpoint.
type RHPRenewResponse struct {
	Error          string                 `json:"error"`
	ContractID     types.FileContractID   `json:"contractID"`
	Contract       rhpv2.ContractRevision `json:"contract"`
	TransactionSet []types.Transaction    `json:"transactionSet"`
}

// RHPFundRequest is the request type for the /rhp/fund endpoint.
type RHPFundRequest struct {
	Contract types.FileContractRevision `json:"contract"`
	HostKey  consensus.PublicKey        `json:"hostKey"`
	HostIP   string                     `json:"hostIP"`
	Account  rhpv3.Account              `json:"account"`
	Amount   types.Currency             `json:"amount"`
}

// RHPPreparePaymentRequest is the request type for the /rhp/prepare/payment
// endpoint.
type RHPPreparePaymentRequest struct {
	Account    rhpv3.Account        `json:"account"`
	Amount     types.Currency       `json:"amount"`
	Expiry     uint64               `json:"expiry"`
	AccountKey consensus.PrivateKey `json:"accountKey"`
}

// RHPRegistryReadRequest is the request type for the /rhp/registry/read
// endpoint.
type RHPRegistryReadRequest struct {
	HostKey     consensus.PublicKey                `json:"hostKey"`
	HostIP      string                             `json:"hostIP"`
	RegistryKey rhpv3.RegistryKey                  `json:"registryKey"`
	Payment     rhpv3.PayByEphemeralAccountRequest `json:"payment"`
}

// RHPRegistryUpdateRequest is the request type for the /rhp/registry/update
// endpoint.
type RHPRegistryUpdateRequest struct {
	HostKey       consensus.PublicKey                `json:"hostKey"`
	HostIP        string                             `json:"hostIP"`
	RegistryKey   rhpv3.RegistryKey                  `json:"registryKey"`
	RegistryValue rhpv3.RegistryValue                `json:"registryValue"`
	Payment       rhpv3.PayByEphemeralAccountRequest `json:"payment"`
}
