package api

import (
	"errors"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

// ErrConsensusNotSynced is returned by the worker API by endpoints that rely on
// consensus and the consensus is not synced.
var ErrConsensusNotSynced = errors.New("consensus is not synced")

type AccountsLockHandlerRequest struct {
	HostKey   types.PublicKey `json:"hostKey"`
	Exclusive bool            `json:"exclusive"`
	Duration  ParamDuration   `json:"duration"`
}

type AccountsLockHandlerResponse struct {
	Account Account `json:"account"`
	LockID  uint64  `json:"lockID"`
}

type AccountsUnlockHandlerRequest struct {
	LockID uint64 `json:"lockID"`
}

// ContractsResponse is the response type for the /rhp/contracts endpoint.
type ContractsResponse struct {
	Contracts []Contract `json:"contracts"`
	Error     string     `json:"error,omitempty"`
}

// RHPScanRequest is the request type for the /rhp/scan endpoint.
type RHPScanRequest struct {
	HostKey types.PublicKey `json:"hostKey"`
	HostIP  string          `json:"hostIP"`
	Timeout time.Duration   `json:"timeout"`
}

// RHPPriceTableRequest is the request type for the /rhp/pricetable endpoint.
type RHPPriceTableRequest struct {
	HostKey    types.PublicKey `json:"hostKey"`
	SiamuxAddr string          `json:"siamuxAddr"`
}

// RHPScanResponse is the response type for the /rhp/scan endpoint.
type RHPScanResponse struct {
	Ping      ParamDuration      `json:"ping"`
	ScanError string             `json:"scanError,omitempty"`
	Settings  rhpv2.HostSettings `json:"settings,omitempty"`
}

// RHPFormRequest is the request type for the /rhp/form endpoint.
type RHPFormRequest struct {
	EndHeight      uint64          `json:"endHeight"`
	HostCollateral types.Currency  `json:"hostCollateral"`
	HostKey        types.PublicKey `json:"hostKey"`
	HostIP         string          `json:"hostIP"`
	RenterFunds    types.Currency  `json:"renterFunds"`
	RenterAddress  types.Address   `json:"renterAddress"`
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
	HostAddress   types.Address        `json:"hostAddress"`
	HostKey       types.PublicKey      `json:"hostKey"`
	SiamuxAddr    string               `json:"siamuxAddr"`
	NewCollateral types.Currency       `json:"newCollateral"`
	RenterAddress types.Address        `json:"renterAddress"`
	RenterFunds   types.Currency       `json:"renterFunds"`
	WindowSize    uint64               `json:"windowSize"`
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
	ContractID types.FileContractID `json:"contractID"`
	HostKey    types.PublicKey      `json:"hostKey"`
	SiamuxAddr string               `json:"siamuxAddr"`
	Balance    types.Currency       `json:"balance"`
}

// RHPSyncRequest is the request type for the /rhp/sync endpoint.
type RHPSyncRequest struct {
	ContractID types.FileContractID `json:"contractID"`
	HostKey    types.PublicKey      `json:"hostKey"`
	SiamuxAddr string               `json:"siamuxAddr"`
}

// RHPPreparePaymentRequest is the request type for the /rhp/prepare/payment
// endpoint.
type RHPPreparePaymentRequest struct {
	Account    rhpv3.Account    `json:"account"`
	Amount     types.Currency   `json:"amount"`
	Expiry     uint64           `json:"expiry"`
	AccountKey types.PrivateKey `json:"accountKey"`
}

// RHPRegistryReadRequest is the request type for the /rhp/registry/read
// endpoint.
type RHPRegistryReadRequest struct {
	HostKey     types.PublicKey                    `json:"hostKey"`
	SiamuxAddr  string                             `json:"siamuxAddr"`
	RegistryKey rhpv3.RegistryKey                  `json:"registryKey"`
	Payment     rhpv3.PayByEphemeralAccountRequest `json:"payment"`
}

// RHPRegistryUpdateRequest is the request type for the /rhp/registry/update
// endpoint.
type RHPRegistryUpdateRequest struct {
	HostKey       types.PublicKey     `json:"hostKey"`
	SiamuxAddr    string              `json:"siamuxAddr"`
	RegistryKey   rhpv3.RegistryKey   `json:"registryKey"`
	RegistryValue rhpv3.RegistryValue `json:"registryValue"`
}

// UploadStatsResponse is the response type for the /stats/uploads endpoint.
type UploadStatsResponse struct {
	AvgSlabUploadSpeedMBPS float64         `json:"avgSlabUploadSpeedMBPS"`
	AvgOverdrivePct        float64         `json:"avgOverdrivePct"`
	HealthyUploaders       uint64          `json:"healthyUploaders"`
	NumUploaders           uint64          `json:"numUploaders"`
	UploadersStats         []UploaderStats `json:"uploadersStats"`
}

type UploaderStats struct {
	HostKey                  types.PublicKey `json:"hostKey"`
	AvgSectorUploadSpeedMBPS float64         `json:"avgSectorUploadSpeedMBPS"`
}
