package api

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

var (
	// ErrConsensusNotSynced is returned by the worker API by endpoints that rely on
	// consensus and the consensus is not synced.
	ErrConsensusNotSynced = errors.New("consensus is not synced")

	// ErrContractSetNotSpecified is returned by the worker API by endpoints that
	// need a contract set to be able to upload data.
	ErrContractSetNotSpecified = errors.New("contract set is not specified")
)

type (
	// AccountsLockHandlerRequest is the request type for the /accounts/:id/lock
	// endpoint.
	AccountsLockHandlerRequest struct {
		HostKey   types.PublicKey `json:"hostKey"`
		Exclusive bool            `json:"exclusive"`
		Duration  DurationMS      `json:"duration"`
	}

	// AccountsLockHandlerResponse is the response type for the
	// /accounts/:id/lock
	AccountsLockHandlerResponse struct {
		Account Account `json:"account"`
		LockID  uint64  `json:"lockID"`
	}

	// AccountsUnlockHandlerRequest is the request type for the
	// /accounts/:id/unlock
	AccountsUnlockHandlerRequest struct {
		LockID uint64 `json:"lockID"`
	}

	// ContractsResponse is the response type for the /rhp/contracts endpoint.
	ContractsResponse struct {
		Contracts []Contract `json:"contracts"`
		Error     string     `json:"error,omitempty"`
	}

	// MigrateSlabResponse is the response type for the /slab/migrate endpoint.
	MigrateSlabResponse struct {
		NumShardsMigrated int `json:"numShardsMigrated"`
	}

	// RHPFormRequest is the request type for the /rhp/form endpoint.
	RHPFormRequest struct {
		EndHeight      uint64          `json:"endHeight"`
		HostCollateral types.Currency  `json:"hostCollateral"`
		HostKey        types.PublicKey `json:"hostKey"`
		HostIP         string          `json:"hostIP"`
		RenterFunds    types.Currency  `json:"renterFunds"`
		RenterAddress  types.Address   `json:"renterAddress"`
	}

	// RHPFormResponse is the response type for the /rhp/form endpoint.
	RHPFormResponse struct {
		ContractID     types.FileContractID   `json:"contractID"`
		Contract       rhpv2.ContractRevision `json:"contract"`
		TransactionSet []types.Transaction    `json:"transactionSet"`
	}

	// RHPFundRequest is the request type for the /rhp/fund endpoint.
	RHPFundRequest struct {
		ContractID types.FileContractID `json:"contractID"`
		HostKey    types.PublicKey      `json:"hostKey"`
		SiamuxAddr string               `json:"siamuxAddr"`
		Balance    types.Currency       `json:"balance"`
	}

	// RHPPruneContractRequest is the request type for the /rhp/contract/:id/prune
	// endpoint.
	RHPPruneContractRequest struct {
		Timeout DurationMS `json:"timeout"`
	}

	// RHPPruneContractResponse is the response type for the /rhp/contract/:id/prune
	// endpoint.
	RHPPruneContractResponse struct {
		Pruned    uint64 `json:"pruned"`
		Remaining uint64 `json:"remaining"`
		Error     error  `json:"error,omitempty"`
	}

	// RHPPriceTableRequest is the request type for the /rhp/pricetable endpoint.
	RHPPriceTableRequest struct {
		HostKey    types.PublicKey `json:"hostKey"`
		SiamuxAddr string          `json:"siamuxAddr"`
		Timeout    DurationMS      `json:"timeout"`
	}

	// RHPRenewRequest is the request type for the /rhp/renew endpoint.
	RHPRenewRequest struct {
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
	RHPRenewResponse struct {
		Error          string                 `json:"error"`
		ContractID     types.FileContractID   `json:"contractID"`
		Contract       rhpv2.ContractRevision `json:"contract"`
		TransactionSet []types.Transaction    `json:"transactionSet"`
	}

	// RHPScanRequest is the request type for the /rhp/scan endpoint.
	RHPScanRequest struct {
		HostKey types.PublicKey `json:"hostKey"`
		HostIP  string          `json:"hostIP"`
		Timeout DurationMS      `json:"timeout"`
	}

	// RHPScanResponse is the response type for the /rhp/scan endpoint.
	RHPScanResponse struct {
		Ping       DurationMS           `json:"ping"`
		ScanError  string               `json:"scanError,omitempty"`
		Settings   rhpv2.HostSettings   `json:"settings,omitempty"`
		PriceTable rhpv3.HostPriceTable `json:"priceTable,omitempty"`
	}

	// RHPSyncRequest is the request type for the /rhp/sync endpoint.
	RHPSyncRequest struct {
		ContractID types.FileContractID `json:"contractID"`
		HostKey    types.PublicKey      `json:"hostKey"`
		SiamuxAddr string               `json:"siamuxAddr"`
	}

	// RHPPreparePaymentRequest is the request type for the /rhp/prepare/payment
	// endpoint.
	RHPPreparePaymentRequest struct {
		Account    rhpv3.Account    `json:"account"`
		Amount     types.Currency   `json:"amount"`
		Expiry     uint64           `json:"expiry"`
		AccountKey types.PrivateKey `json:"accountKey"`
	}

	// RHPRegistryReadRequest is the request type for the /rhp/registry/read
	// endpoint.
	RHPRegistryReadRequest struct {
		HostKey     types.PublicKey                    `json:"hostKey"`
		SiamuxAddr  string                             `json:"siamuxAddr"`
		RegistryKey rhpv3.RegistryKey                  `json:"registryKey"`
		Payment     rhpv3.PayByEphemeralAccountRequest `json:"payment"`
	}

	// RHPRegistryUpdateRequest is the request type for the /rhp/registry/update
	// endpoint.
	RHPRegistryUpdateRequest struct {
		HostKey       types.PublicKey     `json:"hostKey"`
		SiamuxAddr    string              `json:"siamuxAddr"`
		RegistryKey   rhpv3.RegistryKey   `json:"registryKey"`
		RegistryValue rhpv3.RegistryValue `json:"registryValue"`
	}

	// DownloadStatsResponse is the response type for the /stats/downloads endpoint.
	DownloadStatsResponse struct {
		AvgDownloadSpeedMBPS float64           `json:"avgDownloadSpeedMBPS"`
		AvgOverdrivePct      float64           `json:"avgOverdrivePct"`
		HealthyDownloaders   uint64            `json:"healthyDownloaders"`
		NumDownloaders       uint64            `json:"numDownloaders"`
		DownloadersStats     []DownloaderStats `json:"downloadersStats"`
	}
	DownloaderStats struct {
		AvgSectorDownloadSpeedMBPS float64         `json:"avgSectorDownloadSpeedMBPS"`
		HostKey                    types.PublicKey `json:"hostKey"`
		NumDownloads               uint64          `json:"numDownloads"`
	}

	// UploadStatsResponse is the response type for the /stats/uploads endpoint.
	UploadStatsResponse struct {
		AvgSlabUploadSpeedMBPS float64         `json:"avgSlabUploadSpeedMBPS"`
		AvgOverdrivePct        float64         `json:"avgOverdrivePct"`
		HealthyUploaders       uint64          `json:"healthyUploaders"`
		NumUploaders           uint64          `json:"numUploaders"`
		UploadersStats         []UploaderStats `json:"uploadersStats"`
	}
	UploaderStats struct {
		HostKey                  types.PublicKey `json:"hostKey"`
		AvgSectorUploadSpeedMBPS float64         `json:"avgSectorUploadSpeedMBPS"`
	}

	// WorkerStateResponse is the response type for the /worker/state endpoint.
	WorkerStateResponse struct {
		ID        string    `json:"id"`
		StartTime time.Time `json:"startTime"`
		BuildState
	}

	UploadObjectResponse struct {
		ETag string `json:"etag"`
	}

	UploadMultipartUploadPartResponse struct {
		ETag string `json:"etag"`
	}

	GetObjectResponse struct {
		Content     io.ReadCloser  `json:"content"`
		ContentType string         `json:"contentType"`
		ModTime     time.Time      `json:"modTime"`
		Range       *DownloadRange `json:"range,omitempty"`
		Size        int64          `json:"size"`
	}
)

type DownloadRange struct {
	Offset int64
	Length int64
	Size   int64
}

func ParseDownloadRange(contentRange string) (DownloadRange, error) {
	parts := strings.Split(contentRange, " ")
	if len(parts) != 2 || parts[0] != "bytes" {
		return DownloadRange{}, errors.New("missing 'bytes' prefix in range header")
	}
	parts = strings.Split(parts[1], "/")
	if len(parts) != 2 {
		return DownloadRange{}, fmt.Errorf("invalid Content-Range header: %s", contentRange)
	}
	rangeStr := parts[0]
	rangeParts := strings.Split(rangeStr, "-")
	if len(rangeParts) != 2 {
		return DownloadRange{}, errors.New("invalid Content-Range header")
	}
	start, err := strconv.ParseInt(rangeParts[0], 10, 64)
	if err != nil {
		return DownloadRange{}, err
	}
	end, err := strconv.ParseInt(rangeParts[1], 10, 64)
	if err != nil {
		return DownloadRange{}, err
	}
	size, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return DownloadRange{}, err
	}
	return DownloadRange{
		Offset: start,
		Length: end - start + 1,
		Size:   size,
	}, nil
}
