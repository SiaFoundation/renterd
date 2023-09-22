package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

// ErrConsensusNotSynced is returned by the worker API by endpoints that rely on
// consensus and the consensus is not synced.
var ErrConsensusNotSynced = errors.New("consensus is not synced")

// ErrContractSetNotSpecified is returned by the worker API by endpoints that
// need a contract set to be able to upload data.
var ErrContractSetNotSpecified = errors.New("contract set is not specified")

type AccountsLockHandlerRequest struct {
	HostKey   types.PublicKey `json:"hostKey"`
	Exclusive bool            `json:"exclusive"`
	Duration  DurationMS      `json:"duration"`
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

// MigrateSlabResponse is the response type for the /slab/migrate endpoint.
type MigrateSlabResponse struct {
	NumShardsMigrated int `json:"numShardsMigrated"`
}

// RHPScanRequest is the request type for the /rhp/scan endpoint.
type RHPScanRequest struct {
	HostKey types.PublicKey `json:"hostKey"`
	HostIP  string          `json:"hostIP"`
	Timeout DurationMS      `json:"timeout"`
}

// RHPPruneContractRequest is the request type for the /rhp/contract/:id/prune
// endpoint.
type RHPPruneContractRequest struct {
	Timeout DurationMS `json:"timeout"`
}

// RHPPruneContractResponse is the response type for the /rhp/contract/:id/prune
// endpoint.
type RHPPruneContractResponse struct {
	Pruned    uint64 `json:"pruned"`
	Remaining uint64 `json:"remaining"`
}

// RHPPriceTableRequest is the request type for the /rhp/pricetable endpoint.
type RHPPriceTableRequest struct {
	HostKey    types.PublicKey `json:"hostKey"`
	SiamuxAddr string          `json:"siamuxAddr"`
	Timeout    DurationMS      `json:"timeout"`
}

// RHPScanResponse is the response type for the /rhp/scan endpoint.
type RHPScanResponse struct {
	Ping       DurationMS           `json:"ping"`
	ScanError  string               `json:"scanError,omitempty"`
	Settings   rhpv2.HostSettings   `json:"settings,omitempty"`
	PriceTable rhpv3.HostPriceTable `json:"priceTable,omitempty"`
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

// DownloadStatsResponse is the response type for the /stats/downloads endpoint.
type DownloadStatsResponse struct {
	AvgDownloadSpeedMBPS float64           `json:"avgDownloadSpeedMBPS"`
	AvgOverdrivePct      float64           `json:"avgOverdrivePct"`
	HealthyDownloaders   uint64            `json:"healthyDownloaders"`
	NumDownloaders       uint64            `json:"numDownloaders"`
	DownloadersStats     []DownloaderStats `json:"downloadersStats"`
}

type DownloaderStats struct {
	AvgSectorDownloadSpeedMBPS float64         `json:"avgSectorDownloadSpeedMBPS"`
	HostKey                    types.PublicKey `json:"hostKey"`
	NumDownloads               uint64          `json:"numDownloads"`
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

// WorkerStateResponse is the response type for the /worker/state endpoint.
type WorkerStateResponse struct {
	ID        string    `json:"id"`
	StartTime time.Time `json:"startTime"`
	BuildState
}

// An UploadOption overrides an option on the upload and migrate endpoints in
// the worker.
type UploadOption func(url.Values)

// UploadWithRedundancy sets the min and total shards that should be used for an
// upload
func UploadWithRedundancy(minShards, totalShards int) UploadOption {
	return func(v url.Values) {
		v.Set("minshards", strconv.Itoa(minShards))
		v.Set("totalshards", strconv.Itoa(totalShards))
	}
}

// UploadWithContractSet sets the contract set that should be used for an upload
func UploadWithContractSet(set string) UploadOption {
	return func(v url.Values) {
		v.Set("contractset", set)
	}
}

// UploadWithBucket sets the bucket that should be used for an upload
func UploadWithBucket(bucket string) UploadOption {
	return func(v url.Values) {
		v.Set("bucket", bucket)
	}
}

// UploadWithDisablePreshardingEncryption disables presharding encryption for
// the upload
func UploadWithDisabledPreshardingEncryption() UploadOption {
	return func(v url.Values) {
		v.Set("disablepreshardingencryption", "true")
	}
}

type DownloadRange struct {
	Start  int64
	Length int64
}

type GetObjectResponse struct {
	Content     io.ReadCloser  `json:"content"`
	ContentType string         `json:"contentType"`
	ModTime     time.Time      `json:"modTime"`
	Range       *DownloadRange `json:"range,omitempty"`
	Size        int64          `json:"size"`
}

type DownloadObjectOption func(http.Header)

func DownloadWithRange(offset, length int64) DownloadObjectOption {
	return func(h http.Header) {
		if length == -1 {
			h.Set("Range", fmt.Sprintf("bytes=%v-", offset))
		} else {
			h.Set("Range", fmt.Sprintf("bytes=%v-%v", offset, offset+length-1))
		}
	}
}

func DownloadWithBucket(bucket string) DownloadObjectOption {
	return func(h http.Header) {
		h.Set("bucket", bucket)
	}
}

type ObjectsOption func(url.Values)

func ObjectsWithPrefix(prefix string) ObjectsOption {
	return func(v url.Values) {
		v.Set("prefix", prefix)
	}
}

func ObjectsWithOffset(offset int) ObjectsOption {
	return func(v url.Values) {
		v.Set("offset", fmt.Sprint(offset))
	}
}

func ObjectsWithLimit(limit int) ObjectsOption {
	return func(v url.Values) {
		v.Set("limit", fmt.Sprint(limit))
	}
}

func ObjectsWithBucket(bucket string) ObjectsOption {
	return func(v url.Values) {
		v.Set("bucket", bucket)
	}
}

func ObjectsWithIgnoreDelim(ignore bool) ObjectsOption {
	return func(v url.Values) {
		v.Set("ignoreDelim", fmt.Sprint(ignore))
	}
}

func ObjectsWithMarker(marker string) ObjectsOption {
	return func(v url.Values) {
		v.Set("marker", marker)
	}
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
	return DownloadRange{
		Start:  start,
		Length: end - start + 1,
	}, nil
}
