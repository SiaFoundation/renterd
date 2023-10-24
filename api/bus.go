package api

import (
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/object"
)

const (
	HostFilterModeAll     = "all"
	HostFilterModeAllowed = "allowed"
	HostFilterModeBlocked = "blocked"

	ContractArchivalReasonHostPruned = "hostpruned"
	ContractArchivalReasonRemoved    = "removed"
	ContractArchivalReasonRenewed    = "renewed"

	UsabilityFilterModeAll      = "all"
	UsabilityFilterModeUsable   = "usable"
	UsabilityFilterModeUnusable = "unusable"

	DefaultBucketName = "default"
)

var (
	// ErrBucketExists is returned when trying to create a bucket that already
	// exists.
	ErrBucketExists = errors.New("bucket already exists")

	// ErrBucketNotEmpty is returned when trying to delete a bucket that is not
	// empty.
	ErrBucketNotEmpty = errors.New("bucket not empty")

	// ErrBucketNotFound is returned when an bucket can't be retrieved from the
	// database.
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrRequiresSyncSetRecently indicates that an account can't be set to sync
	// yet because it has been set too recently.
	ErrRequiresSyncSetRecently = errors.New("account had 'requiresSync' flag set recently")

	// ErrContractNotFound is returned when a contract can't be retrieved from
	// the database.
	ErrContractNotFound = errors.New("couldn't find contract")

	// ErrContractSetNotFound is returned when a contract set can't be retrieved
	// from the database.
	ErrContractSetNotFound = errors.New("couldn't find contract set")

	// ErrHostNotFound is returned when a host can't be retrieved from the
	// database.
	ErrHostNotFound = errors.New("host doesn't exist in hostdb")

	// ErrMultipartUploadNotFound is returned if the specified multipart upload
	// wasn't found.
	ErrMultipartUploadNotFound = errors.New("multipart upload not found")

	// ErrPartNotFound is returned if the specified part of a multipart upload
	// wasn't found.
	ErrPartNotFound = errors.New("multipart upload part not found")

	// ErrUploadAlreadyExists is returned when starting an upload with an id
	// that's already in use.
	ErrUploadAlreadyExists = errors.New("upload already exists")

	// ErrUnknownUpload is returned when adding sectors for an upload id that's
	// not known.
	ErrUnknownUpload = errors.New("unknown upload")
)

// ArchiveContractsRequest is the request type for the /contracts/archive endpoint.
type ArchiveContractsRequest = map[types.FileContractID]string

// AccountHandlerPOST is the request type for the /account/:id endpoint.
type AccountHandlerPOST struct {
	HostKey types.PublicKey `json:"hostKey"`
}

// BusStateResponse is the response type for the /bus/state endpoint.
type BusStateResponse struct {
	StartTime time.Time `json:"startTime"`
	BuildState
}

// ConsensusState holds the current blockheight and whether we are synced or not.
type ConsensusState struct {
	BlockHeight   uint64    `json:"blockHeight"`
	LastBlockTime time.Time `json:"lastBlockTime"`
	Synced        bool      `json:"synced"`
}

// ConsensusNetwork holds the name of the network.
type ConsensusNetwork struct {
	Name string
}

// ContractsIDAddRequest is the request type for the /contract/:id endpoint.
type ContractsIDAddRequest struct {
	Contract    rhpv2.ContractRevision `json:"contract"`
	StartHeight uint64                 `json:"startHeight"`
	TotalCost   types.Currency         `json:"totalCost"`
}

// UploadSectorRequest is the request type for the /upload/:id/sector endpoint.
type UploadSectorRequest struct {
	ContractID types.FileContractID `json:"contractID"`
	Root       types.Hash256        `json:"root"`
}

// ContractsIDRenewedRequest is the request type for the /contract/:id/renewed
// endpoint.
type ContractsIDRenewedRequest struct {
	Contract    rhpv2.ContractRevision `json:"contract"`
	RenewedFrom types.FileContractID   `json:"renewedFrom"`
	StartHeight uint64                 `json:"startHeight"`
	TotalCost   types.Currency         `json:"totalCost"`
}

// ContractRootsResponse is the response type for the /contract/:id/roots
// endpoint.
type ContractRootsResponse struct {
	Roots     []types.Hash256 `json:"roots"`
	Uploading []types.Hash256 `json:"uploading"`
}

// ContractAcquireRequest is the request type for the /contract/acquire
// endpoint.
type ContractAcquireRequest struct {
	Duration DurationMS `json:"duration"`
	Priority int        `json:"priority"`
}

type ContractKeepaliveRequest struct {
	Duration DurationMS `json:"duration"`
	LockID   uint64     `json:"lockID"`
}

// ContractAcquireRequest is the request type for the /contract/:id/release
// endpoint.
type ContractReleaseRequest struct {
	LockID uint64 `json:"lockID"`
}

// ContractAcquireResponse is the response type for the /contract/:id/acquire
// endpoint.
type ContractAcquireResponse struct {
	LockID uint64 `json:"lockID"`
}

// ContractsPrunableDataResponse is the response type for the
// /contracts/prunable endpoint.
type ContractsPrunableDataResponse struct {
	Contracts     []ContractPrunableData `json:"contracts"`
	TotalPrunable uint64                 `json:"totalPrunable"`
	TotalSize     uint64                 `json:"totalSize"`
}

// ContractPrunableData wraps a contract's size information with its id.
type ContractPrunableData struct {
	ID types.FileContractID `json:"id"`
	ContractSize
}

type HostsScanRequest struct {
	Scans []hostdb.HostScan `json:"scans"`
}

type HostsPriceTablesRequest struct {
	PriceTableUpdates []hostdb.PriceTableUpdate `json:"priceTableUpdates"`
}

// HostsRemoveRequest is the request type for the /hosts/remove endpoint.
type HostsRemoveRequest struct {
	MaxDowntimeHours      DurationH `json:"maxDowntimeHours"`
	MinRecentScanFailures uint64    `json:"minRecentScanFailures"`
}

type SlabBuffer struct {
	ContractSet string `json:"contractSet"` // contract set that be buffer will be uploaded to
	Complete    bool   `json:"complete"`    // whether the slab buffer is complete and ready to upload
	Filename    string `json:"filename"`    // name of the buffer on disk
	Size        int64  `json:"size"`        // size of the buffer
	MaxSize     int64  `json:"maxSize"`     // maximum size of the buffer
	Locked      bool   `json:"locked"`      // whether the slab buffer is locked for uploading
}

// WalletFundRequest is the request type for the /wallet/fund endpoint.
type WalletFundRequest struct {
	Transaction types.Transaction `json:"transaction"`
	Amount      types.Currency    `json:"amount"`
}

// WalletFundResponse is the response type for the /wallet/fund endpoint.
type WalletFundResponse struct {
	Transaction types.Transaction   `json:"transaction"`
	ToSign      []types.Hash256     `json:"toSign"`
	DependsOn   []types.Transaction `json:"dependsOn"`
}

// WalletSignRequest is the request type for the /wallet/sign endpoint.
type WalletSignRequest struct {
	Transaction   types.Transaction   `json:"transaction"`
	ToSign        []types.Hash256     `json:"toSign"`
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
	EndHeight      uint64             `json:"endHeight"`
	HostCollateral types.Currency     `json:"hostCollateral"`
	HostKey        types.PublicKey    `json:"hostKey"`
	HostSettings   rhpv2.HostSettings `json:"hostSettings"`
	RenterAddress  types.Address      `json:"renterAddress"`
	RenterFunds    types.Currency     `json:"renterFunds"`
	RenterKey      types.PublicKey    `json:"renterKey"`
}

// WalletPrepareRenewRequest is the request type for the /wallet/prepare/renew
// endpoint.
type WalletPrepareRenewRequest struct {
	Revision      types.FileContractRevision `json:"revision"`
	EndHeight     uint64                     `json:"endHeight"`
	HostAddress   types.Address              `json:"hostAddress"`
	PriceTable    rhpv3.HostPriceTable       `json:"priceTable"`
	NewCollateral types.Currency             `json:"newCollateral"`
	RenterAddress types.Address              `json:"renterAddress"`
	RenterFunds   types.Currency             `json:"renterFunds"`
	RenterKey     types.PrivateKey           `json:"renterKey"`
	WindowSize    uint64                     `json:"windowSize"`
}

// WalletPrepareRenewResponse is the response type for the /wallet/prepare/renew
// endpoint.
type WalletPrepareRenewResponse struct {
	ToSign         []types.Hash256     `json:"toSign"`
	TransactionSet []types.Transaction `json:"transactionSet"`
}

// WalletTransactionsOption is an option for the WalletTransactions method.
type WalletTransactionsOption func(url.Values)

func WalletTransactionsWithBefore(before time.Time) WalletTransactionsOption {
	return func(q url.Values) {
		q.Set("before", before.Format(time.RFC3339))
	}
}

func WalletTransactionsWithSince(since time.Time) WalletTransactionsOption {
	return func(q url.Values) {
		q.Set("since", since.Format(time.RFC3339))
	}
}

func WalletTransactionsWithLimit(limit int) WalletTransactionsOption {
	return func(q url.Values) {
		q.Set("limit", fmt.Sprint(limit))
	}
}

func WalletTransactionsWithOffset(offset int) WalletTransactionsOption {
	return func(q url.Values) {
		q.Set("offset", fmt.Sprint(offset))
	}
}

// MigrationSlabsRequest is the request type for the /slabs/migration endpoint.
type MigrationSlabsRequest struct {
	ContractSet  string  `json:"contractSet"`
	HealthCutoff float64 `json:"healthCutoff"`
	Limit        int     `json:"limit"`
}

type PackedSlab struct {
	BufferID uint                 `json:"bufferID"`
	Data     []byte               `json:"data"`
	Key      object.EncryptionKey `json:"key"`
}

type UploadedPackedSlab struct {
	BufferID uint
	Shards   []object.Sector
}

// UpdateSlabRequest is the request type for the /slab endpoint.
type UpdateSlabRequest struct {
	ContractSet   string                                   `json:"contractSet"`
	Slab          object.Slab                              `json:"slab"`
	UsedContracts map[types.PublicKey]types.FileContractID `json:"usedContracts"`
}

type UnhealthySlabsResponse struct {
	Slabs []UnhealthySlab `json:"slabs"`
}

type UnhealthySlab struct {
	Key    object.EncryptionKey `json:"key"`
	Health float64              `json:"health"`
}

// UpdateAllowlistRequest is the request type for /hosts/allowlist endpoint.
type UpdateAllowlistRequest struct {
	Add    []types.PublicKey `json:"add"`
	Remove []types.PublicKey `json:"remove"`
	Clear  bool              `json:"clear"`
}

// UpdateBlocklistRequest is the request type for /hosts/blocklist endpoint.
type UpdateBlocklistRequest struct {
	Add    []string `json:"add"`
	Remove []string `json:"remove"`
	Clear  bool     `json:"clear"`
}

// AccountsUpdateBalanceRequest is the request type for /account/:id/update
// endpoint.
type AccountsUpdateBalanceRequest struct {
	HostKey types.PublicKey `json:"hostKey"`
	Amount  *big.Int        `json:"amount"`
}

// AccountsRequiresSyncRequest is the request type for
// /account/:id/requiressync endpoint.
type AccountsRequiresSyncRequest struct {
	HostKey types.PublicKey `json:"hostKey"`
}

// AccountsAddBalanceRequest is the request type for /account/:id/add
// endpoint.
type AccountsAddBalanceRequest struct {
	HostKey types.PublicKey `json:"hostKey"`
	Amount  *big.Int        `json:"amount"`
}

type PackedSlabsRequestGET struct {
	LockingDuration DurationMS `json:"lockingDuration"`
	MinShards       uint8      `json:"minShards"`
	TotalShards     uint8      `json:"totalShards"`
	ContractSet     string     `json:"contractSet"`
	Limit           int        `json:"limit"`
}

type PackedSlabsRequestPOST struct {
	Slabs         []UploadedPackedSlab                     `json:"slabs"`
	UsedContracts map[types.PublicKey]types.FileContractID `json:"usedContracts"`
}

// UploadParams contains the metadata needed by a worker to upload an object.
type UploadParams struct {
	CurrentHeight uint64
	ContractSet   string
	UploadPacking bool
	GougingParams
}

// GougingParams contains the metadata needed by a worker to perform gouging
// checks.
type GougingParams struct {
	ConsensusState     ConsensusState
	GougingSettings    GougingSettings
	RedundancySettings RedundancySettings
	TransactionFee     types.Currency
}

// Option types.
type (
	GetHostsOptions struct {
		Offset int
		Limit  int
	}
	HostsForScanningOptions struct {
		MaxLastScan time.Time
		Limit       int
		Offset      int
	}
	SearchHostOptions struct {
		AddressContains string
		FilterMode      string
		KeyIn           []types.PublicKey
		Limit           int
		Offset          int
	}
)

func DefaultSearchHostOptions() SearchHostOptions {
	return SearchHostOptions{
		Limit:      -1,
		FilterMode: HostFilterModeAll,
	}
}

func (opts GetHostsOptions) Apply(values url.Values) {
	if opts.Offset != 0 {
		values.Set("offset", fmt.Sprint(opts.Offset))
	}
	if opts.Limit != 0 {
		values.Set("limit", fmt.Sprint(opts.Limit))
	}
}

func (opts HostsForScanningOptions) Apply(values url.Values) {
	if opts.Offset != 0 {
		values.Set("offset", fmt.Sprint(opts.Offset))
	}
	if opts.Limit != 0 {
		values.Set("limit", fmt.Sprint(opts.Limit))
	}
	if !opts.MaxLastScan.IsZero() {
		values.Set("maxLastScan", fmt.Sprint(TimeRFC3339(opts.MaxLastScan)))
	}
}

// Types related to multipart uploads.
type (
	CreateMultipartOptions struct {
		Key      object.EncryptionKey
		MimeType string
	}
	MultipartCreateRequest struct {
		Bucket   string               `json:"bucket"`
		Path     string               `json:"path"`
		Key      object.EncryptionKey `json:"key"`
		MimeType string               `json:"mimeType"`
	}
	MultipartCreateResponse struct {
		UploadID string `json:"uploadID"`
	}
	MultipartAbortRequest struct {
		Bucket   string `json:"bucket"`
		Path     string `json:"path"`
		UploadID string `json:"uploadID"`
	}
	MultipartCompleteRequest struct {
		Bucket   string `json:"bucket"`
		Path     string `json:"path"`
		UploadID string `json:"uploadID"`
		Parts    []MultipartCompletedPart
	}
	MultipartCompletedPart struct {
		PartNumber int    `json:"partNumber"`
		ETag       string `json:"eTag"`
	}
	MultipartCompleteResponse struct {
		ETag string `json:"eTag"`
	}
	MultipartAddPartRequest struct {
		Bucket        string                                   `json:"bucket"`
		ETag          string                                   `json:"eTag"`
		Path          string                                   `json:"path"`
		ContractSet   string                                   `json:"contractSet"`
		UploadID      string                                   `json:"uploadID"`
		PartialSlabs  []object.PartialSlab                     `json:"partialSlabs"`
		PartNumber    int                                      `json:"partNumber"`
		Slices        []object.SlabSlice                       `json:"slices"`
		UsedContracts map[types.PublicKey]types.FileContractID `json:"usedContracts"`
	}
	MultipartListUploadsRequest struct {
		Bucket         string `json:"bucket"`
		Prefix         string `json:"prefix"`
		PathMarker     string `json:"pathMarker"`
		UploadIDMarker string `json:"uploadIDMarker"`
		Limit          int    `json:"limit"`
	}
	MultipartListUploadsResponse struct {
		HasMore            bool              `json:"hasMore"`
		NextPathMarker     string            `json:"nextMarker"`
		NextUploadIDMarker string            `json:"nextUploadIDMarker"`
		Uploads            []MultipartUpload `json:"uploads"`
	}
	MultipartUpload struct {
		Bucket    string               `json:"bucket"`
		Key       object.EncryptionKey `json:"key"`
		Path      string               `json:"path"`
		UploadID  string               `json:"uploadID"`
		CreatedAt time.Time            `json:"createdAt"`
	}
	MultipartListPartsRequest struct {
		Bucket           string `json:"bucket"`
		Path             string `json:"path"`
		UploadID         string `json:"uploadID"`
		PartNumberMarker int    `json:"partNumberMarker"`
		Limit            int64  `json:"limit"`
	}
	MultipartListPartsResponse struct {
		HasMore    bool                    `json:"hasMore"`
		NextMarker int                     `json:"nextMarker"`
		Parts      []MultipartListPartItem `json:"parts"`
	}
	MultipartListPartItem struct {
		PartNumber   int       `json:"partNumber"`
		LastModified time.Time `json:"lastModified"`
		ETag         string    `json:"eTag"`
		Size         int64     `json:"size"`
	}
)

type WalletResponse struct {
	ScanHeight  uint64         `json:"scanHeight"`
	Address     types.Address  `json:"address"`
	Spendable   types.Currency `json:"spendable"`
	Confirmed   types.Currency `json:"confirmed"`
	Unconfirmed types.Currency `json:"unconfirmed"`
}

type (
	Bucket struct {
		CreatedAt time.Time    `json:"createdAt"`
		Name      string       `json:"name"`
		Policy    BucketPolicy `json:"policy"`
	}

	BucketPolicy struct {
		PublicReadAccess bool `json:"publicReadAccess"`
	}

	BucketCreateRequest struct {
		Name   string       `json:"name"`
		Policy BucketPolicy `json:"policy"`
	}

	BucketUpdatePolicyRequest struct {
		Policy BucketPolicy `json:"policy"`
	}

	CreateBucketOptions struct {
		Policy BucketPolicy
	}
)

type SearchHostsRequest struct {
	Offset          int               `json:"offset"`
	Limit           int               `json:"limit"`
	FilterMode      string            `json:"filterMode"`
	UsabilityMode   string            `json:"usabilityMode"`
	AddressContains string            `json:"addressContains"`
	KeyIn           []types.PublicKey `json:"keyIn"`
}

type AddPartialSlabResponse struct {
	SlabBufferMaxSizeSoftReached bool                 `json:"slabBufferMaxSizeSoftReached"`
	Slabs                        []object.PartialSlab `json:"slabs"`
}

func FormatETag(ETag string) string {
	return fmt.Sprintf("\"%s\"", ETag)
}

func ObjectPathEscape(path string) string {
	return url.PathEscape(strings.TrimPrefix(path, "/"))
}
