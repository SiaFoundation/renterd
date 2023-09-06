package api

import (
	"errors"
	"fmt"
	"math/big"
	"net/url"
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

	ObjectsRenameModeSingle = "single"
	ObjectsRenameModeMulti  = "multi"

	UsabilityFilterModeAll      = "all"
	UsabilityFilterModeUsable   = "usable"
	UsabilityFilterModeUnusable = "unusable"

	DefaultBucketName = "default"

	SettingContractSet   = "contractset"
	SettingGouging       = "gouging"
	SettingRedundancy    = "redundancy"
	SettingUploadPacking = "uploadpacking"
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

	// ErrObjectNotFound is returned when an object can't be retrieved from the
	// database.
	ErrObjectNotFound = errors.New("object not found")

	// ErrObjectCorrupted is returned if we were unable to retrieve the object
	// from the database.
	ErrObjectCorrupted = errors.New("object corrupted")

	// ErrContractNotFound is returned when a contract can't be retrieved from
	// the database.
	ErrContractNotFound = errors.New("couldn't find contract")

	// ErrContractSetNotFound is returned when a contract set can't be retrieved
	// from the database.
	ErrContractSetNotFound = errors.New("couldn't find contract set")

	// ErrHostNotFound is returned when a host can't be retrieved from the
	// database.
	ErrHostNotFound = errors.New("host doesn't exist in hostdb")

	// ErrSettingNotFound is returned if a requested setting is not present in the
	// database.
	ErrSettingNotFound = errors.New("setting not found")

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
	Duration ParamDuration `json:"duration"`
	Priority int           `json:"priority"`
}

type ContractKeepaliveRequest struct {
	Duration ParamDuration `json:"duration"`
	LockID   uint64        `json:"lockID"`
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
	MaxDowntimeHours      ParamDurationHour `json:"maxDowntimeHours"`
	MinRecentScanFailures uint64            `json:"minRecentScanFailures"`
}

// Object wraps an object.Object with its metadata.
type Object struct {
	ObjectMetadata
	object.Object
}

// ObjectMetadata contains various metadata about an object.
type ObjectMetadata struct {
	Name   string  `json:"name"`
	Size   int64   `json:"size"`
	Health float64 `json:"health"`
}

// ObjectAddRequest is the request type for the /object/*key endpoint.
type ObjectAddRequest struct {
	Bucket        string                                   `json:"bucket"`
	ContractSet   string                                   `json:"contractSet"`
	Object        object.Object                            `json:"object"`
	UsedContracts map[types.PublicKey]types.FileContractID `json:"usedContracts"`
}

// ObjectsResponse is the response type for the /objects endpoint.
type ObjectsResponse struct {
	Entries []ObjectMetadata `json:"entries,omitempty"`
	Object  *Object          `json:"object,omitempty"`
}

type ObjectsCopyRequest struct {
	SourceBucket string `json:"sourceBucket"`
	SourcePath   string `json:"sourcePath"`

	DestinationBucket string `json:"destinationBucket"`
	DestinationPath   string `json:"destinationPath"`
}

// ObjectsRenameRequest is the request type for the /objects/rename endpoint.
type ObjectsRenameRequest struct {
	Bucket string `json:"bucket"`
	From   string `json:"from"`
	To     string `json:"to"`
	Mode   string `json:"mode"`
}

// ObjectsStatsResponse is the response type for the /stats/objects endpoint.
type ObjectsStatsResponse struct {
	NumObjects        uint64 `json:"numObjects"`        // number of objects
	TotalObjectsSize  uint64 `json:"totalObjectsSize"`  // size of all objects
	TotalSectorsSize  uint64 `json:"totalSectorsSize"`  // uploaded size of all objects
	TotalUploadedSize uint64 `json:"totalUploadedSize"` // uploaded size of all objects including redundant sectors
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
	HostKey       types.PublicKey            `json:"hostKey"`
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

// AccountsUpdateBalanceRequest is the request type for /accounts/:id/update
// endpoint.
type AccountsUpdateBalanceRequest struct {
	HostKey types.PublicKey `json:"hostKey"`
	Amount  *big.Int        `json:"amount"`
}

// AccountsRequiresSyncRequest is the request type for
// /accounts/:id/requiressync endpoint.
type AccountsRequiresSyncRequest struct {
	HostKey types.PublicKey `json:"hostKey"`
}

// AccountsAddBalanceRequest is the request type for /accounts/:id/add
// endpoint.
type AccountsAddBalanceRequest struct {
	HostKey types.PublicKey `json:"hostKey"`
	Amount  *big.Int        `json:"amount"`
}

type PackedSlabsRequestGET struct {
	LockingDuration ParamDuration `json:"lockingDuration"`
	MinShards       uint8         `json:"minShards"`
	TotalShards     uint8         `json:"totalShards"`
	ContractSet     string        `json:"contractSet"`
	Limit           int           `json:"limit"`
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

// ContractSetSetting contains the default contract set used by the worker for
// uploads and migrations.
type ContractSetSetting struct {
	Default string `json:"default"`
}

// GougingSettings contain some price settings used in price gouging.
type GougingSettings struct {
	// MinMaxCollateral is the minimum value for 'MaxCollateral' in the host's
	// price settings
	MinMaxCollateral types.Currency `json:"minMaxCollateral"`

	// MaxRPCPrice is the maximum allowed base price for RPCs
	MaxRPCPrice types.Currency `json:"maxRPCPrice"`

	// MaxContractPrice is the maximum allowed price to form a contract
	MaxContractPrice types.Currency `json:"maxContractPrice"`

	// MaxDownloadPrice is the maximum allowed price to download 1TiB of data
	MaxDownloadPrice types.Currency `json:"maxDownloadPrice"`

	// MaxUploadPrice is the maximum allowed price to upload 1TiB of data
	MaxUploadPrice types.Currency `json:"maxUploadPrice"`

	// MaxStoragePrice is the maximum allowed price to store 1 byte per block
	MaxStoragePrice types.Currency `json:"maxStoragePrice"`

	// HostBlockHeightLeeway is the amount of blocks of leeway given to the host
	// block height in the host's price table
	HostBlockHeightLeeway int `json:"hostBlockHeightLeeway"`

	// MinPriceTableValidity is the minimum accepted value for `Validity` in the
	// host's price settings.
	MinPriceTableValidity time.Duration `json:"minPriceTableValidity"`

	// MinAccountExpiry is the minimum accepted value for `AccountExpiry` in the
	// host's price settings.
	MinAccountExpiry time.Duration `json:"minAccountExpiry"`

	// MinMaxEphemeralAccountBalance is the minimum accepted value for
	// `MaxEphemeralAccountBalance` in the host's price settings.
	MinMaxEphemeralAccountBalance types.Currency `json:"minMaxEphemeralAccountBalance"`
}

type WalletResponse struct {
	ScanHeight  uint64         `json:"scanHeight"`
	Address     types.Address  `json:"address"`
	Spendable   types.Currency `json:"spendable"`
	Confirmed   types.Currency `json:"confirmed"`
	Unconfirmed types.Currency `json:"unconfirmed"`
}

// Validate returns an error if the gouging settings are not considered valid.
func (gs GougingSettings) Validate() error {
	if gs.HostBlockHeightLeeway < 3 {
		return errors.New("HostBlockHeightLeeway must be at least 3 blocks")
	}
	if gs.MinAccountExpiry < time.Hour {
		return errors.New("MinAccountExpiry must be at least 1 hour")
	}
	if gs.MinMaxEphemeralAccountBalance.Cmp(types.Siacoins(1)) < 0 {
		return errors.New("MinMaxEphemeralAccountBalance must be at least 1 SC")
	}
	if gs.MinPriceTableValidity < 10*time.Second {
		return errors.New("MinPriceTableValidity must be at least 10 seconds")
	}
	return nil
}

type Bucket struct {
	CreatedAt time.Time `json:"createdAt"`
	Name      string    `json:"name"`
}

type SearchHostsRequest struct {
	Offset          int               `json:"offset"`
	Limit           int               `json:"limit"`
	FilterMode      string            `json:"filterMode"`
	UsabilityMode   string            `json:"usabilityMode"`
	AddressContains string            `json:"addressContains"`
	KeyIn           []types.PublicKey `json:"keyIn"`
}

type UploadPackingSettings struct {
	Enabled bool `json:"enabled"`
}

// RedundancySettings contain settings that dictate an object's redundancy.
type RedundancySettings struct {
	MinShards   int `json:"minShards"`
	TotalShards int `json:"totalShards"`
}

// Redundancy returns the effective storage redundancy of the
// RedundancySettings.
func (rs RedundancySettings) Redundancy() float64 {
	return float64(rs.TotalShards) / float64(rs.MinShards)
}

// Validate returns an error if the redundancy settings are not considered
// valid.
func (rs RedundancySettings) Validate() error {
	if rs.MinShards < 1 {
		return errors.New("MinShards must be greater than 0")
	}
	if rs.TotalShards < rs.MinShards {
		return errors.New("TotalShards must be at least MinShards")
	}
	if rs.TotalShards > 255 {
		return errors.New("TotalShards must be less than 256")
	}
	return nil
}

type AddPartialSlabResponse struct {
	Slabs []object.PartialSlab `json:"slabs"`
}
