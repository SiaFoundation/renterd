package api

import (
	"errors"
	"math/big"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
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
)

const (
	SettingContractSet = "contractset"
	SettingGouging     = "gouging"
	SettingRedundancy  = "redundancy"
)

var (
	// ErrRequiresSyncSetRecently indicates that an account can't be set to sync
	// yet because it has been set too recently.
	ErrRequiresSyncSetRecently = errors.New("account had 'requiresSync' flag set recently")

	// ErrOBjectNotFound is returned if get is unable to retrieve an object from
	// the database.
	ErrObjectNotFound = errors.New("object not found")

	// ErrContractSetNotFound is returned when a contract can't be retrieved
	// from the database.
	ErrContractSetNotFound = errors.New("couldn't find contract set")

	// ErrSettingNotFound is returned if a requested setting is not present in the
	// database.
	ErrSettingNotFound = errors.New("setting not found")

	// DefaultRedundancySettings define the default redundancy settings the bus
	// is configured with on startup. These values can be adjusted using the
	// settings API.
	DefaultRedundancySettings = RedundancySettings{
		MinShards:   10,
		TotalShards: 30,
	}

	// DefaultGougingSettings define the default gouging settings the bus is
	// configured with on startup. These values can be adjusted using the
	// settings API.
	DefaultGougingSettings = GougingSettings{
		MinMaxCollateral:              types.Siacoins(10),                                  // at least up to 10 SC per contract
		MaxRPCPrice:                   types.Siacoins(1).Div64(1000),                       // 1mS per RPC
		MaxContractPrice:              types.Siacoins(15),                                  // 15 SC per contract
		MaxDownloadPrice:              types.Siacoins(3000),                                // 3000 SC per 1 TiB
		MaxUploadPrice:                types.Siacoins(3000),                                // 3000 SC per 1 TiB
		MaxStoragePrice:               types.Siacoins(3000).Div64(1 << 40).Div64(144 * 30), // 3000 SC per TiB per month
		HostBlockHeightLeeway:         6,                                                   // 6 blocks
		MinPriceTableValidity:         5 * time.Minute,                                     // 5 minutes
		MinAccountExpiry:              24 * time.Hour,                                      // 1 day
		MinMaxEphemeralAccountBalance: types.Siacoins(1),                                   // 1 SC
	}
)

// ArchiveContractsRequest is the request type for the /contracts/archive endpoint.
type ArchiveContractsRequest = map[types.FileContractID]string

// AccountHandlerPOST is the request type for the /account/:id endpoint.
type AccountHandlerPOST struct {
	HostKey types.PublicKey `json:"hostKey"`
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

// ContractsIDRenewedRequest is the request type for the /contract/:id/renewed
// endpoint.
type ContractsIDRenewedRequest struct {
	Contract    rhpv2.ContractRevision `json:"contract"`
	RenewedFrom types.FileContractID   `json:"renewedFrom"`
	StartHeight uint64                 `json:"startHeight"`
	TotalCost   types.Currency         `json:"totalCost"`
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

// HostsRemoveRequest is the request type for the /hosts/remove endpoint.
type HostsRemoveRequest struct {
	MaxDowntimeHours      ParamDurationHour `json:"maxDowntimeHours"`
	MinRecentScanFailures uint64            `json:"minRecentScanFailures"`
}

type ObjectMetadata struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// ObjectsStats is the response type for the /stats/objects endpoint.
type ObjectsStats struct {
	NumObjects        uint64 `json:"numObjects"`        // number of objects
	TotalObjectsSize  uint64 `json:"totalObjectsSize"`  // size of all objects
	TotalSectorsSize  uint64 `json:"totalSectorsSize"`  // uploaded size of all objects
	TotalUploadedSize uint64 `json:"totalUploadedSize"` // uploaded size of all objects including redundant sectors
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
	RenterKey      types.PrivateKey   `json:"renterKey"`
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

// ObjectsResponse is the response type for the /objects endpoint.
type ObjectsResponse struct {
	Entries []ObjectMetadata `json:"entries,omitempty"`
	Object  *object.Object   `json:"object,omitempty"`
}

// AddObjectRequest is the request type for the /object/*key endpoint.
type AddObjectRequest struct {
	Object        object.Object                            `json:"object"`
	UsedContracts map[types.PublicKey]types.FileContractID `json:"usedContracts"`
}

// MigrationSlabsRequest is the request type for the /slabs/migration endpoint.
type MigrationSlabsRequest struct {
	ContractSet  string  `json:"contractSet"`
	HealthCutoff float64 `json:"healthCutoff"`
	Limit        int     `json:"limit"`
}

// UpdateSlabRequest is the request type for the /slab endpoint.
type UpdateSlabRequest struct {
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

// UploadParams contains the metadata needed by a worker to upload an object.
type UploadParams struct {
	CurrentHeight uint64
	ContractSet   string
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

// ContractSetSettings contains settings needed by the worker to figure out what
// contract set to use.
type ContractSetSettings struct {
	Set string `json:"set"`
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

type SearchHostsRequest struct {
	Offset          int               `json:"offset"`
	Limit           int               `json:"limit"`
	FilterMode      string            `json:"filterMode"`
	UsabilityMode   string            `json:"usabilityMode"`
	AddressContains string            `json:"addressContains"`
	KeyIn           []types.PublicKey `json:"keyIn"`
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
