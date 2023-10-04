package api

import (
	"errors"
	"time"

	"go.sia.tech/core/types"
)

const (
	SettingContractSet      = "contractset"
	SettingGouging          = "gouging"
	SettingRedundancy       = "redundancy"
	SettingS3Authentication = "s3authentication"
	SettingUploadPacking    = "uploadpacking"
)

var (
	// ErrSettingNotFound is returned if a requested setting is not present in the
	// database.
	ErrSettingNotFound = errors.New("setting not found")
)

type (
	// ContractSetSetting contains the default contract set used by the worker for
	// uploads and migrations.
	ContractSetSetting struct {
		Default string `json:"default"`
	}

	// GougingSettings contain some price settings used in price gouging.
	GougingSettings struct {
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

	// RedundancySettings contain settings that dictate an object's redundancy.
	RedundancySettings struct {
		MinShards   int `json:"minShards"`
		TotalShards int `json:"totalShards"`
	}

	// S3AuthenticationSettings contains S3 auth settings.
	S3AuthenticationSettings struct {
		V4Keypairs map[string]string `json:"v4Keypairs"`
	}

	// UploadPackingSettings contains upload packing settings.
	UploadPackingSettings struct {
		Enabled               bool  `json:"enabled"`
		SlabBufferMaxSizeSoft int64 `json:"slabBufferMaxSizeSoft"`
	}
)

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
