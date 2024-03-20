package api

import (
	"errors"
	"fmt"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
)

const (
	SettingContractSet      = "contractset"
	SettingGouging          = "gouging"
	SettingRedundancy       = "redundancy"
	SettingS3Authentication = "s3authentication"
	SettingUploadPacking    = "uploadpacking"
)

const (
	S3MinAccessKeyLen = 16
	S3MaxAccessKeyLen = 128
	S3SecretKeyLen    = 40
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

		// MigrationSurchargeMultiplier is the multiplier applied to the
		// 'MaxDownloadPrice' when checking whether a host is too expensive,
		// this multiplier is only applied for when trying to migrate critically
		// low-health slabs.
		MigrationSurchargeMultiplier uint64 `json:"migrationSurchargeMultiplier"`
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
	_, overflow := gs.MaxDownloadPrice.Mul64WithOverflow(gs.MigrationSurchargeMultiplier)
	if overflow {
		maxMultiplier := types.MaxCurrency.Div(gs.MaxDownloadPrice).Big().Uint64()
		return fmt.Errorf("MigrationSurchargeMultiplier must be less than %v, otherwise applying it to MaxDownloadPrice overflows the currency type", maxMultiplier)
	}
	return nil
}

// Redundancy returns the effective storage redundancy of the
// RedundancySettings.
func (rs RedundancySettings) Redundancy() float64 {
	return float64(rs.TotalShards) / float64(rs.MinShards)
}

// SlabSize returns the size of a slab.
func (rs RedundancySettings) SlabSize() uint64 {
	return uint64(rs.TotalShards) * rhpv2.SectorSize
}

// SlabSizeNoRedundancy returns the size of a slab without redundancy.
func (rs RedundancySettings) SlabSizeNoRedundancy() uint64 {
	return uint64(rs.MinShards) * rhpv2.SectorSize
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

// Validate returns an error if the authentication settings are not considered
// valid.
func (s3as S3AuthenticationSettings) Validate() error {
	for accessKeyID, secretAccessKey := range s3as.V4Keypairs {
		if accessKeyID == "" {
			return fmt.Errorf("AccessKeyID cannot be empty")
		} else if len(accessKeyID) < S3MinAccessKeyLen || len(accessKeyID) > S3MaxAccessKeyLen {
			return fmt.Errorf("AccessKeyID must be between %d and %d characters long but was %d", S3MinAccessKeyLen, S3MaxAccessKeyLen, len(accessKeyID))
		} else if secretAccessKey == "" {
			return fmt.Errorf("SecretAccessKey cannot be empty")
		} else if len(secretAccessKey) != S3SecretKeyLen {
			return fmt.Errorf("SecretAccessKey must be %d characters long but was %d", S3SecretKeyLen, len(secretAccessKey))
		}
	}
	return nil
}
