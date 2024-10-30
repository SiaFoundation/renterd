package api

import (
	"errors"
	"fmt"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
)

const (
	S3MinAccessKeyLen = 16
	S3MaxAccessKeyLen = 128
	S3SecretKeyLen    = 40
)

var (
	// ErrInvalidRedundancySettings is returned if the redundancy settings are
	// not valid
	ErrInvalidRedundancySettings = errors.New("invalid redundancy settings")
)

var (
	// DefaultGougingSettings define the default gouging settings the bus is
	// configured with on startup.
	DefaultGougingSettings = GougingSettings{
		MaxRPCPrice:                   types.Siacoins(1).Div64(1000),                    // 1mS per RPC
		MaxContractPrice:              types.Siacoins(15),                               // 15 SC per contract
		MaxDownloadPrice:              types.Siacoins(3000).Div64(1e12),                 // 3000 SC per 1 TB
		MaxUploadPrice:                types.Siacoins(3000).Div64(1e12),                 // 3000 SC per 1 TB
		MaxStoragePrice:               types.Siacoins(3000).Div64(1e12).Div64(144 * 30), // 3000 SC per TB per month
		HostBlockHeightLeeway:         6,                                                // 6 blocks
		MinPriceTableValidity:         5 * time.Minute,                                  // 5 minutes
		MinAccountExpiry:              24 * time.Hour,                                   // 1 day
		MinMaxEphemeralAccountBalance: types.Siacoins(1),                                // 1 SC
		MigrationSurchargeMultiplier:  10,                                               // 10x
	}

	// DefaultPinnedSettings define the default price pin settings the bus is
	// configured with on startup. These values can be adjusted using the
	// settings API.
	DefaultPinnedSettings = PinnedSettings{
		Currency:  "usd",
		Threshold: 0.05,
	}

	// DefaultRedundancySettingsTestnet defines redundancy settings for the
	// testnet, these are lower due to the reduced number of hosts on the
	// testnet.
	DefaultRedundancySettingsTestnet = RedundancySettings{
		MinShards:   2,
		TotalShards: 6,
	}

	// DefaultS3Settings defines the 3 settings the bus is configured with on
	// startup.
	DefaultS3Settings = S3Settings{
		Authentication: S3AuthenticationSettings{
			V4Keypairs: map[string]string{},
		},
	}
)

// DefaultUploadSettings define the default upload settings the bus is
// configured with on startup.
func DefaultUploadSettings(network string) UploadSettings {
	rs := RedundancySettings{
		MinShards:   10,
		TotalShards: 30,
	}
	if network != "mainnet" {
		rs = DefaultRedundancySettingsTestnet
	}
	return UploadSettings{
		Packing: UploadPackingSettings{
			Enabled:               true,
			SlabBufferMaxSizeSoft: 1 << 32, // 4 GiB
		},
		Redundancy: rs,
	}
}

type (
	// GougingSettings contain some price settings used in price gouging.
	GougingSettings struct {
		// MaxRPCPrice is the maximum allowed base price for RPCs
		MaxRPCPrice types.Currency `json:"maxRPCPrice"`

		// MaxContractPrice is the maximum allowed price to form a contract
		MaxContractPrice types.Currency `json:"maxContractPrice"`

		// MaxDownloadPrice is the maximum allowed price to download 1TB of data
		MaxDownloadPrice types.Currency `json:"maxDownloadPrice"`

		// MaxUploadPrice is the maximum allowed price to upload 1TB of data
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

	// PinnedSettings holds the configuration for pinning certain settings to a
	// specific currency (e.g., USD). It uses the configured explorer to fetch
	// the current exchange rate, allowing users to set prices in USD instead of
	// SC.
	PinnedSettings struct {
		// Currency is the external three-letter currency code.
		Currency string `json:"currency"`

		// Threshold is a percentage between 0 and 1 that determines when the
		// pinned settings are updated based on the exchange rate at the time.
		Threshold float64 `json:"threshold"`

		// GougingSettingsPins contains the pinned settings for the gouging
		// settings.
		GougingSettingsPins GougingSettingsPins `json:"gougingSettingsPins"`
	}

	// UploadSettings contains various settings related to uploads.
	UploadSettings struct {
		DefaultContractSet string                `json:"defaultContractSet"`
		Packing            UploadPackingSettings `json:"packing"`
		Redundancy         RedundancySettings    `json:"redundancy"`
	}

	UploadPackingSettings struct {
		Enabled               bool  `json:"enabled"`
		SlabBufferMaxSizeSoft int64 `json:"slabBufferMaxSizeSoft"`
	}

	RedundancySettings struct {
		MinShards   int `json:"minShards"`
		TotalShards int `json:"totalShards"`
	}

	// GougingSettingsPins contains the available gouging settings that can be
	// pinned.
	GougingSettingsPins struct {
		MaxDownload Pin `json:"maxDownload"`
		MaxStorage  Pin `json:"maxStorage"`
		MaxUpload   Pin `json:"maxUpload"`
	}

	// A Pin is a pinned price in an external currency.
	Pin struct {
		Pinned bool    `json:"pinned"`
		Value  float64 `json:"value"`
	}

	// S3Settings contains various settings related to the S3 API.
	S3Settings struct {
		Authentication S3AuthenticationSettings `json:"authentication"`
	}

	// S3AuthenticationSettings contains S3 auth settings.
	S3AuthenticationSettings struct {
		V4Keypairs map[string]string `json:"v4Keypairs"`
	}
)

// IsPinned returns true if the pin is enabled and the value is greater than 0.
func (p Pin) IsPinned() bool {
	return p.Pinned && p.Value > 0
}

// Enabled returns true if any pins are enabled.
func (ps PinnedSettings) Enabled() bool {
	if ps.GougingSettingsPins.MaxDownload.Pinned ||
		ps.GougingSettingsPins.MaxStorage.Pinned ||
		ps.GougingSettingsPins.MaxUpload.Pinned {
		return true
	}
	return false
}

// Validate returns an error if the price pin settings are not considered valid.
func (ps PinnedSettings) Validate() error {
	if !ps.Enabled() {
		return nil
	}
	if ps.Currency == "" {
		return fmt.Errorf("price pin settings must have a currency")
	}
	if ps.Threshold <= 0 || ps.Threshold >= 1 {
		return fmt.Errorf("price pin settings must have a threshold between 0 and 1")
	}
	return nil
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
	_, overflow := gs.MaxDownloadPrice.Mul64WithOverflow(gs.MigrationSurchargeMultiplier)
	if overflow {
		maxMultiplier := types.MaxCurrency.Div(gs.MaxDownloadPrice).Big().Uint64()
		return fmt.Errorf("MigrationSurchargeMultiplier must be less than %v, otherwise applying it to MaxDownloadPrice overflows the currency type", maxMultiplier)
	}
	return nil
}

// Validate returns an error if the upload settings are not considered valid.
func (us UploadSettings) Validate() error {
	if us.Packing.Enabled && us.Packing.SlabBufferMaxSizeSoft <= 0 {
		return errors.New("SlabBufferMaxSizeSoft must be greater than zero when upload packing is enabled")
	}
	return us.Redundancy.Validate()
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
		return fmt.Errorf("%w: MinShards must be greater than 0", ErrInvalidRedundancySettings)
	}
	if rs.TotalShards < rs.MinShards {
		return fmt.Errorf("%w: TotalShards must be at least MinShards", ErrInvalidRedundancySettings)
	}
	if rs.TotalShards > 255 {
		return fmt.Errorf("%w: TotalShards must be less than 256", ErrInvalidRedundancySettings)
	}
	return nil
}

// Validate returns an error if the authentication settings are not considered
// valid.
func (s3s S3Settings) Validate() error {
	for accessKeyID, secretAccessKey := range s3s.Authentication.V4Keypairs {
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
