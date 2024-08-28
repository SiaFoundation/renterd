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
	SettingPricePinning     = "pricepinning"
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
	// ErrInvalidRedundancySettings is returned if the redundancy settings are
	// not valid
	ErrInvalidRedundancySettings = errors.New("invalid redundancy settings")

	// ErrSettingNotFound is returned if a requested setting is not present in the
	// database.
	ErrSettingNotFound = errors.New("setting not found")

	// DefaultGougingSettings define the default gouging settings the bus is
	// configured with on startup. These values can be adjusted using the
	// settings API.
	//
	DefaultGougingSettings = GougingSettings{
		MaxRPCPrice:                   types.Siacoins(1).Div64(1000),                    // 1mS per RPC
		MaxContractPrice:              types.Siacoins(15),                               // 15 SC per contract
		MaxDownloadPrice:              types.Siacoins(3000),                             // 3000 SC per 1 TB
		MaxUploadPrice:                types.Siacoins(3000),                             // 3000 SC per 1 TB
		MaxStoragePrice:               types.Siacoins(3000).Div64(1e12).Div64(144 * 30), // 3000 SC per TB per month
		HostBlockHeightLeeway:         6,                                                // 6 blocks
		MinPriceTableValidity:         5 * time.Minute,                                  // 5 minutes
		MinAccountExpiry:              24 * time.Hour,                                   // 1 day
		MinMaxEphemeralAccountBalance: types.Siacoins(1),                                // 1 SC
		MigrationSurchargeMultiplier:  10,                                               // 10x
	}

	// DefaultPricePinSettings define the default price pin settings the bus is
	// configured with on startup. These values can be adjusted using the
	// settings API.
	DefaultPricePinSettings = PricePinSettings{
		Enabled:          false,
		Currency:         "usd",
		ForexEndpointURL: "https://api.siascan.com/exchange-rate/siacoin",
		Threshold:        0.05,
	}

	// DefaultUploadPackingSettings define the default upload packing settings
	// the bus is configured with on startup.
	DefaultUploadPackingSettings = UploadPackingSettings{
		Enabled:               true,
		SlabBufferMaxSizeSoft: 1 << 32, // 4 GiB
	}

	// DefaultRedundancySettings define the default redundancy settings the bus
	// is configured with on startup. These values can be adjusted using the
	// settings API.
	//
	// NOTE: default redundancy settings for testnet are different from mainnet.
	DefaultRedundancySettings = RedundancySettings{
		MinShards:   10,
		TotalShards: 30,
	}

	// Same as DefaultRedundancySettings but for running on testnet networks due
	// to their reduced number of hosts.
	DefaultRedundancySettingsTestnet = RedundancySettings{
		MinShards:   2,
		TotalShards: 6,
	}
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

	// PricePinSettings holds the configuration for pinning certain settings to
	// a specific currency (e.g., USD). It uses a Forex API to fetch the current
	// exchange rate, allowing users to set prices in USD instead of SC.
	PricePinSettings struct {
		// Enabled can be used to either enable or temporarily disable price
		// pinning. If enabled, both the currency and the Forex endpoint URL
		// must be valid.
		Enabled bool `json:"enabled"`

		// Currency is the external three-letter currency code.
		Currency string `json:"currency"`

		// ForexEndpointURL is the endpoint that returns the exchange rate for
		// Siacoin against the underlying currency.
		ForexEndpointURL string `json:"forexEndpointURL"`

		// Threshold is a percentage between 0 and 1 that determines when the
		// pinned settings are updated based on the exchange rate at the time.
		Threshold float64 `json:"threshold"`

		// Autopilots contains the pinned settings for every autopilot.
		Autopilots map[string]AutopilotPins `json:"autopilots"`

		// GougingSettingsPins contains the pinned settings for the gouging
		// settings.
		GougingSettingsPins GougingSettingsPins `json:"gougingSettingsPins"`
	}

	// AutopilotPins contains the available autopilot settings that can be
	// pinned.
	AutopilotPins struct {
		Allowance Pin `json:"allowance"`
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

// IsPinned returns true if the pin is enabled and the value is greater than 0.
func (p Pin) IsPinned() bool {
	return p.Pinned && p.Value > 0
}

// Validate returns an error if the price pin settings are not considered valid.
func (pps PricePinSettings) Validate() error {
	if pps.ForexEndpointURL == "" {
		return fmt.Errorf("price pin settings must have a forex endpoint URL")
	}
	if pps.Currency == "" {
		return fmt.Errorf("price pin settings must have a currency")
	}
	if pps.Threshold <= 0 || pps.Threshold >= 1 {
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

// Validate returns an error if the upload packing settings are not considered
// valid.
func (up UploadPackingSettings) Validate() error {
	if up.Enabled && up.SlabBufferMaxSizeSoft <= 0 {
		return errors.New("SlabBufferMaxSizeSoft must be greater than zero when upload packing is enabled")
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
