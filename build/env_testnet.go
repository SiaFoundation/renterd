//go:build testnet

package build

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

const (
	network = "zen"

	DefaultAPIAddress     = "localhost:9880"
	DefaultGatewayAddress = ":9881"
	DefaultS3Address      = "localhost:7070"
)

var (
	// DefaultGougingSettings define the default gouging settings the bus is
	// configured with on startup. These values can be adjusted using the
	// settings API.
	//
	// NOTE: default gouging settings for testnet are identical to mainnet.
	DefaultGougingSettings = api.GougingSettings{
		MaxRPCPrice:                   types.Siacoins(1).Div64(1000),                       // 1mS per RPC
		MaxContractPrice:              types.Siacoins(15),                                  // 15 SC per contract
		MaxDownloadPrice:              types.Siacoins(3000),                                // 3000 SC per 1 TiB
		MaxUploadPrice:                types.Siacoins(3000),                                // 3000 SC per 1 TiB
		MaxStoragePrice:               types.Siacoins(3000).Div64(1 << 40).Div64(144 * 30), // 3000 SC per TiB per month
		HostBlockHeightLeeway:         6,                                                   // 6 blocks
		MinPriceTableValidity:         5 * time.Minute,                                     // 5 minutes
		MinAccountExpiry:              24 * time.Hour,                                      // 1 day
		MinMaxEphemeralAccountBalance: types.Siacoins(1),                                   // 1 SC
		MigrationSurchargeMultiplier:  10,                                                  // 10x
	}

	// DefaultUploadPackingSettings define the default upload packing settings
	// the bus is configured with on startup.
	DefaultUploadPackingSettings = api.UploadPackingSettings{
		Enabled:               true,
		SlabBufferMaxSizeSoft: 1 << 32, // 4 GiB
	}

	// DefaultRedundancySettings define the default redundancy settings the bus
	// is configured with on startup. These values can be adjusted using the
	// settings API.
	//
	// NOTE: default redundancy settings for testnet are different from mainnet.
	DefaultRedundancySettings = api.RedundancySettings{
		MinShards:   2,
		TotalShards: 6,
	}
)
