package test

import (
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
)

var (
	// AutopilotConfig is the autopilot used for testing unless a different
	// one is explicitly set.
	AutopilotConfig = api.AutopilotConfig{
		Contracts: api.ContractsConfig{
			Amount:      3,
			Period:      144,
			RenewWindow: 72,

			Download: rhpv4.SectorSize * 500,
			Upload:   rhpv4.SectorSize * 500,
			Storage:  rhpv4.SectorSize * 5e3,

			Prune: false,
		},
		Hosts: api.HostsConfig{
			MaxDowntimeHours:           10,
			MaxConsecutiveScanFailures: 10,
		},
	}

	GougingSettings = api.GougingSettings{
		MaxRPCPrice:      types.Siacoins(1).Div64(1000),                    // 1mS per RPC
		MaxContractPrice: types.Siacoins(10),                               // 10 SC per contract
		MaxDownloadPrice: types.Siacoins(1).Mul64(1000).Div64(1e12),        // 1000 SC per 1 TB
		MaxUploadPrice:   types.Siacoins(1).Mul64(1000).Div64(1e12),        // 1000 SC per 1 TB
		MaxStoragePrice:  types.Siacoins(1000).Div64(1e12).Div64(144 * 30), // 1000 SC per TB per month

		HostBlockHeightLeeway: 240, // amount of leeway given to host block height

		MinPriceTableValidity:         api.DurationMS(10 * time.Second), // minimum value for price table validity
		MinAccountExpiry:              api.DurationMS(time.Hour),        // minimum value for account expiry
		MinMaxEphemeralAccountBalance: types.Siacoins(1),                // 1SC
	}

	PricePinSettings = api.DefaultPinnedSettings

	RedundancySettings = api.RedundancySettings{
		MinShards:   2,
		TotalShards: 3,
	}

	UploadSettings = api.UploadSettings{
		Redundancy: RedundancySettings,
	}

	S3AccessKeyID     = "TESTINGYNHUWCPKOPSYQ"
	S3SecretAccessKey = "Rh30BNyj+qNI4ftYRteoZbHJ3X4Ln71QtZkRXzJ9"
)
